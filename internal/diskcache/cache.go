// Package diskcache is a persistent, size-bounded local cache of byte ranges
// of immutable objects: an SST's metadata region, its Bloom sidecar, whole
// small SSTs, and aligned chunks of larger SSTs' data.
//
// The cache is advisory. Entries are written to a temporary file and renamed
// into place without fsync, so a crash can leave an entry empty, short or
// damaged. Startup drops unfinished writes; a read checks each entry's size
// against the size its caller expects and drops a file shorter than its name
// says, and callers check contents with their own checksums and report damage
// with ReportCorrupt. Every failure surfaces as a miss.
//
// Writes are synchronous: Put returns once the entry is stored, so there is
// no queue to bound and nothing held in memory for a write.
//
// No file is held open between calls: a read opens, reads and closes, so an
// evicted entry's space is freed at once and the budgets are exact.
package diskcache

import (
	"container/list"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/gofrs/flock"
)

// Kind is what part of an object an entry holds.
type Kind uint8

const (
	// KindMeta is an SST's metadata region, [MetaOffset, Size).
	KindMeta Kind = iota
	// KindBloom is an SST's Bloom sidecar.
	KindBloom
	// KindWhole is a small SST, [0, Size).
	KindWhole
	// KindChunk is one aligned chunk of an SST's data region.
	KindChunk
	kindCount
)

var kindSuffix = [kindCount]string{KindMeta: "meta", KindBloom: "bloom", KindWhole: "whole", KindChunk: "c"}

// Tier is an independently budgeted part of the cache.
type Tier uint8

const (
	// TierMeta holds metadata regions and Bloom sidecars: small, and needed
	// on every open and lookup, so bulk data cannot evict them.
	TierMeta Tier = iota
	// TierData holds whole small SSTs and data chunks.
	TierData
	tierCount
)

var tierName = [tierCount]string{TierMeta: "meta", TierData: "data"}

func (t Tier) String() string {
	if t < tierCount {
		return tierName[t]
	}
	return fmt.Sprintf("tier(%d)", t)
}

// Tier reports which tier holds entries of kind k.
func (k Kind) Tier() Tier {
	if k == KindMeta || k == KindBloom {
		return TierMeta
	}
	return TierData
}

// Key identifies one entry. Object identifies the immutable object; Index is
// the chunk number for KindChunk and zero otherwise.
type Key struct {
	Object [32]byte
	Kind   Kind
	Index  uint32
}

func (k Key) valid() bool { return k.Kind < kindCount && (k.Kind == KindChunk || k.Index == 0) }

func (k Key) name() string {
	name := hex.EncodeToString(k.Object[:]) + "." + kindSuffix[k.Kind]
	if k.Kind == KindChunk {
		name += strconv.FormatUint(uint64(k.Index), 10)
	}
	return name
}

// Options configures a cache. Dir should be dedicated to the cache: Open
// removes earlier cache layouts from it but leaves other files alone.
type Options struct {
	Dir          string
	MetaMaxBytes int64
	DataMaxBytes int64
}

// Stats reports one tier's activity and occupancy.
type Stats struct {
	Hits        int64
	Misses      int64
	Evictions   int64
	Corruptions int64
	// Bypasses counts entries too large for the whole tier; they are not
	// stored.
	Bypasses int64
	// Failures counts entries that could not be written or renamed.
	Failures int64

	Entries  int
	Bytes    int64
	MaxBytes int64
}

var ErrLocked = errors.New("diskcache: directory is locked by another process")

const (
	lockName     = "LOCK"
	versionDir   = "v4"
	incomingName = "incoming"
)

// Cache is safe for concurrent use.
type Cache struct {
	mu       sync.Mutex
	root     string
	incoming string
	lock     *flock.Flock
	tiers    [tierCount]*tier
	// closed refuses new reads and writes; storing tracks the Puts still in
	// progress, which Close waits for before releasing the directory lock.
	closed  bool
	storing sync.WaitGroup
	// stores holds the keys with a Put in progress (see store).
	stores map[Key]*storeState
	// testHook, set only by tests, runs at named points: "renamed" between a
	// store's rename and its commit, "read" after ReadAt reads a file.
	testHook func(point string, k Key)
}

// storeState is one key's Puts in progress: how many, and its generation.
type storeState struct {
	n   int
	gen uint64
}

type tier struct {
	max   int64
	bytes int64
	lru   list.List // *entry, least recently used first
	index map[Key]*list.Element
	stats Stats
}

type entry struct {
	key  Key
	size int64
}

// Open locks dir for this process, removes unfinished writes and any layout
// other than the current one, and loads the entries already cached, dropping
// any beyond each tier's budget.
func Open(opts Options) (*Cache, error) {
	if opts.Dir == "" {
		return nil, errors.New("diskcache: directory is required")
	}
	if opts.MetaMaxBytes <= 0 || opts.DataMaxBytes <= 0 {
		return nil, fmt.Errorf("diskcache: budgets must be positive: meta=%d data=%d",
			opts.MetaMaxBytes, opts.DataMaxBytes)
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("diskcache: create directory: %w", err)
	}
	lock := flock.New(filepath.Join(opts.Dir, lockName), flock.SetPermissions(0o600))
	locked, err := lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("diskcache: lock %s: %w", opts.Dir, err)
	}
	if !locked {
		return nil, fmt.Errorf("%w: %s", ErrLocked, opts.Dir)
	}

	root := filepath.Join(opts.Dir, versionDir)
	c := &Cache{
		root:     root,
		incoming: filepath.Join(root, incomingName),
		lock:     lock,
		stores:   make(map[Key]*storeState),
	}
	for t, budget := range [tierCount]int64{TierMeta: opts.MetaMaxBytes, TierData: opts.DataMaxBytes} {
		c.tiers[t] = &tier{max: budget, index: make(map[Key]*list.Element)}
	}
	if err := c.prepare(opts.Dir); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return c, nil
}

// Close waits for Puts in progress, then releases the directory lock. Reads
// and new Puts are refused from the start of Close.
func (c *Cache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	c.storing.Wait()
	return c.lock.Close()
}

// ReadAt reads len(p) bytes at off from the entry k, whose full size the
// caller expects to be size. An entry of any other size is damaged and is
// removed. It reports whether p was filled.
func (c *Cache) ReadAt(k Key, size int64, p []byte, off int64) bool {
	if !k.valid() || off < 0 || off+int64(len(p)) > size {
		return false
	}
	c.mu.Lock()
	t := c.tiers[k.Kind.Tier()]
	element, ok := c.lookupLocked(k, size)
	if !ok {
		t.stats.Misses++
		c.mu.Unlock()
		return false
	}
	t.lru.MoveToBack(element)
	path := c.path(k, size)
	c.mu.Unlock()

	err := readAt(path, p, off)
	if c.testHook != nil {
		c.testHook("read", k)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		// A vanished or unreadable file is no longer an entry, unless the
		// entry was replaced while the file was read.
		t.stats.Misses++
		if current, ok := t.index[k]; ok && current == element {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				t.stats.Corruptions++
			}
			c.removeLocked(element)
		}
		return false
	}
	t.stats.Hits++
	return true
}

// Contains reports whether k is cached with the expected size, without
// counting a hit or a miss or changing eviction order.
func (c *Cache) Contains(k Key, size int64) bool {
	if !k.valid() {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	element, ok := c.tiers[k.Kind.Tier()].index[k]
	return ok && element.Value.(*entry).size == size
}

// Put stores data as k before returning. An entry of the same size already
// stored is left as it is.
func (c *Cache) Put(k Key, data []byte) error {
	if !k.valid() || len(data) == 0 {
		return fmt.Errorf("diskcache: invalid entry %v", k)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("diskcache: closed")
	}
	c.storing.Add(1)
	st := c.stores[k]
	if st == nil {
		st = &storeState{}
		c.stores[k] = st
	}
	st.n++
	gen := st.gen
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if st.n--; st.n == 0 {
			delete(c.stores, k)
		}
		c.mu.Unlock()
		c.storing.Done()
	}()
	return c.store(k, data, st, gen)
}

// Remove drops k and deletes its file. After Close it does nothing: the
// directory may belong to another process by then.
func (c *Cache) Remove(k Key) {
	c.drop(k, false)
}

// RemoveAll drops every entry in keys under one hold of the cache's lock and
// deletes their files after releasing it, so reads are not held behind the
// deletes. Close waits for the deletes. After Close it does nothing. It
// returns how many entries it dropped.
func (c *Cache) RemoveAll(keys []Key) int {
	var paths []string
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0
	}
	for _, k := range keys {
		if !k.valid() {
			continue
		}
		if st := c.stores[k]; st != nil {
			st.gen++
		}
		if element, ok := c.tiers[k.Kind.Tier()].index[k]; ok {
			e := c.removeIndexLocked(element)
			paths = append(paths, c.path(e.key, e.size))
		}
	}
	c.storing.Add(1)
	c.mu.Unlock()
	defer c.storing.Done()
	for _, path := range paths {
		_ = os.Remove(path)
	}
	return len(paths)
}

// ReportCorrupt drops k after its contents proved damaged, and counts a
// corruption.
func (c *Cache) ReportCorrupt(k Key) {
	c.drop(k, true)
}

func (c *Cache) drop(k Key, corrupt bool) {
	if !k.valid() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if st := c.stores[k]; st != nil {
		st.gen++
	}
	t := c.tiers[k.Kind.Tier()]
	element, ok := t.index[k]
	if corrupt && ok {
		t.stats.Corruptions++
	}
	if ok {
		c.removeLocked(element)
	}
}

// Purge drops every entry of one tier. After Close it does nothing.
func (c *Cache) Purge(tr Tier) {
	if tr >= tierCount {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	for k, st := range c.stores {
		if k.Kind.Tier() == tr {
			st.gen++
		}
	}
	t := c.tiers[tr]
	for t.lru.Len() > 0 {
		c.removeLocked(t.lru.Front())
	}
}

// Stats reports one tier. An invalid tier reports zero values.
func (c *Cache) Stats(tr Tier) Stats {
	if tr >= tierCount {
		return Stats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tiers[tr]
	stats := t.stats
	stats.Entries = t.lru.Len()
	stats.Bytes = t.bytes
	stats.MaxBytes = t.max
	return stats
}

// store writes data to a temporary file without the lock held, renames it
// into place, then evicts least recently used entries to fit the budget. A
// drop of k bumps its generation; if gen changed meanwhile, the store is not
// kept, since its bytes may be the ones the drop meant to discard.
func (c *Cache) store(k Key, data []byte, st *storeState, gen uint64) error {
	t := c.tiers[k.Kind.Tier()]
	size := int64(len(data))
	c.mu.Lock()
	if size > t.max {
		t.stats.Bypasses++
		c.mu.Unlock()
		return nil
	}
	if element, ok := t.index[k]; ok && element.Value.(*entry).size == size {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	final := c.path(k, size)
	tmp, err := writeTemp(c.incoming, data)
	if err == nil {
		if err = os.MkdirAll(filepath.Dir(final), 0o700); err == nil {
			err = os.Rename(tmp, final)
		}
	}
	if c.testHook != nil && err == nil {
		c.testHook("renamed", k)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		t.stats.Failures++
		if tmp != "" {
			_ = os.Remove(tmp)
		}
		return fmt.Errorf("diskcache: store %s: %w", k.name(), err)
	}
	if st.gen != gen {
		// The renamed file may also have replaced one another store
		// committed meanwhile, so k goes entirely.
		if element, ok := t.index[k]; ok {
			c.removeLocked(element)
		}
		_ = os.Remove(final)
		return nil
	}
	if element, ok := t.index[k]; ok {
		// An entry of the same size had the same file, now replaced; one of
		// another size has a file of its own to delete.
		if element.Value.(*entry).size == size {
			c.removeIndexLocked(element)
		} else {
			c.removeLocked(element)
		}
	}
	c.insertLocked(k, size)
	for t.bytes > t.max {
		c.removeLocked(t.lru.Front())
		t.stats.Evictions++
	}
	return nil
}

func writeTemp(dir string, data []byte) (string, error) {
	file, err := os.CreateTemp(dir, "entry-*")
	if err != nil {
		return "", err
	}
	_, err = file.Write(data)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(file.Name())
		return "", err
	}
	return file.Name(), nil
}

// readAt reads exactly len(p) bytes at off from the file at path.
func readAt(path string, p []byte, off int64) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	n, err := file.ReadAt(p, off)
	if n == len(p) {
		return nil
	}
	if err == nil || err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// lookupLocked finds k with the expected size. An entry of another size is
// damaged, for example by a crash while it was being written, and is removed.
func (c *Cache) lookupLocked(k Key, size int64) (*list.Element, bool) {
	if c.closed {
		return nil, false
	}
	t := c.tiers[k.Kind.Tier()]
	element, ok := t.index[k]
	if !ok {
		return nil, false
	}
	if element.Value.(*entry).size != size {
		t.stats.Corruptions++
		c.removeLocked(element)
		return nil, false
	}
	return element, true
}

func (c *Cache) insertLocked(k Key, size int64) {
	t := c.tiers[k.Kind.Tier()]
	t.index[k] = t.lru.PushBack(&entry{key: k, size: size})
	t.bytes += size
}

// removeLocked drops an entry and deletes its file.
func (c *Cache) removeLocked(element *list.Element) {
	e := c.removeIndexLocked(element)
	_ = os.Remove(c.path(e.key, e.size))
}

func (c *Cache) removeIndexLocked(element *list.Element) *entry {
	e := element.Value.(*entry)
	t := c.tiers[e.key.Kind.Tier()]
	t.lru.Remove(element)
	delete(t.index, e.key)
	t.bytes -= e.size
	return e
}

// path is where entry k of the given size is stored. The size is part of the
// file name, so recovery learns every entry's size by listing directories,
// without a system call per file.
func (c *Cache) path(k Key, size int64) string {
	name := k.name()
	return filepath.Join(c.root, k.Kind.Tier().String(), name[:2], name+"."+strconv.FormatInt(size, 10))
}
