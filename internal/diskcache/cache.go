// Package diskcache is a persistent, size-bounded local cache of byte ranges
// of immutable objects: an SST's metadata region, its Bloom sidecar, whole
// small SSTs, and aligned chunks of larger SSTs' data.
//
// The cache is advisory. Entries are written to a temporary file and renamed
// into place without fsync, so a crash can leave an entry empty, short or
// damaged. Startup drops empty entries and unfinished writes; a read checks
// each entry's size against the size its caller expects, and callers check
// contents with their own checksums and report damage with ReportCorrupt.
// Every failure surfaces as a miss.
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
	// QueueSize bounds writes waiting to be stored by Write; zero selects 256.
	// A Write that finds the queue full is dropped.
	QueueSize int
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
	// Dropped counts writes discarded because the write queue was full.
	Dropped int64

	Entries  int
	Bytes    int64
	MaxBytes int64
}

var ErrLocked = errors.New("diskcache: directory is locked by another process")

const (
	lockName         = "LOCK"
	versionDir       = "v3"
	incomingName     = "incoming"
	defaultQueueSize = 256
	writers          = 2
)

// Cache is safe for concurrent use.
type Cache struct {
	mu       sync.Mutex
	root     string
	incoming string
	lock     *flock.Flock
	tiers    [tierCount]*tier
	// closed refuses new reads and writes; released marks the directory lock
	// given up, after which nothing more is stored.
	closed   bool
	released bool
	// pending holds entries queued by Write until they are stored, so reads
	// see them meanwhile; idle is signalled whenever one is stored or dropped.
	pending map[Key][]byte
	idle    *sync.Cond

	queue   chan write
	writing sync.WaitGroup
}

type write struct {
	key  Key
	data []byte
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
// other than the current one, and loads the entries already cached, oldest
// first, dropping the oldest beyond each tier's budget.
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
		pending:  make(map[Key][]byte),
	}
	c.idle = sync.NewCond(&c.mu)
	for t, budget := range [tierCount]int64{TierMeta: opts.MetaMaxBytes, TierData: opts.DataMaxBytes} {
		c.tiers[t] = &tier{max: budget, index: make(map[Key]*list.Element)}
	}
	if err := c.prepare(opts.Dir); err != nil {
		_ = lock.Close()
		return nil, err
	}
	queueSize := opts.QueueSize
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	c.queue = make(chan write, queueSize)
	for range writers {
		c.writing.Add(1)
		go c.writer(c.queue)
	}
	return c, nil
}

// Close stores the writes already queued, then releases the directory lock.
// Reads and new writes are refused from the start of Close.
func (c *Cache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	close(c.queue)
	c.writing.Wait()
	c.mu.Lock()
	c.released = true
	clear(c.pending)
	c.idle.Broadcast()
	c.mu.Unlock()
	return c.lock.Close()
}

// Sync waits until every write queued so far has been stored or dropped.
func (c *Cache) Sync() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.pending) > 0 && !c.released {
		c.idle.Wait()
	}
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
	if data, ok := c.pending[k]; ok && int64(len(data)) == size {
		copy(p, data[off:])
		t.stats.Hits++
		c.mu.Unlock()
		return true
	}
	element, ok := c.lookupLocked(k, size)
	if !ok {
		t.stats.Misses++
		c.mu.Unlock()
		return false
	}
	t.lru.MoveToBack(element)
	path := c.path(k)
	c.mu.Unlock()

	err := readAt(path, p, off)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		// A vanished or unreadable file is no longer an entry.
		t.stats.Misses++
		if element, ok := t.index[k]; ok {
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

// Contains reports whether k is cached, or queued to be, with the expected
// size, without counting a hit or a miss or changing eviction order.
func (c *Cache) Contains(k Key, size int64) bool {
	if !k.valid() {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if data, ok := c.pending[k]; ok && int64(len(data)) == size {
		return true
	}
	if c.closed {
		return false
	}
	element, ok := c.tiers[k.Kind.Tier()].index[k]
	return ok && element.Value.(*entry).size == size
}

// Write queues data to be stored as k by a background writer, and reports
// whether it was queued. Until it is stored, reads of k see it. data must not
// be modified afterwards. A full queue drops the write.
func (c *Cache) Write(k Key, data []byte) bool {
	if !k.valid() || len(data) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	if _, ok := c.pending[k]; ok {
		return true
	}
	if element, ok := c.tiers[k.Kind.Tier()].index[k]; ok && element.Value.(*entry).size == int64(len(data)) {
		return true
	}
	select {
	case c.queue <- write{key: k, data: data}:
		c.pending[k] = data
		return true
	default:
		c.tiers[k.Kind.Tier()].stats.Dropped++
		return false
	}
}

// Put stores data as k before returning, for callers such as prefetch that
// must not lose writes to a full queue.
func (c *Cache) Put(k Key, data []byte) error {
	if !k.valid() || len(data) == 0 {
		return fmt.Errorf("diskcache: invalid entry %v", k)
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return errors.New("diskcache: closed")
	}
	return c.store(k, data)
}

// Remove drops k and deletes its file.
func (c *Cache) Remove(k Key) {
	c.drop(k, false)
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
	t := c.tiers[k.Kind.Tier()]
	_, queued := c.pending[k]
	delete(c.pending, k)
	c.idle.Broadcast()
	element, ok := t.index[k]
	if corrupt && (ok || queued) {
		t.stats.Corruptions++
	}
	if ok {
		c.removeLocked(element)
	}
}

// Purge drops every entry of one tier.
func (c *Cache) Purge(tr Tier) {
	if tr >= tierCount {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.pending {
		if k.Kind.Tier() == tr {
			delete(c.pending, k)
		}
	}
	c.idle.Broadcast()
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

func (c *Cache) writer(queue <-chan write) {
	defer c.writing.Done()
	for w := range queue {
		c.mu.Lock()
		data, ok := c.pending[w.key]
		c.mu.Unlock()
		// A write removed or purged while queued is not stored.
		if ok && &data[0] == &w.data[0] {
			_ = c.store(w.key, w.data)
		}
		c.mu.Lock()
		if data, ok := c.pending[w.key]; ok && &data[0] == &w.data[0] {
			delete(c.pending, w.key)
		}
		c.idle.Broadcast()
		c.mu.Unlock()
	}
}

// store writes data to a temporary file and renames it into place, then
// evicts the tier's least recently used entries to get back within budget.
func (c *Cache) store(k Key, data []byte) error {
	t := c.tiers[k.Kind.Tier()]
	size := int64(len(data))
	c.mu.Lock()
	switch {
	case c.released:
		c.mu.Unlock()
		return errors.New("diskcache: closed")
	case size > t.max:
		t.stats.Bypasses++
		c.mu.Unlock()
		return nil
	}
	if element, ok := t.index[k]; ok && element.Value.(*entry).size == size {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	tmp, err := writeTemp(c.incoming, data)
	if err == nil {
		final := c.path(k)
		if err = os.MkdirAll(filepath.Dir(final), 0o700); err == nil {
			err = os.Rename(tmp, final)
		}
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
	if c.released {
		_ = os.Remove(c.path(k))
		return nil
	}
	if element, ok := t.index[k]; ok {
		c.removeIndexLocked(element)
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
	_ = os.Remove(c.path(e.key))
}

func (c *Cache) removeIndexLocked(element *list.Element) *entry {
	e := element.Value.(*entry)
	t := c.tiers[e.key.Kind.Tier()]
	t.lru.Remove(element)
	delete(t.index, e.key)
	t.bytes -= e.size
	return e
}

func (c *Cache) path(k Key) string {
	name := k.name()
	return filepath.Join(c.root, k.Kind.Tier().String(), name[:2], name)
}
