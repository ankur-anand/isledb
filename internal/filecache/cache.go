// Package filecache is a persistent, size-bounded local cache of immutable
// files, each named by the SHA-256 of its contents.
//
// The cache is advisory: every failure, whether a missing, truncated or
// corrupt file, a full disk, or a failed rename, surfaces as a cache miss.
// Bytes are checked against their expected size and checksum before they are
// published and are trusted afterwards, except Bloom files, which are checked
// again on every read because a corrupt filter could report present keys as
// absent.
//
// Evicted files are deleted immediately. A caller that still holds one open
// keeps reading it, because on Linux and macOS an open file stays readable
// after it is deleted and its space is freed when the last holder closes it.
package filecache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/ankur-anand/isledb/internal/checksum"
	"github.com/gofrs/flock"
)

// Kind selects one of the cache's independently budgeted tiers.
type Kind uint8

const (
	KindSST Kind = iota
	KindBloom
	kindCount
)

func (k Kind) String() string {
	switch k {
	case KindSST:
		return "sst"
	case KindBloom:
		return "bloom"
	default:
		return fmt.Sprintf("kind(%d)", k)
	}
}

var (
	ErrClosed            = errors.New("filecache: closed")
	ErrLocked            = errors.New("filecache: directory is locked by another process")
	ErrInvalidDescriptor = errors.New("filecache: invalid descriptor")
	ErrSizeMismatch      = errors.New("filecache: size mismatch")
	ErrChecksumMismatch  = errors.New("filecache: checksum mismatch")
)

// Descriptor identifies a file by its kind, exact size and SHA-256 checksum,
// in the "sha256:<hex>" form the manifest records.
type Descriptor struct {
	Kind     Kind
	Size     int64
	Checksum string
}

func (d Descriptor) sum() ([sha256.Size]byte, error) {
	if d.Kind >= kindCount {
		return [sha256.Size]byte{}, fmt.Errorf("%w: kind %d", ErrInvalidDescriptor, d.Kind)
	}
	if d.Size <= 0 {
		return [sha256.Size]byte{}, fmt.Errorf("%w: size %d", ErrInvalidDescriptor, d.Size)
	}
	sum, err := checksum.ParseSHA256(d.Checksum)
	if err != nil {
		return sum, fmt.Errorf("%w: checksum %q", ErrInvalidDescriptor, d.Checksum)
	}
	return sum, nil
}

// Options configures a cache. Dir should be dedicated to the cache: Open
// removes earlier cache layouts from it but leaves other files alone. Both
// budgets must be positive.
type Options struct {
	Dir           string
	SSTMaxBytes   int64
	BloomMaxBytes int64
}

// Stats reports one tier's activity and occupancy. Bytes counts cached files
// only; downloads in progress and evicted files still held open by readers
// use disk space outside the budget.
type Stats struct {
	Hits        int64
	Misses      int64
	Evictions   int64
	Corruptions int64
	// Bypasses counts verified files too large for the whole budget.
	Bypasses int64
	// Failures counts verified files that could not be synced or renamed
	// into the cache.
	Failures int64

	Entries  int
	Bytes    int64
	MaxBytes int64
}

const (
	lockName     = "LOCK"
	versionDir   = "v2"
	incomingName = "incoming"
)

// Cache is safe for concurrent use.
type Cache struct {
	mu       sync.Mutex
	root     string
	incoming string
	lock     *flock.Flock
	tiers    [kindCount]*tier
	closed   bool
}

type tier struct {
	max   int64
	bytes int64
	lru   list.List // *entry, least recently used first
	index map[[sha256.Size]byte]*list.Element
	stats Stats
}

type entry struct {
	sum  [sha256.Size]byte
	size int64
}

// Open locks dir for this process, discards unfinished downloads and any
// layout other than the current one, and loads the files already cached.
func Open(opts Options) (*Cache, error) {
	if opts.Dir == "" {
		return nil, errors.New("filecache: directory is required")
	}
	if opts.SSTMaxBytes <= 0 || opts.BloomMaxBytes <= 0 {
		return nil, fmt.Errorf("filecache: budgets must be positive: sst=%d bloom=%d",
			opts.SSTMaxBytes, opts.BloomMaxBytes)
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("filecache: create directory: %w", err)
	}
	lock := flock.New(filepath.Join(opts.Dir, lockName), flock.SetPermissions(0o600))
	locked, err := lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("filecache: lock %s: %w", opts.Dir, err)
	}
	if !locked {
		return nil, fmt.Errorf("%w: %s", ErrLocked, opts.Dir)
	}

	root := filepath.Join(opts.Dir, versionDir)
	c := &Cache{
		root:     root,
		incoming: filepath.Join(root, incomingName),
		lock:     lock,
	}
	for kind, budget := range [kindCount]int64{KindSST: opts.SSTMaxBytes, KindBloom: opts.BloomMaxBytes} {
		c.tiers[kind] = &tier{max: budget, index: make(map[[sha256.Size]byte]*list.Element)}
	}
	if err := c.prepare(opts.Dir); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return c, nil
}

// Close releases the directory lock. Files already returned stay usable.
// Afterwards the cache no longer touches the directory, which another owner
// may now hold: lookups miss, Create returns ErrClosed, finished writes are
// served without being cached, and removals do nothing.
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.lock.Close()
}

// Contains reports whether d is cached, without counting a hit or a miss or
// changing eviction order.
func (c *Cache) Contains(d Descriptor) bool {
	sum, err := d.sum()
	if err != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.entryLocked(d.Kind, sum, d.Size)
	return ok
}

// OpenFile opens a cached SST for reading. The caller closes the file. The
// file's size is checked but its contents are not re-hashed; SST readers
// verify each block's own checksum as they read it.
func (c *Cache) OpenFile(d Descriptor) (*os.File, bool) {
	sum, path, ok := c.touch(d)
	if !ok {
		return nil, false
	}
	file, err := openFile(path)
	if err == nil {
		err = checkSize(file, d.Size)
		if err == nil {
			c.record(d.Kind, true)
			return file, true
		}
		_ = file.Close()
	}
	c.failed(d.Kind, sum, err)
	return nil, false
}

// ReadVerified reads a cached file whole and checks it against d's checksum.
// A file that fails the check is removed.
func (c *Cache) ReadVerified(d Descriptor) ([]byte, bool) {
	sum, path, ok := c.touch(d)
	if !ok {
		return nil, false
	}
	data, err := readVerified(path, d.Size, sum)
	if err == nil {
		c.record(d.Kind, true)
		return data, true
	}
	c.failed(d.Kind, sum, err)
	return nil, false
}

// Remove drops d from the cache and deletes its file.
func (c *Cache) Remove(d Descriptor) {
	sum, err := d.sum()
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if element, ok := c.tiers[d.Kind].index[sum]; ok {
		c.removeLocked(d.Kind, element)
	}
}

// ReportCorrupt drops d after its contents proved unusable to the caller, for
// example an SST that failed its own block checksums, and counts a
// corruption.
//
// It drops whatever copy of d is cached now. If the caller read an older copy
// that was since evicted and downloaded again, the good replacement is dropped
// too; the only cost is one more download.
func (c *Cache) ReportCorrupt(d Descriptor) {
	sum, err := d.sum()
	if err != nil {
		return
	}
	c.invalidate(d.Kind, sum, ErrChecksumMismatch)
}

// Purge drops every file of one kind.
func (c *Cache) Purge(kind Kind) {
	if kind >= kindCount {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	t := c.tiers[kind]
	for t.lru.Len() > 0 {
		c.removeLocked(kind, t.lru.Front())
	}
}

// Stats reports one tier. An invalid kind reports zero values.
func (c *Cache) Stats(kind Kind) Stats {
	if kind >= kindCount {
		return Stats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tiers[kind]
	stats := t.stats
	stats.Entries = t.lru.Len()
	stats.Bytes = t.bytes
	stats.MaxBytes = t.max
	return stats
}

// touch finds a cached file and marks it recently used. A miss is counted
// here; the caller counts the outcome of reading a file it found.
func (c *Cache) touch(d Descriptor) ([sha256.Size]byte, string, bool) {
	sum, err := d.sum()
	if err != nil {
		return sum, "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tiers[d.Kind]
	c.dropMismatchedLocked(d.Kind, sum, d.Size)
	element, ok := c.entryLocked(d.Kind, sum, d.Size)
	if !ok {
		t.stats.Misses++
		return sum, "", false
	}
	t.lru.MoveToBack(element)
	return sum, c.path(d.Kind, sum), true
}

// dropMismatchedLocked removes a cached file whose recorded size differs from
// size. Files are named by their checksum, so contents of any other size
// cannot be the named file; such a file was damaged, for example while the
// process was down, and is counted as corrupt.
func (c *Cache) dropMismatchedLocked(kind Kind, sum [sha256.Size]byte, size int64) {
	if c.closed {
		return
	}
	t := c.tiers[kind]
	element, ok := t.index[sum]
	if !ok || element.Value.(*entry).size == size {
		return
	}
	c.removeLocked(kind, element)
	t.stats.Corruptions++
}

func (c *Cache) entryLocked(kind Kind, sum [sha256.Size]byte, size int64) (*list.Element, bool) {
	if c.closed {
		return nil, false
	}
	element, ok := c.tiers[kind].index[sum]
	if !ok || element.Value.(*entry).size != size {
		return nil, false
	}
	return element, true
}

func (c *Cache) record(kind Kind, hit bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if hit {
		c.tiers[kind].stats.Hits++
	} else {
		c.tiers[kind].stats.Misses++
	}
}

// failed records a lookup whose file could not be opened or read as a miss.
// A transient failure leaves the entry for the next lookup; any other drops
// it, so a file that keeps failing cannot stay cached and block its own
// replacement.
func (c *Cache) failed(kind Kind, sum [sha256.Size]byte, err error) {
	if !transient(err) {
		c.invalidate(kind, sum, err)
	}
	c.record(kind, false)
}

// transient reports errors caused by the process's circumstances rather than
// the file, such as running out of file descriptors or memory. The file is
// likely fine, so its entry is kept. Windows reports its own error codes for
// these, which are not recognised, so there such entries are dropped.
func transient(err error) bool {
	var errno syscall.Errno
	return errors.As(err, &errno) && (errno.Temporary() || errno == syscall.ENOMEM)
}

// corrupt reports errors showing a file's contents are damaged: the wrong
// size or checksum, a read that ended early, or a failed read of the device.
func corrupt(err error) bool {
	return errors.Is(err, ErrSizeMismatch) || errors.Is(err, ErrChecksumMismatch) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.EIO)
}

// invalidate drops an entry whose file could not be read as expected,
// counting a corruption when the contents proved damaged. A file that
// vanished or could not be opened is simply dropped.
func (c *Cache) invalidate(kind Kind, sum [sha256.Size]byte, cause error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	t := c.tiers[kind]
	if corrupt(cause) {
		t.stats.Corruptions++
	}
	if element, ok := t.index[sum]; ok {
		c.removeLocked(kind, element)
	}
}

func (c *Cache) insertLocked(kind Kind, sum [sha256.Size]byte, size int64) {
	t := c.tiers[kind]
	t.index[sum] = t.lru.PushBack(&entry{sum: sum, size: size})
	t.bytes += size
}

// removeLocked drops an entry and deletes its file. Readers holding the file
// open keep their data until they close it.
func (c *Cache) removeLocked(kind Kind, element *list.Element) {
	t := c.tiers[kind]
	e := t.lru.Remove(element).(*entry)
	delete(t.index, e.sum)
	t.bytes -= e.size
	_ = os.Remove(c.path(kind, e.sum))
}

func (c *Cache) path(kind Kind, sum [sha256.Size]byte) string {
	name := hex.EncodeToString(sum[:])
	return filepath.Join(c.root, kind.String(), name[:2], name)
}

func checkSize(file *os.File, size int64) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != size {
		return fmt.Errorf("%w: file %d bytes, want %d", ErrSizeMismatch, info.Size(), size)
	}
	return nil
}

// openFile opens cached files; tests replace it to simulate open failures.
var openFile = os.Open

func readVerified(path string, size int64, sum [sha256.Size]byte) ([]byte, error) {
	file, err := openFile(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	if err := checkSize(file, size); err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	if sha256.Sum256(data) != sum {
		return nil, ErrChecksumMismatch
	}
	return data, nil
}
