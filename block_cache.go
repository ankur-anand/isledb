package isledb

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// blockCache keeps SST blocks in memory after checksum and decompression, in
// Pebble's block cache, so a hit costs neither and allocates nothing.
//
// Pebble keys blocks by file number and offset. Each SST ID keeps a number
// across opens, so a reopened SST finds its blocks. A corrupt SST, or one that
// left the manifest and is not open, forgets its number; numbers never repeat,
// so its old blocks can never be served again.
type blockCache struct {
	cache    *pebble.Cache
	maxBytes int64
	// opts carries the cache handle, whose type is internal to Pebble; it is
	// copied per open SST with that SST's file number.
	opts sstable.ReaderOptions
	next uint64

	mu    sync.Mutex
	files map[string]uint64

	// opens counts SST opens, whose metadata lookups stats leaves out.
	opens atomic.Int64
}

// metaLookupsPerOpen is how many cache lookups each sstable.NewReader makes
// that can never hit: it reads the metaindex and properties blocks through a
// buffer pool, so they are looked up but never added (see Pebble's
// NewReader). TestBlockCache_OpenMakesTwoUncachedLookups checks the count.
const metaLookupsPerOpen = 2

func newBlockCache(size int64) *blockCache {
	c := pebble.NewCache(size)
	b := &blockCache{cache: c, maxBytes: size, files: make(map[string]uint64)}
	b.opts.CacheOpts.CacheHandle = c.NewHandle()
	return b
}

// fileNum returns sstID's block cache file number, assigning one on first
// use.
func (b *blockCache) fileNum(sstID string) uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	n, ok := b.files[sstID]
	if !ok {
		b.next++
		n = b.next
		b.files[sstID] = n
	}
	return n
}

// readerOptions returns sstable reader options that read and fill the cache
// under file number n. A nil cache returns options without one.
func (b *blockCache) readerOptions(n uint64) sstable.ReaderOptions {
	if b == nil {
		return sstable.ReaderOptions{}
	}
	opts := b.opts
	setFileNum(&opts.CacheOpts.FileNum, n)
	return opts
}

// forget evicts sstID's blocks and drops its number.
func (b *blockCache) forget(sstID string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	n, ok := b.files[sstID]
	delete(b.files, sstID)
	b.mu.Unlock()
	if ok {
		evictFile(b.opts.CacheOpts.CacheHandle.EvictFile, n)
	}
}

// prune forgets every SST that is neither in m nor open, as SSTs leave the
// manifest, keeping the number map bounded. An SST still open, as one a
// snapshot is reading, keeps its blocks.
func (b *blockCache) prune(m *manifestState, open func(sstID string) bool) {
	if b == nil || m == nil {
		return
	}
	live := make(map[string]struct{}, len(m.L0SSTs))
	for _, sst := range m.L0SSTs {
		live[sst.ID] = struct{}{}
	}
	for _, level := range m.Levels {
		for _, sst := range level.SSTs {
			live[sst.ID] = struct{}{}
		}
	}
	b.mu.Lock()
	var retired []string
	for id := range b.files {
		if _, ok := live[id]; !ok {
			retired = append(retired, id)
		}
	}
	b.mu.Unlock()
	for _, id := range retired {
		if !open(id) {
			b.forget(id)
		}
	}
}

// clear evicts every cached block, keeping file numbers.
func (b *blockCache) clear() {
	if b == nil {
		return
	}
	b.mu.Lock()
	numbers := make([]uint64, 0, len(b.files))
	for _, n := range b.files {
		numbers = append(numbers, n)
	}
	b.mu.Unlock()
	for _, n := range numbers {
		evictFile(b.opts.CacheOpts.CacheHandle.EvictFile, n)
	}
}

// noteOpen records a successful SST open.
func (b *blockCache) noteOpen() {
	if b != nil {
		b.opens.Add(1)
	}
}

// stats reports occupancy, hits and misses, leaving out the lookups each open
// makes that can never hit (metaLookupsPerOpen).
func (b *blockCache) stats() CacheStats {
	if b == nil {
		return CacheStats{}
	}
	m := b.cache.Metrics()
	return CacheStats{
		Hits:       m.Hits,
		Misses:     max(m.Misses-metaLookupsPerOpen*b.opens.Load(), 0),
		Bytes:      m.Size,
		MaxBytes:   b.maxBytes,
		EntryCount: int(m.Count),
	}
}

// close releases the cache. Blocks still referenced by open iterators are
// freed when those iterators close.
func (b *blockCache) close() {
	if b == nil {
		return
	}
	b.opts.CacheOpts.CacheHandle.Close()
	b.cache.Unref()
}

// setFileNum and evictFile convert to Pebble's file number type, which is
// internal to Pebble.
func setFileNum[T ~uint64](dst *T, n uint64) { *dst = T(n) }

func evictFile[T ~uint64](evict func(T), n uint64) { evict(T(n)) }
