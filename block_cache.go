package isledb

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// blockCache keeps SST blocks in memory after checksum and decompression, in
// Pebble's block cache, so a hit costs neither and allocates nothing. It
// serves every SST read, whether the SST is on local disk or read by range.
//
// Pebble keys blocks by file number and offset. Each SST ID gets a number from
// a counter that never repeats, the same for every open of that SST, so local
// and range reads of one SST share its blocks. SSTs never change, so cached
// blocks never go stale; numbers are dropped, and their blocks evicted, when
// an SST leaves the manifest or turns out corrupt.
type blockCache struct {
	cache    *pebble.Cache
	maxBytes int64
	// opts carries the cache handle, whose type is internal to Pebble; it is
	// copied per SST with that SST's file number.
	opts sstable.ReaderOptions

	mu    sync.Mutex
	next  uint64
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

// readerOptions returns sstable reader options that read and fill the cache
// under sstID's file number. A nil cache returns options without one.
func (b *blockCache) readerOptions(sstID string) sstable.ReaderOptions {
	if b == nil {
		return sstable.ReaderOptions{}
	}
	b.mu.Lock()
	n, ok := b.files[sstID]
	if !ok {
		b.next++
		n = b.next
		b.files[sstID] = n
	}
	b.mu.Unlock()
	opts := b.opts
	setFileNum(&opts.CacheOpts.FileNum, n)
	return opts
}

// evict drops sstID's cached blocks. A later open gets a new file number;
// blocks a racing read adds under the old one are never read again and age
// out.
func (b *blockCache) evict(sstID string) {
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

// retain evicts the blocks of every SST not in m, as SSTs leave the manifest.
// A read view still holding an older manifest can read a retired SST; it just
// misses the cache.
func (b *blockCache) retain(m *manifestState) {
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
	var retired []uint64
	b.mu.Lock()
	for id, n := range b.files {
		if _, ok := live[id]; !ok {
			retired = append(retired, n)
			delete(b.files, id)
		}
	}
	b.mu.Unlock()
	for _, n := range retired {
		evictFile(b.opts.CacheOpts.CacheHandle.EvictFile, n)
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

// stats reports the cache's occupancy and its hits and misses on index and
// data blocks. The misses every open makes on blocks Pebble never caches are
// left out, so the hit rate reflects blocks the cache could have served.
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
