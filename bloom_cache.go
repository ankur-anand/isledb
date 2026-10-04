package isledb

import (
	"sync/atomic"
)

const (
	defaultBloomCacheSize = 64 << 20
	// Account for the cache entry, map bucket share, string header, and
	// filter slice header in addition to the filter's bit array. The exact
	// Go heap cost is runtime-dependent, so the cache deliberately uses a
	// conservative fixed allowance per entry.
	bloomCacheEntryOverhead = 128
)

type bloomCacheEntry struct {
	filter sstBloomFilter
	bytes  int64
}

// bloomFilterCache bounds loaded bloom filters by their accounted heap cost.
// Eviction is safe because every filter can be reloaded from its immutable SST
// sidecar on the next point lookup. Lookups take no lock (see clockCache).
type bloomFilterCache struct {
	entries  *clockCache[bloomCacheEntry]
	maxBytes int64
	bytes    atomic.Int64 // changed only by writers, under entries.mu
	hits     atomic.Int64
	misses   atomic.Int64
}

func newBloomFilterCache(maxBytes int64) *bloomFilterCache {
	if maxBytes <= 0 {
		maxBytes = defaultBloomCacheSize
	}
	return &bloomFilterCache{entries: newClockCache[bloomCacheEntry](), maxBytes: maxBytes}
}

func (c *bloomFilterCache) get(id string) (sstBloomFilter, bool) {
	return c.lookup(id, true)
}

// peek rechecks the cache after joining a coalesced load without counting
// an additional application-level lookup.
func (c *bloomFilterCache) peek(id string) (sstBloomFilter, bool) {
	return c.lookup(id, false)
}

func (c *bloomFilterCache) lookup(id string, record bool) (sstBloomFilter, bool) {
	if c == nil {
		return sstBloomFilter{}, false
	}
	e, ok := c.entries.get(id)
	if record {
		if ok {
			c.hits.Add(1)
		} else {
			c.misses.Add(1)
		}
	}
	if !ok {
		return sstBloomFilter{}, false
	}
	return e.value.filter, true
}

func (c *bloomFilterCache) put(id string, filter sstBloomFilter) {
	if c == nil || id == "" || filter.sizeBytes() == 0 {
		return
	}
	bytes := bloomFilterCacheCost(id, filter)

	txn := c.entries.begin()
	defer txn.commit()
	if existing, ok := txn.lookup(id); ok {
		txn.remove(existing)
		c.bytes.Add(-existing.value.bytes)
	}
	if bytes > c.maxBytes {
		return
	}
	for c.bytes.Load()+bytes > c.maxBytes {
		evicted := txn.victim()
		if evicted == nil {
			break
		}
		c.bytes.Add(-evicted.value.bytes)
	}
	txn.insert(id, bloomCacheEntry{filter: filter, bytes: bytes})
	c.bytes.Add(bytes)
}

func (c *bloomFilterCache) delete(id string) {
	if c == nil {
		return
	}
	txn := c.entries.begin()
	defer txn.commit()
	if existing, ok := txn.lookup(id); ok {
		txn.remove(existing)
		c.bytes.Add(-existing.value.bytes)
	}
}

func (c *bloomFilterCache) clear() {
	if c == nil {
		return
	}
	txn := c.entries.begin()
	defer txn.commit()
	txn.removeAll()
	c.bytes.Store(0)
}

func (c *bloomFilterCache) stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}
	return CacheStats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		Bytes:      c.bytes.Load(),
		MaxBytes:   c.maxBytes,
		EntryCount: c.entries.len(),
	}
}

func bloomFilterCacheCost(id string, filter sstBloomFilter) int64 {
	return int64(filter.sizeBytes()) + int64(len(id)) + bloomCacheEntryOverhead
}
