package isledb

import (
	"container/list"
	"sync"

	"github.com/dgraph-io/ristretto/v2/z"
)

const (
	defaultBloomCacheSize = 64 << 20
	// Account for the cache entry, list node, map bucket share, string header,
	// and Bloom slice header in addition to the bytes reported by Bloom itself.
	// The exact Go heap cost is runtime-dependent, so the cache deliberately
	// uses a conservative fixed allowance per entry.
	bloomCacheEntryOverhead = 128
)

type bloomCacheEntry struct {
	id     string
	filter *z.Bloom
	bytes  int64
}

// bloomFilterCache bounds decoded bloom filters by their accounted heap cost.
// Eviction is safe because every filter can be reloaded from its immutable SST
// sidecar on the next point lookup.
type bloomFilterCache struct {
	mu       sync.Mutex
	maxBytes int64
	bytes    int64
	entries  map[string]*list.Element
	lru      list.List
	hits     int64
	misses   int64
}

func newBloomFilterCache(maxBytes int64) *bloomFilterCache {
	if maxBytes <= 0 {
		maxBytes = defaultBloomCacheSize
	}
	return &bloomFilterCache{
		maxBytes: maxBytes,
		entries:  make(map[string]*list.Element),
	}
}

func (c *bloomFilterCache) get(id string) (*z.Bloom, bool) {
	return c.lookup(id, true)
}

// peek rechecks the cache after joining a coalesced load without counting
// an additional application-level lookup.
func (c *bloomFilterCache) peek(id string) (*z.Bloom, bool) {
	return c.lookup(id, false)
}

func (c *bloomFilterCache) lookup(id string, record bool) (*z.Bloom, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[id]
	if !ok {
		if record {
			c.misses++
		}
		return nil, false
	}
	if record {
		c.hits++
	}
	c.lru.MoveToBack(element)
	return element.Value.(*bloomCacheEntry).filter, true
}

func (c *bloomFilterCache) put(id string, filter *z.Bloom) {
	if c == nil || id == "" || filter == nil {
		return
	}
	bytes := bloomFilterCacheCost(id, filter)

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.entries[id]; existing != nil {
		c.removeElement(existing)
	}
	if bytes > c.maxBytes {
		return
	}
	for c.bytes+bytes > c.maxBytes && c.lru.Len() > 0 {
		c.removeElement(c.lru.Front())
	}
	entry := &bloomCacheEntry{id: id, filter: filter, bytes: bytes}
	c.entries[id] = c.lru.PushBack(entry)
	c.bytes += bytes
}

func (c *bloomFilterCache) delete(id string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeElement(c.entries[id])
}

func (c *bloomFilterCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.entries)
	c.lru.Init()
	c.bytes = 0
}

func (c *bloomFilterCache) stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Hits:       c.hits,
		Misses:     c.misses,
		Bytes:      c.bytes,
		MaxBytes:   c.maxBytes,
		EntryCount: len(c.entries),
	}
}

func (c *bloomFilterCache) removeElement(element *list.Element) {
	if element == nil {
		return
	}
	entry := element.Value.(*bloomCacheEntry)
	delete(c.entries, entry.id)
	c.bytes -= entry.bytes
	c.lru.Remove(element)
}

func bloomFilterCacheCost(id string, filter *z.Bloom) int64 {
	return int64(filter.TotalSize()) + int64(len(id)) + bloomCacheEntryOverhead
}
