package isledb

import (
	"container/list"
	"sync"
)

const (
	defaultMetaCacheSize = 128 << 20
	// metaCacheEntryOverhead accounts for the entry, list node, map bucket
	// share and string header beside the region's bytes.
	metaCacheEntryOverhead = 128
)

// sstMetaCache keeps SST metadata regions (index, properties, metaindex and
// footer: the bytes in [MetaOffset, Size)) in memory, keyed by SST ID, within
// a byte budget. Every open of an SST reads its metadata before any data, so
// giving metadata its own budget keeps it from being evicted by data chunks
// and saves a request on each later open. SSTs never change, so an entry is
// valid for as long as it is cached.
type sstMetaCache struct {
	mu        sync.Mutex
	maxBytes  int64
	bytes     int64
	entries   map[string]*list.Element
	lru       list.List // *sstMetaCacheEntry, least recently used first
	hits      int64
	misses    int64
	evictions int64
}

type sstMetaCacheEntry struct {
	sstID  string
	region []byte
	cost   int64
}

func newSSTMetaCache(maxBytes int64) *sstMetaCache {
	if maxBytes <= 0 {
		maxBytes = defaultMetaCacheSize
	}
	return &sstMetaCache{maxBytes: maxBytes, entries: make(map[string]*list.Element)}
}

// get returns the SST's metadata region if cached with the expected length.
func (c *sstMetaCache) get(sstID string, length int64) ([]byte, bool) {
	return c.lookup(sstID, length, true)
}

// peek rechecks the cache after joining a coalesced load without counting
// another lookup.
func (c *sstMetaCache) peek(sstID string, length int64) ([]byte, bool) {
	return c.lookup(sstID, length, false)
}

func (c *sstMetaCache) lookup(sstID string, length int64, record bool) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[sstID]
	if !ok || int64(len(element.Value.(*sstMetaCacheEntry).region)) != length {
		if record {
			c.misses++
		}
		return nil, false
	}
	if record {
		c.hits++
	}
	c.lru.MoveToBack(element)
	return element.Value.(*sstMetaCacheEntry).region, true
}

// put caches region for sstID, evicting the least recently used regions to
// stay within budget. A region larger than the whole budget is not cached.
func (c *sstMetaCache) put(sstID string, region []byte) {
	if c == nil || sstID == "" || len(region) == 0 {
		return
	}
	cost := int64(len(region)) + int64(len(sstID)) + metaCacheEntryOverhead

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.entries[sstID]; existing != nil {
		c.removeElement(existing)
	}
	if cost > c.maxBytes {
		return
	}
	for c.bytes+cost > c.maxBytes && c.lru.Len() > 0 {
		c.removeElement(c.lru.Front())
		c.evictions++
	}
	c.entries[sstID] = c.lru.PushBack(&sstMetaCacheEntry{sstID: sstID, region: region, cost: cost})
	c.bytes += cost
}

func (c *sstMetaCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.entries)
	c.lru.Init()
	c.bytes = 0
}

func (c *sstMetaCache) stats() CacheStats {
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
		Evictions:  c.evictions,
	}
}

func (c *sstMetaCache) removeElement(element *list.Element) {
	entry := c.lru.Remove(element).(*sstMetaCacheEntry)
	delete(c.entries, entry.sstID)
	c.bytes -= entry.cost
}
