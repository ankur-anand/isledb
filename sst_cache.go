package isledb

import (
	"sync"
	"sync/atomic"
)

const DefaultSSTCacheSize = 64 * 1024 * 1024

type SSTCache interface {
	Get(path string) ([]byte, bool)
	Set(path string, data []byte)
	Remove(path string)
	Clear()
	Stats() SSTCacheStats
}

type SSTCacheStats struct {
	Hits       int64
	Misses     int64
	Size       int64
	MaxSize    int64
	EntryCount int
}

type lruSSTCache struct {
	mu      sync.Mutex
	maxSize int64
	size    int64
	cache   map[string][]byte
	order   []string

	hits   atomic.Int64
	misses atomic.Int64
}

func NewLRUSSTCache(maxSize int64) SSTCache {
	if maxSize <= 0 {
		maxSize = DefaultSSTCacheSize
	}
	return &lruSSTCache{
		maxSize: maxSize,
		cache:   make(map[string][]byte),
		order:   make([]string, 0),
	}
}

func (c *lruSSTCache) Get(path string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, ok := c.cache[path]
	if !ok {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	c.touch(path)
	return data, true
}

func (c *lruSSTCache) Set(path string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	dataSize := int64(len(data))

	if dataSize > c.maxSize {
		return
	}

	if existing, ok := c.cache[path]; ok {
		c.size -= int64(len(existing))
		c.cache[path] = data
		c.size += dataSize
		c.touch(path)
		return
	}

	for c.size+dataSize > c.maxSize && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		if evicted, ok := c.cache[oldest]; ok {
			c.size -= int64(len(evicted))
			delete(c.cache, oldest)
		}
	}

	c.cache[path] = data
	c.size += dataSize
	c.order = append(c.order, path)
}

func (c *lruSSTCache) Remove(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if data, ok := c.cache[path]; ok {
		c.size -= int64(len(data))
		delete(c.cache, path)

		for i, p := range c.order {
			if p == path {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	}
}

func (c *lruSSTCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string][]byte)
	c.order = c.order[:0]
	c.size = 0
}

func (c *lruSSTCache) Stats() SSTCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	return SSTCacheStats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		Size:       c.size,
		MaxSize:    c.maxSize,
		EntryCount: len(c.cache),
	}
}

func (c *lruSSTCache) touch(path string) {
	for i, cached := range c.order {
		if cached == path {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, path)
			return
		}
	}
	c.order = append(c.order, path)
}

type noopSSTCache struct{}

func NewNoopSSTCache() SSTCache {
	return &noopSSTCache{}
}

func (c *noopSSTCache) Get(path string) ([]byte, bool) {
	return nil, false
}

func (c *noopSSTCache) Set(path string, data []byte) {}

func (c *noopSSTCache) Remove(path string) {}

func (c *noopSSTCache) Clear() {}

func (c *noopSSTCache) Stats() SSTCacheStats {
	return SSTCacheStats{}
}
