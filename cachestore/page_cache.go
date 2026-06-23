package cachestore

import (
	"sync"
	"sync/atomic"
)

const DefaultManifestPageCacheSize = 1024

type ManifestPageCacheStats struct {
	Hits       int64
	Misses     int64
	EntryCount int
	MaxEntries int
}

type LruManifestPageCache struct {
	mu         sync.Mutex
	maxEntries int
	cache      map[string][]byte
	order      []string

	hits   atomic.Int64
	misses atomic.Int64
}

func NewLRUManifestPageCache(maxEntries int) *LruManifestPageCache {
	if maxEntries <= 0 {
		maxEntries = DefaultManifestPageCacheSize
	}
	return &LruManifestPageCache{
		maxEntries: maxEntries,
		cache:      make(map[string][]byte),
		order:      make([]string, 0),
	}
}

func (c *LruManifestPageCache) Get(path string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, ok := c.cache[path]
	if !ok {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	c.touch(path)
	return append([]byte(nil), data...), true
}

func (c *LruManifestPageCache) Set(path string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.cache[path]; ok {
		c.cache[path] = append([]byte(nil), data...)
		c.touch(path)
		return
	}

	for len(c.order) >= c.maxEntries && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.cache, oldest)
	}

	c.cache[path] = append([]byte(nil), data...)
	c.order = append(c.order, path)
}

func (c *LruManifestPageCache) Remove(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.cache[path]; ok {
		delete(c.cache, path)

		for i, p := range c.order {
			if p == path {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	}
}

func (c *LruManifestPageCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string][]byte)
	c.order = c.order[:0]
}

func (c *LruManifestPageCache) Stats() ManifestPageCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	return ManifestPageCacheStats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		EntryCount: len(c.cache),
		MaxEntries: c.maxEntries,
	}
}

func (c *LruManifestPageCache) touch(path string) {
	for i, cached := range c.order {
		if cached == path {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, path)
			return
		}
	}
	c.order = append(c.order, path)
}

type noopManifestPageCache struct{}

func NewNoopManifestPageCache() ManifestPageCache {
	return &noopManifestPageCache{}
}

func (c *noopManifestPageCache) Get(path string) ([]byte, bool) {
	return nil, false
}

func (c *noopManifestPageCache) Set(path string, data []byte) {}

func (c *noopManifestPageCache) Remove(path string) {}

func (c *noopManifestPageCache) Clear() {}

func (c *noopManifestPageCache) Stats() ManifestPageCacheStats {
	return ManifestPageCacheStats{}
}
