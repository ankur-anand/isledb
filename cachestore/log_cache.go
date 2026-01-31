package cachestore

import (
	"sync"
	"sync/atomic"
)

const DefaultManifestLogCacheSize = 1024

type ManifestLogCacheStats struct {
	Hits       int64
	Misses     int64
	EntryCount int
	MaxEntries int
}

type LruManifestLogCache struct {
	mu         sync.Mutex
	maxEntries int
	cache      map[string][]byte
	order      []string

	hits   atomic.Int64
	misses atomic.Int64
}

func NewLRUManifestLogCache(maxEntries int) *LruManifestLogCache {
	if maxEntries <= 0 {
		maxEntries = DefaultManifestLogCacheSize
	}
	return &LruManifestLogCache{
		maxEntries: maxEntries,
		cache:      make(map[string][]byte),
		order:      make([]string, 0),
	}
}

func (c *LruManifestLogCache) Get(path string) ([]byte, bool) {
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

func (c *LruManifestLogCache) Set(path string, data []byte) {
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

func (c *LruManifestLogCache) Remove(path string) {
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

func (c *LruManifestLogCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string][]byte)
	c.order = c.order[:0]
}

func (c *LruManifestLogCache) Stats() ManifestLogCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	return ManifestLogCacheStats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		EntryCount: len(c.cache),
		MaxEntries: c.maxEntries,
	}
}

func (c *LruManifestLogCache) touch(path string) {
	for i, cached := range c.order {
		if cached == path {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, path)
			return
		}
	}
	c.order = append(c.order, path)
}

type noopManifestLogCache struct{}

func NewNoopManifestLogCache() ManifestLogCache {
	return &noopManifestLogCache{}
}

func (c *noopManifestLogCache) Get(path string) ([]byte, bool) {
	return nil, false
}

func (c *noopManifestLogCache) Set(path string, data []byte) {}

func (c *noopManifestLogCache) Remove(path string) {}

func (c *noopManifestLogCache) Clear() {}

func (c *noopManifestLogCache) Stats() ManifestLogCacheStats {
	return ManifestLogCacheStats{}
}
