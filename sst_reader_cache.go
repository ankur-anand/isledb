package isledb

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2/sstable"
)

const DefaultSSTReaderCacheSize = 32

type SSTReaderCache interface {
	Get(path string) (*sstable.Reader, bool)
	Set(path string, reader *sstable.Reader, data []byte)
	Remove(path string)
	Clear()
	Stats() SSTReaderCacheStats
}

type SSTReaderCacheStats struct {
	Hits       int64
	Misses     int64
	EntryCount int
	MaxEntries int
}

type cachedReader struct {
	reader *sstable.Reader
	data   []byte
}

type lruSSTReaderCache struct {
	mu         sync.Mutex
	maxEntries int
	cache      map[string]*cachedReader
	order      []string

	hits   atomic.Int64
	misses atomic.Int64
}

func NewLRUSSTReaderCache(maxEntries int) SSTReaderCache {
	if maxEntries <= 0 {
		maxEntries = DefaultSSTReaderCacheSize
	}
	return &lruSSTReaderCache{
		maxEntries: maxEntries,
		cache:      make(map[string]*cachedReader),
		order:      make([]string, 0, maxEntries),
	}
}

func (c *lruSSTReaderCache) Get(path string) (*sstable.Reader, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cached, ok := c.cache[path]
	if !ok {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	c.touch(path)
	return cached.reader, true
}

func (c *lruSSTReaderCache) Set(path string, reader *sstable.Reader, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.cache[path]; ok {

		_ = existing.reader.Close()
		c.cache[path] = &cachedReader{reader: reader, data: data}
		c.touch(path)
		return
	}

	for len(c.cache) >= c.maxEntries && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		if evicted, ok := c.cache[oldest]; ok {
			_ = evicted.reader.Close()
			delete(c.cache, oldest)
		}
	}

	c.cache[path] = &cachedReader{reader: reader, data: data}
	c.order = append(c.order, path)
}

func (c *lruSSTReaderCache) Remove(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cached, ok := c.cache[path]; ok {
		_ = cached.reader.Close()
		delete(c.cache, path)

		for i, p := range c.order {
			if p == path {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	}
}

func (c *lruSSTReaderCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cached := range c.cache {
		_ = cached.reader.Close()
	}
	c.cache = make(map[string]*cachedReader)
	c.order = c.order[:0]
}

func (c *lruSSTReaderCache) Stats() SSTReaderCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	return SSTReaderCacheStats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		EntryCount: len(c.cache),
		MaxEntries: c.maxEntries,
	}
}

func (c *lruSSTReaderCache) touch(path string) {
	for i, p := range c.order {
		if p == path {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, path)
			return
		}
	}
	c.order = append(c.order, path)
}

type noopSSTReaderCache struct{}

func NewNoopSSTReaderCache() SSTReaderCache {
	return &noopSSTReaderCache{}
}

func (c *noopSSTReaderCache) Get(path string) (*sstable.Reader, bool) {
	return nil, false
}

func (c *noopSSTReaderCache) Set(path string, reader *sstable.Reader, data []byte) {

	_ = reader.Close()
}

func (c *noopSSTReaderCache) Remove(path string) {}

func (c *noopSSTReaderCache) Clear() {}

func (c *noopSSTReaderCache) Stats() SSTReaderCacheStats {
	return SSTReaderCacheStats{}
}
