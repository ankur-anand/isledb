package isledb

import (
	"sync"
	"sync/atomic"
)

const DefaultBlobCacheSize = 128 * 1024 * 1024

const DefaultBlobCacheMaxItemSize = 16 * 1024 * 1024

type BlobCache interface {
	Get(blobID string) ([]byte, bool)
	Set(blobID string, data []byte)
	Remove(blobID string)
	Clear()
	Stats() BlobCacheStats
}

type BlobCacheStats struct {
	Hits       int64
	Misses     int64
	EntryCount int
	TotalBytes int64
	MaxBytes   int64
}

type lruBlobCache struct {
	mu          sync.Mutex
	maxBytes    int64
	maxItemSize int64
	cache       map[string][]byte
	order       []string
	totalBytes  int64

	hits   atomic.Int64
	misses atomic.Int64
}

func NewLRUBlobCache(maxBytes, maxItemSize int64) BlobCache {
	if maxBytes <= 0 {
		maxBytes = DefaultBlobCacheSize
	}
	if maxItemSize <= 0 {
		maxItemSize = DefaultBlobCacheMaxItemSize
	}
	return &lruBlobCache{
		maxBytes:    maxBytes,
		maxItemSize: maxItemSize,
		cache:       make(map[string][]byte),
		order:       make([]string, 0),
	}
}

func (c *lruBlobCache) Get(blobID string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, ok := c.cache[blobID]
	if !ok {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	c.touch(blobID)

	result := make([]byte, len(data))
	copy(result, data)
	return result, true
}

func (c *lruBlobCache) Set(blobID string, data []byte) {

	if int64(len(data)) > c.maxItemSize {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.cache[blobID]; ok {
		c.totalBytes -= int64(len(existing))
		c.totalBytes += int64(len(data))

		c.cache[blobID] = append([]byte(nil), data...)
		c.touch(blobID)
		return
	}

	dataSize := int64(len(data))
	for c.totalBytes+dataSize > c.maxBytes && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		if evicted, ok := c.cache[oldest]; ok {
			c.totalBytes -= int64(len(evicted))
			delete(c.cache, oldest)
		}
	}

	c.cache[blobID] = append([]byte(nil), data...)
	c.order = append(c.order, blobID)
	c.totalBytes += dataSize
}

func (c *lruBlobCache) Remove(blobID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if data, ok := c.cache[blobID]; ok {
		c.totalBytes -= int64(len(data))
		delete(c.cache, blobID)

		for i, id := range c.order {
			if id == blobID {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	}
}

func (c *lruBlobCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string][]byte)
	c.order = c.order[:0]
	c.totalBytes = 0
}

func (c *lruBlobCache) Stats() BlobCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	return BlobCacheStats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		EntryCount: len(c.cache),
		TotalBytes: c.totalBytes,
		MaxBytes:   c.maxBytes,
	}
}

func (c *lruBlobCache) touch(blobID string) {
	for i, id := range c.order {
		if id == blobID {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, blobID)
			return
		}
	}
	c.order = append(c.order, blobID)
}

type noopBlobCache struct{}

func NewNoopBlobCache() BlobCache {
	return &noopBlobCache{}
}

func (c *noopBlobCache) Get(blobID string) ([]byte, bool) {
	return nil, false
}

func (c *noopBlobCache) Set(blobID string, data []byte) {}

func (c *noopBlobCache) Remove(blobID string) {}

func (c *noopBlobCache) Clear() {}

func (c *noopBlobCache) Stats() BlobCacheStats {
	return BlobCacheStats{}
}
