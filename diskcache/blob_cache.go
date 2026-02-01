package diskcache

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// BlobCacheOptions configures a BlobCache.
type BlobCacheOptions struct {
	// Dir is the directory where cache files are stored.
	Dir string

	// MaxSize is the maximum bytes on disk (default 1GB).
	MaxSize int64

	// MaxItemSize is the maximum size for a single item.
	// Items larger than this will not be cached.
	// Default 0 means no limit.
	MaxItemSize int64
}

type blobEntry struct {
	path string
	size int64
}

type blobCache struct {
	mu          sync.RWMutex
	dir         string
	maxSize     int64
	maxItemSize int64
	currentSize int64

	index map[string]*blobEntry
	order []string

	hits   atomic.Int64
	misses atomic.Int64
}

// NewBlobCache creates a new blob cache with the given options.
func NewBlobCache(opts BlobCacheOptions) (Cache, error) {
	if opts.Dir == "" {
		return nil, errors.New("diskcache: cache directory is required")
	}

	maxSize := opts.MaxSize
	if maxSize <= 0 {
		maxSize = defaultMaxSize
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}

	return &blobCache{
		dir:         opts.Dir,
		maxSize:     maxSize,
		maxItemSize: opts.MaxItemSize,
		index:       make(map[string]*blobEntry),
		order:       make([]string, 0),
	}, nil
}

func (c *blobCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	entry, ok := c.index[key]
	c.mu.RUnlock()

	if !ok {
		c.misses.Add(1)
		return nil, false
	}

	data, err := os.ReadFile(entry.path)
	if err != nil {
		c.misses.Add(1)
		c.mu.Lock()
		c.removeLocked(key)
		c.mu.Unlock()
		return nil, false
	}

	c.hits.Add(1)

	c.mu.Lock()
	c.moveToEnd(key)
	c.mu.Unlock()

	return data, true
}

func (c *blobCache) Set(key string, data []byte) error {
	dataSize := int64(len(data))

	if c.maxItemSize > 0 && dataSize > c.maxItemSize {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.index[key]; exists {
		c.removeLocked(key)
	}

	for c.currentSize+dataSize > c.maxSize && len(c.order) > 0 {
		c.evictOldest()
	}

	filename := cacheFileName(key)
	localPath := filepath.Join(c.dir, filename)

	if err := os.WriteFile(localPath, data, 0644); err != nil {
		return err
	}

	entry := &blobEntry{
		path: localPath,
		size: dataSize,
	}

	c.index[key] = entry
	c.order = append(c.order, key)
	c.currentSize += dataSize

	return nil
}

func (c *blobCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(key)
}

func (c *blobCache) removeLocked(key string) {
	entry, ok := c.index[key]
	if !ok {
		return
	}

	os.Remove(entry.path)
	c.currentSize -= entry.size
	delete(c.index, key)
	c.removeFromOrder(key)
}

func (c *blobCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.index {
		os.Remove(entry.path)
	}

	c.index = make(map[string]*blobEntry)
	c.order = make([]string, 0)
	c.currentSize = 0

	return nil
}

func (c *blobCache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return Stats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		Size:       c.currentSize,
		MaxSize:    c.maxSize,
		EntryCount: len(c.index),
	}
}

func (c *blobCache) Close() error {
	return c.Clear()
}

func (c *blobCache) evictOldest() {
	if len(c.order) == 0 {
		return
	}
	key := c.order[0]
	c.removeLocked(key)
}

func (c *blobCache) moveToEnd(key string) {
	c.removeFromOrder(key)
	c.order = append(c.order, key)
}

func (c *blobCache) removeFromOrder(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}
