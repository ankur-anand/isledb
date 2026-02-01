package diskcache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const defaultMaxSize = 1 << 30

// SSTCacheOptions configures an SSTCache.
type SSTCacheOptions struct {
	Dir string
	// MaxSize is the maximum bytes on disk (default 1GB).
	MaxSize int64
}

type sstEntry struct {
	localPath string
	size      int64
	mmap      []byte
	file      *os.File
	refs      atomic.Int32
}

type sstCache struct {
	mu          sync.RWMutex
	dir         string
	maxSize     int64
	currentSize int64

	index map[string]*sstEntry
	order []string

	hits   atomic.Int64
	misses atomic.Int64
}

// NewSSTCache creates a new SST cache with the given options.
func NewSSTCache(opts SSTCacheOptions) (RefCountedCache, error) {
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

	return &sstCache{
		dir:     opts.Dir,
		maxSize: maxSize,
		index:   make(map[string]*sstEntry),
		order:   make([]string, 0),
	}, nil
}

func (c *sstCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	entry, ok := c.index[key]
	c.mu.RUnlock()

	if !ok {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	c.mu.Lock()
	c.moveToEnd(key)
	c.mu.Unlock()

	return entry.mmap, true
}

func (c *sstCache) Set(key string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.index[key]; exists {
		c.removeLocked(key)
	}

	dataSize := int64(len(data))
	for c.currentSize+dataSize > c.maxSize && len(c.order) > 0 {
		if !c.evictOldest() {
			break
		}
	}

	filename := cacheFileName(key)
	localPath := filepath.Join(c.dir, filename)

	if err := os.WriteFile(localPath, data, 0644); err != nil {
		return err
	}

	f, err := os.Open(localPath)
	if err != nil {
		os.Remove(localPath)
		return err
	}

	mmap, err := MmapFile(f)
	if err != nil {
		f.Close()
		os.Remove(localPath)
		return err
	}

	entry := &sstEntry{
		localPath: localPath,
		size:      dataSize,
		mmap:      mmap,
		file:      f,
	}

	c.index[key] = entry
	c.order = append(c.order, key)
	c.currentSize += dataSize

	return nil
}

func (c *sstCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(key)
}

func (c *sstCache) removeLocked(key string) {
	entry, ok := c.index[key]
	if !ok {
		return
	}

	// IMP: Unmap and close
	if entry.mmap != nil {
		Munmap(entry.mmap)
	}
	if entry.file != nil {
		entry.file.Close()
	}

	os.Remove(entry.localPath)
	c.currentSize -= entry.size
	delete(c.index, key)
	c.removeFromOrder(key)
}

func (c *sstCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.index {
		if entry.mmap != nil {
			Munmap(entry.mmap)
		}
		if entry.file != nil {
			entry.file.Close()
		}
		os.Remove(entry.localPath)
	}

	c.index = make(map[string]*sstEntry)
	c.order = make([]string, 0)
	c.currentSize = 0

	return nil
}

func (c *sstCache) Stats() Stats {
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

func (c *sstCache) Close() error {
	return c.Clear()
}

func (c *sstCache) Acquire(key string) ([]byte, bool) {
	c.mu.RLock()
	entry, ok := c.index[key]
	c.mu.RUnlock()

	if !ok {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	entry.refs.Add(1)

	c.mu.Lock()
	c.moveToEnd(key)
	c.mu.Unlock()

	return entry.mmap, true
}

func (c *sstCache) Release(key string) {
	c.mu.RLock()
	entry, ok := c.index[key]
	c.mu.RUnlock()

	if ok {
		entry.refs.Add(-1)
	}
}

func (c *sstCache) evictOldest() bool {
	for _, key := range c.order {
		entry := c.index[key]
		if entry.refs.Load() == 0 {
			c.removeLocked(key)
			return true
		}
	}
	return false
}

func (c *sstCache) moveToEnd(key string) {
	c.removeFromOrder(key)
	c.order = append(c.order, key)
}

func (c *sstCache) removeFromOrder(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

func cacheFileName(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:16])
}
