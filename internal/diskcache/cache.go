package diskcache

type Stats struct {
	Hits       int64
	Misses     int64
	Size       int64
	MaxSize    int64
	EntryCount int
}

// RefCountedCache provides pinned access to memory-mapped cache entries.
// Reference counting prevents entries from being evicted while in use.
type RefCountedCache interface {
	Set(key string, data []byte) error
	Remove(key string)
	// Clear removes unpinned entries and removes pinned entries after their last
	// Release. The cache remains usable.
	Clear() error
	Stats() Stats
	// Close rejects new acquisitions and removes pinned entries after their last
	// Release.
	Close() error

	// Acquire retrieves data and increments its reference count.
	// The entry will not be evicted until Release is called.
	// Returns the data and true if found, nil and false otherwise.
	Acquire(key string) ([]byte, bool)

	// Release decrements the reference count for an entry.
	// When the count reaches zero, the entry becomes eligible for eviction.
	Release(key string)
}

type FileBackedCache interface {
	RefCountedCache
	CacheDir() string

	// SetFromFile promotes a temp file into the cache under key.
	// tempPath should point to a file on the same filesystem as CacheDir.
	SetFromFile(key, tempPath string, size int64) error
}
