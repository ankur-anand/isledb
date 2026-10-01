package isledb

import (
	"container/list"
	"crypto/sha256"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2/sstable"
)

const defaultOpenSSTCacheSize = 1024

// openSST is one opened SST: its parsed sstable.Reader, shared by every
// iterator over it. It is closed when its last reference goes: the cache's,
// while it is cached, and one per open iterator.
type openSST struct {
	id     string
	reader *sstable.Reader
	// local reports whether the SST is read from a file in the local disk
	// cache, whose checksum is sum, rather than by range from object storage.
	local bool
	sum   [sha256.Size]byte
	// release, if set, runs after the reader closes; for a local SST it
	// closes the file.
	release func()
	// onCorruption, if set, runs when an iterator fails on corrupt data.
	onCorruption func()
	refs         atomic.Int32
}

func (s *openSST) unref() {
	if s.refs.Add(-1) != 0 {
		return
	}
	_ = s.reader.Close()
	if s.release != nil {
		s.release()
	}
}

// openSSTCache keeps up to max SSTs open, least recently used first out, so a
// read of a cached SST opens no file and parses no metadata. An SST leaves the
// cache when it is evicted, leaves the manifest, is reported corrupt, or, for
// a local SST, when the disk cache removes its file; it closes once the last
// iterator over it does.
type openSSTCache struct {
	mu        sync.Mutex
	max       int
	entries   map[string]*list.Element
	lru       list.List // *openSST, least recently used first
	hits      int64
	misses    int64
	evictions int64
}

// newOpenSSTCache returns a cache of up to max open SSTs, or nil, which opens
// every SST afresh, when max is zero or less.
func newOpenSSTCache(max int) *openSSTCache {
	if max <= 0 {
		return nil
	}
	return &openSSTCache{max: max, entries: make(map[string]*list.Element)}
}

// acquire returns the cached SST with a reference for the caller, or nil.
func (c *openSSTCache) acquire(id string) *openSST {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[id]
	if !ok {
		c.misses++
		return nil
	}
	c.hits++
	c.lru.MoveToBack(element)
	s := element.Value.(*openSST)
	s.refs.Add(1)
	return s
}

// add caches s, which the caller has just opened, and returns it with a
// reference for the caller. If another caller cached the SST first, add
// keeps theirs, closes s and returns theirs instead.
func (c *openSSTCache) add(s *openSST) *openSST {
	s.refs.Store(1)
	if c == nil {
		return s
	}
	c.mu.Lock()
	if element, ok := c.entries[s.id]; ok {
		existing := element.Value.(*openSST)
		existing.refs.Add(1)
		c.lru.MoveToBack(element)
		c.mu.Unlock()
		s.unref()
		return existing
	}
	s.refs.Add(1) // the cache's reference
	c.entries[s.id] = c.lru.PushBack(s)
	var evicted []*openSST
	for c.lru.Len() > c.max {
		evicted = append(evicted, c.removeLocked(c.lru.Front()))
		c.evictions++
	}
	c.mu.Unlock()
	for _, e := range evicted {
		e.unref()
	}
	return s
}

// remove drops id from the cache.
func (c *openSSTCache) remove(id string) {
	c.removeIf(func(s *openSST) bool { return s.id == id })
}

// drop removes s, if it is still the cached SST for its ID.
func (c *openSSTCache) drop(s *openSST) {
	c.removeIf(func(cached *openSST) bool { return cached == s })
}

// removeLocalFile drops the local SSTs reading the disk cache file sum, which
// the disk cache has removed, so the file closes and its space is freed once
// their iterators finish.
func (c *openSSTCache) removeLocalFile(sum [sha256.Size]byte) {
	c.removeIf(func(s *openSST) bool { return s.local && s.sum == sum })
}

// retain drops every SST not in m, as SSTs leave the manifest.
func (c *openSSTCache) retain(m *manifestState) {
	if c == nil || m == nil {
		return
	}
	live := make(map[string]struct{}, len(m.L0SSTs))
	for _, sst := range m.L0SSTs {
		live[sst.ID] = struct{}{}
	}
	for _, level := range m.Levels {
		for _, sst := range level.SSTs {
			live[sst.ID] = struct{}{}
		}
	}
	c.removeIf(func(s *openSST) bool {
		_, ok := live[s.id]
		return !ok
	})
}

// clear drops every SST.
func (c *openSSTCache) clear() {
	c.removeIf(func(*openSST) bool { return true })
}

// removeIf drops every cached SST match accepts, closing those no iterator
// still uses.
func (c *openSSTCache) removeIf(match func(*openSST) bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	var removed []*openSST
	for element := c.lru.Front(); element != nil; {
		next := element.Next()
		if match(element.Value.(*openSST)) {
			removed = append(removed, c.removeLocked(element))
		}
		element = next
	}
	c.mu.Unlock()
	for _, s := range removed {
		s.unref()
	}
}

func (c *openSSTCache) removeLocked(element *list.Element) *openSST {
	s := c.lru.Remove(element).(*openSST)
	delete(c.entries, s.id)
	return s
}

func (c *openSSTCache) stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Hits:       c.hits,
		Misses:     c.misses,
		EntryCount: c.lru.Len(),
		MaxEntries: c.max,
		Evictions:  c.evictions,
	}
}
