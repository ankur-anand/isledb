package isledb

import (
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
	// onDamage, if set, runs when an iterator fails on damage (see damaged).
	onDamage func()
	refs     atomic.Int32
	// closed records that the reader was closed; tests check it.
	closed atomic.Bool
}

// tryRef takes a reference unless the last one is already gone: a reader
// that is closing, or closed, is never brought back.
func (s *openSST) tryRef() bool {
	for {
		refs := s.refs.Load()
		if refs <= 0 {
			return false
		}
		if s.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
}

func (s *openSST) unref() {
	if s.refs.Add(-1) != 0 {
		return
	}
	_ = s.reader.Close()
	s.closed.Store(true)
}

// openSSTCache keeps up to max SSTs open, evicting with CLOCK, so a read of a
// cached SST parses no metadata. An SST leaves the cache when it is evicted
// or reported corrupt, and closes once the last iterator over it does.
// Lookups take no lock (see clockCache).
type openSSTCache struct {
	entries   *clockCache[*openSST]
	max       int
	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
}

// newOpenSSTCache returns a cache of up to max open SSTs, or nil, which opens
// every SST afresh, when max is zero or less.
func newOpenSSTCache(max int) *openSSTCache {
	if max <= 0 {
		return nil
	}
	return &openSSTCache{entries: newClockCache[*openSST](), max: max}
}

// acquire returns the cached SST with a reference for the caller, or nil. A
// lookup may find an SST a writer just removed: if it is already closing,
// tryRef refuses it and acquire reports a miss; otherwise it is still open
// and safe to use.
func (c *openSSTCache) acquire(id string) *openSST {
	if c == nil {
		return nil
	}
	e, ok := c.entries.get(id)
	if !ok || !e.value.tryRef() {
		c.misses.Add(1)
		return nil
	}
	c.hits.Add(1)
	return e.value
}

// add caches s, which the caller has just opened, and returns it with a
// reference for the caller. If another caller cached the SST first, add
// keeps theirs, closes s and returns theirs instead.
func (c *openSSTCache) add(s *openSST) *openSST {
	s.refs.Store(1)
	if c == nil {
		return s
	}
	txn := c.entries.begin()
	if existing, ok := txn.lookup(s.id); ok {
		// The cache holds a reference to every cached SST, so it is open.
		existing.value.refs.Add(1)
		existing.referenced.Store(true)
		txn.commit()
		s.unref()
		return existing.value
	}
	var evicted []*openSST
	for txn.len() >= c.max {
		e := txn.victim()
		if e == nil {
			break
		}
		evicted = append(evicted, e.value)
		c.evictions.Add(1)
	}
	s.refs.Add(1) // the cache's reference
	txn.insert(s.id, s)
	txn.commit()
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

// isOpen reports whether id is cached open. It takes no lock.
func (c *openSSTCache) isOpen(id string) bool {
	if c == nil {
		return false
	}
	_, ok := c.entries.peek(id)
	return ok
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
	txn := c.entries.begin()
	var matched []*clockEntry[*openSST]
	txn.each(func(e *clockEntry[*openSST]) {
		if match(e.value) {
			matched = append(matched, e)
		}
	})
	for _, e := range matched {
		txn.remove(e)
	}
	txn.commit()
	for _, e := range matched {
		e.value.unref()
	}
}

func (c *openSSTCache) stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}
	return CacheStats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		EntryCount: c.entries.len(),
		MaxEntries: c.max,
		Evictions:  c.evictions.Load(),
	}
}
