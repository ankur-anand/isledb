package isledb

import (
	"maps"
	"sync"
	"sync/atomic"
)

// clockCache is the core of the reader's metadata caches: a map read without
// locks, with CLOCK eviction.
//
// Every point lookup consults these caches, from every goroutine at once, and
// nearly always hits. An LRU list would need an exclusive lock on every hit
// to move the entry; under parallel reads that lock, not the lookup, becomes
// the cost. Here a reader loads an immutable snapshot of the map and, on a
// hit, sets the entry's referenced flag, only if it is not already set, so a
// hot entry is not written by every reader. Writers, which insert, evict and
// remove, are rare: they serialize on mu, change a copy of the map, and
// publish it.
//
// Eviction is CLOCK: a hand sweeps the entries, clearing referenced flags and
// evicting the first entry not referenced since the hand last passed it. It
// approximates least-recently-used without ordering every hit.
type clockCache[V any] struct {
	snapshot atomic.Pointer[map[string]*clockEntry[V]]

	mu   sync.Mutex // serializes writers; guards ring and hand
	ring []*clockEntry[V]
	hand int
}

type clockEntry[V any] struct {
	key        string
	value      V
	referenced atomic.Bool
	slot       int // index in ring; guarded by the cache's mu
}

func newClockCache[V any]() *clockCache[V] {
	c := &clockCache[V]{}
	empty := map[string]*clockEntry[V]{}
	c.snapshot.Store(&empty)
	return c
}

// get returns the entry for key and marks it referenced. It takes no lock.
func (c *clockCache[V]) get(key string) (*clockEntry[V], bool) {
	e, ok := (*c.snapshot.Load())[key]
	if ok && !e.referenced.Load() {
		e.referenced.Store(true)
	}
	return e, ok
}

// peek returns the entry for key without marking it referenced.
func (c *clockCache[V]) peek(key string) (*clockEntry[V], bool) {
	e, ok := (*c.snapshot.Load())[key]
	return e, ok
}

// len returns the number of entries. It takes no lock.
func (c *clockCache[V]) len() int { return len(*c.snapshot.Load()) }

// clockTxn is one writer's change: a copy of the map, published by commit.
// The caller holds c.mu from begin until commit.
type clockTxn[V any] struct {
	c    *clockCache[V]
	next map[string]*clockEntry[V]
}

func (c *clockCache[V]) begin() *clockTxn[V] {
	c.mu.Lock()
	return &clockTxn[V]{c: c, next: maps.Clone(*c.snapshot.Load())}
}

// commit publishes the changed map and releases the writer lock.
func (t *clockTxn[V]) commit() {
	t.c.snapshot.Store(&t.next)
	t.c.mu.Unlock()
}

// len returns the number of entries the transaction will publish.
func (t *clockTxn[V]) len() int { return len(t.next) }

func (t *clockTxn[V]) lookup(key string) (*clockEntry[V], bool) {
	e, ok := t.next[key]
	return e, ok
}

// insert adds a new entry for key, not yet referenced: an entry loaded once
// and never read again is the first to go, while entries that keep being read
// survive the hand. Writers make room before inserting, so a new entry is
// never its own victim. key must not be present.
func (t *clockTxn[V]) insert(key string, value V) *clockEntry[V] {
	e := &clockEntry[V]{key: key, value: value, slot: len(t.c.ring)}
	t.c.ring = append(t.c.ring, e)
	t.next[key] = e
	return e
}

// remove deletes e. The last entry of the ring takes its slot.
func (t *clockTxn[V]) remove(e *clockEntry[V]) {
	c := t.c
	delete(t.next, e.key)
	last := len(c.ring) - 1
	if e.slot != last {
		c.ring[e.slot] = c.ring[last]
		c.ring[e.slot].slot = e.slot
	}
	c.ring[last] = nil
	c.ring = c.ring[:last]
	if c.hand >= len(c.ring) {
		c.hand = 0
	}
}

// victim sweeps the hand to the first entry not referenced since it last
// passed, removes it and returns it; it returns nil when the cache is empty.
func (t *clockTxn[V]) victim() *clockEntry[V] {
	c := t.c
	for len(c.ring) > 0 {
		if c.hand >= len(c.ring) {
			c.hand = 0
		}
		e := c.ring[c.hand]
		if e.referenced.Load() {
			e.referenced.Store(false)
			c.hand++
			continue
		}
		t.remove(e)
		return e
	}
	return nil
}

// removeAll deletes every entry and returns them.
func (t *clockTxn[V]) removeAll() []*clockEntry[V] {
	removed := t.c.ring
	t.c.ring, t.c.hand = nil, 0
	t.next = map[string]*clockEntry[V]{}
	return removed
}

// each calls f for every entry; f may not change the cache.
func (t *clockTxn[V]) each(f func(*clockEntry[V])) {
	for _, e := range t.c.ring {
		f(e)
	}
}
