package isledb

import "container/list"

const (
	defaultDeletionPlanCacheEntries = 128
	defaultDeletionPlanCacheBytes   = 8 << 20
)

type boundedPlanCacheEntry[T any] struct {
	key          string
	value        T
	encodedBytes int
}

// boundedPlanCache is an in-memory optimization for immutable deletion plans.
// Both limits are required because one plan can contain many long object keys.
type boundedPlanCache[T any] struct {
	maxEntries   int
	maxBytes     int
	encodedBytes int
	entries      map[string]*list.Element
	lru          list.List
}

func newBoundedPlanCache[T any](maxEntries, maxBytes int) *boundedPlanCache[T] {
	return &boundedPlanCache[T]{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		entries:    make(map[string]*list.Element),
	}
}

func newDeletionPlanCache[T any]() *boundedPlanCache[T] {
	return newBoundedPlanCache[T](defaultDeletionPlanCacheEntries, defaultDeletionPlanCacheBytes)
}

func (c *boundedPlanCache[T]) get(key string) (T, bool) {
	var zero T
	if c == nil {
		return zero, false
	}
	element, ok := c.entries[key]
	if !ok {
		return zero, false
	}
	c.lru.MoveToBack(element)
	return element.Value.(*boundedPlanCacheEntry[T]).value, true
}

func (c *boundedPlanCache[T]) put(key string, value T, encodedBytes int) {
	if c == nil || key == "" || c.maxEntries <= 0 || c.maxBytes <= 0 || encodedBytes <= 0 || encodedBytes > c.maxBytes {
		return
	}
	if existing, ok := c.entries[key]; ok {
		entry := existing.Value.(*boundedPlanCacheEntry[T])
		c.encodedBytes -= entry.encodedBytes
		entry.value = value
		entry.encodedBytes = encodedBytes
		c.encodedBytes += encodedBytes
		c.lru.MoveToBack(existing)
	} else {
		entry := &boundedPlanCacheEntry[T]{key: key, value: value, encodedBytes: encodedBytes}
		c.entries[key] = c.lru.PushBack(entry)
		c.encodedBytes += encodedBytes
	}
	for len(c.entries) > c.maxEntries || c.encodedBytes > c.maxBytes {
		c.removeElement(c.lru.Front())
	}
}

func (c *boundedPlanCache[T]) remove(key string) {
	if c == nil {
		return
	}
	c.removeElement(c.entries[key])
}

func (c *boundedPlanCache[T]) removeElement(element *list.Element) {
	if c == nil || element == nil {
		return
	}
	entry := element.Value.(*boundedPlanCacheEntry[T])
	delete(c.entries, entry.key)
	c.encodedBytes -= entry.encodedBytes
	c.lru.Remove(element)
}
