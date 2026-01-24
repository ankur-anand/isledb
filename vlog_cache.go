package isledb

import (
	"sync"

	"github.com/segmentio/ksuid"
)

// VLogCache implementations must be safe for concurrent use.
type VLogCache interface {
	Get(id ksuid.KSUID) ([]byte, bool)
	Set(id ksuid.KSUID, data []byte)
	Clear()
}

type lruVLogCache struct {
	mu    sync.Mutex
	max   int
	cache map[ksuid.KSUID][]byte
	order []ksuid.KSUID
}

func NewLRUVLogCache(max int) VLogCache {
	if max <= 0 {
		max = DefaultVLogCacheSize
	}
	return &lruVLogCache{
		max:   max,
		cache: make(map[ksuid.KSUID][]byte),
		order: make([]ksuid.KSUID, 0, max),
	}
}

func (c *lruVLogCache) Get(id ksuid.KSUID) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, ok := c.cache[id]
	if !ok {
		return nil, false
	}
	c.touch(id)
	return data, true
}

func (c *lruVLogCache) Set(id ksuid.KSUID, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.cache[id]; ok {
		c.cache[id] = data
		c.touch(id)
		return
	}

	for len(c.cache) >= c.max && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.cache, oldest)
	}
	c.cache[id] = data
	c.order = append(c.order, id)
}

func (c *lruVLogCache) Clear() {
	c.mu.Lock()
	c.cache = make(map[ksuid.KSUID][]byte)
	c.order = c.order[:0]
	c.mu.Unlock()
}

func (c *lruVLogCache) touch(id ksuid.KSUID) {
	for i, cached := range c.order {
		if cached == id {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, id)
			return
		}
	}
	c.order = append(c.order, id)
}
