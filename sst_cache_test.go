package isledb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLRUSSTCache_BasicOperations(t *testing.T) {
	cache := NewLRUSSTCache(1024)

	_, ok := cache.Get("path1")
	require.False(t, ok)

	data := []byte("hello world")
	cache.Set("path1", data)
	got, ok := cache.Get("path1")
	require.True(t, ok)
	require.Equal(t, data, got)

	stats := cache.Stats()
	require.Equal(t, int64(1), stats.Hits)
	require.Equal(t, int64(1), stats.Misses)
	require.Equal(t, int64(len(data)), stats.Size)
	require.Equal(t, 1, stats.EntryCount)
}

func TestLRUSSTCache_Eviction(t *testing.T) {
	cache := NewLRUSSTCache(100)

	cache.Set("path1", make([]byte, 40))
	cache.Set("path2", make([]byte, 40))
	cache.Set("path3", make([]byte, 40))

	_, ok1 := cache.Get("path1")
	_, ok2 := cache.Get("path2")
	_, ok3 := cache.Get("path3")

	require.False(t, ok1, "path1 should be evicted")
	require.True(t, ok2, "path2 should still be cached")
	require.True(t, ok3, "path3 should still be cached")

	stats := cache.Stats()
	require.Equal(t, int64(80), stats.Size)
	require.Equal(t, 2, stats.EntryCount)
}

func TestLRUSSTCache_LRUOrder(t *testing.T) {
	cache := NewLRUSSTCache(100)

	cache.Set("path1", make([]byte, 30))
	cache.Set("path2", make([]byte, 30))
	cache.Set("path3", make([]byte, 30))

	cache.Get("path1")

	cache.Set("path4", make([]byte, 30))

	_, ok1 := cache.Get("path1")
	_, ok2 := cache.Get("path2")
	_, ok3 := cache.Get("path3")
	_, ok4 := cache.Get("path4")

	require.True(t, ok1, "path1 should be cached (recently accessed)")
	require.False(t, ok2, "path2 should be evicted (oldest)")
	require.True(t, ok3, "path3 should be cached")
	require.True(t, ok4, "path4 should be cached")
}

func TestLRUSSTCache_Clear(t *testing.T) {
	cache := NewLRUSSTCache(1024)

	cache.Set("path1", []byte("data1"))
	cache.Set("path2", []byte("data2"))

	cache.Clear()

	_, ok1 := cache.Get("path1")
	_, ok2 := cache.Get("path2")
	require.False(t, ok1)
	require.False(t, ok2)

	stats := cache.Stats()
	require.Equal(t, int64(0), stats.Size)
	require.Equal(t, 0, stats.EntryCount)
}

func TestLRUSSTCache_OversizedItem(t *testing.T) {
	cache := NewLRUSSTCache(100)

	cache.Set("big", make([]byte, 200))

	_, ok := cache.Get("big")
	require.False(t, ok, "oversized item should not be cached")

	stats := cache.Stats()
	require.Equal(t, int64(0), stats.Size)
	require.Equal(t, 0, stats.EntryCount)
}

func TestLRUSSTCache_Update(t *testing.T) {
	cache := NewLRUSSTCache(1024)

	cache.Set("path1", []byte("old"))
	cache.Set("path1", []byte("new value"))

	got, ok := cache.Get("path1")
	require.True(t, ok)
	require.Equal(t, []byte("new value"), got)

	stats := cache.Stats()
	require.Equal(t, int64(9), stats.Size)
	require.Equal(t, 1, stats.EntryCount)
}

func TestLRUSSTCache_Remove(t *testing.T) {
	cache := NewLRUSSTCache(1024)

	cache.Set("path1", []byte("data1"))
	cache.Set("path2", []byte("data2"))
	cache.Set("path3", []byte("data3"))

	_, ok1 := cache.Get("path1")
	_, ok2 := cache.Get("path2")
	_, ok3 := cache.Get("path3")
	require.True(t, ok1)
	require.True(t, ok2)
	require.True(t, ok3)

	cache.Remove("path2")

	_, ok1 = cache.Get("path1")
	_, ok2 = cache.Get("path2")
	_, ok3 = cache.Get("path3")
	require.True(t, ok1, "path1 should still exist")
	require.False(t, ok2, "path2 should be removed")
	require.True(t, ok3, "path3 should still exist")

	stats := cache.Stats()
	require.Equal(t, 2, stats.EntryCount)
	require.Equal(t, int64(10), stats.Size)
}

func TestLRUSSTCache_RemoveNonExistent(t *testing.T) {
	cache := NewLRUSSTCache(1024)

	cache.Set("path1", []byte("data1"))

	cache.Remove("nonexistent")

	_, ok := cache.Get("path1")
	require.True(t, ok, "existing entry should not be affected")
}

func TestNoopSSTCache(t *testing.T) {
	cache := NewNoopSSTCache()

	cache.Set("path1", []byte("data"))
	_, ok := cache.Get("path1")
	require.False(t, ok, "noop cache should never return data")

	cache.Remove("path1")

	stats := cache.Stats()
	require.Equal(t, int64(0), stats.Hits)
	require.Equal(t, int64(0), stats.Misses)
}
