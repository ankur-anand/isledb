package diskcache

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSTCache_RefCounting(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSTCache(SSTCacheOptions{
		Dir:     dir,
		MaxSize: 100,
	})
	require.NoError(t, err)
	defer cache.Close()

	data := make([]byte, 50)
	err = cache.Set("protected", data)
	require.NoError(t, err)

	got, ok := cache.Acquire("protected")
	require.True(t, ok)
	require.Equal(t, data, got)

	for i := range 5 {
		key := fmt.Sprintf("key%d", i)
		newData := make([]byte, 30)
		cache.Set(key, newData)
	}

	_, ok = cache.Get("protected")
	require.True(t, ok, "protected entry should not be evicted while acquired")

	cache.Release("protected")
	cache.Set("final", make([]byte, 80))

	stats := cache.Stats()
	require.LessOrEqual(t, stats.Size, int64(100))
}

func TestSSTCache_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory test in short mode")
	}

	dir := t.TempDir()
	cache, err := NewSSTCache(SSTCacheOptions{
		Dir:     dir,
		MaxSize: 200 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer cache.Close()

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	dataSize := 1024 * 1024
	numEntries := 100
	for i := range numEntries {
		key := fmt.Sprintf("key%d", i)
		data := make([]byte, dataSize)
		err := cache.Set(key, data)
		require.NoError(t, err)
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	heapGrowth := m2.HeapAlloc - m1.HeapAlloc
	require.Less(t, heapGrowth, uint64(10*1024*1024),
		"heap grew too much - data may not be off-heap")
}
