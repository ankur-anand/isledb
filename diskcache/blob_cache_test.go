package diskcache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlobCache_MaxItemSize(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewBlobCache(BlobCacheOptions{
		Dir:         dir,
		MaxSize:     1024 * 1024,
		MaxItemSize: 100,
	})
	require.NoError(t, err)
	defer cache.Close()

	smallData := make([]byte, 50)
	err = cache.Set("small", smallData)
	require.NoError(t, err)

	got, ok := cache.Get("small")
	require.True(t, ok)
	require.Equal(t, smallData, got)

	largeData := make([]byte, 200)
	err = cache.Set("large", largeData)
	require.NoError(t, err)

	_, ok = cache.Get("large")
	require.False(t, ok, "large item should not be cached")

	stats := cache.Stats()
	require.Equal(t, 1, stats.EntryCount)
	require.Equal(t, int64(50), stats.Size)
}

func TestBlobCache_ReturnsCopy(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewBlobCache(BlobCacheOptions{Dir: dir})
	require.NoError(t, err)
	defer cache.Close()

	original := []byte("original data")
	err = cache.Set("key", original)
	require.NoError(t, err)

	got, ok := cache.Get("key")
	require.True(t, ok)
	require.Equal(t, original, got)

	got[0] = 'X'

	got2, ok := cache.Get("key")
	require.True(t, ok)
	require.Equal(t, original, got2, "BlobCache should return copies, not shared data")
}
