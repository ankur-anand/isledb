package isledb

import (
	"context"
	"os"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/stretchr/testify/require"
)

func setupReaderCacheFixture(t *testing.T, validate bool) (*Reader, context.Context, sstMetadata, []byte, string, func()) {
	t.Helper()

	ctx := context.Background()
	store := blobstore.NewMemory("cache-test")
	ms := manifest.NewStore(store)

	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}
	res := writeTestSST(t, ctx, store, ms, entries, 0, 1)

	opts := defaultReaderOptions()
	opts.CacheDir = t.TempDir()
	opts.ValidateSSTChecksum = validate

	reader, err := newReader(ctx, store, opts)
	require.NoError(t, err)

	cleanup := func() {
		_ = reader.Close()
		_ = store.Close()
	}

	return reader, ctx, res.Meta, res.SSTData, store.SSTPath(res.Meta.ID), cleanup
}

func TestReader_cacheSST_StreamedToFileBackedCache(t *testing.T) {
	reader, ctx, meta, data, path, cleanup := setupReaderCacheFixture(t, true)
	defer cleanup()

	err := reader.cacheSST(ctx, &meta, path)
	require.NoError(t, err)

	got, ok := reader.sstCache.Acquire(path)
	require.True(t, ok)
	require.Equal(t, data, got)
	reader.sstCache.Release(path)

	fb, ok := reader.sstCache.(diskcache.FileBackedCache)
	require.True(t, ok)

	entries, err := os.ReadDir(fb.CacheDir())
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestReader_cacheSSTStream_ChecksumMismatch(t *testing.T) {
	reader, ctx, meta, _, path, cleanup := setupReaderCacheFixture(t, true)
	defer cleanup()

	_, err := reader.store.Write(ctx, path, []byte("corrupt"))
	require.NoError(t, err)

	err = reader.cacheSST(ctx, &meta, path)
	require.Error(t, err)

	_, ok := reader.sstCache.Acquire(path)
	require.False(t, ok)

	fb, ok := reader.sstCache.(diskcache.FileBackedCache)
	require.True(t, ok)

	entries, err := os.ReadDir(fb.CacheDir())
	require.NoError(t, err)
	require.Len(t, entries, 0)
}
