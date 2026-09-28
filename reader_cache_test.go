package isledb

import (
	"context"
	"os"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/filecache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/stretchr/testify/require"
)

// readCachedSST returns the bytes of an SST opened from the local cache and
// closes it.
func readCachedSST(t *testing.T, file *os.File, size int64) []byte {
	t.Helper()
	defer func() { _ = file.Close() }()
	data := make([]byte, size)
	_, err := file.ReadAt(data, 0)
	require.NoError(t, err)
	return data
}

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

func TestReader_cacheSST_StreamedToFileCache(t *testing.T) {
	reader, ctx, meta, data, path, cleanup := setupReaderCacheFixture(t, true)
	defer cleanup()

	err := reader.cacheSST(ctx, &meta, path)
	require.NoError(t, err)

	file, ok := reader.acquireSST(meta)
	require.True(t, ok)
	require.Equal(t, data[:meta.Size], readCachedSST(t, file, meta.Size))
	require.Equal(t, 1, reader.SSTCacheStats().EntryCount)
}

func TestReader_cacheSSTArtifact_ChecksumMismatch(t *testing.T) {
	reader, ctx, meta, _, path, cleanup := setupReaderCacheFixture(t, true)
	defer cleanup()

	_, err := reader.store.Write(ctx, path, []byte("corrupt"))
	require.NoError(t, err)

	err = reader.cacheSST(ctx, &meta, path)
	require.Error(t, err)

	_, ok := reader.acquireSST(meta)
	require.False(t, ok)
	require.Equal(t, 0, reader.SSTCacheStats().EntryCount)
}

func TestReaderFileCacheInvalidDescriptorIsMiss(t *testing.T) {
	cache, err := filecache.Open(filecache.Options{
		Dir: t.TempDir(), SSTMaxBytes: 1 << 20, BloomMaxBytes: 1 << 20,
	})
	require.NoError(t, err)
	defer cache.Close()
	reader := &Reader{fileCache: cache}

	_, ok := reader.acquireSST(sstMetadata{
		ID: "accepted-by-reader", Size: 1, Checksum: "invalid",
	})
	require.False(t, ok)
	require.False(t, reader.sstResident(sstMetadata{ID: "accepted-by-reader", Size: 1, Checksum: "invalid"}))
}

func TestReaderCachedOpenFailurePreservesCauseWhenOriginRetryFails(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("cached-open-error")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	require.NoError(t, err)
	defer reader.Close()

	data := []byte("verified but not an SST")
	meta := sstMetadata{
		ID:       "invalid-cached-sst",
		Size:     int64(len(data)),
		Checksum: bloomChecksum(data),
	}
	require.NoError(t, reader.fileCache.Put(sstFileDescriptor(meta), data))

	file, ok := reader.acquireSST(meta)
	require.True(t, ok)
	_, _, parseErr := reader.openSSTIterFromFile(ctx, meta, file, nil, nil, func() { _ = file.Close() })
	require.Error(t, parseErr)
	_, _, err = reader.openSSTIterBounded(ctx, meta, nil, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, parseErr.Error())
	// An SST that cannot be opened is dropped from the cache.
	require.False(t, reader.sstResident(meta))
}

func TestReader_OversizedSSTBypassesCacheAndServesRead(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("oversized-sst-cache-bypass")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	result := writeTestSST(t, ctx, store, manifestStore, []internal.MemEntry{
		{Key: []byte("key"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}, 0, 1)
	if result.Meta.Size <= 1 {
		t.Fatalf("test SST size=%d, want >1", result.Meta.Size)
	}

	opts := defaultReaderOptions()
	opts.CacheDir = t.TempDir()
	opts.SSTCacheSize = result.Meta.Size - 1
	reader, err := newReader(ctx, store, opts)
	require.NoError(t, err)
	defer reader.Close()

	value, found, err := reader.Get(ctx, []byte("key"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("value"), value)

	stats := reader.SSTCacheStats()
	require.Zero(t, stats.EntryCount)
	require.Zero(t, stats.Bytes)
	require.EqualValues(t, 1, stats.Bypasses)
}

// TestReader_EvictsSSTInUseAndServesRead fills a one-SST cache while the first
// SST is still open. The first is evicted, yet its open file stays readable.
func TestReader_EvictsSSTInUseAndServesRead(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("evict-sst-in-use")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	first := writeTestSST(t, ctx, store, manifestStore, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("first")},
	}, 0, 1)
	second := writeTestSST(t, ctx, store, manifestStore, []internal.MemEntry{
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Value: []byte("second")},
	}, 0, 2)

	opts := defaultReaderOptions()
	opts.CacheDir = t.TempDir()
	opts.SSTCacheSize = max(first.Meta.Size, second.Meta.Size)
	reader, err := newReader(ctx, store, opts)
	require.NoError(t, err)
	defer reader.Close()

	require.NoError(t, reader.cacheSST(ctx, &first.Meta, store.SSTPath(first.Meta.ID)))
	held, ok := reader.acquireSST(first.Meta)
	require.True(t, ok)

	value, found, err := reader.Get(ctx, []byte("b"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("second"), value)

	stats := reader.SSTCacheStats()
	require.Equal(t, 1, stats.EntryCount)
	require.Equal(t, second.Meta.Size, stats.Bytes)
	require.EqualValues(t, 1, stats.Evictions)
	require.False(t, reader.sstResident(first.Meta))

	firstBytes, _, err := store.Read(ctx, store.SSTPath(first.Meta.ID))
	require.NoError(t, err)
	require.Equal(t, firstBytes[:first.Meta.Size], readCachedSST(t, held, first.Meta.Size))
}

// TestReader_CorruptBlockInCachedSSTIsDroppedAndRefetched corrupts a data
// block of a cached SST without changing its size. Opening the SST still
// succeeds, so the corruption surfaces while reading: that read fails, the SST
// is dropped from the cache, and the next read downloads it again.
func TestReader_CorruptBlockInCachedSSTIsDroppedAndRefetched(t *testing.T) {
	reader, ctx, meta, _, path, cleanup := setupReaderCacheFixture(t, false)
	defer cleanup()
	require.NoError(t, reader.cacheSST(ctx, &meta, path))

	file, ok := reader.acquireSST(meta)
	require.True(t, ok)
	cachedPath := file.Name()
	require.NoError(t, file.Close())
	cached, err := os.ReadFile(cachedPath)
	require.NoError(t, err)
	cached[0] ^= 0xff // the first data block starts at offset 0
	require.NoError(t, os.WriteFile(cachedPath, cached, 0o600))

	_, _, err = reader.Get(ctx, []byte("a"))
	require.Error(t, err)
	require.False(t, reader.sstResident(meta))
	require.EqualValues(t, 1, reader.SSTCacheStats().Corruptions)

	value, found, err := reader.Get(ctx, []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("value"), value)
	require.True(t, reader.sstResident(meta))
}
