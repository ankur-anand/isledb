package isledb

import (
	"context"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
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

func TestReader_cacheSST_StreamedToArtifactCache(t *testing.T) {
	reader, ctx, meta, data, path, cleanup := setupReaderCacheFixture(t, true)
	defer cleanup()

	err := reader.cacheSST(ctx, &meta, path)
	require.NoError(t, err)

	got, release, ok, err := reader.acquireSST(meta)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, data, got)
	release()
	require.Equal(t, 1, reader.SSTCacheStats().EntryCount)
}

func TestReader_cacheSSTArtifact_ChecksumMismatch(t *testing.T) {
	reader, ctx, meta, _, path, cleanup := setupReaderCacheFixture(t, true)
	defer cleanup()

	_, err := reader.store.Write(ctx, path, []byte("corrupt"))
	require.NoError(t, err)

	err = reader.cacheSST(ctx, &meta, path)
	require.Error(t, err)

	_, _, ok, acquireErr := reader.acquireSST(meta)
	require.NoError(t, acquireErr)
	require.False(t, ok)
	require.Equal(t, 0, reader.SSTCacheStats().EntryCount)
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
	handle, _, err := reader.artifactCache.AdmitBytes(sstArtifactDescriptor(meta), data)
	require.NoError(t, err)
	require.NoError(t, handle.Close())

	_, _, parseErr := reader.openSSTIterFromData(ctx, meta, data, nil, nil, nil)
	require.Error(t, parseErr)
	_, _, err = reader.openSSTIterBounded(ctx, meta, nil, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, parseErr.Error())
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
	require.EqualValues(t, 1, stats.AdmissionBypasses)
}

func TestReader_PinnedCapacityBypassServesRead(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("pinned-sst-cache-bypass")
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
	_, releaseFirst, ok, err := reader.acquireSST(first.Meta)
	require.NoError(t, err)
	require.True(t, ok)
	defer releaseFirst()

	value, found, err := reader.Get(ctx, []byte("b"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("second"), value)

	stats := reader.SSTCacheStats()
	require.Equal(t, 1, stats.EntryCount)
	require.Equal(t, first.Meta.Size, stats.Bytes)
	require.Equal(t, first.Meta.Size, stats.PinnedBytes)
	require.EqualValues(t, 1, stats.AdmissionBypasses)
}
