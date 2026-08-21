package isledb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestReaderArtifactCachePersistsSSTAndBloomAcrossReopen(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-artifact-persistence")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	result := writeReaderArtifactCacheTestSST(t, ctx, store, manifestStore, []internal.MemEntry{
		{Key: []byte("key"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}, 1)

	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	value, found, err := reader.Get(ctx, []byte("key"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("initial Get value=%q found=%t err=%v", value, found, err)
	}
	if got := reader.SSTCacheStats().EntryCount; got != 1 {
		t.Fatalf("SST disk entries=%d want=1", got)
	}
	if got := reader.BloomDiskCacheStats().EntryCount; got != 1 {
		t.Fatalf("Bloom disk entries=%d want=1", got)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(ctx, store.SSTPath(result.Meta.ID)); err != nil {
		t.Fatal(err)
	}
	reopened, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	contains, err := reopened.bloomMayContain(ctx, result.Meta, []byte("key"))
	if err != nil || !contains {
		t.Fatalf("recovered Bloom contains=%t err=%v", contains, err)
	}
	value, found, err = reopened.Get(ctx, []byte("key"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("recovered Get value=%q found=%t err=%v", value, found, err)
	}
	if stats := reopened.SSTCacheStats(); stats.EntryCount != 1 || stats.Hits == 0 {
		t.Fatalf("recovered SST stats=%+v", stats)
	}
	if stats := reopened.BloomDiskCacheStats(); stats.EntryCount != 1 || stats.Hits == 0 {
		t.Fatalf("recovered Bloom stats=%+v", stats)
	}
}

func TestReaderArtifactCacheCorruptionSelfHealsFromOrigin(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-artifact-corruption")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	result := writeReaderArtifactCacheTestSST(t, ctx, store, manifestStore, []internal.MemEntry{
		{Key: []byte("key"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}, 1)

	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := reader.Get(ctx, []byte("key")); err != nil || !found {
		t.Fatalf("prime Get found=%t err=%v", found, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	corruptSingleArtifactFile(t, filepath.Join(cacheDir, "artifacts", "v1", "sst", "*", "*.sst"))
	corruptSingleArtifactFile(t, filepath.Join(cacheDir, "artifacts", "v1", "bloom", "*", "*.bloom"))

	reopened, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if contains, err := reopened.bloomMayContain(ctx, result.Meta, []byte("key")); err != nil || !contains {
		t.Fatalf("self-healed Bloom contains=%t err=%v", contains, err)
	}
	value, found, err := reopened.Get(ctx, []byte("key"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("self-healed Get value=%q found=%t err=%v", value, found, err)
	}
	if stats := reopened.SSTCacheStats(); stats.Corruptions != 1 {
		t.Fatalf("SST corruption stats=%+v", stats)
	}
	if stats := reopened.BloomDiskCacheStats(); stats.Corruptions != 1 {
		t.Fatalf("Bloom corruption stats=%+v", stats)
	}
}

func TestReaderArtifactCacheExclusivelyLocksCacheDirectory(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-artifact-lock")
	defer store.Close()
	cacheDir := t.TempDir()

	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir}); !errors.Is(err, diskcache.ErrArtifactCacheLocked) {
		t.Fatalf("second Reader error=%v want=%v", err, diskcache.ErrArtifactCacheLocked)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("reopen after owner close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func corruptSingleArtifactFile(t *testing.T, pattern string) {
	t.Helper()
	paths, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("artifact matches for %q=%v want one", pattern, paths)
	}
	data, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatalf("empty artifact %s", paths[0])
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(paths[0], data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeReaderArtifactCacheTestSST(
	t *testing.T,
	ctx context.Context,
	store *blobstore.Store,
	manifestStore *manifest.Store,
	entries []internal.MemEntry,
	epoch uint64,
) writeSSTResult {
	t.Helper()
	if manifestStore.WriterEpoch() == 0 {
		if _, err := manifestStore.ClaimWriter(ctx, "reader-artifact-cache-writer"); err != nil {
			t.Fatal(err)
		}
	}
	result, err := writeSST(ctx, &sliceSSTIter{entries: entries}, sstWriterOptions{
		BlockSize: 4096, Compression: "none", BloomBitsPerKey: 10,
	}, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
		t.Fatal(err)
	}
	if _, err := manifestStore.AppendAddSSTableWithFence(ctx, result.Meta); err != nil {
		t.Fatal(err)
	}
	return result
}
