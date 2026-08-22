package isledb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestReaderDiagnosticLimiterBoundsLogsAndReportsSuppression(t *testing.T) {
	var limiter readerDiagnosticLimiter
	now := time.Unix(1_700_000_000, 0)
	if allowed, suppressed := limiter.allow(now); !allowed || suppressed != 0 {
		t.Fatalf("first allow=(%t,%d), want (true,0)", allowed, suppressed)
	}
	if allowed, _ := limiter.allow(now.Add(time.Second)); allowed {
		t.Fatal("diagnostic inside interval was allowed")
	}
	if allowed, _ := limiter.allow(now.Add(2 * time.Second)); allowed {
		t.Fatal("second diagnostic inside interval was allowed")
	}
	if allowed, suppressed := limiter.allow(now.Add(readerDiagnosticLogInterval)); !allowed || suppressed != 2 {
		t.Fatalf("next allow=(%t,%d), want (true,2)", allowed, suppressed)
	}

	var concurrent readerDiagnosticLimiter
	var allowed atomic.Int64
	const callers = 32
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			if ok, _ := concurrent.allow(now); ok {
				allowed.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := allowed.Load(); got != 1 {
		t.Fatalf("concurrent allowed=%d, want 1", got)
	}
	if ok, suppressed := concurrent.allow(now.Add(readerDiagnosticLogInterval)); !ok || suppressed != callers-1 {
		t.Fatalf("concurrent suppression=(%t,%d), want (true,%d)",
			ok, suppressed, callers-1)
	}
}

func TestReaderCacheStatsExposeArtifactFailureCounters(t *testing.T) {
	stats := cacheStatsFromArtifact(diskcache.ArtifactStats{
		SyncFailures: 2, PublicationFailures: 3,
	})
	if stats.SyncFailures != 2 || stats.PublicationFailures != 3 {
		t.Fatalf("reader cache stats=%+v", stats)
	}
}

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

	contains := reopened.bloomMayContain(ctx, result.Meta, []byte("key"))
	if !contains {
		t.Fatal("recovered Bloom returned definitely absent")
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
	if contains := reopened.bloomMayContain(ctx, result.Meta, []byte("key")); !contains {
		t.Fatal("self-healed Bloom returned definitely absent")
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

func TestReaderCorruptOriginBloomFallsThroughToValidSST(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-origin-bloom-fail-open")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	result := writeReaderArtifactCacheTestSST(t, ctx, store, manifestStore, []internal.MemEntry{
		{Key: []byte("key"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}, 1)

	corrupt := append([]byte(nil), result.SSTData...)
	corrupt[result.Meta.Bloom.Offset] ^= 0xff
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), corrupt); err != nil {
		t.Fatal(err)
	}
	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(), Metrics: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	value, found, err := reader.Get(ctx, []byte("key"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("Get through corrupt Bloom value=%q found=%t err=%v", value, found, err)
	}
	if got := testutil.ToFloat64(metrics.BloomFilterErrors); got != 1 {
		t.Fatalf("Bloom errors=%v, want 1", got)
	}
	if stats := reader.BloomCacheStats(); stats.EntryCount != 0 {
		t.Fatalf("corrupt Bloom entered decoded cache: %+v", stats)
	}
}

func TestPinnedSnapshotReadsRetiredArtifactsWithoutOrigin(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-retired-artifact-snapshot")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	result := writeReaderArtifactCacheTestSST(t, ctx, store, manifestStore, []internal.MemEntry{
		{Key: []byte("key"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}, 1)

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if value, found, err := reader.Get(ctx, []byte("key")); err != nil || !found || string(value) != "value" {
		t.Fatalf("prime Get value=%q found=%t err=%v", value, found, err)
	}
	snapshot, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	reader.publishManifestView(&manifestState{}, &manifest.Current{
		MaxPinnedViewAge: time.Hour,
	}, time.Now())
	if err := reader.clearBloomDiskCache(); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, store.SSTPath(result.Meta.ID)); err != nil {
		t.Fatal(err)
	}

	value, found, err := snapshot.Get(ctx, []byte("key"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("retired snapshot Get value=%q found=%t err=%v", value, found, err)
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

func TestReaderArtifactCacheRemovesLegacySSTCacheOnUpgrade(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-artifact-legacy-cleanup")
	defer store.Close()
	cacheDir := t.TempDir()
	legacyDir := filepath.Join(cacheDir, "sst")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "sst-orphan"), []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}

	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := os.Stat(legacyDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy SST cache survived upgrade: %v", err)
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
