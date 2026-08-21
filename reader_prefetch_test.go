package isledb

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestPrefixRange(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		min    []byte
		max    []byte
	}{
		{name: "normal", prefix: []byte("user:"), min: []byte("user:"), max: []byte("user;")},
		{name: "carry", prefix: []byte{0x01, 0xff}, min: []byte{0x01, 0xff}, max: []byte{0x02}},
		{name: "all_ff", prefix: []byte{0xff}, min: []byte{0xff}, max: nil},
		{name: "empty", prefix: nil, min: nil, max: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PrefixRange(tt.prefix)
			if !bytes.Equal(got.Min, tt.min) {
				t.Fatalf("min = %v, want %v", got.Min, tt.min)
			}
			if !bytes.Equal(got.Max, tt.max) {
				t.Fatalf("max = %v, want %v", got.Max, tt.max)
			}
		})
	}
}

func TestReader_PrefetchRejectsEmptyOptions(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{})
	defer reader.Close()

	if _, err := reader.Prefetch(ctx, PrefetchOptions{}); err == nil {
		t.Fatal("Prefetch with empty options succeeded, want error")
	}
	if _, err := reader.Prefetch(ctx, PrefetchOptions{
		All:   true,
		Range: PrefixRange([]byte("user:")),
	}); err == nil {
		t.Fatal("Prefetch with All and Range succeeded, want error")
	}
}

func TestReader_PrefetchRangeAfterRefresh(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	manifestStore := newManifestStore(store, nil)
	writer := newPrefetchTestWriter(t, ctx, store, manifestStore)
	defer writer.close(ctx)

	writePrefetchBatch(t, ctx, writer, "account", 0, 3)
	writePrefetchBatch(t, ctx, writer, "user", 0, 3)
	writePrefetchBatch(t, ctx, writer, "z", 0, 3)

	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{})
	defer reader.Close()

	stats, err := reader.Prefetch(ctx, PrefetchOptions{
		Range:       PrefixRange([]byte("user:")),
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("Prefetch: %v", err)
	}
	if stats.MatchedSSTs != 1 || stats.CachedSSTs != 1 || stats.SkippedSSTs != 0 || stats.BytesRead <= 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if got := reader.SSTCacheStats().EntryCount; got != 1 {
		t.Fatalf("cache entries = %d, want 1", got)
	}

	val, found, err := reader.Get(ctx, []byte("user:001"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(val) != "user:value:001" {
		t.Fatalf("Get user:001 = %q, %v", val, found)
	}
}

func TestReader_PrefetchDoesNotRefresh(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	manifestStore := newManifestStore(store, nil)

	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{})
	defer reader.Close()

	writer := newPrefetchTestWriter(t, ctx, store, manifestStore)
	defer writer.close(ctx)
	writePrefetchBatch(t, ctx, writer, "user", 0, 3)

	stats, err := reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil {
		t.Fatalf("Prefetch before Refresh: %v", err)
	}
	if stats != (PrefetchStats{}) {
		t.Fatalf("Prefetch before Refresh stats = %+v, want zero", stats)
	}

	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	stats, err = reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil {
		t.Fatalf("Prefetch after Refresh: %v", err)
	}
	if stats.MatchedSSTs != 1 || stats.CachedSSTs != 1 {
		t.Fatalf("Prefetch after Refresh stats = %+v, want one cached SST", stats)
	}
}

func TestReader_PrefetchAllAndSkipCached(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	manifestStore := newManifestStore(store, nil)
	writer := newPrefetchTestWriter(t, ctx, store, manifestStore)
	defer writer.close(ctx)

	writePrefetchBatch(t, ctx, writer, "a", 0, 2)
	writePrefetchBatch(t, ctx, writer, "b", 0, 2)
	writePrefetchBatch(t, ctx, writer, "c", 0, 2)

	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{})
	defer reader.Close()

	stats, err := reader.Prefetch(ctx, PrefetchOptions{All: true, Concurrency: 2})
	if err != nil {
		t.Fatalf("Prefetch first: %v", err)
	}
	if stats.MatchedSSTs != 3 || stats.CachedSSTs != 3 || stats.SkippedSSTs != 0 {
		t.Fatalf("first stats = %+v, want three cached", stats)
	}

	stats, err = reader.Prefetch(ctx, PrefetchOptions{All: true, Concurrency: 2})
	if err != nil {
		t.Fatalf("Prefetch second: %v", err)
	}
	if stats.MatchedSSTs != 3 || stats.CachedSSTs != 0 || stats.SkippedSSTs != 3 {
		t.Fatalf("second stats = %+v, want three skipped", stats)
	}
}

func TestReader_PrefetchOversizedSSTReportsBypass(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("prefetch-oversized-bypass")
	manifestStore := newManifestStore(store, nil)
	writer := newPrefetchTestWriter(t, ctx, store, manifestStore)
	defer writer.close(ctx)
	writePrefetchBatch(t, ctx, writer, "oversized", 0, 3)

	manifest, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.L0SSTs) != 1 || manifest.L0SSTs[0].Size <= 1 {
		t.Fatalf("unexpected test manifest: %+v", manifest.L0SSTs)
	}
	sstSize := manifest.L0SSTs[0].Size
	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{
		SSTCacheSize: sstSize - 1,
	})
	defer reader.Close()

	stats, err := reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.MatchedSSTs != 1 || stats.CachedSSTs != 0 || stats.BytesRead != sstSize {
		t.Fatalf("oversized prefetch stats=%+v", stats)
	}
	cacheStats := reader.SSTCacheStats()
	if cacheStats.EntryCount != 0 || cacheStats.Bytes != 0 || cacheStats.AdmissionBypasses != 1 {
		t.Fatalf("oversized cache stats=%+v", cacheStats)
	}
}

func TestReader_PrefetchRespectsMaxSSTs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	manifestStore := newManifestStore(store, nil)
	writer := newPrefetchTestWriter(t, ctx, store, manifestStore)
	defer writer.close(ctx)

	writePrefetchBatch(t, ctx, writer, "a", 0, 2)
	writePrefetchBatch(t, ctx, writer, "b", 0, 2)
	writePrefetchBatch(t, ctx, writer, "c", 0, 2)

	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{})
	defer reader.Close()

	stats, err := reader.Prefetch(ctx, PrefetchOptions{All: true, MaxSSTs: 2})
	if err != nil {
		t.Fatalf("Prefetch: %v", err)
	}
	if stats.MatchedSSTs != 3 || stats.CachedSSTs != 2 || stats.SkippedSSTs != 1 {
		t.Fatalf("stats = %+v, want matched=3 cached=2 skipped=1", stats)
	}
	if got := reader.SSTCacheStats().EntryCount; got != 2 {
		t.Fatalf("cache entries = %d, want 2", got)
	}
}

func TestReader_PrefetchByteBudgetSkipsUnknownSize(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	manifestStore := newManifestStore(store, nil)
	writer := newPrefetchTestWriter(t, ctx, store, manifestStore)
	defer writer.close(ctx)

	writePrefetchBatch(t, ctx, writer, "unknown-size", 0, 2)
	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{})
	defer reader.Close()

	reader.mu.Lock()
	if len(reader.manifest.L0SSTs) != 1 {
		reader.mu.Unlock()
		t.Fatalf("L0 SST count = %d, want 1", len(reader.manifest.L0SSTs))
	}
	reader.manifest.L0SSTs[0].Size = 0
	reader.mu.Unlock()

	stats, err := reader.Prefetch(ctx, PrefetchOptions{All: true, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Prefetch: %v", err)
	}
	if stats.MatchedSSTs != 1 || stats.CachedSSTs != 0 || stats.SkippedSSTs != 1 || stats.BytesRead != 0 {
		t.Fatalf("stats = %+v, want unknown-size SST skipped", stats)
	}
}

func TestReader_PrefetchValidatesChecksum(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	manifestStore := newManifestStore(store, nil)
	writer := newPrefetchTestWriter(t, ctx, store, manifestStore)
	defer writer.close(ctx)

	writePrefetchBatch(t, ctx, writer, "user", 0, 3)

	m, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(m.L0SSTs) != 1 {
		t.Fatalf("L0 SST count = %d, want 1", len(m.L0SSTs))
	}
	path := store.SSTPath(m.L0SSTs[0].ID)
	data, _, err := store.Read(ctx, path)
	if err != nil {
		t.Fatalf("Read SST: %v", err)
	}
	data[0] ^= 0xff
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("Write corrupted SST: %v", err)
	}

	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{
		ValidateSSTChecksum: true,
	})
	defer reader.Close()

	stats, err := reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err == nil {
		t.Fatal("Prefetch succeeded, want checksum error")
	}
	if stats.MatchedSSTs != 1 || stats.CachedSSTs != 0 {
		t.Fatalf("stats after error = %+v, want matched=1 cached=0", stats)
	}
	if resident, probeErr := reader.sstResident(m.L0SSTs[0]); probeErr != nil {
		t.Fatal(probeErr)
	} else if resident {
		t.Fatal("corrupted SST was cached")
	}
}

func TestReader_TruncatedSSTCacheSelfHealsAfterOriginRecovers(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	writer, err := newWriterWithMaintenanceWake(ctx, store, manifestStore, opts, nil,
		StorePolicy{MaxPinnedViewAge: DefaultMaxPinnedViewAge},
		SSTEncodingOptions{Compression: "none", BlockBytes: 4096, BloomBitsPerKey: 0})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.put(ctx, []byte("key"), bytes.Repeat([]byte("v"), 4096)); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := writer.close(ctx); err != nil {
		t.Fatal(err)
	}
	m := replayManifestForTest(t, ctx, store)
	if len(m.L0SSTs) != 1 {
		t.Fatalf("L0 SST count=%d, want 1", len(m.L0SSTs))
	}
	path := store.SSTPath(m.L0SSTs[0].ID)
	valid, _, err := store.Read(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Write(ctx, path, valid[:len(valid)/2]); err != nil {
		t.Fatal(err)
	}

	ropts := defaultReaderOptions()
	ropts.CacheDir = t.TempDir()
	reader, err := newReader(ctx, store, ropts)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, _, err := reader.Get(ctx, []byte("key")); err == nil {
		t.Fatal("first read of truncated SST unexpectedly succeeded")
	}
	if got := reader.SSTCacheStats().EntryCount; got != 0 {
		t.Fatalf("truncated SST was retained in cache; entries=%d", got)
	}
	if _, err := store.Write(ctx, path, valid); err != nil {
		t.Fatal(err)
	}
	value, found, err := reader.Get(ctx, []byte("key"))
	if err != nil || !found || len(value) != 4096 {
		t.Fatalf("reader did not evict and redownload poisoned SST: found=%t bytes=%d err=%v",
			found, len(value), err)
	}
}

func TestReader_EvictsInvalidCachedSSTAndRedownloads(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	writer, err := newWriterWithMaintenanceWake(ctx, store, manifestStore, opts, nil,
		StorePolicy{MaxPinnedViewAge: DefaultMaxPinnedViewAge},
		SSTEncodingOptions{Compression: "none", BlockBytes: 4096, BloomBitsPerKey: 0})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.put(ctx, []byte("key"), bytes.Repeat([]byte("v"), 4096)); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := writer.close(ctx); err != nil {
		t.Fatal(err)
	}
	m := replayManifestForTest(t, ctx, store)
	if len(m.L0SSTs) != 1 {
		t.Fatalf("L0 SST count=%d, want 1", len(m.L0SSTs))
	}
	path := store.SSTPath(m.L0SSTs[0].ID)
	valid, _, err := store.Read(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	ropts := defaultReaderOptions()
	ropts.CacheDir = t.TempDir()
	reader, err := newReader(ctx, store, ropts)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	truncated := valid[:len(valid)/2]
	handle, _, err := reader.artifactCache.AdmitBytes(diskcache.ArtifactDescriptor{
		Key: diskcache.ArtifactKey{
			Kind:  diskcache.ArtifactSST,
			SSTID: m.L0SSTs[0].ID,
		},
		Size:     int64(len(truncated)),
		Checksum: bloomChecksum(truncated),
	}, truncated)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}

	value, found, err := reader.Get(ctx, []byte("key"))
	if err != nil || !found || len(value) != 4096 {
		t.Fatalf("reader did not replace invalid cached SST: found=%t bytes=%d err=%v",
			found, len(value), err)
	}
}

func newPrefetchTestWriter(t *testing.T, ctx context.Context, store *blobstore.Store, manifestStore *manifest.Store) *writer {
	t.Helper()

	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	return w
}

func newPrefetchTestReader(t *testing.T, ctx context.Context, store *blobstore.Store, opts ReaderOpenOptions) *Reader {
	t.Helper()

	if opts.CacheDir == "" {
		opts.CacheDir = t.TempDir()
	}
	return openReaderFromDBForTest(t, ctx, store, opts)
}

func writePrefetchBatch(t *testing.T, ctx context.Context, w *writer, prefix string, start, count int) {
	t.Helper()

	for i := start; i < start+count; i++ {
		key := fmt.Sprintf("%s:%03d", prefix, i)
		value := fmt.Sprintf("%s:value:%03d", prefix, i)
		if err := w.put(ctx, []byte(key), []byte(value)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush %s: %v", prefix, err)
	}
}
