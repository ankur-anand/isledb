package isledb

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
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
	if got := reader.DiskCacheStats().Data.EntryCount; got != 1 {
		t.Fatalf("data tier entries = %d, want 1", got)
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

// TestReader_PrefetchCachedSSTFetchesNothing prefetches an SST already on
// disk: nothing is fetched.
func TestReader_PrefetchCachedSSTFetchesNothing(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("prefetch-cached")
	manifestStore := newManifestStore(store, nil)
	writer := newPrefetchTestWriter(t, ctx, store, manifestStore)
	defer writer.close(ctx)
	writePrefetchBatch(t, ctx, writer, "cached", 0, 3)
	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{})
	defer reader.Close()
	meta := reader.currentManifest().L0SSTs[0]

	if fetched, err := reader.prefetchSST(ctx, meta); err != nil || fetched == 0 {
		t.Fatalf("first prefetch fetched=%d err=%v, want the SST", fetched, err)
	}
	if fetched, err := reader.prefetchSST(ctx, meta); err != nil || fetched != 0 {
		t.Fatalf("second prefetch fetched=%d err=%v, want nothing", fetched, err)
	}
}

// TestReader_PrefetchSkipsSSTLargerThanBudget selects nothing that could not
// stay on disk: an SST larger than the data tier is skipped, not fetched.
func TestReader_PrefetchSkipsSSTLargerThanBudget(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("prefetch-oversized")
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
	// The data tier is seven eighths of the budget: smaller than the SST.
	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{
		DiskCacheSize: manifest.L0SSTs[0].Size,
	})
	defer reader.Close()

	stats, err := reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.MatchedSSTs != 1 || stats.SkippedSSTs != 1 || stats.CachedSSTs != 0 || stats.BytesRead != 0 {
		t.Fatalf("oversized prefetch stats=%+v, want it skipped unfetched", stats)
	}
	if data := reader.DiskCacheStats().Data; data.EntryCount != 0 || data.Bypasses != 0 {
		t.Fatalf("oversized data tier stats=%+v", data)
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
	if got := reader.DiskCacheStats().Data.EntryCount; got != 2 {
		t.Fatalf("data tier entries = %d, want 2", got)
	}
}

// TestReader_PrefetchTierBudgetCountsCachedSSTs repeats a prefetch of more
// than the disk cache holds: SSTs the first one cached count against it, so
// the second fetches nothing rather than evicting them.
func TestReader_PrefetchTierBudgetCountsCachedSSTs(t *testing.T) {
	ctx := context.Background()
	store := newPrefetchBudgetTestStore(t, ctx, "prefetch-tier-budget")
	sizes := prefetchTestSSTSizes(t, ctx, store)
	// A disk cache with room for any two of the three SSTs, not all three.
	var total, smallest int64 = 0, sizes[0]
	for _, size := range sizes {
		total += size
		smallest = min(smallest, size)
	}
	cacheDir := t.TempDir()
	disk, err := diskcache.Open(diskcache.Options{
		Dir: filepath.Join(cacheDir, "artifacts"), MaxBytes: total - smallest,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir, DiskCache: disk})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	first, err := reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil {
		t.Fatalf("Prefetch first: %v", err)
	}
	if first.CachedSSTs != 2 {
		t.Fatalf("first stats = %+v, want two cached", first)
	}
	second, err := reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil {
		t.Fatalf("Prefetch second: %v", err)
	}
	if second.CachedSSTs != 0 || second.BytesRead != 0 || second.SkippedSSTs != 3 {
		t.Fatalf("second stats = %+v, want nothing fetched", second)
	}
}

// TestReader_PrefetchMaxBytesBoundsEachCall warms SSTs in steps with a
// MaxBytes that fits one SST's download, Bloom filter included: SSTs already
// cached do not count against it, so each call downloads one more.
func TestReader_PrefetchMaxBytesBoundsEachCall(t *testing.T) {
	ctx := context.Background()
	store := newPrefetchBudgetTestStore(t, ctx, "prefetch-max-bytes-steps")
	sizes := prefetchTestSSTSizes(t, ctx, store)
	opts := PrefetchOptions{All: true, MaxBytes: max(sizes[0], sizes[1], sizes[2])}
	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{})
	defer reader.Close()

	for call := 1; call <= 3; call++ {
		stats, err := reader.Prefetch(ctx, opts)
		if err != nil {
			t.Fatalf("Prefetch %d: %v", call, err)
		}
		if stats.CachedSSTs != 1 {
			t.Fatalf("call %d stats = %+v, want one more SST cached", call, stats)
		}
	}
}

// newPrefetchBudgetTestStore writes three single-batch L0 SSTs.
func newPrefetchBudgetTestStore(t *testing.T, ctx context.Context, name string) *blobstore.Store {
	t.Helper()
	store := blobstore.NewMemory(name)
	writer := newPrefetchTestWriter(t, ctx, store, newManifestStore(store, nil))
	writePrefetchBatch(t, ctx, writer, "a", 0, 2)
	writePrefetchBatch(t, ctx, writer, "b", 0, 2)
	writePrefetchBatch(t, ctx, writer, "c", 0, 2)
	writer.close(ctx)
	return store
}

// prefetchTestSSTSizes returns what each SST takes in the disk cache, its
// Bloom filter included.
func prefetchTestSSTSizes(t *testing.T, ctx context.Context, store *blobstore.Store) []int64 {
	t.Helper()
	m, err := newManifestStore(store, nil).Replay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.L0SSTs) != 3 {
		t.Fatalf("L0 SST count = %d, want 3", len(m.L0SSTs))
	}
	sizes := make([]int64, len(m.L0SSTs))
	for i, sst := range m.L0SSTs {
		sizes[i] = sst.Size + sst.Bloom.Length
	}
	return sizes
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

	reader := newPrefetchTestReader(t, ctx, store, ReaderOpenOptions{})
	defer reader.Close()

	stats, err := reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err == nil {
		t.Fatal("Prefetch succeeded, want checksum error")
	}
	if stats.MatchedSSTs != 1 || stats.CachedSSTs != 0 {
		t.Fatalf("stats after error = %+v, want matched=1 cached=0", stats)
	}
	if reader.fetcher.resident(reader.fetcher.object(m.L0SSTs[0])) {
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
	if _, err := writer.put(ctx, []byte("key"), bytes.Repeat([]byte("v"), 4096)); err != nil {
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
	if got := reader.DiskCacheStats().Data.EntryCount; got != 0 {
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
	if _, err := writer.put(ctx, []byte("key"), bytes.Repeat([]byte("v"), 4096)); err != nil {
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
	// A cached copy of the wrong size, as a crash mid-write leaves.
	whole := reader.fetcher.object(m.L0SSTs[0]).entry(diskcache.KindWhole, 0)
	if err := reader.diskCache.Put(whole, valid[:len(valid)/2]); err != nil {
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
		if _, err := w.put(ctx, []byte(key), []byte(value)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush %s: %v", prefix, err)
	}
}

// TestPruneDiskCacheMakesRoomForPrefetch reopens on a disk cache full of SSTs
// no view names, as a restart after compactions leaves it: a prefetch, using
// free space only, caches nothing until PruneDiskCache deletes them.
func TestPruneDiskCacheMakesRoomForPrefetch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("prune-disk-cache")
	t.Cleanup(func() { _ = store.Close() })
	ms := manifest.NewStore(store)
	live := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("1")},
	}, 0, 1).Meta
	cache, err := diskcache.Open(diskcache.Options{Dir: t.TempDir(), MaxBytes: live.Size})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	var dead []diskcache.Key
	for i := range uint32(4) {
		k := diskcache.Key{Object: [32]byte{0xde, 0xad}, Kind: diskcache.KindChunk, Index: i}
		if err := cache.Put(k, make([]byte, live.Size/4)); err != nil {
			t.Fatal(err)
		}
		dead = append(dead, k)
	}
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir(), DiskCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	stats, err := reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil || stats.CachedSSTs != 0 {
		t.Fatalf("prefetch into a cache full of dead SSTs: %+v, %v; want nothing cached", stats, err)
	}
	removed, err := reader.PruneDiskCache(ctx)
	if err != nil || removed != len(dead) {
		t.Fatalf("PruneDiskCache = %d, %v; want %d", removed, err, len(dead))
	}
	for _, k := range dead {
		if cache.Contains(k, live.Size/4) {
			t.Fatalf("dead entry %v survived the prune", k)
		}
	}
	stats, err = reader.Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil || stats.CachedSSTs != 1 {
		t.Fatalf("prefetch after the prune: %+v, %v; want the live SST cached", stats, err)
	}
	if removed, err := reader.PruneDiskCache(ctx); err != nil || removed != 0 {
		t.Fatalf("second prune = %d, %v; want the live SST kept", removed, err)
	}
}
