package isledb

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestReader_RangeRead_UsesBlockCacheForLargeSST(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := blobstore.NewMemory("range-read")
	ms := manifest.NewStore(store)
	t.Cleanup(func() { _ = store.Close() })

	value := bytes.Repeat([]byte("v"), 2048)
	entries := make([]internal.MemEntry, 0, 200)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%06d", i)
		entries = append(entries, internal.MemEntry{
			Key:   []byte(key),
			Value: value,
			Kind:  internal.OpPut,
			Seq:   uint64(i + 1),
		})
	}

	res := writeTestSST(t, ctx, store, ms, entries, 0, 1)
	if res.Meta.Size <= 32<<10 {
		t.Fatalf("expected large SST, got size %d", res.Meta.Size)
	}

	opts := readerOptions{
		CacheDir:            t.TempDir(),
		RangeRead:           true,
		BlockCacheSize:      1 << 20,
		RangeReadMinSSTSize: 32 << 10,
	}
	reader, err := newReader(ctx, store, opts)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	if reader.blockCache == nil {
		t.Fatalf("expected block cache to be initialized")
	}

	beforeSSTEntries := reader.SSTCacheStats().EntryCount

	got, found, err := reader.Get(ctx, []byte("key-000100"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || len(got) == 0 {
		t.Fatalf("expected value for key")
	}

	reader.blockCache.Wait()
	afterSSTEntries := reader.SSTCacheStats().EntryCount

	if afterSSTEntries != beforeSSTEntries {
		t.Fatalf("expected no SST cache entries, got %d -> %d", beforeSSTEntries, afterSSTEntries)
	}

	cached := cachedDataBlocks(t, reader, store, res.Meta.ID)
	for i, h := range cached {
		if i >= 5 {
			t.Logf("cached data block offsets: (showing first 5 of %d)", len(cached))
			break
		}
		t.Logf("cached data block offset=%d length=%d", h.Offset, h.Length)
	}
	if len(cached) == 0 {
		t.Fatalf("expected at least one data block cached")
	}

	if err := store.Delete(ctx, store.SSTPath(res.Meta.ID)); err != nil {
		t.Fatalf("delete sst: %v", err)
	}

	got2, found, err := reader.Get(ctx, []byte("key-000100"))
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if !found || len(got2) == 0 {
		t.Fatalf("expected value after delete")
	}
}

func TestReader_RangeRead_MetricsSeparateFromDownload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := blobstore.NewMemory("range-read-metrics")
	ms := manifest.NewStore(store)
	t.Cleanup(func() { _ = store.Close() })

	value := bytes.Repeat([]byte("v"), 2048)
	entries := make([]internal.MemEntry, 0, 200)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%06d", i)
		entries = append(entries, internal.MemEntry{
			Key:   []byte(key),
			Value: value,
			Kind:  internal.OpPut,
			Seq:   uint64(i + 1),
		})
	}

	res := writeTestSST(t, ctx, store, ms, entries, 0, 1)
	if res.Meta.Size <= 32<<10 {
		t.Fatalf("expected large SST, got size %d", res.Meta.Size)
	}

	metrics := DefaultReaderMetrics(nil)
	opts := readerOptions{
		CacheDir:            t.TempDir(),
		Metrics:             metrics,
		RangeRead:           true,
		BlockCacheSize:      1 << 20,
		RangeReadMinSSTSize: 32 << 10,
	}
	reader, err := newReader(ctx, store, opts)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	if _, found, err := reader.Get(ctx, []byte("key-000100")); err != nil || !found {
		t.Fatalf("Get #1 failed: found=%v err=%v", found, err)
	}
	reader.blockCache.Wait()
	if _, found, err := reader.Get(ctx, []byte("key-000100")); err != nil || !found {
		t.Fatalf("Get #2 failed: found=%v err=%v", found, err)
	}

	if got := testutil.ToFloat64(metrics.SSTDownloadTotal); got != 0 {
		t.Fatalf("sst_download_total mismatch: got=%v want=0", got)
	}
	if got := testutil.ToFloat64(metrics.SSTDownloadBytes); got != 0 {
		t.Fatalf("sst_download_bytes_total mismatch: got=%v want=0", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadTotal); got <= 0 {
		t.Fatalf("sst_range_read_total must be > 0, got=%v", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadErrors); got != 0 {
		t.Fatalf("sst_range_read_errors_total mismatch: got=%v want=0", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadBytes); got <= 0 {
		t.Fatalf("sst_range_read_bytes_total must be > 0, got=%v", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeBlockCacheMisses); got <= 0 {
		t.Fatalf("sst_range_block_cache_misses_total must be > 0, got=%v", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeBlockCacheHits); got <= 0 {
		t.Fatalf("sst_range_block_cache_hits_total must be > 0, got=%v", got)
	}
}

func cachedDataBlocks(t *testing.T, reader *Reader, store *blobstore.Store, sstID string) []block.Handle {
	t.Helper()

	data, _, err := store.Read(context.Background(), store.SSTPath(sstID))
	if err != nil {
		t.Fatalf("read sst: %v", err)
	}

	r, err := sstable.NewReader(context.Background(), newMemReadable(data), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer func() {
		_ = r.Close()
	}()

	layout, err := r.Layout()
	if err != nil {
		t.Fatalf("layout: %v", err)
	}

	var cached []block.Handle
	for _, h := range layout.Data {
		key := blockCacheKey(sstID, int64(h.Offset), int(h.Length)+block.TrailerLen)
		if _, ok := reader.blockCache.Get(key); ok {
			cached = append(cached, h.Handle)
		}
	}
	return cached
}

func TestReader_RangeRead_DefaultsDownloadSmallSSTsWhole(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := blobstore.NewMemory("range-read-defaults")
	ms := manifest.NewStore(store)
	t.Cleanup(func() { _ = store.Close() })

	value := bytes.Repeat([]byte("v"), 2048)
	entries := make([]internal.MemEntry, 0, 200)
	for i := 0; i < 200; i++ {
		entries = append(entries, internal.MemEntry{
			Key:   []byte(fmt.Sprintf("key-%06d", i)),
			Value: value,
			Kind:  internal.OpPut,
			Seq:   uint64(i + 1),
		})
	}
	res := writeTestSST(t, ctx, store, ms, entries, 0, 1)

	opts := defaultReaderOptions()
	opts.CacheDir = t.TempDir()
	reader, err := newReader(ctx, store, opts)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	if reader.blockCache == nil || reader.blockCache.MaxCost() != defaultBlockCacheSize {
		t.Fatalf("block cache not created with the default budget")
	}
	if reader.rangeReadMinSSTSize != defaultRangeReadMinSSTSize ||
		reader.rangeReadAheadMin != defaultRangeReadAheadMin ||
		reader.rangeReadAheadMax != defaultRangeReadAheadMax {
		t.Fatalf("range read sizes minSST=%d ahead=%d..%d, want defaults",
			reader.rangeReadMinSSTSize, reader.rangeReadAheadMin, reader.rangeReadAheadMax)
	}

	for _, test := range []struct {
		size int64
		want bool
	}{
		{size: defaultRangeReadMinSSTSize - 1, want: false},
		{size: defaultRangeReadMinSSTSize, want: true},
	} {
		got, err := reader.shouldRangeRead(sstMetadata{ID: "sized", Size: test.size})
		if err != nil || got != test.want {
			t.Fatalf("shouldRangeRead(size=%d)=%v, %v; want %v", test.size, got, err, test.want)
		}
	}
	if _, err := reader.shouldRangeRead(sstMetadata{ID: "unsized"}); err == nil {
		t.Fatal("shouldRangeRead without a size: want error")
	}

	// The fixture SST is under the default threshold, so a lookup downloads it
	// whole into the disk cache.
	if res.Meta.Size >= defaultRangeReadMinSSTSize {
		t.Fatalf("fixture SST is %d bytes, want under %d", res.Meta.Size, defaultRangeReadMinSSTSize)
	}
	if _, found, err := reader.Get(ctx, []byte("key-000100")); err != nil || !found {
		t.Fatalf("Get: found=%v err=%v", found, err)
	}
	if got := reader.SSTCacheStats().EntryCount; got != 1 {
		t.Fatalf("SST cache entries=%d, want 1", got)
	}
}

func TestReader_RangeRead_DisabledDownloadsWhole(t *testing.T) {
	t.Parallel()

	opts := defaultReaderOptions()
	opts.CacheDir = t.TempDir()
	opts.RangeRead = false
	reader, err := newReader(context.Background(), blobstore.NewMemory("range-read-off"), opts)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	if reader.blockCache != nil {
		t.Fatal("block cache created with range reads disabled")
	}
	got, err := reader.shouldRangeRead(sstMetadata{ID: "large", Size: 1 << 30})
	if err != nil || got {
		t.Fatalf("shouldRangeRead=%v, %v; want false", got, err)
	}
}
