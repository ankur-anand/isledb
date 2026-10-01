package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestReaderCacheTierBudgetsAndRestart reopens a reader's disk cache with
// smaller tier budgets: recovery trims each tier to its own budget, and the
// tiers stay independent, so churn in the data tier never evicts Bloom
// filters from the meta tier.
func TestReaderCacheTierBudgetsAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bucketURL := setupFakeS3BucketURL(t)
	cacheDir := t.TempDir()
	db := openArtifactCacheTestDB(t, ctx, bucketURL, "tier-budgets")
	defer db.Close()

	writeArtifactCacheTestBatches(t, ctx, db, []map[string]string{
		{"budget/a": "value-a"},
		{"budget/b": "value-b"},
		{"budget/c": "value-c"},
	})
	manifest, err := db.manifestStore.ReplayWithArtifactValidation(ctx)
	if err != nil {
		t.Fatalf("replay tier-budget manifest: %v", err)
	}
	metas := artifactCacheTestSortedSSTs(manifest)
	if len(metas) != 3 {
		t.Fatalf("tier-budget SST count=%d, want 3", len(metas))
	}
	// Each SST is small, so it is cached whole in the data tier, and its
	// Bloom filter in the meta tier.
	oneSSTBytes := metas[0].Size
	oneBloomBytes := metas[0].Bloom.Length
	oneLoadedBloomBytes := artifactCacheTestLoadedBloomCost(t, ctx, db, metas[0])
	for _, meta := range metas[1:] {
		if meta.Size != oneSSTBytes || meta.Bloom.Length != oneBloomBytes {
			t.Fatalf("fixture SSTs have unequal sizes: first=(%d,%d) %s=(%d,%d)",
				oneSSTBytes, oneBloomBytes, meta.ID, meta.Size, meta.Bloom.Length)
		}
	}

	// Room for all three; the parsed Bloom cache holds only one filter.
	reader, done := openTierTestReader(t, ctx, db, cacheDir, 3*oneBloomBytes, 3*oneSSTBytes, oneLoadedBloomBytes)
	assertArtifactCacheBudgetValues(t, ctx, reader)
	stats := reader.DiskCacheStats()
	assertArtifactCacheTierBound(t, "data tier", stats.Data, 3, 3*oneSSTBytes)
	assertArtifactCacheTierBound(t, "meta tier", stats.Meta, 3, 3*oneBloomBytes)
	assertArtifactCacheTierBound(t, "parsed Bloom cache", reader.BloomCacheStats(), 1, oneLoadedBloomBytes)
	done()

	// Shrink only the data tier. Recovery trims it, and reading all three
	// churns it, while every Bloom filter stays on disk and is reused.
	reader, done = openTierTestReader(t, ctx, db, cacheDir, 3*oneBloomBytes, oneSSTBytes, oneLoadedBloomBytes)
	stats = reader.DiskCacheStats()
	assertArtifactCacheTierBound(t, "recovered data tier", stats.Data, 1, oneSSTBytes)
	assertArtifactCacheTierBound(t, "recovered meta tier", stats.Meta, 3, 3*oneBloomBytes)
	assertArtifactCacheEmptyL1(t, reader, "first budget restart")
	assertArtifactCacheBudgetValues(t, ctx, reader)
	stats = reader.DiskCacheStats()
	assertArtifactCacheTierBound(t, "churning data tier", stats.Data, 1, oneSSTBytes)
	if stats.Data.Evictions == 0 || stats.Data.Bypasses != 0 {
		t.Fatalf("data tier did not evict cleanly under its reduced budget: %+v", stats.Data)
	}
	if stats.Meta.Hits == 0 || stats.Meta.Evictions != 0 {
		t.Fatalf("meta tier was not reused independently: %+v", stats.Meta)
	}
	done()

	// Shrink the meta tier too. Both tiers stay within their budgets.
	reader, done = openTierTestReader(t, ctx, db, cacheDir, oneBloomBytes, oneSSTBytes, oneLoadedBloomBytes)
	defer done()
	stats = reader.DiskCacheStats()
	assertArtifactCacheTierBound(t, "recovered one-entry data tier", stats.Data, 1, oneSSTBytes)
	assertArtifactCacheTierBound(t, "recovered one-entry meta tier", stats.Meta, 1, oneBloomBytes)
	assertArtifactCacheBudgetValues(t, ctx, reader)
	stats = reader.DiskCacheStats()
	assertArtifactCacheTierBound(t, "bounded data tier", stats.Data, 1, oneSSTBytes)
	assertArtifactCacheTierBound(t, "bounded meta tier", stats.Meta, 1, oneBloomBytes)
	if stats.Meta.Evictions == 0 || stats.Meta.Bypasses != 0 {
		t.Fatalf("bounded meta tier churn stats=%+v", stats.Meta)
	}
}

// TestReaderProcessLocalL1RestartsWithPersistentBloomL2 restarts a reader:
// its memory caches start empty, and the disk cache alone serves the SST and
// its Bloom filter, with no read of object storage.
func TestReaderProcessLocalL1RestartsWithPersistentBloomL2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bucketURL := setupFakeS3BucketURL(t)
	cacheDir := t.TempDir()
	db := openArtifactCacheTestDB(t, ctx, bucketURL, "l1-restart")
	defer db.Close()

	batch := make(map[string]string, 128)
	for index := range 128 {
		batch[fmt.Sprintf("range/%03d", index)] = artifactCacheTestLargeValue(index, 768)
	}
	writeArtifactCacheTestBatches(t, ctx, db, []map[string]string{batch})

	const blockCacheBytes = int64(1 << 20)
	options := DefaultReaderOpenOptions(cacheDir)
	options.BlockCacheSize = blockCacheBytes
	options.BloomCacheSize = 1 << 20
	firstMetrics := DefaultReaderMetrics(nil)
	options.Metrics = firstMetrics
	reader := openArtifactCacheTestReaderWithOptions(t, ctx, db, options)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	wantValue := artifactCacheTestLargeValue(64, 768)
	assertArtifactCacheTestValue(t, ctx, reader, "range/064", wantValue)
	assertArtifactCacheBlockBound(t, reader, blockCacheBytes)
	if testutil.ToFloat64(firstMetrics.SSTRangeReadTotal) == 0 || reader.BlockCacheStats().Misses == 0 {
		t.Fatalf("cold read did not reach object storage: reads=%v block cache=%+v",
			testutil.ToFloat64(firstMetrics.SSTRangeReadTotal), reader.BlockCacheStats())
	}
	hitsBefore := reader.BlockCacheStats().Hits
	assertArtifactCacheTestValue(t, ctx, reader, "range/064", wantValue)
	if reader.BlockCacheStats().Hits == hitsBefore {
		t.Fatal("warm lookup recorded no block cache hits")
	}
	rows, err := reader.Scan(ctx, nil, nil)
	if err != nil || len(rows) != len(batch) {
		t.Fatalf("scan rows=%d err=%v, want=%d", len(rows), err, len(batch))
	}
	assertArtifactCacheBlockBound(t, reader, blockCacheBytes)
	if err := reader.Close(); err != nil {
		t.Fatalf("close first Reader: %v", err)
	}
	reader = nil

	secondMetrics := DefaultReaderMetrics(nil)
	options.Metrics = secondMetrics
	reader = openArtifactCacheTestReaderWithOptions(t, ctx, db, options)
	assertArtifactCacheEmptyL1(t, reader, "Reader restart")
	if stats := reader.BlockCacheStats(); stats.MaxBytes != blockCacheBytes ||
		stats.Bytes != 0 || stats.EntryCount != 0 {
		t.Fatalf("block cache was not empty after restart: %+v", stats)
	}
	assertArtifactCacheTestValue(t, ctx, reader, "range/064", wantValue)
	if got := testutil.ToFloat64(secondMetrics.SSTRangeReadTotal); got != 0 {
		t.Fatalf("restarted Reader read object storage %v times, want the disk cache only", got)
	}
	if stats := reader.DiskCacheStats(); stats.Data.Hits == 0 || stats.Meta.Hits == 0 {
		t.Fatalf("restarted Reader did not use the disk cache: %+v", stats)
	}
	if stats := reader.BloomCacheStats(); stats.EntryCount != 1 || stats.Misses == 0 {
		t.Fatalf("restarted Reader did not repopulate parsed Bloom filters: %+v", stats)
	}
	hitsBefore = reader.BlockCacheStats().Hits
	assertArtifactCacheTestValue(t, ctx, reader, "range/064", wantValue)
	if reader.BlockCacheStats().Hits == hitsBefore {
		t.Fatal("restarted warm lookup recorded no block cache hits")
	}
}

// openTierTestReader opens a reader on db whose disk cache under cacheDir has
// the given tier budgets. done closes the reader and then the cache.
func openTierTestReader(
	t *testing.T,
	ctx context.Context,
	db *DB,
	cacheDir string,
	metaBytes, dataBytes, bloomCacheBytes int64,
) (*Reader, func()) {
	t.Helper()
	disk, err := diskcache.Open(diskcache.Options{
		Dir: filepath.Join(cacheDir, "artifacts"), MetaMaxBytes: metaBytes, DataMaxBytes: dataBytes,
	})
	if err != nil {
		t.Fatalf("open disk cache: %v", err)
	}
	reader, err := newReader(ctx, db.store, readerOptions{
		CacheDir: cacheDir, DiskCache: disk, BloomCacheSize: bloomCacheBytes,
	})
	if err != nil {
		_ = disk.Close()
		t.Fatalf("open tier test Reader: %v", err)
	}
	return reader, func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close tier test Reader: %v", err)
		}
		if err := disk.Close(); err != nil {
			t.Errorf("close disk cache: %v", err)
		}
	}
}

func openArtifactCacheTestReaderWithOptions(
	t *testing.T,
	ctx context.Context,
	db *DB,
	options ReaderOpenOptions,
) *Reader {
	t.Helper()
	reader, err := db.OpenReader(ctx, options)
	if err != nil {
		t.Fatalf("open cache test Reader: %v", err)
	}
	return reader
}

func artifactCacheTestSortedSSTs(manifest *manifestState) []sstMetadata {
	metas := append([]sstMetadata(nil), manifest.L0SSTs...)
	for _, level := range manifest.Levels {
		metas = append(metas, level.SSTs...)
	}
	sort.Slice(metas, func(i, j int) bool {
		return bytes.Compare(metas[i].MinKey, metas[j].MinKey) < 0
	})
	return metas
}

func artifactCacheTestLoadedBloomCost(
	t *testing.T,
	ctx context.Context,
	db *DB,
	meta sstMetadata,
) int64 {
	t.Helper()
	data, err := db.store.ReadRange(
		ctx, db.store.SSTPath(meta.ID), meta.Bloom.Offset, meta.Bloom.Length)
	if err != nil {
		t.Fatalf("read Bloom %s: %v", meta.ID, err)
	}
	filter, err := parseSSTBloomFilter(data)
	if err != nil {
		t.Fatalf("parse Bloom %s: %v", meta.ID, err)
	}
	return bloomFilterCacheCost(meta.ID, filter)
}

func artifactCacheTestLargeValue(index, size int) string {
	value := make([]byte, 0, size)
	for block := 0; len(value) < size; block++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%d/%d", index, block)))
		value = append(value, digest[:]...)
	}
	return string(value[:size])
}

func assertArtifactCacheBudgetValues(t *testing.T, ctx context.Context, reader *Reader) {
	t.Helper()
	for index, key := range []string{"budget/a", "budget/b", "budget/c"} {
		assertArtifactCacheTestValue(t, ctx, reader, key, fmt.Sprintf("value-%c", 'a'+index))
	}
}

// assertArtifactCacheRecoveredTiers checks a reopened reader recovered the
// expected disk entries: whole small SSTs in the data tier and Bloom filters
// in the meta tier.
func assertArtifactCacheRecoveredTiers(
	t *testing.T,
	reader *Reader,
	wantDataEntries int,
	wantMetaEntries int,
) {
	t.Helper()
	stats := reader.DiskCacheStats()
	if stats.Data.EntryCount != wantDataEntries || stats.Data.Bytes == 0 {
		t.Fatalf("recovered data tier stats=%+v, want entries=%d", stats.Data, wantDataEntries)
	}
	if stats.Meta.EntryCount != wantMetaEntries || stats.Meta.Bytes == 0 {
		t.Fatalf("recovered meta tier stats=%+v, want entries=%d", stats.Meta, wantMetaEntries)
	}
}

func assertArtifactCacheEmptyL1(t *testing.T, reader *Reader, label string) {
	t.Helper()
	if stats := reader.BloomCacheStats(); stats.EntryCount != 0 || stats.Bytes != 0 ||
		stats.Hits != 0 || stats.Misses != 0 {
		t.Fatalf("%s parsed Bloom cache is not empty: %+v", label, stats)
	}
}

func assertArtifactCacheTierBound(
	t *testing.T,
	label string,
	stats CacheStats,
	wantEntries int,
	wantMaxBytes int64,
) {
	t.Helper()
	if stats.EntryCount != wantEntries || stats.MaxBytes != wantMaxBytes ||
		stats.Bytes <= 0 || stats.Bytes > stats.MaxBytes ||
		stats.Failures != 0 {
		t.Fatalf("%s stats=%+v, want entries=%d max_bytes=%d",
			label, stats, wantEntries, wantMaxBytes)
	}
}

func assertArtifactCacheBlockBound(t *testing.T, reader *Reader, maxBytes int64) {
	t.Helper()
	if stats := reader.BlockCacheStats(); stats.MaxBytes != maxBytes ||
		stats.Bytes <= 0 || stats.Bytes > maxBytes {
		t.Fatalf("block cache outside bound: %+v want_max=%d", stats, maxBytes)
	}
}
