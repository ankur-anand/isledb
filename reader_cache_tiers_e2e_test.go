package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

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

	oneSSTBytes := metas[0].Size
	oneBloomBytes := metas[0].Bloom.Length
	oneDecodedBloomBytes := artifactCacheTestDecodedBloomCost(t, ctx, db, metas[0])
	for _, meta := range metas[1:] {
		if meta.Size != oneSSTBytes || meta.Bloom.Length != oneBloomBytes {
			t.Fatalf(
				"fixture artifacts have unequal sizes: first=(%d,%d) %s=(%d,%d)",
				oneSSTBytes, oneBloomBytes, meta.ID, meta.Size, meta.Bloom.Length)
		}
		if cost := artifactCacheTestDecodedBloomCost(t, ctx, db, meta); cost != oneDecodedBloomBytes {
			t.Fatalf("fixture decoded Bloom cost=%d for %s, want=%d",
				cost, meta.ID, oneDecodedBloomBytes)
		}
	}

	largeOptions := DefaultReaderOpenOptions(cacheDir)
	largeOptions.SSTCacheSize = int64(len(metas)) * oneSSTBytes
	largeOptions.BloomDiskCacheSize = int64(len(metas)) * oneBloomBytes
	// The decoded Bloom L1 intentionally holds only one of the three filters.
	largeOptions.BloomCacheSize = oneDecodedBloomBytes
	reader := openArtifactCacheTestReaderWithOptions(t, ctx, db, largeOptions)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	assertArtifactCacheBudgetValues(t, ctx, reader)
	assertArtifactCacheTierBound(
		t, "large SST L2", reader.SSTCacheStats(), 3, largeOptions.SSTCacheSize)
	assertArtifactCacheTierBound(
		t, "large Bloom L2", reader.BloomDiskCacheStats(), 3, largeOptions.BloomDiskCacheSize)
	assertArtifactCacheTierBound(
		t, "decoded Bloom L1", reader.BloomCacheStats(), 1, largeOptions.BloomCacheSize)
	if err := reader.Close(); err != nil {
		t.Fatalf("close large-budget Reader: %v", err)
	}
	reader = nil

	// Shrink only SST L2 on reopen. Recovery trims that tier while retaining
	// every raw Bloom under its independently configured disk budget.
	shrunkSSTOptions := largeOptions
	shrunkSSTOptions.SSTCacheSize = oneSSTBytes
	reader = openArtifactCacheTestReaderWithOptions(t, ctx, db, shrunkSSTOptions)
	assertArtifactCacheTierBound(
		t, "recovered shrunk SST L2", reader.SSTCacheStats(), 1, oneSSTBytes)
	assertArtifactCacheTierBound(
		t, "recovered full Bloom L2", reader.BloomDiskCacheStats(), 3,
		shrunkSSTOptions.BloomDiskCacheSize)
	assertArtifactCacheEmptyL1(t, reader, "first budget restart")
	assertArtifactCacheBudgetValues(t, ctx, reader)
	assertArtifactCacheTierBound(
		t, "churning SST L2", reader.SSTCacheStats(), 1, oneSSTBytes)
	assertArtifactCacheTierBound(
		t, "retained Bloom L2", reader.BloomDiskCacheStats(), 3,
		shrunkSSTOptions.BloomDiskCacheSize)
	if stats := reader.SSTCacheStats(); stats.Evictions == 0 || stats.AdmissionBypasses != 0 {
		t.Fatalf("SST L2 did not evict cleanly under its reduced budget: %+v", stats)
	}
	if stats := reader.BloomDiskCacheStats(); stats.Hits == 0 || stats.Evictions != 0 {
		t.Fatalf("independent Bloom L2 was not reused: %+v", stats)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close shrunk-SST Reader: %v", err)
	}
	reader = nil

	// Shrink Bloom L2 as well. Recovery and subsequent admission churn must
	// keep all three independently-accounted tiers within their byte limits.
	shrunkBothOptions := shrunkSSTOptions
	shrunkBothOptions.BloomDiskCacheSize = oneBloomBytes
	reader = openArtifactCacheTestReaderWithOptions(t, ctx, db, shrunkBothOptions)
	assertArtifactCacheTierBound(
		t, "recovered one-entry SST L2", reader.SSTCacheStats(), 1, oneSSTBytes)
	assertArtifactCacheTierBound(
		t, "recovered one-entry Bloom L2", reader.BloomDiskCacheStats(), 1, oneBloomBytes)
	assertArtifactCacheEmptyL1(t, reader, "second budget restart")
	assertArtifactCacheBudgetValues(t, ctx, reader)
	assertArtifactCacheTierBound(
		t, "bounded SST L2", reader.SSTCacheStats(), 1, oneSSTBytes)
	assertArtifactCacheTierBound(
		t, "bounded Bloom L2", reader.BloomDiskCacheStats(), 1, oneBloomBytes)
	assertArtifactCacheTierBound(
		t, "bounded decoded Bloom L1", reader.BloomCacheStats(), 1, oneDecodedBloomBytes)
	if stats := reader.SSTCacheStats(); stats.Evictions == 0 || stats.AdmissionBypasses != 0 {
		t.Fatalf("bounded SST L2 churn stats=%+v", stats)
	}
	if stats := reader.BloomDiskCacheStats(); stats.Evictions == 0 || stats.AdmissionBypasses != 0 {
		t.Fatalf("bounded Bloom L2 churn stats=%+v", stats)
	}
}

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

	// Leave enough room for deterministic Ristretto admission of the point
	// lookup's metadata and data blocks. The bound itself is still asserted
	// before and after a full multi-block scan.
	const blockCacheBytes = int64(1 << 20)
	options := DefaultReaderOpenOptions(cacheDir)
	options.BlockCacheSize = blockCacheBytes
	options.AllowUnverifiedRangeRead = true
	options.RangeReadMinSSTSize = 1
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
	reader.blockCache.Wait()
	assertArtifactCacheBlockBound(t, reader, blockCacheBytes)
	if stats := reader.SSTCacheStats(); stats.EntryCount != 0 {
		t.Fatalf("range-read unexpectedly populated SST L2: %+v", stats)
	}
	assertArtifactCacheTierBound(
		t, "range-read raw Bloom L2", reader.BloomDiskCacheStats(), 1,
		options.BloomDiskCacheSize)
	assertArtifactCacheTierBound(
		t, "range-read decoded Bloom L1", reader.BloomCacheStats(), 1,
		options.BloomCacheSize)
	firstRangeReads := testutil.ToFloat64(firstMetrics.SSTRangeReadTotal)
	if firstRangeReads == 0 || testutil.ToFloat64(firstMetrics.SSTRangeBlockCacheMisses) == 0 {
		t.Fatalf("cold range read metrics: reads=%v misses=%v",
			firstRangeReads, testutil.ToFloat64(firstMetrics.SSTRangeBlockCacheMisses))
	}
	assertArtifactCacheTestValue(t, ctx, reader, "range/064", wantValue)
	reader.blockCache.Wait()
	if testutil.ToFloat64(firstMetrics.SSTRangeBlockCacheHits) == 0 {
		t.Fatal("warm block L1 lookup recorded no hits")
	}
	// Read the full SST through the range path to exercise multiple block-cache
	// entries. Ristretto may choose its admissions, but its accounted cost must
	// remain within the configured maximum.
	rows, err := reader.Scan(ctx, nil, nil)
	if err != nil || len(rows) != len(batch) {
		t.Fatalf("range scan rows=%d err=%v, want=%d", len(rows), err, len(batch))
	}
	reader.blockCache.Wait()
	assertArtifactCacheBlockBound(t, reader, blockCacheBytes)
	if err := reader.Close(); err != nil {
		t.Fatalf("close first range Reader: %v", err)
	}
	reader = nil

	secondMetrics := DefaultReaderMetrics(nil)
	options.Metrics = secondMetrics
	reader = openArtifactCacheTestReaderWithOptions(t, ctx, db, options)
	assertArtifactCacheEmptyL1(t, reader, "range Reader restart")
	if reader.blockCache.MaxCost() != blockCacheBytes ||
		reader.blockCache.RemainingCost() != blockCacheBytes {
		t.Fatalf("block L1 was not empty after restart: max=%d remaining=%d",
			reader.blockCache.MaxCost(), reader.blockCache.RemainingCost())
	}
	assertArtifactCacheTierBound(
		t, "recovered raw Bloom L2", reader.BloomDiskCacheStats(), 1,
		options.BloomDiskCacheSize)
	assertArtifactCacheTestValue(t, ctx, reader, "range/064", wantValue)
	reader.blockCache.Wait()
	if testutil.ToFloat64(secondMetrics.SSTRangeReadTotal) == 0 ||
		testutil.ToFloat64(secondMetrics.SSTRangeBlockCacheMisses) == 0 {
		t.Fatalf("restarted block L1 did not take a cold origin path: reads=%v misses=%v",
			testutil.ToFloat64(secondMetrics.SSTRangeReadTotal),
			testutil.ToFloat64(secondMetrics.SSTRangeBlockCacheMisses))
	}
	if stats := reader.BloomDiskCacheStats(); stats.Hits == 0 {
		t.Fatalf("restarted Reader did not reuse raw Bloom L2: %+v", stats)
	}
	if stats := reader.BloomCacheStats(); stats.EntryCount != 1 || stats.Misses == 0 {
		t.Fatalf("restarted Reader did not repopulate decoded Bloom L1: %+v", stats)
	}
	readsBeforeWarm := testutil.ToFloat64(secondMetrics.SSTRangeReadTotal)
	assertArtifactCacheTestValue(t, ctx, reader, "range/064", wantValue)
	reader.blockCache.Wait()
	if testutil.ToFloat64(secondMetrics.SSTRangeBlockCacheHits) == 0 {
		t.Fatal("restarted warm block L1 lookup recorded no hits")
	}
	if readsAfterWarm := testutil.ToFloat64(secondMetrics.SSTRangeReadTotal); readsAfterWarm != readsBeforeWarm {
		t.Fatalf("warm block L1 still read origin: before=%v after=%v",
			readsBeforeWarm, readsAfterWarm)
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

func artifactCacheTestDecodedBloomCost(
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
	filter, err := parseBloomFilter(data)
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

func assertArtifactCacheRecoveredTiers(
	t *testing.T,
	reader *Reader,
	wantSSTEntries int,
	wantBloomEntries int,
) {
	t.Helper()
	if stats := reader.SSTCacheStats(); stats.EntryCount != wantSSTEntries || stats.Bytes == 0 {
		t.Fatalf("recovered SST L2 stats=%+v, want entries=%d", stats, wantSSTEntries)
	}
	if stats := reader.BloomDiskCacheStats(); stats.EntryCount != wantBloomEntries || stats.Bytes == 0 {
		t.Fatalf("recovered Bloom L2 stats=%+v, want entries=%d", stats, wantBloomEntries)
	}
}

func assertArtifactCacheEmptyL1(t *testing.T, reader *Reader, label string) {
	t.Helper()
	if stats := reader.BloomCacheStats(); stats.EntryCount != 0 || stats.Bytes != 0 ||
		stats.Hits != 0 || stats.Misses != 0 {
		t.Fatalf("%s decoded Bloom L1 is not empty: %+v", label, stats)
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
		stats.PinnedEntries != 0 || stats.PinnedBytes != 0 ||
		stats.SyncFailures != 0 || stats.PublicationFailures != 0 {
		t.Fatalf("%s stats=%+v, want entries=%d max_bytes=%d",
			label, stats, wantEntries, wantMaxBytes)
	}
}

func assertArtifactCacheBlockBound(t *testing.T, reader *Reader, maxBytes int64) {
	t.Helper()
	if reader.blockCache == nil || reader.blockCache.MaxCost() != maxBytes ||
		reader.blockCache.RemainingCost() < 0 ||
		reader.blockCache.RemainingCost() > maxBytes {
		var maxCost, remaining int64
		if reader.blockCache != nil {
			maxCost = reader.blockCache.MaxCost()
			remaining = reader.blockCache.RemainingCost()
		}
		t.Fatalf("block L1 outside bound: max=%d remaining=%d want_max=%d",
			maxCost, remaining, maxBytes)
	}
}
