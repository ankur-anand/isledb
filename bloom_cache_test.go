package isledb

import (
	"context"
	"strings"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestBloomChecksumValidation(t *testing.T) {
	data := []byte("encoded-bloom")
	checksum := bloomChecksum(data)
	if !strings.HasPrefix(checksum, "sha256:") {
		t.Fatalf("bloom checksum=%q", checksum)
	}
	if err := validateBloomChecksum(checksum, data); err != nil {
		t.Fatalf("validate bloom checksum: %v", err)
	}
	if err := validateBloomChecksum(checksum, []byte("corrupt")); err == nil ||
		!strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("corrupt bloom checksum error=%v", err)
	}
	if err := validateBloomChecksum("md5:abcd", data); err == nil ||
		!strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported bloom checksum error=%v", err)
	}
}

func TestBloomFilterCacheEvictsLeastRecentlyUsedWithinByteLimit(t *testing.T) {
	filter := bloomFilterForCacheTest(t, []byte("key"))
	entryBytes := bloomFilterCacheCost("a", filter)
	cache := newBloomFilterCache(2 * entryBytes)

	cache.put("a", filter)
	cache.put("b", filter)
	if _, ok := cache.get("a"); !ok {
		t.Fatal("recently inserted filter a is missing")
	}
	cache.put("c", filter)

	if _, ok := cache.get("b"); ok {
		t.Fatal("least recently used filter b was retained")
	}
	if _, ok := cache.get("a"); !ok {
		t.Fatal("recently used filter a was evicted")
	}
	if _, ok := cache.get("c"); !ok {
		t.Fatal("new filter c is missing")
	}
	stats := cache.stats()
	if stats.EntryCount != 2 {
		t.Fatalf("cache entries=%d want=2", stats.EntryCount)
	}
	if stats.Bytes > stats.MaxBytes {
		t.Fatalf("cache bytes=%d exceed max=%d", stats.Bytes, stats.MaxBytes)
	}
}

func TestBloomFilterCacheRejectsOversizedFilter(t *testing.T) {
	filter := bloomFilterForCacheTest(t, []byte("key"))
	cache := newBloomFilterCache(bloomFilterCacheCost("oversized", filter) - 1)
	cache.put("oversized", filter)

	stats := cache.stats()
	if stats.EntryCount != 0 || stats.Bytes != 0 {
		t.Fatalf("oversized filter was cached: %+v", stats)
	}
}

func TestReaderBloomCacheEvictionReloadsFromObjectStorage(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-bloom-cache-eviction")
	defer store.Close()

	keyA := []byte("alpha")
	keyB := []byte("beta")
	dataA := bloomBytesForCacheTest(t, keyA)
	dataB := bloomBytesForCacheTest(t, keyB)
	metaA := sstMetadata{
		ID: "sst-a",
		Bloom: bloomMetadata{
			Format:   manifest.BloomFormatExactV1,
			Offset:   0,
			Length:   int64(len(dataA)),
			Checksum: bloomChecksum(dataA),
		},
	}
	metaB := sstMetadata{
		ID: "sst-b",
		Bloom: bloomMetadata{
			Format:   manifest.BloomFormatExactV1,
			Offset:   0,
			Length:   int64(len(dataB)),
			Checksum: bloomChecksum(dataB),
		},
	}
	if _, err := store.Write(ctx, store.SSTPath(metaA.ID), dataA); err != nil {
		t.Fatalf("write bloom A: %v", err)
	}
	if _, err := store.Write(ctx, store.SSTPath(metaB.ID), dataB); err != nil {
		t.Fatalf("write bloom B: %v", err)
	}

	filterA := bloomFilterForCacheTest(t, keyA)
	metrics := DefaultReaderMetrics(nil)
	reader := &Reader{
		store:      store,
		fetcher:    newSSTFetcher(store, nil, metrics),
		bloomCache: newBloomFilterCache(bloomFilterCacheCost(metaA.ID, filterA)),
		metrics:    metrics,
	}

	if contains := reader.bloomMayContain(ctx, metaA, keyA); !contains {
		t.Fatal("first bloom A lookup returned definitely absent")
	}
	// A cached filter remains usable without its origin object.
	if err := store.Delete(ctx, store.SSTPath(metaA.ID)); err != nil {
		t.Fatalf("delete bloom A: %v", err)
	}
	if contains := reader.bloomMayContain(ctx, metaA, keyA); !contains {
		t.Fatal("cached bloom A lookup returned definitely absent")
	}
	if _, err := store.Write(ctx, store.SSTPath(metaA.ID), dataA); err != nil {
		t.Fatalf("restore bloom A: %v", err)
	}

	// Loading B uses the entire one-entry budget and evicts A.
	if contains := reader.bloomMayContain(ctx, metaB, keyB); !contains {
		t.Fatal("bloom B lookup returned definitely absent")
	}
	if err := store.Delete(ctx, store.SSTPath(metaA.ID)); err != nil {
		t.Fatalf("delete evicted bloom A: %v", err)
	}
	if contains := reader.bloomMayContain(ctx, metaA, keyA); !contains {
		t.Fatal("unavailable bloom did not fail open")
	}
	if got := testutil.ToFloat64(metrics.BloomFilterErrors); got != 1 {
		t.Fatalf("Bloom errors=%v, want 1", got)
	}

	stats := reader.BloomCacheStats()
	if stats.EntryCount != 1 || stats.Bytes > stats.MaxBytes {
		t.Fatalf("bloom cache outside its bound: %+v", stats)
	}
}

func TestReaderRejectsBloomChecksumMismatchBeforeCaching(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-bloom-checksum")
	defer store.Close()

	key := []byte("present")
	data := bloomBytesForCacheTest(t, key)
	meta := sstMetadata{
		ID: "sst-corrupt-bloom",
		Bloom: bloomMetadata{
			Format:   manifest.BloomFormatExactV1,
			Length:   int64(len(data)),
			Checksum: bloomChecksum(data),
		},
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)/2] ^= 0x01
	if _, err := store.Write(ctx, store.SSTPath(meta.ID), corrupt); err != nil {
		t.Fatalf("write corrupt bloom: %v", err)
	}

	metrics := DefaultReaderMetrics(nil)
	reader := &Reader{
		store: store, fetcher: newSSTFetcher(store, nil, metrics),
		bloomCache: newBloomFilterCache(1 << 20), metrics: metrics,
	}
	if contains := reader.bloomMayContain(ctx, meta, key); !contains {
		t.Fatal("corrupt bloom did not fail open")
	}
	if stats := reader.BloomCacheStats(); stats.EntryCount != 0 {
		t.Fatalf("corrupt bloom entered cache: %+v", stats)
	}
	if got := testutil.ToFloat64(metrics.BloomFilterErrors); got != 1 {
		t.Fatalf("Bloom errors=%v, want 1", got)
	}
}

func TestReaderBloomWithoutChecksumFailsOpen(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-legacy-bloom")
	defer store.Close()

	key := []byte("present")
	data := bloomBytesForCacheTest(t, key)
	meta := sstMetadata{
		ID:    "sst-legacy-bloom",
		Bloom: bloomMetadata{Format: manifest.BloomFormatExactV1, Length: int64(len(data))},
	}
	if _, err := store.Write(ctx, store.SSTPath(meta.ID), data); err != nil {
		t.Fatalf("write legacy bloom: %v", err)
	}

	reader := &Reader{store: store, fetcher: newSSTFetcher(store, nil, nil), bloomCache: newBloomFilterCache(1 << 20)}
	contains := reader.bloomMayContain(ctx, meta, []byte("definitely-absent"))
	if !contains {
		t.Fatal("checksum-less bloom did not fail open")
	}
}

func bloomBytesForCacheTest(t testing.TB, key []byte) []byte {
	t.Helper()
	data, err := buildSSTBloomFilter([]uint64{bloomHashKey(key)}, 12)
	if err != nil {
		t.Fatalf("build bloom for %q: %v", key, err)
	}
	return data
}

func bloomFilterForCacheTest(t testing.TB, key []byte) sstBloomFilter {
	t.Helper()
	filter, err := parseSSTBloomFilter(bloomBytesForCacheTest(t, key))
	if err != nil {
		t.Fatalf("parse bloom for %q: %v", key, err)
	}
	return filter
}
