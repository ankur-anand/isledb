package isledb

import (
	"context"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/dgraph-io/ristretto/v2/z"
)

func TestBloomFilterCacheEvictsLeastRecentlyUsedWithinByteLimit(t *testing.T) {
	filter := z.NewBloomFilter(64, 2)
	filter.Add(1)
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
	filter := z.NewBloomFilter(64, 2)
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
			Offset: 0,
			Length: int64(len(dataA)),
		},
	}
	metaB := sstMetadata{
		ID: "sst-b",
		Bloom: bloomMetadata{
			Offset: 0,
			Length: int64(len(dataB)),
		},
	}
	if _, err := store.Write(ctx, store.SSTPath(metaA.ID), dataA); err != nil {
		t.Fatalf("write bloom A: %v", err)
	}
	if _, err := store.Write(ctx, store.SSTPath(metaB.ID), dataB); err != nil {
		t.Fatalf("write bloom B: %v", err)
	}

	filterA, err := parseBloomFilter(dataA)
	if err != nil {
		t.Fatalf("parse bloom A: %v", err)
	}
	reader := &Reader{
		store:      store,
		bloomCache: newBloomFilterCache(bloomFilterCacheCost(metaA.ID, filterA)),
	}

	if contains, err := reader.bloomMayContain(ctx, metaA, keyA); err != nil || !contains {
		t.Fatalf("first bloom A lookup contains=%t err=%v", contains, err)
	}
	// A cached filter remains usable without its origin object.
	if err := store.Delete(ctx, store.SSTPath(metaA.ID)); err != nil {
		t.Fatalf("delete bloom A: %v", err)
	}
	if contains, err := reader.bloomMayContain(ctx, metaA, keyA); err != nil || !contains {
		t.Fatalf("cached bloom A lookup contains=%t err=%v", contains, err)
	}
	if _, err := store.Write(ctx, store.SSTPath(metaA.ID), dataA); err != nil {
		t.Fatalf("restore bloom A: %v", err)
	}

	// Loading B uses the entire one-entry budget and evicts A.
	if contains, err := reader.bloomMayContain(ctx, metaB, keyB); err != nil || !contains {
		t.Fatalf("bloom B lookup contains=%t err=%v", contains, err)
	}
	if err := store.Delete(ctx, store.SSTPath(metaA.ID)); err != nil {
		t.Fatalf("delete evicted bloom A: %v", err)
	}
	if _, err := reader.bloomMayContain(ctx, metaA, keyA); err == nil {
		t.Fatal("evicted bloom A was returned without reloading its deleted origin")
	}

	stats := reader.BloomCacheStats()
	if stats.EntryCount != 1 || stats.Bytes > stats.MaxBytes {
		t.Fatalf("bloom cache outside its bound: %+v", stats)
	}
}

func bloomBytesForCacheTest(t testing.TB, key []byte) []byte {
	t.Helper()
	data, _, err := buildBloomBytes([]uint64{bloomHashKey(key)}, 10)
	if err != nil {
		t.Fatalf("build bloom for %q: %v", key, err)
	}
	return data
}
