package isledb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/dgraph-io/ristretto/v2/z"
)

func TestBuildBloomBytesHonorsBitsPerKey(t *testing.T) {
	const (
		keyCount   = 4096
		bitsPerKey = 10
	)

	hashes := make([]uint64, keyCount)
	for i := range hashes {
		hashes[i] = bloomHashKey([]byte(fmt.Sprintf("present-%08d", i)))
	}
	data, _, err := buildBloomBytes(hashes, bitsPerKey)
	if err != nil {
		t.Fatalf("build bloom: %v", err)
	}

	var sidecar bloomSidecar
	if err := json.Unmarshal(data, &sidecar); err != nil {
		t.Fatalf("decode bloom sidecar: %v", err)
	}
	minimumBytes := (keyCount*bitsPerKey + 7) / 8
	if len(sidecar.FilterSet) < minimumBytes {
		t.Fatalf("bloom bit vector bytes=%d want at least %d for %d keys at %d bits/key",
			len(sidecar.FilterSet), minimumBytes, keyCount, bitsPerKey)
	}

	filter, err := parseBloomFilter(data)
	if err != nil {
		t.Fatalf("parse bloom: %v", err)
	}
	for i, hash := range hashes {
		if !filter.Has(hash) {
			t.Fatalf("inserted key %d is absent from bloom", i)
		}
	}

	falsePositives := 0
	for i := range keyCount {
		hash := bloomHashKey([]byte(fmt.Sprintf("absent-%08d", i)))
		if filter.Has(hash) {
			falsePositives++
		}
	}
	// Ten configured bits per key should remain comfortably below this
	// deterministic five-percent ceiling. The old one-bit-per-key allocation
	// returns true for nearly every absent key.
	if falsePositives > keyCount/20 {
		t.Fatalf("bloom false positives=%d/%d exceed 5%%", falsePositives, keyCount)
	}
}

func TestBuildBloomBytesRejectsOversizedFilter(t *testing.T) {
	_, _, err := buildBloomBytes(
		[]uint64{bloomHashKey([]byte("key"))}, int(maxBloomBitsetBits)+1)
	if err == nil {
		t.Fatal("oversized bloom filter was accepted")
	}
}

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
			Offset:   0,
			Length:   int64(len(dataA)),
			Checksum: bloomChecksum(dataA),
		},
	}
	metaB := sstMetadata{
		ID: "sst-b",
		Bloom: bloomMetadata{
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

func TestReaderRejectsBloomChecksumMismatchBeforeCaching(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-bloom-checksum")
	defer store.Close()

	key := []byte("present")
	data := bloomBytesForCacheTest(t, key)
	meta := sstMetadata{
		ID: "sst-corrupt-bloom",
		Bloom: bloomMetadata{
			Length:   int64(len(data)),
			Checksum: bloomChecksum(data),
		},
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)/2] ^= 0x01
	if _, err := store.Write(ctx, store.SSTPath(meta.ID), corrupt); err != nil {
		t.Fatalf("write corrupt bloom: %v", err)
	}

	reader := &Reader{store: store, bloomCache: newBloomFilterCache(1 << 20)}
	if _, err := reader.bloomMayContain(ctx, meta, key); err == nil ||
		!strings.Contains(err.Error(), "bloom checksum mismatch") {
		t.Fatalf("bloom checksum error=%v", err)
	}
	if stats := reader.BloomCacheStats(); stats.EntryCount != 0 {
		t.Fatalf("corrupt bloom entered cache: %+v", stats)
	}
}

func TestReaderAllowsLegacyBloomWithoutChecksum(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-legacy-bloom")
	defer store.Close()

	key := []byte("present")
	data := bloomBytesForCacheTest(t, key)
	meta := sstMetadata{
		ID:    "sst-legacy-bloom",
		Bloom: bloomMetadata{Length: int64(len(data))},
	}
	if _, err := store.Write(ctx, store.SSTPath(meta.ID), data); err != nil {
		t.Fatalf("write legacy bloom: %v", err)
	}

	reader := &Reader{store: store, bloomCache: newBloomFilterCache(1 << 20)}
	contains, err := reader.bloomMayContain(ctx, meta, key)
	if err != nil || !contains {
		t.Fatalf("legacy bloom contains=%t err=%v", contains, err)
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
