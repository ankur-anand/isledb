package isledb

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestSSTMetaCache_EvictsLeastRecentlyUsedWithinBudget(t *testing.T) {
	region := bytes.Repeat([]byte("m"), 1000)
	entry := int64(len(region)) + int64(len("sst-a")) + metaCacheEntryOverhead
	c := newSSTMetaCache(2 * entry)

	c.put("sst-a", region)
	c.put("sst-b", region)
	if _, ok := c.get("sst-a", 1000); !ok {
		t.Fatal("sst-a missing")
	}
	c.put("sst-c", region) // evicts sst-b, the least recently used

	if _, ok := c.get("sst-b", 1000); ok {
		t.Fatal("sst-b was not evicted")
	}
	for _, id := range []string{"sst-a", "sst-c"} {
		if _, ok := c.get(id, 1000); !ok {
			t.Fatalf("%s missing", id)
		}
	}
	stats := c.stats()
	if stats.EntryCount != 2 || stats.Bytes > stats.MaxBytes || stats.Evictions != 1 ||
		stats.Hits != 3 || stats.Misses != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestSSTMetaCache_RejectsOversizedAndMismatchedRegions(t *testing.T) {
	c := newSSTMetaCache(500)
	c.put("big", bytes.Repeat([]byte("m"), 1000))
	if stats := c.stats(); stats.EntryCount != 0 || stats.Bytes != 0 {
		t.Fatalf("oversized region was cached: %+v", stats)
	}

	c.put("sst", bytes.Repeat([]byte("m"), 100))
	if _, ok := c.get("sst", 99); ok {
		t.Fatal("region of another length was served")
	}
	if _, ok := c.get("sst", 100); !ok {
		t.Fatal("region missing")
	}
}

// TestSSTRangeReadable_MetaRegionSurvivesBlockCacheEviction reads an SST's
// metadata, empties the block cache, and opens the SST again: its metadata
// must come from the metadata cache without a request.
func TestSSTRangeReadable_MetaRegionSurvivesBlockCacheEviction(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	cache := newChunkTestCache(t)
	meta := newSSTMetaCache(1 << 20)
	open := func() *sstRangeReadable {
		r := s.readable(t, cache, 1000)
		r.useMetaRegion(4500)
		r.useMetaCache(meta)
		return r
	}

	if got := readChunked(t, open(), 4600, 100); !bytes.Equal(got, s.data[4600:4700]) {
		t.Fatal("metadata bytes differ")
	}
	cache.Wait()
	if s.gets.Load() != 1 || s.ranges[0] != "bytes=4500-4999" {
		t.Fatalf("ranges = %v, want one metadata request", s.ranges)
	}
	if _, ok := cache.Get(blockCacheKey("chunked-sst", 4500, 500)); ok {
		t.Fatal("metadata region was also stored in the block cache")
	}

	cache.Clear()
	s.reset()
	if got := readChunked(t, open(), 4510, 300); !bytes.Equal(got, s.data[4510:4810]) {
		t.Fatal("metadata bytes differ after reopen")
	}
	if got := s.gets.Load(); got != 0 {
		t.Fatalf("reopen made %d requests (%v), want 0", got, s.ranges)
	}
}

func TestSSTRangeReadable_ConcurrentMetaMissesShareOneRequest(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	s.delay = 20 * time.Millisecond
	meta := newSSTMetaCache(1 << 20)

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			r := s.readable(t, nil, 1000)
			r.useMetaRegion(4500)
			r.useMetaCache(meta)
			p := make([]byte, 100)
			<-start
			if err := r.ReadAt(context.Background(), p, 4600); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(p, s.data[4600:4700]) {
				errs <- fmt.Errorf("metadata bytes differ")
			}
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := s.gets.Load(); got != 1 {
		t.Fatalf("GETs = %d, want one shared request", got)
	}
}

// TestReader_MetaCacheSavesRequestsUnderBlockCachePressure runs point lookups
// through the reader with the block cache emptied between them, as data churn
// would. Only the first lookup fetches the SST's metadata.
func TestReader_MetaCacheSavesRequestsUnderBlockCachePressure(t *testing.T) {
	ctx := context.Background()
	counts := &kvS3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(t, counts.observe)
	store, err := blobstore.Open(ctx, bucketURL, fmt.Sprintf("meta-cache-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(), BlockCacheSize: 16 << 20, AllowUnverifiedRangeRead: true,
		RangeReadMinSSTSize: 1, RangeReadChunkSize: 16 << 10,
	})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	entries := make([]internal.MemEntry, 5_000)
	for i := range entries {
		entries[i] = internal.MemEntry{
			Key: kvLeveledBenchmarkKey(i), Seq: uint64(i + 1), Kind: internal.OpPut,
			Value: bytes.Repeat([]byte{byte(i)}, 100),
		}
	}
	result, err := writeSST(ctx, &sliceSSTIter{entries: entries},
		sstWriterOptions{BlockSize: 4096, Compression: "snappy"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
		t.Fatalf("store SST: %v", err)
	}
	meta := result.Meta
	meta.Level = 1
	state := &manifestState{Levels: []manifest.Level{{Number: 1, SSTs: []manifest.SSTMeta{meta}}}}

	get := func(i int) int64 {
		t.Helper()
		waitKVReaderBenchmarkCache(reader)
		reader.blockCache.Clear()
		counts.reset()
		value, found, err := reader.getWithManifest(ctx, state, kvLeveledBenchmarkKey(i))
		if err != nil || !found || !bytes.Equal(value, entries[i].Value) {
			t.Fatalf("Get(%d) found=%t err=%v", i, found, err)
		}
		return counts.ssts.Load()
	}
	if got := get(10); got != 2 {
		t.Fatalf("first lookup made %d requests, want metadata + data", got)
	}
	for _, i := range []int{2_000, 4_000} {
		if got := get(i); got != 1 {
			t.Fatalf("lookup %d made %d requests, want data only", i, got)
		}
	}
	if stats := reader.MetaCacheStats(); stats.EntryCount != 1 || stats.Hits != 2 {
		t.Fatalf("meta cache stats = %+v", stats)
	}
}
