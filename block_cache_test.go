package isledb

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func TestBlockCache_FileNumbers(t *testing.T) {
	b := newBlockCache(1 << 20)
	defer b.close()
	fileNum := func(id string) uint64 { return uint64(b.readerOptions(id).CacheOpts.FileNum) }

	a1, a2, c := fileNum("a"), fileNum("a"), fileNum("c")
	if a1 == 0 || a1 != a2 || a1 == c {
		t.Fatalf("file numbers a=%d,%d c=%d: want one nonzero number per SST", a1, a2, c)
	}
	b.evict("a")
	if again := fileNum("a"); again == a1 || again == c {
		t.Fatalf("evicted SST reused number %d", again)
	}
	b.retain(&manifestState{L0SSTs: []manifest.SSTMeta{{ID: "c"}}})
	if _, ok := b.files["a"]; ok {
		t.Fatal("retain kept an SST missing from the manifest")
	}
	if got := fileNum("c"); got != c {
		t.Fatalf("retain changed a live SST's number: %d -> %d", c, got)
	}
}

// TestBlockCache_OpenMakesTwoUncachedLookups pins the Pebble behaviour
// stats relies on: each SST open looks up exactly metaLookupsPerOpen blocks
// that are never cached. If a Pebble upgrade changes it, this fails rather
// than the reported misses going wrong.
func TestBlockCache_OpenMakesTwoUncachedLookups(t *testing.T) {
	for _, rangeRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("range=%t", rangeRead), func(t *testing.T) {
			ctx := context.Background()
			store := blobstore.NewMemory(fmt.Sprintf("block-cache-opens-%t", rangeRead))
			defer store.Close()
			opts := readerOptions{CacheDir: t.TempDir()}
			if rangeRead {
				opts.RangeRead, opts.RangeReadMinSSTSize = true, 1
			}
			reader, err := newReader(ctx, store, opts)
			if err != nil {
				t.Fatalf("open reader: %v", err)
			}
			defer reader.Close()
			entries, state := blockCacheTestSST(t, ctx, store, 2_000)
			meta := state.Levels[0].SSTs[0]
			blockCacheTestGet(t, ctx, reader, state, entries, 10)

			const opens = 5
			before := reader.blockCache.cache.Metrics()
			for range opens {
				_, iter, err := reader.openSSTIterBounded(ctx, meta, nil, nil, false)
				if err != nil {
					t.Fatalf("open SST: %v", err)
				}
				_ = iter.Close()
			}
			after := reader.blockCache.cache.Metrics()
			if got := after.Misses - before.Misses; got != metaLookupsPerOpen*opens {
				t.Fatalf("%d opens made %d uncached lookups, want %d",
					opens, got, metaLookupsPerOpen*opens)
			}
		})
	}
}

// TestReader_BlockCache_WarmGetReportsOnlyHits checks that a repeated lookup
// reports its index and data blocks as hits, and no misses.
func TestReader_BlockCache_WarmGetReportsOnlyHits(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("block-cache-warm-stats")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	entries, state := blockCacheTestSST(t, ctx, store, 2_000)

	// A cold lookup misses each index and data block it reads, and caches
	// each; a warm one hits them all.
	blockCacheTestGet(t, ctx, reader, state, entries, 10)
	cold := reader.BlockCacheStats()
	if cold.Hits != 0 || cold.Misses < 2 || cold.Misses != int64(cold.EntryCount) {
		t.Fatalf("cold lookup stats %+v, want one miss per cached block", cold)
	}
	blockCacheTestGet(t, ctx, reader, state, entries, 10)
	warm := reader.BlockCacheStats()
	if hits, misses := warm.Hits-cold.Hits, warm.Misses-cold.Misses; hits != cold.Misses || misses != 0 {
		t.Fatalf("warm lookup: %d hits, %d misses, want %d and 0", hits, misses, cold.Misses)
	}
}

// blockCacheTestSST writes one SST of n entries, with 4 KiB blocks, and a
// manifest listing it at L1.
func blockCacheTestSST(t *testing.T, ctx context.Context, store *blobstore.Store, n int) ([]internal.MemEntry, *manifestState) {
	t.Helper()
	entries := make([]internal.MemEntry, n)
	for i := range entries {
		entries[i] = internal.MemEntry{
			Key: kvLeveledBenchmarkKey(i), Seq: uint64(i + 1), Kind: internal.OpPut,
			Value: []byte(fmt.Sprintf("value-%08d-%s", i, bytes.Repeat([]byte("x"), 100))),
		}
	}
	result, err := writeSST(ctx, &sliceSSTIter{entries: entries},
		sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 12, Compression: "snappy"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
		t.Fatalf("store SST: %v", err)
	}
	meta := result.Meta
	meta.Level = 1
	return entries, &manifestState{Levels: []manifest.Level{{Number: 1, SSTs: []manifest.SSTMeta{meta}}}}
}

func blockCacheTestGet(t *testing.T, ctx context.Context, reader *Reader, state *manifestState, entries []internal.MemEntry, i int) {
	t.Helper()
	value, found, err := reader.getWithManifest(ctx, state, entries[i].Key)
	if err != nil || !found || !bytes.Equal(value, entries[i].Value) {
		t.Fatalf("Get(%d) found=%t err=%v", i, found, err)
	}
}

// TestReader_BlockCache_ScanFillsOnlyItsStart checks, for local and range
// reads, that a full scan adds only about its first scanCacheFillBytes to the
// cache, and that a short scan is cached whole, so repeating it adds nothing
// and needs no request.
func TestReader_BlockCache_ScanFillsOnlyItsStart(t *testing.T) {
	for _, rangeRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("range=%t", rangeRead), func(t *testing.T) {
			ctx := context.Background()
			counts := &kvS3ReadCounts{}
			bucketURL := setupFakeS3BucketURLWithObserver(t, counts.observe)
			store, err := blobstore.Open(ctx, bucketURL, fmt.Sprintf("block-cache-scan-%d", time.Now().UnixNano()))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer store.Close()
			opts := readerOptions{CacheDir: t.TempDir()}
			if rangeRead {
				opts.RangeRead, opts.RangeReadMinSSTSize = true, 1
			}
			reader, err := newReader(ctx, store, opts)
			if err != nil {
				t.Fatalf("open reader: %v", err)
			}
			defer reader.Close()
			entries, state := blockCacheTestSST(t, ctx, store, 20_000)

			kvs, err := reader.scanInternalWithManifest(ctx, state, nil, nil, len(entries))
			if err != nil || len(kvs) != len(entries) {
				t.Fatalf("scan rows=%d err=%v", len(kvs), err)
			}
			// The budget, plus the block read when it ran out and the index
			// blocks the filling iterator read.
			if stats := reader.BlockCacheStats(); stats.Bytes == 0 || stats.Bytes > scanCacheFillBytes+32<<10 {
				t.Fatalf("full scan cached %d bytes, want at most about %d", stats.Bytes, scanCacheFillBytes)
			}

			short := func() {
				t.Helper()
				kvs, err := reader.scanInternalWithManifest(ctx, state, entries[12_345].Key, nil, 10)
				if err != nil || len(kvs) != 10 || !bytes.Equal(kvs[0].Key, entries[12_345].Key) {
					t.Fatalf("short scan rows=%d err=%v", len(kvs), err)
				}
			}
			short()
			before := reader.BlockCacheStats()
			counts.reset()
			short()
			after := reader.BlockCacheStats()
			if after.EntryCount != before.EntryCount || after.Hits == before.Hits {
				t.Fatalf("repeated short scan was not served from the cache: before %+v after %+v", before, after)
			}
			if got := counts.ssts.Load(); got != 0 {
				t.Fatalf("repeated short scan made %d requests, want 0", got)
			}
		})
	}
}

// TestScanSSTSource_SwitchKeepsEveryEntry reads an SST holding several
// versions of each key, and tombstones, with budgets that switch at every
// kind of position, including between versions of one key: each must return
// exactly the entries of a scan that never switches, and seeking after the
// switch must still work.
func TestScanSSTSource_SwitchKeepsEveryEntry(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("scan-switch")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	var entries []internal.MemEntry
	seq := uint64(1 << 20)
	for i := range 3_000 {
		for v := range 3 {
			e := internal.MemEntry{Key: kvLeveledBenchmarkKey(i), Seq: seq, Kind: internal.OpPut,
				Value: []byte(fmt.Sprintf("value-%d-%d-%s", i, v, bytes.Repeat([]byte("x"), 60)))}
			if (i+v)%7 == 0 {
				e.Kind, e.Value = internal.OpDelete, nil
			}
			entries = append(entries, e)
			seq--
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

	type entry struct {
		key     string
		trailer uint64
		value   string
	}
	read := func(budget int64, seekAt int) ([]entry, bool) {
		t.Helper()
		src, err := openScanSSTSource(reader, ctx, result.Meta, nil, nil, budget)
		if err != nil {
			t.Fatalf("open source: %v", err)
		}
		defer src.close()
		var got []entry
		key, value := src.first()
		for key != nil {
			got = append(got, entry{string(key.UserKey), uint64(key.Trailer), string(value)})
			if len(got) == seekAt {
				key, value = src.seekGE(kvLeveledBenchmarkKey(2_000))
				continue
			}
			key, value = src.next()
		}
		if err := src.err(); err != nil {
			t.Fatalf("budget %d: %v", budget, err)
		}
		return got, src.private
	}

	want, switched := read(1<<40, -1)
	if switched || len(want) != len(entries) {
		t.Fatalf("reference scan switched=%t rows=%d, want %d", switched, len(want), len(entries))
	}
	for _, budget := range []int64{0, 1, 90, 1_000, 4_096, 64 << 10} {
		got, switched := read(budget, -1)
		if !switched {
			t.Fatalf("budget %d never switched", budget)
		}
		if len(got) != len(want) {
			t.Fatalf("budget %d: %d rows, want %d", budget, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("budget %d: row %d = %+v, want %+v", budget, i, got[i], want[i])
			}
		}
	}

	// Seek forward after the switch, as a merge does when another source
	// jumps ahead.
	wantSeek, _ := read(1<<40, 5_000)
	gotSeek, switched := read(1_000, 5_000)
	if !switched || len(gotSeek) != len(wantSeek) {
		t.Fatalf("seek after switch: switched=%t rows=%d, want %d", switched, len(gotSeek), len(wantSeek))
	}
	for i := range wantSeek {
		if gotSeek[i] != wantSeek[i] {
			t.Fatalf("seek after switch: row %d = %+v, want %+v", i, gotSeek[i], wantSeek[i])
		}
	}
}

// TestReader_BlockCache_EvictsRetiredAndCorruptSSTs checks that an SST's
// blocks leave the cache when it leaves the manifest or is reported corrupt.
func TestReader_BlockCache_EvictsRetiredAndCorruptSSTs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("block-cache-evict")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	entries, state := blockCacheTestSST(t, ctx, store, 2_000)
	meta := state.Levels[0].SSTs[0]

	blockCacheTestGet(t, ctx, reader, state, entries, 10)
	if reader.BlockCacheStats().EntryCount == 0 {
		t.Fatal("lookup cached nothing")
	}
	reader.blockCache.retain(&manifestState{})
	if stats := reader.BlockCacheStats(); stats.EntryCount != 0 || stats.Bytes != 0 {
		t.Fatalf("retired SST still cached: %+v", stats)
	}

	blockCacheTestGet(t, ctx, reader, state, entries, 10)
	if reader.BlockCacheStats().EntryCount == 0 {
		t.Fatal("lookup cached nothing")
	}
	reader.reportCorruptSST(meta)
	if stats := reader.BlockCacheStats(); stats.EntryCount != 0 || stats.Bytes != 0 {
		t.Fatalf("corrupt SST still cached: %+v", stats)
	}
}

// TestReader_BlockCache_LocalAndRangeReadsShareBlocks reads an SST by range,
// then downloads it: the local read finds the same blocks in the cache.
func TestReader_BlockCache_LocalAndRangeReadsShareBlocks(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("block-cache-share")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(), RangeRead: true, RangeReadMinSSTSize: 1,
	})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	entries, state := blockCacheTestSST(t, ctx, store, 2_000)
	meta := state.Levels[0].SSTs[0]

	blockCacheTestGet(t, ctx, reader, state, entries, 700)
	if err := reader.cacheSST(ctx, &meta, store.SSTPath(meta.ID)); err != nil {
		t.Fatalf("download SST: %v", err)
	}
	if !reader.sstResident(meta) {
		t.Fatal("SST not on local disk")
	}
	before := reader.BlockCacheStats()
	blockCacheTestGet(t, ctx, reader, state, entries, 700)
	after := reader.BlockCacheStats()
	if after.EntryCount != before.EntryCount || after.Misses != before.Misses ||
		after.Hits < before.Hits+2 {
		t.Fatalf("local read did not reuse the range read's blocks: before %+v after %+v", before, after)
	}
}

// TestReader_BlockCache_ConcurrentColdLookupsShareRequests has many lookups of
// one key miss at once: each byte range is fetched once.
func TestReader_BlockCache_ConcurrentColdLookupsShareRequests(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	ranges := map[string]int{}
	bucketURL := setupFakeS3BucketURLWithObserver(t, func(request *http.Request) {
		if request.Method != http.MethodGet || request.Header.Get("Range") == "" {
			return
		}
		mu.Lock()
		ranges[request.URL.Path+" "+request.Header.Get("Range")]++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	})
	store, err := blobstore.Open(ctx, bucketURL, fmt.Sprintf("block-cache-concurrent-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(), RangeRead: true, RangeReadMinSSTSize: 1,
	})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	entries, state := blockCacheTestSST(t, ctx, store, 2_000)
	mu.Lock()
	clear(ranges)
	mu.Unlock()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 16 {
		wg.Go(func() {
			<-start
			blockCacheTestGet(t, ctx, reader, state, entries, 1_234)
		})
	}
	close(start)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(ranges) == 0 {
		t.Fatal("no ranged GETs recorded")
	}
	for r, n := range ranges {
		if n != 1 {
			t.Errorf("%s fetched %d times, want once", r, n)
		}
	}
}

// blockCacheTestSource opens a scan source over the single SST in state, on a
// reader that reads it locally or, with rangeRead, by range from fake S3
// whose ranged GETs counts records.
func blockCacheTestSource(t *testing.T, rangeRead bool) (*Reader, *scanSSTSource, []internal.MemEntry, *kvS3ReadCounts) {
	t.Helper()
	ctx := context.Background()
	counts := &kvS3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(t, counts.observe)
	store, err := blobstore.Open(ctx, bucketURL, fmt.Sprintf("scan-seek-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	opts := readerOptions{CacheDir: t.TempDir()}
	if rangeRead {
		opts.RangeRead, opts.RangeReadMinSSTSize = true, 1
	}
	reader, err := newReader(ctx, store, opts)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	entries, state := blockCacheTestSST(t, ctx, store, 20_000)
	src, err := openScanSSTSource(reader, ctx, state.Levels[0].SSTs[0], nil, nil, scanCacheFillBytes)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	t.Cleanup(func() { _ = src.close() })
	return reader, src, entries, counts
}

// seekAndRead seeks src to entries[i] and reads n rows, checking each key.
// The source returns values as stored, encoded, so only keys are compared.
func seekAndRead(t *testing.T, src *scanSSTSource, entries []internal.MemEntry, i, n int) {
	t.Helper()
	key, _ := src.seekGE(entries[i].Key)
	for j := 0; j < n; j++ {
		if key == nil || !bytes.Equal(key.UserKey, entries[i+j].Key) {
			t.Fatalf("row %d after seek to %d: err=%v", j, i, src.err())
		}
		if j < n-1 {
			key, _ = src.next()
		}
	}
}

// TestScanSSTSource_SeekAfterLongReadIsCached reads past the budget, so the
// source switches to its own buffers, then seeks elsewhere and reads a few
// rows: that read is cached, so repeating it hits only, adds nothing, makes
// no request and does not reopen the SST.
func TestScanSSTSource_SeekAfterLongReadIsCached(t *testing.T) {
	for _, rangeRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("range=%t", rangeRead), func(t *testing.T) {
			reader, src, entries, counts := blockCacheTestSource(t, rangeRead)

			key, _ := src.first()
			for i := 0; key != nil && i < 2_000; i++ {
				key, _ = src.next()
			}
			if !src.private {
				t.Fatal("long read did not switch to private buffers")
			}

			seekAndRead(t, src, entries, 15_000, 3)
			if src.private {
				t.Fatal("seek did not return to filling the cache")
			}
			before, opensBefore := reader.BlockCacheStats(), reader.blockCache.opens.Load()
			counts.reset()
			seekAndRead(t, src, entries, 15_000, 3)
			after := reader.BlockCacheStats()
			if after.EntryCount != before.EntryCount || after.Misses != before.Misses || after.Hits == before.Hits {
				t.Fatalf("repeated short read after a seek not served from the cache: before %+v after %+v", before, after)
			}
			if got := counts.ssts.Load(); got != 0 {
				t.Fatalf("repeated short read made %d requests, want 0", got)
			}
			if got := reader.blockCache.opens.Load() - opensBefore; got != 0 {
				t.Fatalf("seek while filling reopened the SST %d times", got)
			}
		})
	}
}

// TestScanSSTSource_SeekLoopIsCached seeks to many keys, reading one row
// each, as batched lookups through one iterator do: the second round hits
// only and never reopens the SST.
func TestScanSSTSource_SeekLoopIsCached(t *testing.T) {
	for _, rangeRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("range=%t", rangeRead), func(t *testing.T) {
			reader, src, entries, counts := blockCacheTestSource(t, rangeRead)
			round := func() {
				t.Helper()
				for i := 0; i < len(entries); i += 97 {
					seekAndRead(t, src, entries, i, 1)
				}
			}
			round()
			before, opensBefore := reader.BlockCacheStats(), reader.blockCache.opens.Load()
			counts.reset()
			round()
			after := reader.BlockCacheStats()
			if after.EntryCount != before.EntryCount || after.Misses != before.Misses || after.Hits == before.Hits {
				t.Fatalf("repeated seeks not served from the cache: before %+v after %+v", before, after)
			}
			if got := counts.ssts.Load(); got != 0 {
				t.Fatalf("repeated seeks made %d requests, want 0", got)
			}
			if src.private || reader.blockCache.opens.Load() != opensBefore {
				t.Fatalf("seek loop switched (private=%t) or reopened the SST", src.private)
			}
		})
	}
}

// TestScanSSTSource_SeeksAndReadsKeepEveryEntry runs one random script of
// seeks and forward reads, over an SST with several versions of each key and
// tombstones, at budgets that switch in both directions at every kind of
// position: each must return exactly the rows of a source that never
// switches.
func TestScanSSTSource_SeeksAndReadsKeepEveryEntry(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("scan-seek-script")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	var entries []internal.MemEntry
	seq := uint64(1 << 20)
	for i := range 3_000 {
		for v := range 3 {
			e := internal.MemEntry{Key: kvLeveledBenchmarkKey(i), Seq: seq, Kind: internal.OpPut,
				Value: []byte(fmt.Sprintf("value-%d-%d-%s", i, v, bytes.Repeat([]byte("x"), 60)))}
			if (i+v)%7 == 0 {
				e.Kind, e.Value = internal.OpDelete, nil
			}
			entries = append(entries, e)
			seq--
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

	// Each step seeks to a random key, then reads on for up to 1,500 rows,
	// enough to run past the smaller budgets and switch.
	type step struct{ seek, reads int }
	rng := rand.New(rand.NewSource(7))
	script := make([]step, 200)
	for i := range script {
		script[i] = step{seek: rng.Intn(3_100), reads: rng.Intn(1_500)}
	}
	run := func(budget int64) []string {
		t.Helper()
		src, err := openScanSSTSource(reader, ctx, result.Meta, nil, nil, budget)
		if err != nil {
			t.Fatalf("open source: %v", err)
		}
		defer src.close()
		var rows []string
		record := func(key *sstable.InternalKey, value []byte) bool {
			if key == nil {
				rows = append(rows, "<end>")
				return false
			}
			rows = append(rows, fmt.Sprintf("%s#%d=%s", key.UserKey, key.Trailer, value))
			return true
		}
		for _, st := range script {
			if !record(src.seekGE(kvLeveledBenchmarkKey(st.seek))) {
				continue
			}
			for range st.reads {
				if !record(src.next()) {
					break
				}
			}
		}
		if err := src.err(); err != nil {
			t.Fatalf("budget %d: %v", budget, err)
		}
		return rows
	}

	want := run(1 << 40)
	for _, budget := range []int64{1, 90, 1_000, 4_096, 64 << 10} {
		got := run(budget)
		if len(got) != len(want) {
			t.Fatalf("budget %d: %d rows, want %d", budget, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("budget %d: row %d = %s, want %s", budget, i, got[i], want[i])
			}
		}
	}
}
