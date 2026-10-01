package isledb

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// testOpenSST opens a small SST in memory as an openSST whose release counts
// into closed.
func testOpenSST(t *testing.T, id string, closed *atomic.Int32) *openSST {
	t.Helper()
	entries := []internal.MemEntry{{Key: []byte("k"), Seq: 1, Kind: internal.OpPut, Value: []byte("v")}}
	result, err := writeSST(context.Background(), &sliceSSTIter{entries: entries},
		sstWriterOptions{BlockSize: 4096, Compression: "snappy"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}
	reader, err := sstable.NewReader(context.Background(),
		newMemReadable(result.SSTData[:result.Meta.Size]), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return &openSST{id: id, reader: reader, release: func() { closed.Add(1) }}
}

func TestOpenSSTCache_LRUAndReferences(t *testing.T) {
	c := newOpenSSTCache(2)
	var closedA, closedB, closedC, closedD, closedE, closedDup atomic.Int32

	c.add(testOpenSST(t, "a", &closedA)).unref()
	c.add(testOpenSST(t, "b", &closedB)).unref()
	if s := c.acquire("a"); s == nil { // a is now the most recently used
		t.Fatal("a not cached")
	} else {
		s.unref()
	}
	c.add(testOpenSST(t, "c", &closedC)).unref()
	if closedB.Load() != 1 || closedA.Load() != 0 {
		t.Fatalf("adding c closed a=%d b=%d, want the least recently used, b", closedA.Load(), closedB.Load())
	}

	// An SST in use when evicted stays open until its last user is done.
	held := c.acquire("a")                         // order: c, a
	c.add(testOpenSST(t, "d", &closedD)).unref()   // evicts c
	c.add(testOpenSST(t, "e", &closedE)).unref()   // evicts a, still held
	c.add(testOpenSST(t, "d", &closedDup)).unref() // d is cached: the duplicate closes at once
	if closedC.Load() != 1 || closedDup.Load() != 1 || closedD.Load() != 0 {
		t.Fatalf("closed c=%d duplicate=%d d=%d, want 1, 1, 0", closedC.Load(), closedDup.Load(), closedD.Load())
	}
	if _, ok := c.entries["a"]; ok {
		t.Fatal("a still cached after two newer adds")
	}
	if closedA.Load() != 0 {
		t.Fatal("a closed while in use")
	}
	held.unref()
	if closedA.Load() != 1 {
		t.Fatal("a not closed once its last user finished")
	}

	if stats := c.stats(); stats.EntryCount != 2 || stats.MaxEntries != 2 || stats.Evictions != 3 {
		t.Fatalf("stats = %+v, want 2 entries of 2 and 3 evictions", stats)
	}
	c.clear()
	if closedD.Load() != 1 || closedE.Load() != 1 || c.stats().EntryCount != 0 {
		t.Fatalf("clear left d=%d e=%d entries=%d", closedD.Load(), closedE.Load(), c.stats().EntryCount)
	}
}

// openSSTTestSSTs writes count SSTs of n entries each, with disjoint keys,
// and a manifest listing them at L1.
func openSSTTestSSTs(t *testing.T, ctx context.Context, store *blobstore.Store, count, n int) ([][]internal.MemEntry, *manifestState) {
	t.Helper()
	state := &manifestState{Levels: []manifest.Level{{Number: 1}}}
	var all [][]internal.MemEntry
	for s := range count {
		entries := make([]internal.MemEntry, n)
		for i := range entries {
			entries[i] = internal.MemEntry{
				Key: kvLeveledBenchmarkKey(s*n + i), Seq: uint64(s*n + i + 1), Kind: internal.OpPut,
				Value: []byte(fmt.Sprintf("value-%08d-%s", s*n+i, bytes.Repeat([]byte("x"), 100))),
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
		state.Levels[0].SSTs = append(state.Levels[0].SSTs, meta)
		all = append(all, entries)
	}
	return all, state
}

// TestReader_OpenSSTCache_ConcurrentReads runs lookups and scans of one SST
// from many goroutines, locally and by range, through the one open reader.
func TestReader_OpenSSTCache_ConcurrentReads(t *testing.T) {
	for _, rangeRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("range=%t", rangeRead), func(t *testing.T) {
			ctx := context.Background()
			store := blobstore.NewMemory(fmt.Sprintf("open-sst-concurrent-%t", rangeRead))
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
			all, state := openSSTTestSSTs(t, ctx, store, 1, 5_000)
			entries := all[0]

			var wg sync.WaitGroup
			errs := make(chan error, 16)
			for g := range 16 {
				wg.Go(func() {
					for i := range 200 {
						k := (g*331 + i*17) % len(entries)
						value, found, err := reader.getWithManifest(ctx, state, entries[k].Key)
						if err != nil || !found || !bytes.Equal(value, entries[k].Value) {
							errs <- fmt.Errorf("get %d: found=%t err=%v", k, found, err)
							return
						}
						if i%20 == 0 {
							kvs, err := reader.scanInternalWithManifest(ctx, state, entries[k].Key, nil, 50)
							if err != nil || len(kvs) != min(50, len(entries)-k) {
								errs <- fmt.Errorf("scan %d: rows=%d err=%v", k, len(kvs), err)
								return
							}
						}
					}
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
			if stats := reader.OpenSSTCacheStats(); stats.EntryCount != 1 || stats.Hits == 0 {
				t.Fatalf("open SST cache stats = %+v, want one SST, opened once and reused", stats)
			}
		})
	}
}

// TestReader_OpenSSTCache_DiskEvictionClosesFile downloads two SSTs into a
// disk cache that holds only one: evicting the first file drops its open SST,
// which closes, so the file's space is freed.
func TestReader_OpenSSTCache_DiskEvictionClosesFile(t *testing.T) {
	ctx := context.Background()
	// The same SSTs built in a scratch store give their sizes, to size a disk
	// cache that holds one but not both. The reader must open before its
	// store holds any SSTs.
	scratch := blobstore.NewMemory("open-sst-disk-evict-sizes")
	_, sized := openSSTTestSSTs(t, ctx, scratch, 2, 2_000)
	_ = scratch.Close()
	store := blobstore.NewMemory("open-sst-disk-evict")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir:     t.TempDir(),
		SSTCacheSize: sized.Levels[0].SSTs[0].Size + sized.Levels[0].SSTs[1].Size - 1,
	})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	all, state := openSSTTestSSTs(t, ctx, store, 2, 2_000)
	first := state.Levels[0].SSTs[0]

	blockCacheTestGet(t, ctx, reader, state, all[0], 10)
	held := reader.openSSTs.acquire(first.ID)
	if held == nil || !held.local {
		t.Fatal("first SST not held open locally")
	}
	held.unref()

	blockCacheTestGet(t, ctx, reader, state, all[1], 10)
	if reader.sstResident(first) {
		t.Fatal("disk cache kept both SSTs; the test needs it to evict the first")
	}
	if _, ok := reader.openSSTs.entries[first.ID]; ok {
		t.Fatal("evicted file's SST still open in the cache")
	}
	if refs := held.refs.Load(); refs != 0 {
		t.Fatalf("evicted file's SST has %d references, want closed", refs)
	}
	blockCacheTestGet(t, ctx, reader, state, all[0], 10) // reopens: downloads again
}

// TestReader_OpenSSTCache_RemoteBecomesLocal reads an SST by range, then
// downloads it: the next read reopens it from the local file.
func TestReader_OpenSSTCache_RemoteBecomesLocal(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("open-sst-remote-local")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(), RangeRead: true, RangeReadMinSSTSize: 1,
	})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	all, state := openSSTTestSSTs(t, ctx, store, 1, 2_000)
	meta := state.Levels[0].SSTs[0]

	isLocal := func() bool {
		t.Helper()
		s := reader.openSSTs.acquire(meta.ID)
		if s == nil {
			t.Fatal("SST not open")
		}
		defer s.unref()
		return s.local
	}
	blockCacheTestGet(t, ctx, reader, state, all[0], 10)
	if isLocal() {
		t.Fatal("SST open locally before it was downloaded")
	}
	if err := reader.cacheSST(ctx, &meta, store.SSTPath(meta.ID)); err != nil {
		t.Fatalf("download SST: %v", err)
	}
	blockCacheTestGet(t, ctx, reader, state, all[0], 10)
	if !isLocal() {
		t.Fatal("downloaded SST still read by range")
	}
}

// TestReader_OpenSSTCache_DropsRetiredAndCorruptSSTs checks that an SST
// leaving the manifest, or reported corrupt, leaves the cache, and that an
// iterator still using it keeps reading until it closes.
func TestReader_OpenSSTCache_DropsRetiredAndCorruptSSTs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("open-sst-retire")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	all, state := openSSTTestSSTs(t, ctx, store, 1, 2_000)
	meta := state.Levels[0].SSTs[0]

	_, iter, err := reader.openSSTIterBounded(ctx, meta, nil, nil, false)
	if err != nil {
		t.Fatalf("open iterator: %v", err)
	}
	reader.retainSSTs(&manifestState{})
	if reader.OpenSSTCacheStats().EntryCount != 0 {
		t.Fatal("retired SST still cached")
	}
	rows := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		rows++
	}
	if err := iter.Close(); err != nil || rows != len(all[0]) {
		t.Fatalf("iterator over a retired SST read %d rows, err=%v; want %d", rows, err, len(all[0]))
	}

	blockCacheTestGet(t, ctx, reader, state, all[0], 10)
	if reader.OpenSSTCacheStats().EntryCount != 1 {
		t.Fatal("SST not reopened after retirement")
	}
	reader.reportCorruptSST(meta)
	if reader.OpenSSTCacheStats().EntryCount != 0 {
		t.Fatal("corrupt SST still cached")
	}
}

// TestSSTRangeReadable_MetaRegionNotPinnedWhenCacheable checks that an open
// range readable takes its metadata from the metadata cache rather than
// keeping its own copy, unless the region is too large for that cache.
func TestSSTRangeReadable_MetaRegionNotPinnedWhenCacheable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cache    int64
		wantPin  bool
		wantGets int64
	}{
		{name: "fits", cache: 1 << 20, wantPin: false, wantGets: 1},
		{name: "too large", cache: 100, wantPin: true, wantGets: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newChunkTestStore(t, 5000)
			r := s.readable(t, 1000)
			r.useMetaRegion(4500)
			r.useMetaCache(newSSTMetaCache(tc.cache))
			for range 3 {
				if got := readChunked(t, r, 4600, 100); !bytes.Equal(got, s.data[4600:4700]) {
					t.Fatal("metadata bytes differ")
				}
			}
			if pinned := r.metaBytes != nil; pinned != tc.wantPin {
				t.Fatalf("region pinned=%t, want %t", pinned, tc.wantPin)
			}
			if got := s.gets.Load(); got != tc.wantGets {
				t.Fatalf("GETs = %d, want %d", got, tc.wantGets)
			}
		})
	}
}
