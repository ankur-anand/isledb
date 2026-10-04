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
	"github.com/cockroachdb/pebble/v2/sstable"
)

// testOpenSST opens a small SST in memory as an openSST.
func testOpenSST(t *testing.T, id string) *openSST {
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
	return &openSST{id: id, reader: reader}
}

func closed(s *openSST) bool { return s.closed.Load() }

func TestOpenSSTCache_ClockAndReferences(t *testing.T) {
	c := newOpenSSTCache(2)
	add := func(id string) *openSST {
		s := testOpenSST(t, id)
		c.add(s).unref()
		return s
	}

	a := add("a")
	b := add("b")
	if s := c.acquire("a"); s == nil { // a has been read since it was cached; b has not
		t.Fatal("a not cached")
	} else {
		s.unref()
	}
	cs := add("c")
	if !closed(b) || closed(a) {
		t.Fatalf("adding c closed a=%t b=%t, want b, the SST not read since it was cached", closed(a), closed(b))
	}

	// An SST in use when evicted stays open until its last user is done.
	held := c.acquire("a") // order: c, a
	d := add("d")          // evicts c
	e := add("e")          // evicts a, still held
	dup := testOpenSST(t, "d")
	c.add(dup).unref() // d is cached: the duplicate closes at once
	if !closed(cs) || !closed(dup) || closed(d) {
		t.Fatalf("closed c=%t duplicate=%t d=%t, want true, true, false", closed(cs), closed(dup), closed(d))
	}
	if c.isOpen("a") {
		t.Fatal("a still cached after two newer adds")
	}
	if closed(a) {
		t.Fatal("a closed while in use")
	}
	held.unref()
	if !closed(a) {
		t.Fatal("a not closed once its last user finished")
	}

	if stats := c.stats(); stats.EntryCount != 2 || stats.MaxEntries != 2 || stats.Evictions != 3 {
		t.Fatalf("stats = %+v, want 2 entries of 2 and 3 evictions", stats)
	}
	c.clear()
	if !closed(d) || !closed(e) || c.stats().EntryCount != 0 {
		t.Fatalf("clear left d=%t e=%t entries=%d", !closed(d), !closed(e), c.stats().EntryCount)
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
// from many goroutines, fetched whole and in chunks, through the one open
// reader.
func TestReader_OpenSSTCache_ConcurrentReads(t *testing.T) {
	for _, rangeRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunked=%t", rangeRead), func(t *testing.T) {
			ctx := context.Background()
			store := blobstore.NewMemory(fmt.Sprintf("open-sst-concurrent-%t", rangeRead))
			defer store.Close()
			reader := newBlockCacheTestReader(t, ctx, store, readerOptions{}, rangeRead)
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

// TestReader_OpenSSTCache_DropsCorruptSSTs checks that an SST reported
// corrupt leaves the cache while an iterator still using it keeps reading
// until it closes, and that an SST leaving the manifest stays open.
func TestReader_OpenSSTCache_DropsCorruptSSTs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("open-sst-retire")
	defer store.Close()
	reader := newBlockCacheTestReader(t, ctx, store, readerOptions{}, false)
	all, state := openSSTTestSSTs(t, ctx, store, 1, 2_000)
	meta := state.Levels[0].SSTs[0]

	_, iter, err := reader.openSSTIterBounded(ctx, meta, nil, nil, false)
	if err != nil {
		t.Fatalf("open iterator: %v", err)
	}
	reader.publishManifestView(&manifestState{}, &manifest.Current{}, time.Now())
	if reader.OpenSSTCacheStats().EntryCount != 1 {
		t.Fatal("SST leaving the manifest was closed")
	}
	reader.dropSST(meta)
	if reader.OpenSSTCacheStats().EntryCount != 0 {
		t.Fatal("corrupt SST still cached")
	}
	rows := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		rows++
	}
	if err := iter.Close(); err != nil || rows != len(all[0]) {
		t.Fatalf("iterator over a dropped SST read %d rows, err=%v; want %d", rows, err, len(all[0]))
	}
	blockCacheTestGet(t, ctx, reader, state, all[0], 10)
	if reader.OpenSSTCacheStats().EntryCount != 1 {
		t.Fatal("SST not reopened after being dropped")
	}
}
