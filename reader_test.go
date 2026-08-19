package isledb

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func writeTestSST(t *testing.T, ctx context.Context, store *blobstore.Store, ms *manifest.Store, entries []internal.MemEntry, level int, epoch uint64) writeSSTResult {
	t.Helper()

	if ms.WriterEpoch() == 0 {
		if _, err := ms.ClaimWriter(ctx, "reader-test-writer"); err != nil {
			t.Fatalf("claim writer fence: %v", err)
		}
	}

	it := &sliceSSTIter{entries: entries}
	res, err := writeSST(ctx, it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, epoch)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}

	if _, err := store.Write(ctx, store.SSTPath(res.Meta.ID), res.SSTData); err != nil {
		t.Fatalf("write sst: %v", err)
	}

	if _, err := ms.AppendAddSSTableWithFence(ctx, res.Meta); err != nil {
		t.Fatalf("append manifest log: %v", err)
	}
	if level > 0 {
		if ms.CompactorEpoch() == 0 {
			if _, err := ms.ClaimCompactor(ctx, "reader-test-compactor"); err != nil {
				t.Fatalf("claim compactor fence: %v", err)
			}
		}
		for destination := uint32(1); destination <= uint32(level); destination++ {
			res.Meta.Level = destination
			if _, err := ms.AppendCompactionWithFence(ctx, manifest.CompactionLogPayload{
				RemoveSSTableIDs: []string{res.Meta.ID},
				SourceLevel:      destination - 1,
				DestinationLevel: destination,
				AddSSTables:      []manifest.SSTMeta{res.Meta},
			}, nil); err != nil {
				t.Fatalf("promote test sst to L%d: %v", destination, err)
			}
		}
	}

	return res
}

func setupReaderFixture(t *testing.T) (*Reader, context.Context, func()) {
	t.Helper()

	ctx := context.Background()
	store := blobstore.NewMemory("reader-test")
	ms := manifest.NewStore(store)
	if _, err := ms.ClaimWriter(ctx, "reader-test-writer"); err != nil {
		t.Fatalf("claim writer fence: %v", err)
	}

	l1Entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("l1-a")},
		{Key: []byte("c"), Seq: 1, Kind: internal.OpPut, Value: []byte("l1-c")},
		{Key: []byte("d"), Seq: 1, Kind: internal.OpPut, Value: []byte("l1-d")},
	}
	writeTestSST(t, ctx, store, ms, l1Entries, 1, 1)

	l0Entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 3, Kind: internal.OpDelete},
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Value: []byte("l0-b")},
		{Key: []byte("e"), Seq: 2, Kind: internal.OpPut, Value: []byte("l0-e")},
	}
	writeTestSST(t, ctx, store, ms, l0Entries, 0, 2)

	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		store.Close()
		t.Fatalf("newReader: %v", err)
	}

	cleanup := func() {
		_ = reader.Close()
		_ = store.Close()
	}

	return reader, ctx, cleanup
}

func TestReader_Get_MultiLevelValues(t *testing.T) {
	reader, ctx, cleanup := setupReaderFixture(t)
	defer cleanup()

	if _, found, err := reader.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("Get a: %v", err)
	} else if found {
		t.Fatalf("expected a to be deleted")
	}

	if value, found, err := reader.Get(ctx, []byte("b")); err != nil {
		t.Fatalf("Get b: %v", err)
	} else if !found || !bytes.Equal(value, []byte("l0-b")) {
		t.Fatalf("unexpected b value: %q found=%v", value, found)
	}

	if value, found, err := reader.Get(ctx, []byte("c")); err != nil {
		t.Fatalf("Get c: %v", err)
	} else if !found || !bytes.Equal(value, []byte("l1-c")) {
		t.Fatalf("unexpected c value: %q found=%v", value, found)
	}

	if value, found, err := reader.Get(ctx, []byte("e")); err != nil {
		t.Fatalf("Get e: %v", err)
	} else if !found || !bytes.Equal(value, []byte("l0-e")) {
		t.Fatalf("unexpected e value: %q found=%v", value, found)
	}

	if _, found, err := reader.Get(ctx, []byte("z")); err != nil {
		t.Fatalf("Get z: %v", err)
	} else if found {
		t.Fatalf("expected z to be missing")
	}
}

func TestReader_Scan_Range(t *testing.T) {
	reader, ctx, cleanup := setupReaderFixture(t)
	defer cleanup()

	results, err := reader.Scan(ctx, []byte("a"), []byte("d"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	expected := []KV{
		{Key: []byte("b"), Value: []byte("l0-b")},
		{Key: []byte("c"), Value: []byte("l1-c")},
		{Key: []byte("d"), Value: []byte("l1-d")},
	}
	if len(results) != len(expected) {
		t.Fatalf("scan length: got %d want %d", len(results), len(expected))
	}
	for i := range expected {
		if !bytes.Equal(results[i].Key, expected[i].Key) {
			t.Fatalf("scan key[%d]: got %q want %q", i, results[i].Key, expected[i].Key)
		}
		if !bytes.Equal(results[i].Value, expected[i].Value) {
			t.Fatalf("scan value[%d]: got %q want %q", i, results[i].Value, expected[i].Value)
		}
	}
}

func TestReader_Scan_DedupesL0BySeq(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-dedupe")
	defer store.Close()

	ms := manifest.NewStore(store)

	oldEntries := []internal.MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: internal.OpPut, Value: []byte("old")},
	}
	writeTestSST(t, ctx, store, ms, oldEntries, 0, 1)

	newEntries := []internal.MemEntry{
		{Key: []byte("k"), Seq: 3, Kind: internal.OpPut, Value: []byte("new")},
	}
	writeTestSST(t, ctx, store, ms, newEntries, 0, 2)

	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	results, err := reader.Scan(ctx, []byte("k"), []byte("k"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("scan length: got %d want 1", len(results))
	}
	if !bytes.Equal(results[0].Key, []byte("k")) {
		t.Fatalf("scan key: got %q want %q", results[0].Key, []byte("k"))
	}
	if !bytes.Equal(results[0].Value, []byte("new")) {
		t.Fatalf("scan value: got %q want %q", results[0].Value, []byte("new"))
	}
}

func TestReader_Refresh_PicksUpNewSSTs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-refresh")
	defer store.Close()

	ms := manifest.NewStore(store)
	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	if _, found, err := reader.Get(ctx, []byte("x")); err != nil {
		t.Fatalf("Get before refresh: %v", err)
	} else if found {
		t.Fatalf("expected key to be missing before refresh")
	}

	entries := []internal.MemEntry{
		{Key: []byte("x"), Seq: 5, Kind: internal.OpPut, Value: []byte("value")},
	}
	writeTestSST(t, ctx, store, ms, entries, 0, 1)

	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	if value, found, err := reader.Get(ctx, []byte("x")); err != nil {
		t.Fatalf("Get after refresh: %v", err)
	} else if !found || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("unexpected value after refresh: %q found=%v", value, found)
	}
}

func TestReader_Get_L0PrefersNewerSeq(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-get-l0")
	defer store.Close()

	ms := manifest.NewStore(store)

	older := []internal.MemEntry{
		{Key: []byte("k"), Seq: 2, Kind: internal.OpPut, Value: []byte("old")},
	}
	writeTestSST(t, ctx, store, ms, older, 0, 1)

	newer := []internal.MemEntry{
		{Key: []byte("k"), Seq: 5, Kind: internal.OpPut, Value: []byte("new")},
	}
	writeTestSST(t, ctx, store, ms, newer, 0, 2)

	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	value, found, err := reader.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || !bytes.Equal(value, []byte("new")) {
		t.Fatalf("unexpected value: %q found=%v", value, found)
	}
}

func TestReader_Scan_L1NonOverlapping(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-scan-l1")
	defer store.Close()

	ms := manifest.NewStore(store)

	first := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
		{Key: []byte("b"), Seq: 1, Kind: internal.OpPut, Value: []byte("vb")},
	}
	writeTestSST(t, ctx, store, ms, first, 1, 1)

	second := []internal.MemEntry{
		{Key: []byte("d"), Seq: 2, Kind: internal.OpPut, Value: []byte("vd")},
		{Key: []byte("e"), Seq: 2, Kind: internal.OpPut, Value: []byte("ve")},
	}
	writeTestSST(t, ctx, store, ms, second, 1, 2)

	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	results, err := reader.Scan(ctx, []byte("a"), []byte("e"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("scan length: got %d want 4", len(results))
	}
	if !bytes.Equal(results[0].Key, []byte("a")) || !bytes.Equal(results[0].Value, []byte("va")) {
		t.Fatalf("scan[0]: %q=%q", results[0].Key, results[0].Value)
	}
	if !bytes.Equal(results[1].Key, []byte("b")) || !bytes.Equal(results[1].Value, []byte("vb")) {
		t.Fatalf("scan[1]: %q=%q", results[1].Key, results[1].Value)
	}
	if !bytes.Equal(results[2].Key, []byte("d")) || !bytes.Equal(results[2].Value, []byte("vd")) {
		t.Fatalf("scan[2]: %q=%q", results[2].Key, results[2].Value)
	}
	if !bytes.Equal(results[3].Key, []byte("e")) || !bytes.Equal(results[3].Value, []byte("ve")) {
		t.Fatalf("scan[3]: %q=%q", results[3].Key, results[3].Value)
	}
}

func TestReader_ScanLimit_LazilyOpensSortedLevel(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-scan-limit-lazy-level")
	defer store.Close()

	ms := manifest.NewStore(store)
	first := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 1, 1)
	second := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("m"), Seq: 2, Kind: internal.OpPut, Value: []byte("vm")},
	}, 1, 2)
	third := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("z"), Seq: 3, Kind: internal.OpPut, Value: []byte("vz")},
	}, 1, 3)
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	results, err := reader.ScanLimit(ctx, nil, nil, 1)
	if err != nil {
		t.Fatalf("ScanLimit: %v", err)
	}
	if len(results) != 1 || !bytes.Equal(results[0].Key, []byte("a")) {
		t.Fatalf("ScanLimit result: %+v", results)
	}
	if !reader.sstCached(first.Meta.ID) {
		t.Fatal("first L1 SST was not opened")
	}
	if reader.sstCached(second.Meta.ID) {
		t.Fatal("second L1 SST was opened after the scan reached its limit")
	}
	if reader.sstCached(third.Meta.ID) {
		t.Fatal("third L1 SST was opened before the scan reached it")
	}
}

func TestReader_ScanLimit_DoesNotReadPastLimit(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-scan-limit-no-read-ahead")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 1, 1)
	second := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("z"), Seq: 2, Kind: internal.OpPut, Value: []byte("vz")},
	}, 1, 2)
	if err := store.Delete(ctx, store.SSTPath(second.Meta.ID)); err != nil {
		t.Fatalf("delete second SST: %v", err)
	}

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	results, err := reader.ScanLimit(ctx, nil, nil, 1)
	if err != nil {
		t.Fatalf("ScanLimit read beyond its limit: %v", err)
	}
	if len(results) != 1 || !bytes.Equal(results[0].Key, []byte("a")) {
		t.Fatalf("ScanLimit result: %+v", results)
	}
}

func TestReader_IteratorReturnsCurrentBeforeLaterSSTError(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-iterator-later-sst-error")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 1, 1)
	second := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("z"), Seq: 2, Kind: internal.OpPut, Value: []byte("vz")},
	}, 1, 2)
	if err := store.Delete(ctx, store.SSTPath(second.Meta.ID)); err != nil {
		t.Fatalf("delete second SST: %v", err)
	}

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIterator(ctx, IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer iter.Close()

	if !iter.Next() {
		t.Fatalf("first Next was suppressed by a later SST error: %v", iter.Err())
	}
	if got := iter.Key(); !bytes.Equal(got, []byte("a")) {
		t.Fatalf("first key: got %q want a", got)
	}
	if iter.Next() {
		t.Fatal("second Next unexpectedly succeeded")
	}
	if err := iter.Err(); err == nil {
		t.Fatal("later SST error was not reported through Iterator.Err")
	}
}

func TestReader_Iterator_SeekGESkipsEarlierSortedLevelSSTs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-seek-lazy-level")
	defer store.Close()

	ms := manifest.NewStore(store)
	first := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 1, 1)
	second := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("m"), Seq: 2, Kind: internal.OpPut, Value: []byte("vm")},
	}, 1, 2)
	third := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("z"), Seq: 3, Kind: internal.OpPut, Value: []byte("vz")},
	}, 1, 3)
	l0 := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("b"), Seq: 4, Kind: internal.OpPut, Value: []byte("vb")},
	}, 0, 4)

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIterator(ctx, IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer iter.Close()

	// Construction alone must not perform object I/O. Otherwise a subsequent
	// seek pays for the first SST before jumping to the target SST.
	if reader.sstCached(first.Meta.ID) {
		t.Fatal("first L1 SST was opened before the iterator was positioned")
	}
	if reader.sstCached(second.Meta.ID) {
		t.Fatal("middle L1 SST was opened before the iterator was positioned")
	}
	if reader.sstCached(third.Meta.ID) {
		t.Fatal("target L1 SST was opened before the iterator was positioned")
	}
	if reader.sstCached(l0.Meta.ID) {
		t.Fatal("L0 SST was opened before the iterator was positioned")
	}
	if !iter.SeekGE([]byte("z")) {
		t.Fatalf("SeekGE(z): %v", iter.Err())
	}
	if got := iter.Key(); !bytes.Equal(got, []byte("z")) {
		t.Fatalf("SeekGE key: got %q want z", got)
	}
	if reader.sstCached(second.Meta.ID) {
		t.Fatal("middle L1 SST was opened by a seek that skipped over it")
	}
	if reader.sstCached(first.Meta.ID) {
		t.Fatal("first L1 SST was opened by a seek that skipped over it")
	}
	if !reader.sstCached(third.Meta.ID) {
		t.Fatal("target L1 SST was not opened")
	}
	if reader.sstCached(l0.Meta.ID) {
		t.Fatal("L0 SST below the seek target was opened")
	}
}

func TestReader_IteratorReportsInitialLazyOpenError(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-iterator-initial-open-error")
	defer store.Close()

	ms := manifest.NewStore(store)
	first := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 1, 1)
	if err := store.Delete(ctx, store.SSTPath(first.Meta.ID)); err != nil {
		t.Fatalf("delete first SST: %v", err)
	}

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIterator(ctx, IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator should remain lazy: %v", err)
	}
	defer iter.Close()
	if iter.Next() {
		t.Fatal("Next unexpectedly succeeded for a missing first SST")
	}
	if err := iter.Err(); err == nil {
		t.Fatal("initial lazy-open error was not reported through Iterator.Err")
	}
}

func TestLevelMergeIteratorSourceCopiesSelectedMetadata(t *testing.T) {
	all := make([]sstMetadata, 1024)
	all[500].ID = "selected"
	selected := all[500:501]

	source := newLevelMergeIteratorSource(nil, context.Background(), selected, nil, nil)
	if len(source.ssts) != 1 {
		t.Fatalf("source metadata length=%d want=1", len(source.ssts))
	}
	if cap(source.ssts) != len(source.ssts) {
		t.Fatalf("source retains level backing array: len=%d cap=%d", len(source.ssts), cap(source.ssts))
	}

	all[500].ID = "mutated"
	if source.ssts[0].ID != "selected" {
		t.Fatalf("source metadata aliases manifest storage: got ID %q", source.ssts[0].ID)
	}
}

func TestReader_Get_TombstoneShadowsLowerLevel(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-tombstone-get")
	defer store.Close()

	ms := manifest.NewStore(store)

	l2Entries := []internal.MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: internal.OpPut, Value: []byte("old")},
	}
	writeTestSST(t, ctx, store, ms, l2Entries, 2, 1)

	l1Entries := []internal.MemEntry{
		{Key: []byte("k"), Seq: 5, Kind: internal.OpDelete},
	}
	writeTestSST(t, ctx, store, ms, l1Entries, 1, 2)

	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	if _, found, err := reader.Get(ctx, []byte("k")); err != nil {
		t.Fatalf("Get: %v", err)
	} else if found {
		t.Fatalf("expected k to be deleted")
	}
}

func TestReader_Scan_TombstoneShadowsLowerLevel(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-tombstone-scan")
	defer store.Close()

	ms := manifest.NewStore(store)

	l2Entries := []internal.MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: internal.OpPut, Value: []byte("old")},
		{Key: []byte("m"), Seq: 1, Kind: internal.OpPut, Value: []byte("keep")},
	}
	writeTestSST(t, ctx, store, ms, l2Entries, 2, 1)

	l1Entries := []internal.MemEntry{
		{Key: []byte("k"), Seq: 5, Kind: internal.OpDelete},
	}
	writeTestSST(t, ctx, store, ms, l1Entries, 1, 2)

	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	results, err := reader.Scan(ctx, []byte("k"), []byte("m"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("scan length: got %d want 1", len(results))
	}
	if !bytes.Equal(results[0].Key, []byte("m")) || !bytes.Equal(results[0].Value, []byte("keep")) {
		t.Fatalf("scan result: %q=%q", results[0].Key, results[0].Value)
	}
}

func TestReader_ChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-checksum")
	ms := manifest.NewStore(store)
	if _, err := ms.ClaimWriter(ctx, "reader-checksum-writer"); err != nil {
		t.Fatalf("claim writer fence: %v", err)
	}

	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("v")},
	}

	it := &sliceSSTIter{entries: entries}
	res, err := writeSST(ctx, it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}
	res.Meta.Level = 0

	sstPath := store.SSTPath(res.Meta.ID)
	if _, err := store.Write(ctx, sstPath, res.SSTData); err != nil {
		t.Fatalf("write sst: %v", err)
	}
	if _, err := ms.AppendAddSSTableWithFence(ctx, res.Meta); err != nil {
		t.Fatalf("append manifest log: %v", err)
	}

	corrupt := append([]byte(nil), res.SSTData...)
	if len(corrupt) == 0 {
		t.Fatalf("unexpected empty sst data")
	}
	corrupt[0] ^= 0xff
	if _, err := store.Write(ctx, sstPath, corrupt); err != nil {
		t.Fatalf("write corrupt sst: %v", err)
	}

	cacheDir := t.TempDir()
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir:            cacheDir,
		ValidateSSTChecksum: true,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("newReader: %v", err)
	}
	defer func() {
		_ = reader.Close()
		_ = store.Close()
	}()

	if _, _, err := reader.Get(ctx, []byte("a")); err == nil {
		t.Fatalf("expected checksum mismatch error")
	} else if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReader_SSTCacheReleaseOnIteratorClose(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-cache-release")
	ms := manifest.NewStore(store)

	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}
	res := writeTestSST(t, ctx, store, ms, entries, 0, 1)

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		store.Close()
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()
	defer store.Close()

	path := store.SSTPath(res.Meta.ID)

	_, iter, err := reader.openSSTIterBounded(ctx, res.Meta, nil, nil)
	if err != nil {
		t.Fatalf("openSSTIterBounded: %v", err)
	}

	if _, ok := reader.sstCache.Acquire(path); !ok {
		iter.Close()
		t.Fatalf("expected sst cache entry after iterator open")
	} else {
		reader.sstCache.Release(path)
	}

	reader.sstCache.Remove(path)
	if _, ok := reader.sstCache.Acquire(path); !ok {
		iter.Close()
		t.Fatalf("expected sst cache entry to remain while iterator open")
	} else {
		reader.sstCache.Release(path)
	}

	if err := iter.Close(); err != nil {
		t.Fatalf("iter close: %v", err)
	}

	if _, ok := reader.sstCache.Acquire(path); ok {
		reader.sstCache.Release(path)
		t.Fatalf("expected sst cache entry removed after iterator close")
	}
}

func TestReader_MetricsGetScanRefresh(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-metrics")
	defer store.Close()

	ms := manifest.NewStore(store)
	if _, err := ms.ClaimWriter(ctx, "reader-metrics-writer"); err != nil {
		t.Fatalf("claim writer fence: %v", err)
	}

	l1Entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("l1-a")},
		{Key: []byte("c"), Seq: 1, Kind: internal.OpPut, Value: []byte("l1-c")},
		{Key: []byte("d"), Seq: 1, Kind: internal.OpPut, Value: []byte("l1-d")},
	}
	writeTestSST(t, ctx, store, ms, l1Entries, 1, 1)

	l0Entries := []internal.MemEntry{
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Value: []byte("l0-b")},
	}
	writeTestSST(t, ctx, store, ms, l0Entries, 0, 2)

	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(),
		Metrics:  metrics,
	})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	if _, found, err := reader.Get(ctx, []byte("b")); err != nil || !found {
		t.Fatalf("Get hit failed: found=%v err=%v", found, err)
	}
	if _, found, err := reader.Get(ctx, []byte("missing")); err != nil || found {
		t.Fatalf("Get miss failed: found=%v err=%v", found, err)
	}
	if _, _, err := reader.Get(ctx, nil); err == nil {
		t.Fatalf("expected Get error for empty key")
	}

	results, err := reader.Scan(ctx, []byte("a"), []byte("d"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("unexpected Scan result count: got=%d want=4", len(results))
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := reader.Scan(cancelCtx, []byte("a"), []byte("d")); err == nil {
		t.Fatalf("expected Scan error with canceled context")
	}

	results, err = reader.ScanLimit(ctx, []byte("a"), []byte("d"), 2)
	if err != nil {
		t.Fatalf("ScanLimit: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("unexpected ScanLimit result count: got=%d want=2", len(results))
	}

	if _, err := reader.ScanLimit(cancelCtx, []byte("a"), []byte("d"), 2); err == nil {
		t.Fatalf("expected ScanLimit error with canceled context")
	}

	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	if got := testutil.ToFloat64(metrics.GetTotal); got != 3 {
		t.Fatalf("get_total mismatch: got=%v want=3", got)
	}
	if got := testutil.ToFloat64(metrics.GetHits); got != 1 {
		t.Fatalf("get_hits_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.GetMisses); got != 1 {
		t.Fatalf("get_misses_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.GetErrors); got != 1 {
		t.Fatalf("get_errors_total mismatch: got=%v want=1", got)
	}

	if got := testutil.ToFloat64(metrics.ScanTotal); got != 2 {
		t.Fatalf("scan_total mismatch: got=%v want=2", got)
	}
	if got := testutil.ToFloat64(metrics.ScanErrors); got != 1 {
		t.Fatalf("scan_errors_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.ScanResults); got != 4 {
		t.Fatalf("scan_results_total mismatch: got=%v want=4", got)
	}

	if got := testutil.ToFloat64(metrics.ScanLimitTotal); got != 2 {
		t.Fatalf("scan_limit_total mismatch: got=%v want=2", got)
	}
	if got := testutil.ToFloat64(metrics.ScanLimitErrors); got != 1 {
		t.Fatalf("scan_limit_errors_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.ScanLimitResults); got != 2 {
		t.Fatalf("scan_limit_results_total mismatch: got=%v want=2", got)
	}

	if got := testutil.ToFloat64(metrics.RefreshTotal); got != 1 {
		t.Fatalf("refresh_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.RefreshErrors); got != 0 {
		t.Fatalf("refresh_errors_total mismatch: got=%v want=0", got)
	}
}

func TestReader_MetricsSSTCacheAndDownload(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-metrics-sst-cache")
	defer store.Close()

	ms := manifest.NewStore(store)
	entries := []internal.MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: internal.OpPut, Value: []byte("v")},
	}
	_ = writeTestSST(t, ctx, store, ms, entries, 0, 1)

	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(),
		Metrics:  metrics,
	})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	if _, found, err := reader.Get(ctx, []byte("k")); err != nil || !found {
		t.Fatalf("Get #1 failed: found=%v err=%v", found, err)
	}
	if _, found, err := reader.Get(ctx, []byte("k")); err != nil || !found {
		t.Fatalf("Get #2 failed: found=%v err=%v", found, err)
	}

	if got := testutil.ToFloat64(metrics.SSTCacheMisses); got != 1 {
		t.Fatalf("sst_cache_misses_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTCacheHits); got != 1 {
		t.Fatalf("sst_cache_hits_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTDownloadTotal); got != 1 {
		t.Fatalf("sst_download_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTDownloadErrors); got != 0 {
		t.Fatalf("sst_download_errors_total mismatch: got=%v want=0", got)
	}
	if got := testutil.ToFloat64(metrics.SSTDownloadBytes); got <= 0 {
		t.Fatalf("sst_download_bytes_total must be > 0, got=%v", got)
	}
}

func TestReader_MetricsSSTDownloadError(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-metrics-sst-download-error")
	defer store.Close()

	ms := manifest.NewStore(store)
	entries := []internal.MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: internal.OpPut, Value: []byte("v")},
	}
	res := writeTestSST(t, ctx, store, ms, entries, 0, 1)
	if err := store.Delete(ctx, store.SSTPath(res.Meta.ID)); err != nil {
		t.Fatalf("delete sst: %v", err)
	}

	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(),
		Metrics:  metrics,
	})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	if _, _, err := reader.Get(ctx, []byte("k")); err == nil {
		t.Fatalf("expected Get error with missing sst object")
	}

	if got := testutil.ToFloat64(metrics.SSTCacheMisses); got != 1 {
		t.Fatalf("sst_cache_misses_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTCacheHits); got != 0 {
		t.Fatalf("sst_cache_hits_total mismatch: got=%v want=0", got)
	}
	if got := testutil.ToFloat64(metrics.SSTDownloadTotal); got != 1 {
		t.Fatalf("sst_download_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTDownloadErrors); got != 1 {
		t.Fatalf("sst_download_errors_total mismatch: got=%v want=1", got)
	}
}
