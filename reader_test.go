package isledb

import (
	"bytes"
	"context"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

func writeTestSST(t *testing.T, ctx context.Context, store *blobstore.Store, ms *manifest.Store, entries []MemEntry, level int, epoch uint64) WriteSSTResult {
	t.Helper()

	it := &sliceSSTIter{entries: entries}
	res, err := WriteSST(ctx, it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, epoch)
	if err != nil {
		t.Fatalf("WriteSST: %v", err)
	}

	res.Meta.Level = level
	if _, err := store.Write(ctx, store.SSTPath(res.Meta.ID), res.SSTData); err != nil {
		t.Fatalf("write sst: %v", err)
	}

	if _, err := ms.AppendAddSSTable(ctx, res.Meta); err != nil {
		t.Fatalf("append manifest log: %v", err)
	}

	return res
}

func setupReaderFixture(t *testing.T) (*Reader, context.Context, func()) {
	t.Helper()

	ctx := context.Background()
	store := blobstore.NewMemory("reader-test")
	ms := manifest.NewStore(store)

	l1Entries := []MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("l1-a")},
		{Key: []byte("c"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("l1-c")},
		{Key: []byte("d"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("l1-d")},
	}
	writeTestSST(t, ctx, store, ms, l1Entries, 1, 1)

	l0Entries := []MemEntry{
		{Key: []byte("a"), Seq: 3, Kind: OpDelete},
		{Key: []byte("b"), Seq: 2, Kind: OpPut, Inline: true, Value: []byte("l0-b")},
		{Key: []byte("e"), Seq: 2, Kind: OpPut, Inline: true, Value: []byte("l0-e")},
	}
	writeTestSST(t, ctx, store, ms, l0Entries, 0, 2)

	reader, err := NewReader(ctx, store, ReaderOptions{})
	if err != nil {
		store.Close()
		t.Fatalf("NewReader: %v", err)
	}

	cleanup := func() {
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

	oldEntries := []MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("old")},
	}
	writeTestSST(t, ctx, store, ms, oldEntries, 0, 1)

	newEntries := []MemEntry{
		{Key: []byte("k"), Seq: 3, Kind: OpPut, Inline: true, Value: []byte("new")},
	}
	writeTestSST(t, ctx, store, ms, newEntries, 0, 2)

	reader, err := NewReader(ctx, store, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

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
	reader, err := NewReader(ctx, store, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if _, found, err := reader.Get(ctx, []byte("x")); err != nil {
		t.Fatalf("Get before refresh: %v", err)
	} else if found {
		t.Fatalf("expected key to be missing before refresh")
	}

	entries := []MemEntry{
		{Key: []byte("x"), Seq: 5, Kind: OpPut, Inline: true, Value: []byte("value")},
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

	older := []MemEntry{
		{Key: []byte("k"), Seq: 2, Kind: OpPut, Inline: true, Value: []byte("old")},
	}
	writeTestSST(t, ctx, store, ms, older, 0, 1)

	newer := []MemEntry{
		{Key: []byte("k"), Seq: 5, Kind: OpPut, Inline: true, Value: []byte("new")},
	}
	writeTestSST(t, ctx, store, ms, newer, 0, 2)

	reader, err := NewReader(ctx, store, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

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

	first := []MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("va")},
		{Key: []byte("b"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("vb")},
	}
	writeTestSST(t, ctx, store, ms, first, 1, 1)

	second := []MemEntry{
		{Key: []byte("d"), Seq: 2, Kind: OpPut, Inline: true, Value: []byte("vd")},
		{Key: []byte("e"), Seq: 2, Kind: OpPut, Inline: true, Value: []byte("ve")},
	}
	writeTestSST(t, ctx, store, ms, second, 1, 2)

	reader, err := NewReader(ctx, store, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

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

func TestReader_Get_TombstoneShadowsLowerLevel(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-tombstone-get")
	defer store.Close()

	ms := manifest.NewStore(store)

	l2Entries := []MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("old")},
	}
	writeTestSST(t, ctx, store, ms, l2Entries, 2, 1)

	l1Entries := []MemEntry{
		{Key: []byte("k"), Seq: 5, Kind: OpDelete},
	}
	writeTestSST(t, ctx, store, ms, l1Entries, 1, 2)

	reader, err := NewReader(ctx, store, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

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

	l2Entries := []MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("old")},
		{Key: []byte("m"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("keep")},
	}
	writeTestSST(t, ctx, store, ms, l2Entries, 2, 1)

	l1Entries := []MemEntry{
		{Key: []byte("k"), Seq: 5, Kind: OpDelete},
	}
	writeTestSST(t, ctx, store, ms, l1Entries, 1, 2)

	reader, err := NewReader(ctx, store, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

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
