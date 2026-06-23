package isledb

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
)

func TestReaderSnapshotPinsLoadedState(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-snapshot-refresh")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	snap1 := reader.Snapshot()
	if snap1 == nil {
		t.Fatal("Snapshot() = nil")
	}
	defer snap1.Close()

	if value, found, err := snap1.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("snap1.Get(a): %v", err)
	} else if !found || !bytes.Equal(value, []byte("va")) {
		t.Fatalf("unexpected snap1 value: %q found=%v", value, found)
	}

	version1 := snap1.Version()
	if version1.IsZero() {
		t.Fatal("expected non-zero version")
	}

	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("vb")},
	}, 0, 1)

	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	snap2 := reader.Snapshot()
	if snap2 == nil {
		t.Fatal("Snapshot() after refresh = nil")
	}
	defer snap2.Close()

	if snap2.Version() == version1 {
		t.Fatalf("expected version advance, still got %q", version1.String())
	}

	if value, found, err := snap2.Get(ctx, []byte("b")); err != nil {
		t.Fatalf("snap2.Get(b): %v", err)
	} else if !found || !bytes.Equal(value, []byte("vb")) {
		t.Fatalf("unexpected snap2 value: %q found=%v", value, found)
	}

	if _, found, err := snap1.Get(ctx, []byte("b")); err != nil {
		t.Fatalf("snap1.Get(b) after refresh: %v", err)
	} else if found {
		t.Fatal("expected old snapshot to remain immutable and not see b")
	}
}

func TestReaderSnapshotCatchUpUsesFixedState(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-snapshot-catchup")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("vb")},
		{Key: []byte("c"), Seq: 3, Kind: internal.OpPut, Inline: true, Value: []byte("vc")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	snap1 := reader.Snapshot()
	if snap1 == nil {
		t.Fatal("Snapshot() = nil")
	}
	defer snap1.Close()

	var got []string
	result, err := snap1.CatchUp(ctx, CatchUpOptions{
		StartAfterKey: []byte("a"),
	}, func(kv KV) error {
		got = append(got, string(kv.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("snap1.CatchUp: %v", err)
	}
	if want := []string{"b", "c"}; !sameStrings(got, want) {
		t.Fatalf("unexpected catch-up keys: got=%v want=%v", got, want)
	}
	if result.Count != 2 || result.Truncated {
		t.Fatalf("unexpected catch-up result: %+v", result)
	}
	if !bytes.Equal(result.LastKey, []byte("c")) {
		t.Fatalf("unexpected last key: %q", result.LastKey)
	}

	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("d"), Seq: 4, Kind: internal.OpPut, Inline: true, Value: []byte("vd")},
	}, 0, 1)

	got = got[:0]
	result, err = snap1.CatchUp(ctx, CatchUpOptions{
		StartAfterKey: []byte("c"),
	}, func(kv KV) error {
		got = append(got, string(kv.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("snap1.CatchUp after append: %v", err)
	}
	if len(got) != 0 || result.Count != 0 {
		t.Fatalf("expected old snapshot to see no new records, got keys=%v result=%+v", got, result)
	}

	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh after append: %v", err)
	}
	snap2 := reader.Snapshot()
	if snap2 == nil {
		t.Fatal("Snapshot() after refresh = nil")
	}
	defer snap2.Close()

	got = got[:0]
	result, err = snap2.CatchUp(ctx, CatchUpOptions{
		StartAfterKey: []byte("c"),
	}, func(kv KV) error {
		got = append(got, string(kv.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("snap2.CatchUp: %v", err)
	}
	if want := []string{"d"}; !sameStrings(got, want) {
		t.Fatalf("unexpected catch-up keys after refresh: got=%v want=%v", got, want)
	}
	if result.Count != 1 || result.Truncated {
		t.Fatalf("unexpected catch-up result after refresh: %+v", result)
	}
}

func TestReaderSnapshotScanLimitAndIterator(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-snapshot-readers")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("vb")},
		{Key: []byte("c"), Seq: 3, Kind: internal.OpPut, Inline: true, Value: []byte("vc")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	snap := reader.Snapshot()
	if snap == nil {
		t.Fatal("Snapshot() = nil")
	}
	defer snap.Close()

	rows, err := snap.ScanLimit(ctx, []byte("a"), []byte("z"), 2)
	if err != nil {
		t.Fatalf("ScanLimit: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("unexpected ScanLimit row count: got=%d want=2", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte("a")) || !bytes.Equal(rows[1].Key, []byte("b")) {
		t.Fatalf("unexpected ScanLimit keys: got=%q,%q", rows[0].Key, rows[1].Key)
	}

	iter, err := snap.NewIterator(ctx, IteratorOptions{
		MinKey: []byte("b"),
		MaxKey: []byte("c"),
	})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer iter.Close()

	var got []string
	for iter.Next() {
		got = append(got, string(iter.Key()))
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator err: %v", err)
	}
	if want := []string{"b", "c"}; !sameStrings(got, want) {
		t.Fatalf("unexpected iterator keys: got=%v want=%v", got, want)
	}
}

func TestReaderSnapshotCloseIsIdempotentAndRejectsFurtherUse(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-snapshot-close")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	snap := reader.Snapshot()
	if snap == nil {
		t.Fatal("Snapshot() = nil")
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if _, _, err := snap.Get(ctx, []byte("a")); err != ErrSnapshotClosed {
		t.Fatalf("Get after Close error = %v, want %v", err, ErrSnapshotClosed)
	}
	if _, err := snap.NewIterator(ctx, IteratorOptions{}); err != ErrSnapshotClosed {
		t.Fatalf("NewIterator after Close error = %v, want %v", err, ErrSnapshotClosed)
	}
	if _, err := snap.ScanLimit(ctx, nil, nil, 1); err != ErrSnapshotClosed {
		t.Fatalf("ScanLimit after Close error = %v, want %v", err, ErrSnapshotClosed)
	}
	if _, err := snap.CatchUp(ctx, CatchUpOptions{}, func(KV) error { return nil }); err != ErrSnapshotClosed {
		t.Fatalf("CatchUp after Close error = %v, want %v", err, ErrSnapshotClosed)
	}
}

func TestReaderSnapshotMaxCommittedLSN(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-snapshot-max-lsn")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{
		CommittedLSNExtractor: BigEndianUint64LSNExtractor,
	})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer writer.Close(ctx)

	putLSN := func(lsn uint64, value string) {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, lsn)
		if err := writer.Put(ctx, key, []byte(value)); err != nil {
			t.Fatalf("Put(%d): %v", lsn, err)
		}
	}

	putLSN(7, "v7")
	putLSN(42, "v42")
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	snap := reader.Snapshot()
	if snap == nil {
		t.Fatal("Snapshot() = nil")
	}
	defer snap.Close()

	lsn, found := snap.MaxCommittedLSN()
	if !found {
		t.Fatal("expected max committed lsn to be found")
	}
	if lsn != 42 {
		t.Fatalf("unexpected max committed lsn: got=%d want=42", lsn)
	}

	low, found := snap.LowWatermarkLSN()
	if !found {
		t.Fatal("expected low watermark lsn to be found")
	}
	if low != 7 {
		t.Fatalf("unexpected low watermark lsn: got=%d want=7", low)
	}
}

func openTestReader(t *testing.T, ctx context.Context, store *blobstore.Store) *Reader {
	t.Helper()

	opts := DefaultReaderOpenOptions()
	opts.CacheDir = t.TempDir()
	reader, err := OpenReader(ctx, store, opts)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	return reader
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
