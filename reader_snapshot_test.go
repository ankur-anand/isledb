package isledb

import (
	"bytes"
	"context"
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
}

func TestReaderCloseRejectsFurtherUse(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-close")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	snap := reader.Snapshot()
	if snap == nil {
		t.Fatal("Snapshot() = nil")
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if _, _, err := reader.Get(ctx, []byte("a")); err != ErrReaderClosed {
		t.Fatalf("Get after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.Scan(ctx, nil, nil); err != ErrReaderClosed {
		t.Fatalf("Scan after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.ScanLimit(ctx, nil, nil, 1); err != ErrReaderClosed {
		t.Fatalf("ScanLimit after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.NewIterator(ctx, IteratorOptions{}); err != ErrReaderClosed {
		t.Fatalf("NewIterator after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if err := reader.Refresh(ctx); err != ErrReaderClosed {
		t.Fatalf("Refresh after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.Prefetch(ctx, PrefetchOptions{All: true}); err != ErrReaderClosed {
		t.Fatalf("Prefetch after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if got := reader.Manifest(); got != nil {
		t.Fatalf("Manifest after Reader.Close = %#v, want nil", got)
	}
	if got := reader.Snapshot(); got != nil {
		t.Fatalf("Snapshot after Reader.Close = %#v, want nil", got)
	}
	if _, _, err := snap.Get(ctx, []byte("a")); err != ErrReaderClosed {
		t.Fatalf("Snapshot.Get after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
}

func TestOpenReaderDefaultOptionsAreOpenable(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-default-options")
	defer store.Close()

	cacheDir := t.TempDir()
	reader, err := OpenReader(ctx, store, DefaultReaderOpenOptions(cacheDir))
	if err != nil {
		t.Fatalf("OpenReader with default options: %v", err)
	}
	if reader.cacheDir != cacheDir {
		t.Fatalf("reader cacheDir=%q, want %q", reader.cacheDir, cacheDir)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOpenReaderRequiresExplicitCacheDir(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-cache-dir-required")
	defer store.Close()

	if _, err := OpenReader(ctx, store, DefaultReaderOpenOptions("")); err == nil {
		t.Fatal("OpenReader with empty cache dir succeeded, want error")
	}
}

func openTestReader(t *testing.T, ctx context.Context, store *blobstore.Store) *Reader {
	t.Helper()

	opts := DefaultReaderOpenOptions(t.TempDir())
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
