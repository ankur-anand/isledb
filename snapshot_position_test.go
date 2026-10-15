package isledb

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestSnapshotReportsItsOwnViewPosition(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("snapshot-position")
	t.Cleanup(func() { _ = store.Close() })
	open := func() *DB {
		t.Helper()
		db, err := openDB(ctx, store, dbOpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	writer, err := open().OpenWriter(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(ctx) })
	commit := func(key string) ViewPosition {
		t.Helper()
		seq, err := writer.Put(ctx, []byte(key), []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		position, err := writer.WaitCommitted(ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		return position
	}
	first := commit("a")
	readerOpts := DefaultReaderOpenOptions(t.TempDir())
	readerOpts.Views.RefreshAfter = time.Hour
	reader, err := open().OpenReader(ctx, readerOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	old, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = old.Close() }()
	if old.ViewPosition() != reader.ViewPosition() || old.ViewPosition() != first {
		t.Fatalf("snapshot at %d, reader at %d, commit at %d; want all equal", old.ViewPosition(), reader.ViewPosition(), first)
	}

	// The reader moves on; the snapshot keeps its position, and reads what
	// that position holds.
	second := commit("b")
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if reader.ViewPosition() != second || old.ViewPosition() != first {
		t.Fatalf("after a refresh: reader %d, old snapshot %d; want %d and %d", reader.ViewPosition(), old.ViewPosition(), second, first)
	}
	if _, found, err := old.Get(ctx, []byte("b")); err != nil || found {
		t.Fatalf("old snapshot sees b: %t, %v", found, err)
	}
	fresh, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fresh.Close() }()
	if fresh.ViewPosition() != second {
		t.Fatalf("new snapshot at %d, want %d", fresh.ViewPosition(), second)
	}
	if _, found, err := fresh.Get(ctx, []byte("b")); err != nil || !found {
		t.Fatalf("new snapshot misses b: %t, %v", found, err)
	}
}

func TestNextViewSnapshotReportsTheNextPosition(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	next := takeNextView(t, reader)
	snap, err := next.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snap.Close() }()
	if snap.ViewPosition() != next.Next() || snap.ViewPosition() == reader.ViewPosition() {
		t.Fatalf("next view's snapshot at %d, next %d, published %d; want next, not the published one",
			snap.ViewPosition(), next.Next(), reader.ViewPosition())
	}
}
