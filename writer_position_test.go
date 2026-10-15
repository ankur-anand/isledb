package isledb

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestWaitCommittedReturnsTheVisiblePosition(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("visible-position")
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
	readerOpts := DefaultReaderOpenOptions(t.TempDir())
	readerOpts.Views.RefreshAfter = time.Hour
	reader, err := open().OpenReader(ctx, readerOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	seq, err := writer.Put(ctx, []byte("k"), []byte("v"))
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

	// Exactly the view right after the commit: CURRENT's next log slot.
	data, _, err := manifest.NewBlobStoreBackend(store).ReadCurrent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	current, err := manifest.DecodeCurrent(data)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(position) != current.NextSeq {
		t.Fatalf("position %d, want CURRENT's next slot %d", position, current.NextSeq)
	}

	// A reader before it does not see the write; at or past it, it does.
	if reader.ViewPosition() >= position {
		t.Fatalf("reader at %d before refreshing, want below %d", reader.ViewPosition(), position)
	}
	if _, found, err := reader.Get(ctx, []byte("k")); err != nil || found {
		t.Fatalf("Get before refreshing: found %t, %v; want not found", found, err)
	}
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if reader.ViewPosition() < position {
		t.Fatalf("reader at %d after refreshing, want at least %d", reader.ViewPosition(), position)
	}
	if _, found, err := reader.Get(ctx, []byte("k")); err != nil || !found {
		t.Fatalf("Get after refreshing: found %t, %v; want found", found, err)
	}
	if writer.State().Epoch != writer.Epoch() {
		t.Fatalf("State().Epoch %d, want %d", writer.State().Epoch, writer.Epoch())
	}
}
