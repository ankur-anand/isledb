package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// countingCurrent counts reads of CURRENT.
type countingCurrent struct {
	*manifest.BlobStoreBackend
	reads atomic.Int64
}

func (c *countingCurrent) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	c.reads.Add(1)
	return c.BlobStoreBackend.ReadCurrent(ctx)
}

// openCountingChangeFeed opens a change-feed database with 20 commits of 50
// changes each, counting reads of CURRENT.
func openCountingChangeFeed(t *testing.T, prefix string, pinned time.Duration) (*DB, *Writer, *countingCurrent) {
	t.Helper()
	ctx := context.Background()
	store := blobstore.NewMemory(prefix)
	t.Cleanup(func() { _ = store.Close() })
	storage := &countingCurrent{BlobStoreBackend: manifest.NewBlobStoreBackend(store)}
	db, err := openDB(ctx, store, dbOpenOptions{
		manifestStorage:   storage,
		changeFeedPayload: manifest.ChangeFeedPayloadFullValues,
		storePolicy:       StorePolicy{MaxPinnedViewAge: pinned},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	writer, err := db.OpenWriter(ctx, testChangeWriterOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(ctx) })
	for commit := range 20 {
		for i := range 50 {
			if _, err := writer.Put(ctx, fmt.Appendf(nil, "key-%02d-%02d", commit, i), []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db, writer, storage
}

func TestChangeReaderReadsOnWithItsViewUntilCaughtUp(t *testing.T) {
	ctx := context.Background()
	db, writer, storage := openCountingChangeFeed(t, "change-view-reuse", DefaultMaxPinnedViewAge)
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		t.Fatal(err)
	}

	before := storage.reads.Load()
	cursor, changes, calls := bounds.Oldest, 0, 0
	for cursor != bounds.Head {
		page, err := reader.Read(ctx, cursor, ChangeReadOptions{MaxChanges: 30})
		if err != nil {
			t.Fatal(err)
		}
		changes += len(page.Changes)
		cursor, calls = page.Next, calls+1
	}
	if changes != 1000 {
		t.Fatalf("read %d changes, want 1000", changes)
	}
	if n := storage.reads.Load() - before; n != 0 {
		t.Fatalf("%d reads of CURRENT over %d calls behind the view's head, want none", n, calls)
	}

	if _, err := writer.Put(ctx, []byte("new"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	before = storage.reads.Load()
	page, err := reader.Read(ctx, cursor, ChangeReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if storage.reads.Load()-before == 0 || len(page.Changes) != 1 || string(page.Changes[0].Key) != "new" {
		t.Fatalf("caught up: %d changes, %d reads of CURRENT; want the new change, read with a fresh view",
			len(page.Changes), storage.reads.Load()-before)
	}
}

func TestChangeReaderReloadsAnExpiredView(t *testing.T) {
	ctx := context.Background()
	db, _, storage := openCountingChangeFeed(t, "change-view-expiry", 300*time.Millisecond)
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	page, err := reader.Read(ctx, bounds.Oldest, ChangeReadOptions{MaxChanges: 30})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	before := storage.reads.Load()
	if _, err := reader.Read(ctx, page.Next, ChangeReadOptions{MaxChanges: 30}); err != nil {
		t.Fatal(err)
	}
	if n := storage.reads.Load() - before; n == 0 {
		t.Fatal("read on with an expired view, without reading CURRENT")
	}
}

// pausingCurrent pauses the first CURRENT read after it is armed, once it
// has read storage.
type pausingCurrent struct {
	*manifest.BlobStoreBackend
	armed   atomic.Bool
	read    chan struct{}
	release chan struct{}
}

func (p *pausingCurrent) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	data, etag, err := p.BlobStoreBackend.ReadCurrent(ctx)
	if p.armed.CompareAndSwap(true, false) {
		close(p.read)
		<-p.release
	}
	return data, etag, err
}

func TestConcurrentViewLoadsDoNotReportTheStoreWentBack(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-view-overlap")
	t.Cleanup(func() { _ = store.Close() })
	storage := &pausingCurrent{BlobStoreBackend: manifest.NewBlobStoreBackend(store),
		read: make(chan struct{}), release: make(chan struct{})}
	db, err := openDB(ctx, store, dbOpenOptions{manifestStorage: storage, changeFeedPayload: manifest.ChangeFeedPayloadFullValues})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	writer, err := db.OpenWriter(ctx, testChangeWriterOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(ctx) })
	put := func(key string) {
		t.Helper()
		if _, err := writer.Put(ctx, []byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	put("a")
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	storage.armed.Store(true)
	older := make(chan error, 1)
	go func() { _, err := reader.refreshView(ctx, false); older <- err }()
	<-storage.read // this load holds the CURRENT before the next commit
	put("b")
	newer, err := reader.refreshView(ctx, true) // a newer view, set first
	if err != nil {
		t.Fatal(err)
	}
	close(storage.release)
	if err := <-older; err != nil {
		t.Fatalf("the overtaken load: %v, want the newer view", err)
	}
	reader.viewMu.RLock()
	head := reader.view.Head()
	reader.viewMu.RUnlock()
	if head != newer.Head() {
		t.Fatalf("view head %d after the overtaken load, want the newer %d", head, newer.Head())
	}
}

func TestChangeReaderReportsAStoreThatWentBack(t *testing.T) {
	ctx := context.Background()
	store, db, writer := openChangeReaderTestDB(t, "change-view-went-back")
	put := func(key string) {
		t.Helper()
		if _, err := writer.Put(ctx, []byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	put("a")
	older, _, err := store.Read(ctx, store.ManifestPath())
	if err != nil {
		t.Fatal(err)
	}
	put("b")
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if _, err := reader.Bounds(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Write(ctx, store.ManifestPath(), older); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Bounds(ctx); !errors.Is(err, ErrCorruptChangeFeed) {
		t.Fatalf("CURRENT behind the view held, with nothing newer loaded meanwhile: %v, want ErrCorruptChangeFeed", err)
	}
}
