package isledb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestDBWriterPublishesStorePolicy(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-store-policy")
	defer store.Close()

	const age = 30 * time.Minute
	db, err := openDB(ctx, store, dbOpenOptions{
		changeFeedPayload: manifest.ChangeFeedPayloadFullValues,
		storePolicy:       StorePolicy{MaxPinnedViewAge: age},
	})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
	if got := db.manifestStore.CurrentData().MaxPinnedViewAge; got != age {
		t.Fatalf("MaxPinnedViewAge=%s, want %s", got, age)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}

	reopened, err := openDB(ctx, store, dbOpenOptions{storePolicy: StorePolicy{MaxPinnedViewAge: time.Hour}})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.OpenWriter(ctx, writerOpts); !errors.Is(err, ErrStorePolicyMismatch) {
		t.Fatalf("OpenWriter with changed policy error=%v, want %v", err, ErrStorePolicyMismatch)
	}
}

func TestReaderHandlesInheritLoadedViewDeadline(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-store-policy")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriterWithPolicy(ctx, "writer", time.Hour); err != nil {
		t.Fatalf("ClaimWriterWithPolicy: %v", err)
	}

	reader := openTestReader(t, ctx, store)
	defer reader.Close()
	snapshot, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snapshot.Close()
	iterator, err := snapshot.NewIterator(ctx, IteratorOptions{})
	if err != nil {
		t.Fatalf("Snapshot.NewIterator: %v", err)
	}
	defer iterator.Close()

	if !snapshot.expiresAt.Equal(reader.viewExpiresAt) {
		t.Fatalf("snapshot deadline=%s, reader deadline=%s", snapshot.expiresAt, reader.viewExpiresAt)
	}
	if !iterator.expiresAt.Equal(reader.viewExpiresAt) {
		t.Fatalf("iterator deadline=%s, reader deadline=%s", iterator.expiresAt, reader.viewExpiresAt)
	}
}

func TestSnapshotExpiresAtPersistedStoreDeadline(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-store-policy-expiry")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriterWithPolicy(ctx, "writer", 75*time.Millisecond); err != nil {
		t.Fatalf("ClaimWriterWithPolicy: %v", err)
	}

	reader := openTestReader(t, ctx, store)
	defer reader.Close()
	snapshot, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, _, err := snapshot.Get(ctx, []byte("key")); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("Snapshot.Get error=%v, want %v", err, ErrSnapshotExpired)
	}
}

func TestNormalizeStorePolicyRejectsNegativeAge(t *testing.T) {
	if _, err := normalizeStorePolicy(StorePolicy{MaxPinnedViewAge: -time.Second}); !errors.Is(err, ErrInvalidDBOptions) {
		t.Fatalf("normalizeStorePolicy error=%v, want %v", err, ErrInvalidDBOptions)
	}
}

type delayedCurrentStorage struct {
	manifest.Storage
	delay time.Duration
}

func (s *delayedCurrentStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-timer.C:
		return s.Storage.ReadCurrent(ctx)
	}
}

func TestReaderViewAgeStartsBeforeCurrentFetch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-view-load-start")
	defer store.Close()
	base := manifest.NewBlobStoreBackend(store)
	manifestStore := manifest.NewStoreWithStorage(base)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	const age = time.Second
	if _, err := manifestStore.ClaimWriterWithPolicy(ctx, "writer", age); err != nil {
		t.Fatalf("ClaimWriterWithPolicy: %v", err)
	}

	opts := defaultReaderOptions()
	opts.CacheDir = t.TempDir()
	opts.ManifestStorage = &delayedCurrentStorage{Storage: base, delay: 100 * time.Millisecond}
	openedAt := time.Now()
	reader, err := newReader(ctx, store, opts)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()
	openedAfter := time.Now()

	if loadTime := openedAfter.Sub(openedAt); loadTime < 100*time.Millisecond {
		t.Fatalf("reader load time=%s, want at least 100ms", loadTime)
	}
	if remaining := reader.viewExpiresAt.Sub(openedAfter); remaining >= 950*time.Millisecond {
		t.Fatalf("remaining view age=%s; CURRENT fetch time did not consume the view lifetime", remaining)
	}
}
