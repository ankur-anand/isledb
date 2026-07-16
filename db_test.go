package isledb

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

func TestDBOpenWriterRejectsSecondActiveWriter(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-single-writer")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	first, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter(first): %v", err)
	}

	if _, err := db.OpenWriter(ctx, WriterOptions{}); !errors.Is(err, ErrWriterAlreadyOpen) {
		t.Fatalf("OpenWriter(second) error=%v, want %v", err, ErrWriterAlreadyOpen)
	}

	if err := first.Close(ctx); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	second, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter(after close): %v", err)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
}

func TestDBOpenWriterConcurrentCallsAllowOneWriter(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-concurrent-single-writer")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	const callers = 16
	type result struct {
		writer *Writer
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			writer, err := db.OpenWriter(ctx, WriterOptions{})
			results <- result{writer: writer, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var opened *Writer
	for result := range results {
		switch {
		case result.err == nil:
			if opened != nil {
				t.Fatal("more than one concurrent OpenWriter call succeeded")
			}
			opened = result.writer
		case !errors.Is(result.err, ErrWriterAlreadyOpen):
			t.Fatalf("OpenWriter error=%v, want %v", result.err, ErrWriterAlreadyOpen)
		}
	}
	if opened == nil {
		t.Fatal("no concurrent OpenWriter call succeeded")
	}
	if err := opened.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDBOpenWriterReleasesReservationAfterConstructionFailure(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-writer-construction-failure")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := db.OpenWriter(canceled, WriterOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenWriter(canceled) error=%v, want %v", err, context.Canceled)
	}

	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter(after failure): %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDBWriterCloseErrorRetainsReservation(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-writer-close-error")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := writer.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close(canceled) error=%v, want %v", err, context.Canceled)
	}
	if _, err := db.OpenWriter(ctx, WriterOptions{}); !errors.Is(err, ErrWriterAlreadyOpen) {
		t.Fatalf("OpenWriter(after failed close) error=%v, want %v", err, ErrWriterAlreadyOpen)
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close(retry): %v", err)
	}
	reopened, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter(after successful close): %v", err)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatalf("Close(reopened): %v", err)
	}
}

type testGCMarkStorage struct{}

func (t *testGCMarkStorage) LoadPendingDeleteMarks(context.Context) ([]byte, string, bool, error) {
	return nil, "", false, nil
}

func (t *testGCMarkStorage) StorePendingDeleteMarks(context.Context, []byte, string, bool) error {
	return nil
}

func (t *testGCMarkStorage) LoadGCCheckpoint(context.Context) ([]byte, string, bool, error) {
	return nil, "", false, nil
}

func (t *testGCMarkStorage) StoreGCCheckpoint(context.Context, []byte, string, bool) error {
	return nil
}

var _ manifest.GCMarkStorage = (*testGCMarkStorage)(nil)

func TestOpenDBSharesManifestStore(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-test")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer writer.Close(ctx)

	compactor, err := db.OpenCompactor(ctx, CompactorOptions{})
	if err != nil {
		t.Fatalf("OpenCompactor: %v", err)
	}
	defer compactor.Close(ctx)

	retentionCompactor, err := db.OpenRetentionCompactor(ctx, RetentionCompactorOptions{})
	if err != nil {
		t.Fatalf("OpenRetentionCompactor: %v", err)
	}
	defer retentionCompactor.Close(ctx)

	if writer.w.manifestLog != db.manifestStore {
		t.Fatal("writer does not share manifest store with db")
	}
	if compactor.manifestLog != db.manifestStore {
		t.Fatal("compactor does not share manifest store with db")
	}
	if retentionCompactor.manifestLog != db.manifestStore {
		t.Fatal("retention compactor does not share manifest store with db")
	}
}

func TestOpenDBClosed(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-closed")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := db.OpenWriter(ctx, WriterOptions{}); err == nil {
		t.Fatal("expected OpenWriter to fail after DB is closed")
	}
	if _, err := db.OpenCompactor(ctx, CompactorOptions{}); err == nil {
		t.Fatal("expected OpenCompactor to fail after DB is closed")
	}
	if _, err := db.OpenRetentionCompactor(ctx, RetentionCompactorOptions{}); err == nil {
		t.Fatal("expected OpenRetentionCompactor to fail after DB is closed")
	}
}

func TestDBCloseClosesHandles(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-close-handles")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	compactor, err := db.OpenCompactor(ctx, CompactorOptions{})
	if err != nil {
		t.Fatalf("OpenCompactor: %v", err)
	}

	retentionCompactor, err := db.OpenRetentionCompactor(ctx, RetentionCompactorOptions{})
	if err != nil {
		t.Fatalf("OpenRetentionCompactor: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if !writer.w.closed.Load() {
		t.Fatal("expected writer to be closed by DB.Close")
	}
	if !compactor.closed.Load() {
		t.Fatal("expected compactor to be closed by DB.Close")
	}
	if !retentionCompactor.closed.Load() {
		t.Fatal("expected retention compactor to be closed by DB.Close")
	}
}

func TestOpenDBPropagatesGCMarkStorageToCompactors(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-gc-mark-storage")
	defer store.Close()

	custom := &testGCMarkStorage{}
	db, err := OpenDB(ctx, store, DBOptions{GCMarkStorage: custom})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	compactor, err := db.OpenCompactor(ctx, CompactorOptions{})
	if err != nil {
		t.Fatalf("OpenCompactor: %v", err)
	}
	defer compactor.Close(ctx)

	retentionCompactor, err := db.OpenRetentionCompactor(ctx, RetentionCompactorOptions{})
	if err != nil {
		t.Fatalf("OpenRetentionCompactor: %v", err)
	}
	defer retentionCompactor.Close(ctx)

	if compactor.gcMarkStore != custom {
		t.Fatal("compactor did not inherit db gc mark storage")
	}
	if retentionCompactor.gcMarkStore != custom {
		t.Fatal("retention compactor did not inherit db gc mark storage")
	}
}

func TestOpenDBCompactorOptionsOverrideGCMarkStorage(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-gc-mark-storage-override")
	defer store.Close()

	dbStorage := &testGCMarkStorage{}
	compactorStorage := &testGCMarkStorage{}
	retentionStorage := &testGCMarkStorage{}

	db, err := OpenDB(ctx, store, DBOptions{GCMarkStorage: dbStorage})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	compactor, err := db.OpenCompactor(ctx, CompactorOptions{GCMarkStorage: compactorStorage})
	if err != nil {
		t.Fatalf("OpenCompactor: %v", err)
	}
	defer compactor.Close(ctx)

	retentionCompactor, err := db.OpenRetentionCompactor(ctx, RetentionCompactorOptions{GCMarkStorage: retentionStorage})
	if err != nil {
		t.Fatalf("OpenRetentionCompactor: %v", err)
	}
	defer retentionCompactor.Close(ctx)

	if compactor.gcMarkStore != compactorStorage {
		t.Fatal("compactor gc mark storage override not applied")
	}
	if retentionCompactor.gcMarkStore != retentionStorage {
		t.Fatal("retention compactor gc mark storage override not applied")
	}
}
