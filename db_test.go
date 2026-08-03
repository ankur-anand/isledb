package isledb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestDBWriterTerminalFailureReleasesReservationOnClose(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-writer-terminal-failure")
	defer store.Close()

	rootCause := errors.New("injected background publish failure")
	storage := &failOnceStorage{
		Storage:     manifest.NewBlobStoreBackend(store),
		failOnWrite: 3,
		failErr:     rootCause,
	}
	db, err := OpenDB(ctx, store, DBOptions{ManifestStorage: storage})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	type callbackResult struct {
		flushErr error
		closeErr error
	}
	callback := make(chan callbackResult, 1)
	var writerRef atomic.Pointer[Writer]
	opts := DefaultWriterOptions()
	opts.Flush.Interval = time.Millisecond
	opts.OnFlushError = func(flushErr error) {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		callback <- callbackResult{
			flushErr: flushErr,
			closeErr: writerRef.Load().Close(closeCtx),
		}
	}
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	writerRef.Store(writer)
	if err := writer.Put(ctx, []byte("a"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	select {
	case result := <-callback:
		if !errors.Is(result.flushErr, ErrWriterFailed) || !errors.Is(result.flushErr, rootCause) {
			t.Fatalf("callback error=%v", result.flushErr)
		}
		if result.closeErr != result.flushErr {
			t.Fatalf("Close error=%v, want callback error %v", result.closeErr, result.flushErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnFlushError calling Writer.Close deadlocked")
	}

	reopened, err := db.OpenWriter(ctx, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("OpenWriter after terminal close: %v", err)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatalf("Close reopened writer: %v", err)
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

	retention := DefaultRetentionPolicy()
	maintenance, err := db.OpenMaintenance(ctx, MaintenanceOptions{Retention: &retention})
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	if writer.w.manifestLog != db.manifestStore {
		t.Fatal("writer does not share manifest store with db")
	}
	if maintenance.manifestLog != db.manifestStore {
		t.Fatal("maintenance does not share manifest store with db")
	}
	if maintenance.compactor.manifestLog != db.manifestStore || maintenance.retention.manifestLog != db.manifestStore {
		t.Fatal("maintenance stages do not share manifest store with db")
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
	if _, err := db.OpenMaintenance(ctx, MaintenanceOptions{}); err == nil {
		t.Fatal("expected OpenMaintenance to fail after DB is closed")
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

	retention := DefaultRetentionPolicy()
	maintenance, err := db.OpenMaintenance(ctx, MaintenanceOptions{Retention: &retention})
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if !writer.w.closed.Load() {
		t.Fatal("expected writer to be closed by DB.Close")
	}
	if !maintenance.closed.Load() {
		t.Fatal("expected maintenance to be closed by DB.Close")
	}
	if !maintenance.compactor.closed.Load() || !maintenance.retention.closed.Load() {
		t.Fatal("expected maintenance stages to be closed by DB.Close")
	}
}

func TestOpenDBPropagatesGCMarkStorageToMaintenance(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-gc-mark-storage")
	defer store.Close()

	custom := &testGCMarkStorage{}
	db, err := OpenDB(ctx, store, DBOptions{GCMarkStorage: custom})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	retention := DefaultRetentionPolicy()
	maintenance, err := db.OpenMaintenance(ctx, MaintenanceOptions{Retention: &retention})
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	if maintenance.compactor.gcMarkStore != custom {
		t.Fatal("compactor did not inherit db gc mark storage")
	}
	if maintenance.retention.gcMarkStore != custom {
		t.Fatal("retention compactor did not inherit db gc mark storage")
	}
}
