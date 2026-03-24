package isledb

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

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

	writer, err := db.OpenWriter(ctx, WriterOptions{FlushInterval: -1})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer writer.Close()

	compactor, err := db.OpenCompactor(ctx, CompactorOptions{})
	if err != nil {
		t.Fatalf("OpenCompactor: %v", err)
	}
	defer compactor.Close()

	retentionCompactor, err := db.OpenRetentionCompactor(ctx, RetentionCompactorOptions{})
	if err != nil {
		t.Fatalf("OpenRetentionCompactor: %v", err)
	}
	defer retentionCompactor.Close()

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

	if _, err := db.OpenWriter(ctx, WriterOptions{FlushInterval: -1}); err == nil {
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

	writer, err := db.OpenWriter(ctx, WriterOptions{FlushInterval: -1})
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
	defer compactor.Close()

	retentionCompactor, err := db.OpenRetentionCompactor(ctx, RetentionCompactorOptions{})
	if err != nil {
		t.Fatalf("OpenRetentionCompactor: %v", err)
	}
	defer retentionCompactor.Close()

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
	defer compactor.Close()

	retentionCompactor, err := db.OpenRetentionCompactor(ctx, RetentionCompactorOptions{GCMarkStorage: retentionStorage})
	if err != nil {
		t.Fatalf("OpenRetentionCompactor: %v", err)
	}
	defer retentionCompactor.Close()

	if compactor.gcMarkStore != compactorStorage {
		t.Fatal("compactor gc mark storage override not applied")
	}
	if retentionCompactor.gcMarkStore != retentionStorage {
		t.Fatal("retention compactor gc mark storage override not applied")
	}
}

func TestReaderMaxCommittedLSN(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-max-committed-lsn")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{
		CommittedLSNExtractor: BigEndianUint64LSNExtractor,
	})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	writer, err := db.OpenWriter(ctx, WriterOptions{FlushInterval: -1})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer writer.Close()

	putLSN := func(lsn uint64, value string) {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, lsn)
		if err := writer.Put(key, []byte(value)); err != nil {
			t.Fatalf("Put(%d): %v", lsn, err)
		}
	}

	putLSN(7, "v7")
	putLSN(42, "v42")
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	reader, err := OpenReader(ctx, store, ReaderOpenOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()

	lsn, found, err := reader.MaxCommittedLSN(ctx)
	if err != nil {
		t.Fatalf("MaxCommittedLSN: %v", err)
	}
	if !found {
		t.Fatal("expected max committed lsn to be found")
	}
	if lsn != 42 {
		t.Fatalf("unexpected max committed lsn: got=%d want=42", lsn)
	}
}
