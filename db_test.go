package isledb

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

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

func TestReaderMaxCommittedPosition(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-max-committed-position")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{
		KeyPositionExtractor: BigEndianUint64KeyPositionExtractor,
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

	putPosition := func(position uint64, value string) {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, position)
		if err := writer.Put(ctx, key, []byte(value)); err != nil {
			t.Fatalf("Put(%d): %v", position, err)
		}
	}

	putPosition(7, "v7")
	putPosition(42, "v42")
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	reader, err := OpenReader(ctx, store, ReaderOpenOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()

	position, found, err := reader.MaxCommittedPosition(ctx)
	if err != nil {
		t.Fatalf("MaxCommittedPosition: %v", err)
	}
	if !found {
		t.Fatal("expected max committed position to be found")
	}
	if position != 42 {
		t.Fatalf("unexpected max committed position: got=%d want=42", position)
	}

	low, found, err := reader.LowWatermarkPosition(ctx)
	if err != nil {
		t.Fatalf("LowWatermarkPosition: %v", err)
	}
	if !found {
		t.Fatal("expected low watermark position to be found")
	}
	if low != 7 {
		t.Fatalf("unexpected low watermark position: got=%d want=7", low)
	}
}

func TestRetentionCompactorUpdatesLowWatermarkPosition(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-low-watermark-retention")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{
		KeyPositionExtractor: BigEndianUint64KeyPositionExtractor,
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

	put := func(position uint64, value string) {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, position)
		if err := writer.Put(ctx, key, []byte(value)); err != nil {
			t.Fatalf("Put(%d): %v", position, err)
		}
	}

	put(10, "v10")
	put(11, "v11")
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush batch 1: %v", err)
	}

	put(20, "v20")
	put(21, "v21")
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush batch 2: %v", err)
	}

	put(30, "v30")
	put(31, "v31")
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush batch 3: %v", err)
	}

	retentionCompactor, err := db.OpenRetentionCompactor(ctx, RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: time.Nanosecond,
		RetentionCount:  2,
		CheckInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("OpenRetentionCompactor: %v", err)
	}
	defer retentionCompactor.Close(ctx)

	if err := retentionCompactor.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	reader, err := OpenReader(ctx, store, ReaderOpenOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()

	low, found, err := reader.LowWatermarkPosition(ctx)
	if err != nil {
		t.Fatalf("LowWatermarkPosition: %v", err)
	}
	if !found {
		t.Fatal("expected low watermark position to be found")
	}
	if low != 20 {
		t.Fatalf("unexpected low watermark position after cleanup: got=%d want=20", low)
	}

	max, found, err := reader.MaxCommittedPosition(ctx)
	if err != nil {
		t.Fatalf("MaxCommittedPosition: %v", err)
	}
	if !found {
		t.Fatal("expected max committed position to be found")
	}
	if max != 31 {
		t.Fatalf("unexpected max committed position after cleanup: got=%d want=31", max)
	}
}
