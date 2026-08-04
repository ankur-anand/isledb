package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

func TestRetirementSweeperAdvancesWithinEntryInBoundedBatches(t *testing.T) {
	ctx := context.Background()
	store, manifestStore, fence, entry := seedRetiredSSTs(t, ctx, 2, time.Now().Add(-time.Hour))
	defer store.Close()
	cursorStorage := newGCCursorStorage(store)

	stats, err := runRetirementSweeper(ctx, store, manifestStore, cursorStorage, fence, 1)
	if err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	if stats.Deleted != 1 || stats.NextManifestSeq != entry.Seq || stats.NextObjectIndex != 1 {
		t.Fatalf("first sweep stats: %+v", stats)
	}

	cursor, _, _, err := loadGCCursorWithCAS(ctx, cursorStorage)
	if err != nil {
		t.Fatalf("load cursor after first sweep: %v", err)
	}
	if cursor.NextManifestSeq != entry.Seq || cursor.NextObjectIndex != 1 {
		t.Fatalf("cursor after first sweep: %+v", cursor)
	}

	stats, err = runRetirementSweeper(ctx, store, manifestStore, cursorStorage, fence, 1)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if stats.Deleted != 1 || stats.NextManifestSeq != entry.Seq+1 || stats.NextObjectIndex != 0 {
		t.Fatalf("second sweep stats: %+v", stats)
	}

	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if current.RetirementLogStart != entry.Seq+1 {
		t.Fatalf("retirement floor=%d, want %d", current.RetirementLogStart, entry.Seq+1)
	}
	for _, retired := range entry.RetiredObjects {
		if _, _, err := store.Read(ctx, retired.Key); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("retired object %q read error=%v, want not found", retired.Key, err)
		}
	}
}

func TestRetirementSweeperStopsAtNotBefore(t *testing.T) {
	ctx := context.Background()
	store, manifestStore, fence, entry := seedRetiredSSTs(t, ctx, 1, time.Now().Add(time.Hour))
	defer store.Close()

	stats, err := runRetirementSweeper(ctx, store, manifestStore, newGCCursorStorage(store), fence, 128)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if stats.Deleted != 0 || stats.NextManifestSeq != entry.Seq || stats.NextObjectIndex != 0 {
		t.Fatalf("blocked sweep stats: %+v", stats)
	}
	if _, _, err := store.Read(ctx, entry.RetiredObjects[0].Key); err != nil {
		t.Fatalf("future retirement was deleted: %v", err)
	}
}

func TestRetirementSweeperRetriesCursorConflictAfterDelete(t *testing.T) {
	ctx := context.Background()
	store, manifestStore, fence, entry := seedRetiredSSTs(t, ctx, 1, time.Now().Add(-time.Hour))
	defer store.Close()
	storage := &conflictingGCCursorStorage{
		inner:        newGCCursorStorage(store),
		failAdvances: 1,
	}

	stats, err := runRetirementSweeper(ctx, store, manifestStore, storage, fence, 1)
	if err != nil {
		t.Fatalf("sweep with cursor conflict: %v", err)
	}
	if stats.Deleted != 1 {
		t.Fatalf("sweep stats after retry: %+v", stats)
	}
	cursor, _, _, err := loadGCCursorWithCAS(ctx, storage)
	if err != nil {
		t.Fatalf("load cursor: %v", err)
	}
	if cursor.NextManifestSeq != entry.Seq+1 || cursor.NextObjectIndex != 0 {
		t.Fatalf("cursor after retry: %+v", cursor)
	}
}

func TestManifestRejectsIncompleteRetirementBatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("retirement-validation")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := manifestStore.ClaimCompactor(ctx, "gc-validation"); err != nil {
		t.Fatalf("claim compactor: %v", err)
	}

	if _, err := manifestStore.AppendRemoveSSTablesWithFence(ctx, []string{"sst-a"}, nil); !errors.Is(err, manifest.ErrInvalidRetirement) {
		t.Fatalf("missing retirement error=%v, want %v", err, manifest.ErrInvalidRetirement)
	}

	retired := make([]manifest.RetiredObject, manifest.MaxRetiredObjectsPerEntry+1)
	for i := range retired {
		id := fmt.Sprintf("sst-%03d", i)
		retired[i] = manifest.RetiredObject{Kind: manifest.RetiredObjectSST, ID: id, Key: "sstable/" + id, NotBefore: time.Now().UTC()}
	}
	if _, err := manifestStore.AppendRemoveSSTablesWithFence(ctx, []string{"sst-000"}, retired); !errors.Is(err, manifest.ErrInvalidRetirement) {
		t.Fatalf("oversized retirement error=%v, want %v", err, manifest.ErrInvalidRetirement)
	}
}

type conflictingGCCursorStorage struct {
	mu           sync.Mutex
	inner        manifest.GCCursorStorage
	failAdvances int
}

func (s *conflictingGCCursorStorage) LoadGCCursor(ctx context.Context) ([]byte, string, bool, error) {
	return s.inner.LoadGCCursor(ctx)
}

func (s *conflictingGCCursorStorage) StoreGCCursor(ctx context.Context, data []byte, token string, exists bool) error {
	s.mu.Lock()
	if exists && s.failAdvances > 0 {
		s.failAdvances--
		s.mu.Unlock()
		return manifest.ErrPreconditionFailed
	}
	s.mu.Unlock()
	return s.inner.StoreGCCursor(ctx, data, token, exists)
}

func seedRetiredSSTs(t *testing.T, ctx context.Context, count int, notBefore time.Time) (*blobstore.Store, *manifest.Store, *manifest.FenceToken, *manifest.ManifestLogEntry) {
	t.Helper()
	store := blobstore.NewMemory("retirement-sweep")
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "retirement-writer"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	ids := make([]string, count)
	retired := make([]manifest.RetiredObject, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("retired-%03d.sst", i)
		key := store.SSTPath(id)
		if _, err := store.Write(ctx, key, []byte("sst")); err != nil {
			t.Fatalf("write retired object: %v", err)
		}
		if _, err := manifestStore.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{ID: id, Level: 0, Size: 3}); err != nil {
			t.Fatalf("append sst: %v", err)
		}
		ids[i] = id
		retired[i] = manifest.RetiredObject{Kind: manifest.RetiredObjectSST, ID: id, Key: key, Size: 3, NotBefore: notBefore.UTC()}
	}
	fence, err := manifestStore.ClaimCompactor(ctx, "retirement-compactor")
	if err != nil {
		t.Fatalf("claim compactor: %v", err)
	}
	entry, err := manifestStore.AppendRemoveSSTablesWithFence(ctx, ids, retired)
	if err != nil {
		t.Fatalf("append removal: %v", err)
	}
	return store, manifestStore, fence, entry
}
