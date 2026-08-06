package manifest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

type pageCASConflictStorage struct {
	*BlobStoreBackend

	mu        sync.Mutex
	conflicts int
	snapshots int
}

func (s *pageCASConflictStorage) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	s.mu.Lock()
	s.snapshots++
	s.mu.Unlock()
	return s.BlobStoreBackend.WriteSnapshot(ctx, id, data)
}

func (s *pageCASConflictStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	s.mu.Lock()
	if s.conflicts > 0 {
		s.conflicts--
		s.mu.Unlock()
		return "", ErrPreconditionFailed
	}
	s.mu.Unlock()
	return s.BlobStoreBackend.WriteCurrentCAS(ctx, data, expectedETag)
}

func (s *pageCASConflictStorage) failNextCAS() {
	s.mu.Lock()
	s.conflicts++
	s.mu.Unlock()
}

func (s *pageCASConflictStorage) snapshotWrites() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshots
}

func TestCurrentClonePreservesStateReplayAccounting(t *testing.T) {
	current := &Current{StateReplayPages: 17, StateReplayBytes: 4096}
	clone := current.Clone()
	if clone.StateReplayPages != current.StateReplayPages || clone.StateReplayBytes != current.StateReplayBytes {
		t.Fatalf("clone replay accounting=(%d pages, %d bytes), want (%d pages, %d bytes)",
			clone.StateReplayPages, clone.StateReplayBytes, current.StateReplayPages, current.StateReplayBytes)
	}
}

func TestStateReplayAccountingTracksPublishedLeafPage(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("state-replay-accounting")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	manifestStore := NewStoreWithStorage(backend)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := range defaultActiveEntryLimit {
		if _, err := manifestStore.AppendAddSSTableWithFence(ctx, SSTMeta{
			ID:    fmt.Sprintf("sst-%03d", i),
			Level: 0,
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%d): %v", i, err)
		}
	}

	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.StateReplayPages != 1 {
		t.Fatalf("StateReplayPages=%d, want 1", current.StateReplayPages)
	}
	if len(current.IndexFrontier) != 1 {
		t.Fatalf("IndexFrontier=%d, want 1", len(current.IndexFrontier))
	}
	ref := current.IndexFrontier[0]
	pageData, err := backend.ReadPage(ctx, ref.Path)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if ref.EncodedBytes != uint64(len(pageData)) {
		t.Fatalf("EncodedBytes=%d, want %d", ref.EncodedBytes, len(pageData))
	}
	if current.StateReplayBytes != ref.EncodedBytes {
		t.Fatalf("StateReplayBytes=%d, want %d", current.StateReplayBytes, ref.EncodedBytes)
	}
}

func TestStateReplayAccountingIncludesPromotedIndexPage(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("state-replay-index-accounting")
	defer store.Close()

	manifestStore := NewStoreWithStorage(NewBlobStoreBackend(store))
	current := &Current{NextEpoch: 1}
	var leafBytes uint64
	for pageIndex := range defaultPageFanout {
		entries := make([]ManifestLogEntry, defaultActiveEntryLimit)
		for entryIndex := range entries {
			entries[entryIndex] = testManifestEntry(uint64(pageIndex*defaultActiveEntryLimit + entryIndex))
		}
		ref, err := manifestStore.writeEntryPage(ctx, entries)
		if err != nil {
			t.Fatalf("writeEntryPage(%d): %v", pageIndex, err)
		}
		leafBytes += ref.EncodedBytes
		if err := manifestStore.addPageRef(ctx, current, ref); err != nil {
			t.Fatalf("addPageRef(%d): %v", pageIndex, err)
		}
	}

	if len(current.IndexFrontier) != 1 || current.IndexFrontier[0].Level != 1 {
		t.Fatalf("IndexFrontier=%+v, want one level-1 page", current.IndexFrontier)
	}
	if current.StateReplayPages != uint64(defaultPageFanout+1) {
		t.Fatalf("StateReplayPages=%d, want %d", current.StateReplayPages, defaultPageFanout+1)
	}
	wantBytes := leafBytes + current.IndexFrontier[0].EncodedBytes
	if current.StateReplayBytes != wantBytes {
		t.Fatalf("StateReplayBytes=%d, want %d", current.StateReplayBytes, wantBytes)
	}
}

func TestStateReplayAccountingIgnoresOrphanPageFromCASRetry(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("state-replay-cas-retry")
	defer store.Close()

	storage := &pageCASConflictStorage{BlobStoreBackend: NewBlobStoreBackend(store)}
	manifestStore := NewStoreWithStorage(storage)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := 0; i < defaultActiveEntryLimit-1; i++ {
		if _, err := manifestStore.AppendAddSSTableWithFence(ctx, SSTMeta{
			ID:    fmt.Sprintf("sst-%03d", i),
			Level: 0,
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%d): %v", i, err)
		}
	}

	storage.failNextCAS()
	if _, err := manifestStore.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "rollover", Level: 0}); err != nil {
		t.Fatalf("AppendAddSSTableWithFence(rollover): %v", err)
	}
	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.StateReplayPages != 1 {
		t.Fatalf("StateReplayPages=%d, want 1 reachable page", current.StateReplayPages)
	}
	if len(current.IndexFrontier) != 1 {
		t.Fatalf("IndexFrontier=%d, want 1 reachable page", len(current.IndexFrontier))
	}
}

func TestWriteCheckpointRequiresCurrentFenceAndResetsReplayAccounting(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("fenced-checkpoint")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	manifestStore := NewStoreWithStorage(backend)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := range defaultActiveEntryLimit {
		if _, err := manifestStore.AppendAddSSTableWithFence(ctx, SSTMeta{
			ID:    fmt.Sprintf("sst-%03d", i),
			Level: 0,
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%d): %v", i, err)
		}
	}
	token, err := manifestStore.ClaimCompactor(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	state, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay before checkpoint: %v", err)
	}
	if _, err := manifestStore.WriteCheckpoint(ctx, state, token); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.StateReplayPages != 0 || current.StateReplayBytes != 0 {
		t.Fatalf("replay accounting after checkpoint=(%d pages, %d bytes), want zero",
			current.StateReplayPages, current.StateReplayBytes)
	}
	if current.Snapshot == "" {
		t.Fatal("checkpoint did not publish a snapshot")
	}

	otherStore := NewStoreWithStorage(backend)
	if _, err := otherStore.ClaimCompactor(ctx, "maintenance-2"); err != nil {
		t.Fatalf("ClaimCompactor(new owner): %v", err)
	}
	if _, err := manifestStore.WriteCheckpoint(ctx, state, token); !errors.Is(err, ErrFenced) {
		t.Fatalf("WriteCheckpoint(stale token) error=%v, want %v", err, ErrFenced)
	}
}

func TestWriteCheckpointCASConflictPreservesReplayAccounting(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("checkpoint-cas-conflict")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	manifestStore := NewStoreWithStorage(backend)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := range defaultActiveEntryLimit {
		if _, err := manifestStore.AppendAddSSTableWithFence(ctx, SSTMeta{
			ID:    fmt.Sprintf("sst-%03d", i),
			Level: 0,
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%d): %v", i, err)
		}
	}
	token, err := manifestStore.ClaimCompactor(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	state, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay before checkpoint: %v", err)
	}
	before, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(before checkpoint): %v", err)
	}

	blocking := &blockingSnapshotStorage{
		Storage:     backend,
		PageStorage: backend,
		block:       make(chan struct{}),
		started:     make(chan struct{}),
	}
	checkpointStore := NewStoreWithStorage(blocking)
	result := make(chan error, 1)
	go func() {
		_, err := checkpointStore.WriteCheckpoint(ctx, state, token)
		result <- err
	}()
	<-blocking.started

	if _, err := manifestStore.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "concurrent", Level: 0}); err != nil {
		t.Fatalf("AppendAddSSTableWithFence(concurrent): %v", err)
	}
	close(blocking.block)
	if err := <-result; !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("WriteCheckpoint error=%v, want %v", err, ErrPreconditionFailed)
	}

	after, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(after checkpoint): %v", err)
	}
	if after.StateReplayPages != before.StateReplayPages || after.StateReplayBytes != before.StateReplayBytes {
		t.Fatalf("replay accounting after failed checkpoint=(%d pages, %d bytes), want (%d pages, %d bytes)",
			after.StateReplayPages, after.StateReplayBytes, before.StateReplayPages, before.StateReplayBytes)
	}
	if after.Snapshot != before.Snapshot {
		t.Fatalf("snapshot after failed checkpoint=%q, want %q", after.Snapshot, before.Snapshot)
	}
}

func TestCreateCheckpointPreservesWriterPagesCommittedDuringUpload(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("concurrent-writer-checkpoint")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	writerStore := NewStoreWithStorage(backend)
	if _, err := writerStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := writerStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := range defaultActiveEntryLimit {
		if _, err := writerStore.AppendAddSSTableWithFence(ctx, SSTMeta{
			ID:    fmt.Sprintf("before-fence-%03d", i),
			Level: 0,
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(before fence %d): %v", i, err)
		}
	}
	token, err := writerStore.ClaimCompactor(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	for i := 0; i < defaultActiveEntryLimit-2; i++ {
		if _, err := writerStore.AppendAddSSTableWithFence(ctx, SSTMeta{
			ID:    fmt.Sprintf("before-checkpoint-%03d", i),
			Level: 0,
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(before checkpoint %d): %v", i, err)
		}
	}
	base, err := writerStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(base): %v", err)
	}
	if base.StateReplayPages == 0 {
		t.Fatal("base has no replay pages")
	}

	blocking := &blockingSnapshotStorage{
		Storage:     backend,
		PageStorage: backend,
		block:       make(chan struct{}),
		started:     make(chan struct{}),
	}
	maintenanceStore := NewStoreWithStorage(blocking)
	results := make(chan CheckpointResult, 1)
	errs := make(chan error, 1)
	go func() {
		result, err := maintenanceStore.CreateCheckpoint(ctx, token)
		results <- result
		errs <- err
	}()
	<-blocking.started

	if _, err := writerStore.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "during-upload", Level: 0}); err != nil {
		t.Fatalf("AppendAddSSTableWithFence(during upload): %v", err)
	}
	close(blocking.block)
	result := <-results
	if err := <-errs; err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}
	if result.SnapshotNextSeq != base.NextSeq {
		t.Fatalf("SnapshotNextSeq=%d, want %d", result.SnapshotNextSeq, base.NextSeq)
	}
	if result.HeadNextSeq != base.NextSeq+1 {
		t.Fatalf("HeadNextSeq=%d, want %d", result.HeadNextSeq, base.NextSeq+1)
	}
	if result.FoldedReplayPages != base.StateReplayPages {
		t.Fatalf("FoldedReplayPages=%d, want %d", result.FoldedReplayPages, base.StateReplayPages)
	}
	if result.RemainingReplayPages != 1 {
		t.Fatalf("RemainingReplayPages=%d, want 1", result.RemainingReplayPages)
	}

	current, err := writerStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(after checkpoint): %v", err)
	}
	if current.LogSeqStart != base.NextSeq || current.NextSeq != base.NextSeq+1 {
		t.Fatalf("CURRENT range=[%d,%d), want [%d,%d)",
			current.LogSeqStart, current.NextSeq, base.NextSeq, base.NextSeq+1)
	}
	if current.StateReplayPages != 1 {
		t.Fatalf("StateReplayPages=%d, want 1", current.StateReplayPages)
	}

	replayed, err := NewStoreWithStorage(backend).Replay(ctx)
	if err != nil {
		t.Fatalf("Replay after checkpoint: %v", err)
	}
	if replayed.LookupSST("during-upload") == nil {
		t.Fatal("writer SST committed during checkpoint upload was lost")
	}
}

func TestCreateCheckpointRetriesCurrentCASWithoutRebuildingSnapshot(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("checkpoint-cas-retry")
	defer store.Close()

	storage := &pageCASConflictStorage{BlobStoreBackend: NewBlobStoreBackend(store)}
	manifestStore := NewStoreWithStorage(storage)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := range defaultActiveEntryLimit {
		if _, err := manifestStore.AppendAddSSTableWithFence(ctx, SSTMeta{
			ID:    fmt.Sprintf("sst-%03d", i),
			Level: 0,
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%d): %v", i, err)
		}
	}
	token, err := manifestStore.ClaimCompactor(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}

	storage.failNextCAS()
	result, err := manifestStore.CreateCheckpoint(ctx, token)
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}
	if result.FoldedReplayPages == 0 || result.RemainingReplayPages != 0 {
		t.Fatalf("checkpoint replay pages=(folded=%d remaining=%d), want folded>0 and remaining=0",
			result.FoldedReplayPages, result.RemainingReplayPages)
	}
	if writes := storage.snapshotWrites(); writes != 1 {
		t.Fatalf("snapshot writes=%d, want 1", writes)
	}
}

func TestWriteCheckpointRejectsStaleManifestBeforeUpload(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("stale-checkpoint")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	manifestStore := NewStoreWithStorage(backend)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	token, err := manifestStore.ClaimCompactor(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	staleState, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay before concurrent append: %v", err)
	}
	if _, err := manifestStore.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "concurrent", Level: 0}); err != nil {
		t.Fatalf("AppendAddSSTableWithFence: %v", err)
	}

	blocking := &blockingSnapshotStorage{
		Storage:     backend,
		PageStorage: backend,
		block:       make(chan struct{}),
		started:     make(chan struct{}),
	}
	checkpointStore := NewStoreWithStorage(blocking)
	if _, err := checkpointStore.WriteCheckpoint(ctx, staleState, token); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("WriteCheckpoint(stale state) error=%v, want %v", err, ErrPreconditionFailed)
	}
	select {
	case <-blocking.started:
		t.Fatal("stale checkpoint uploaded a snapshot candidate")
	default:
	}
}
