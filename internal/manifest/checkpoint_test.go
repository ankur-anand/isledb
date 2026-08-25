package manifest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/segmentio/ksuid"
)

type pageCASConflictStorage struct {
	*BlobStoreBackend

	mu        sync.Mutex
	conflicts int
	snapshots int
	reads     int
}

type checkpointReadErrorStorage struct {
	*BlobStoreBackend
	err error
}

func (s *checkpointReadErrorStorage) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.BlobStoreBackend.ReadSnapshot(ctx, path)
}

func (s *pageCASConflictStorage) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.BlobStoreBackend.ReadSnapshot(ctx, path)
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

func (s *pageCASConflictStorage) snapshotReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func TestCurrentClonePreservesStateReplayAccounting(t *testing.T) {
	current := &Current{StateReplayPages: 17, StateReplayBytes: 4096, ManifestPageMaxLevel: 3}
	clone := current.Clone()
	if clone.StateReplayPages != current.StateReplayPages || clone.StateReplayBytes != current.StateReplayBytes ||
		clone.ManifestPageMaxLevel != current.ManifestPageMaxLevel {
		t.Fatalf("clone replay accounting=(%d pages, %d bytes, max level %d), want (%d pages, %d bytes, max level %d)",
			clone.StateReplayPages, clone.StateReplayBytes, clone.ManifestPageMaxLevel,
			current.StateReplayPages, current.StateReplayBytes, current.ManifestPageMaxLevel)
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
	if current.ManifestPageMaxLevel != 1 {
		t.Fatalf("ManifestPageMaxLevel=%d, want 1", current.ManifestPageMaxLevel)
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

func TestStagedCheckpointResetsReplayAccounting(t *testing.T) {
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
	prepareAndApplyCheckpointForTest(t, ctx, manifestStore)

	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.StateReplayPages != 0 || current.StateReplayBytes != 0 {
		t.Fatalf("replay accounting after checkpoint=(%d pages, %d bytes), want zero",
			current.StateReplayPages, current.StateReplayBytes)
	}
	if current.Snapshot == nil {
		t.Fatal("checkpoint did not publish a snapshot")
	}
}

func TestStagedCheckpointPreservesWriterPagesCommittedDuringUpload(t *testing.T) {
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
	results := make(chan CheckpointCommand, 1)
	errs := make(chan error, 1)
	go func() {
		result, err := maintenanceStore.PrepareCheckpoint(ctx)
		results <- result
		errs <- err
	}()
	<-blocking.started

	for _, id := range []string{"during-upload-1", "during-upload-2"} {
		if _, err := writerStore.AppendAddSSTableWithFence(ctx, SSTMeta{ID: id, Level: 0}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%s): %v", id, err)
		}
	}
	close(blocking.block)
	result := <-results
	if err := <-errs; err != nil {
		t.Fatalf("PrepareCheckpoint: %v", err)
	}
	stageAndApplyCheckpointForTest(t, ctx, writerStore, result)
	if result.SnapshotNextSeq != base.NextSeq {
		t.Fatalf("SnapshotNextSeq=%d, want %d", result.SnapshotNextSeq, base.NextSeq)
	}
	if result.FoldedReplayPages != base.StateReplayPages {
		t.Fatalf("FoldedReplayPages=%d, want %d", result.FoldedReplayPages, base.StateReplayPages)
	}

	current, err := writerStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(after checkpoint): %v", err)
	}
	if current.LogSeqStart != base.NextSeq || current.NextSeq != base.NextSeq+2 {
		t.Fatalf("CURRENT range=[%d,%d), want [%d,%d)",
			current.LogSeqStart, current.NextSeq, base.NextSeq, base.NextSeq+2)
	}
	if current.StateReplayPages != 1 {
		t.Fatalf("StateReplayPages=%d, want 1", current.StateReplayPages)
	}

	replayed, err := NewStoreWithStorage(backend).Replay(ctx)
	if err != nil {
		t.Fatalf("Replay after checkpoint: %v", err)
	}
	for _, id := range []string{"during-upload-1", "during-upload-2"} {
		if replayed.LookupSST(id) == nil {
			t.Fatalf("writer SST %q committed during checkpoint upload was lost", id)
		}
	}
}

func TestApplyCheckpointRetriesCurrentCASWithoutRebuildingSnapshot(t *testing.T) {
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
	command, err := manifestStore.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("PrepareCheckpoint: %v", err)
	}
	token, err := manifestStore.ClaimMaintenance(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}
	staged, err := manifestStore.StageMaintenance(ctx, MaintenanceCommand{
		ID:         "checkpoint-cas-retry",
		Kind:       MaintenanceCommandCheckpoint,
		Checkpoint: &command,
	}, token)
	if err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}

	storage.failNextCAS()
	result, err := manifestStore.ApplyPendingMaintenance(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingMaintenance: %v", err)
	}
	if result.Status != MaintenanceStatusApplied || !result.Changed {
		t.Fatalf("result=%+v, want changed applied", result)
	}
	if writes := storage.snapshotWrites(); writes != 1 {
		t.Fatalf("snapshot writes=%d, want 1", writes)
	}
	if reads := storage.snapshotReads(); reads != 1 {
		t.Fatalf("snapshot reads=%d, want 1 across CURRENT CAS retry", reads)
	}
	repeated, err := manifestStore.ApplyPendingMaintenance(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingMaintenance(repeated): %v", err)
	}
	if repeated.Changed {
		t.Fatalf("repeated apply changed CURRENT: %+v", repeated)
	}
	if reads := storage.snapshotReads(); reads != 1 {
		t.Fatalf("snapshot reads=%d, want 1 after receipt replay", reads)
	}
	if _, err := manifestStore.ClearMaintenance(ctx, staged.Pending.ID, staged.Pending.Epoch, staged.Pending.Generation, token); err != nil {
		t.Fatalf("ClearMaintenance: %v", err)
	}
}

func TestWriterRejectsMissingCheckpointSnapshotBeforePublishing(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("checkpoint-missing-snapshot")
	defer blob.Close()

	store := NewStoreWithStorage(NewBlobStoreBackend(blob))
	if _, err := store.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	current, err := store.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	command := CheckpointCommand{
		Snapshot:          testObjectRef("manifest/snapshots/missing.manifest.zst"),
		BaseSnapshot:      current.Snapshot.Clone(),
		BaseLogSeqStart:   current.LogSeqStart,
		SnapshotNextSeq:   current.NextSeq,
		FoldedReplayPages: current.StateReplayPages,
		FoldedReplayBytes: current.StateReplayBytes,
	}
	assertCheckpointRejected(t, ctx, store, command)
}

func TestWriterRejectsInvalidCheckpointSnapshotBeforePublishing(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, ctx context.Context, store *Store, command *CheckpointCommand)
	}{
		{
			name: "checksum mismatch",
			mutate: func(_ *testing.T, _ context.Context, _ *Store, command *CheckpointCommand) {
				command.Snapshot.Checksum = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
			},
		},
		{
			name: "wrong sequence",
			mutate: func(t *testing.T, ctx context.Context, store *Store, command *CheckpointCommand) {
				state, err := store.Replay(ctx)
				if err != nil {
					t.Fatalf("Replay candidate: %v", err)
				}
				state.LogSeq++
				ref, err := store.writeSnapshotObject(ctx, state)
				if err != nil {
					t.Fatalf("write wrong-sequence candidate: %v", err)
				}
				command.Snapshot = ref
			},
		},
		{
			name: "invalid topology",
			mutate: func(t *testing.T, ctx context.Context, store *Store, command *CheckpointCommand) {
				state, err := store.Replay(ctx)
				if err != nil {
					t.Fatalf("Replay candidate: %v", err)
				}
				state.L0SSTs = append(state.L0SSTs,
					SSTMeta{ID: "duplicate", Level: 0},
					SSTMeta{ID: "duplicate", Level: 0})
				ref, err := store.writeSnapshotObject(ctx, state)
				if err != nil {
					t.Fatalf("write invalid-topology candidate: %v", err)
				}
				command.Snapshot = ref
			},
		},
		{
			name: "unsupported manifest version",
			mutate: func(t *testing.T, ctx context.Context, store *Store, command *CheckpointCommand) {
				state, err := store.Replay(ctx)
				if err != nil {
					t.Fatalf("Replay candidate: %v", err)
				}
				state.Version = 1
				ref, err := store.writeSnapshotObject(ctx, state)
				if err != nil {
					t.Fatalf("write wrong-version candidate: %v", err)
				}
				command.Snapshot = ref
			},
		},
		{
			name: "future epoch",
			mutate: func(t *testing.T, ctx context.Context, store *Store, command *CheckpointCommand) {
				state, err := store.Replay(ctx)
				if err != nil {
					t.Fatalf("Replay candidate: %v", err)
				}
				state.NextEpoch += 100
				ref, err := store.writeSnapshotObject(ctx, state)
				if err != nil {
					t.Fatalf("write future-epoch candidate: %v", err)
				}
				command.Snapshot = ref
			},
		},
		{
			name: "invalid writer fence",
			mutate: func(t *testing.T, ctx context.Context, store *Store, command *CheckpointCommand) {
				state, err := store.Replay(ctx)
				if err != nil {
					t.Fatalf("Replay candidate: %v", err)
				}
				current, err := store.ReadCurrentData(ctx)
				if err != nil {
					t.Fatalf("ReadCurrentData: %v", err)
				}
				state.WriterFence = current.WriterFence.Clone()
				state.WriterFence.Owner = ""
				ref, err := store.writeSnapshotObject(ctx, state)
				if err != nil {
					t.Fatalf("write invalid-fence candidate: %v", err)
				}
				command.Snapshot = ref
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			blob := blobstore.NewMemory("checkpoint-invalid-" + test.name)
			defer blob.Close()

			store := NewStoreWithStorage(NewBlobStoreBackend(blob))
			if _, err := store.Replay(ctx); err != nil {
				t.Fatalf("Replay: %v", err)
			}
			if _, err := store.ClaimWriter(ctx, "writer-1"); err != nil {
				t.Fatalf("ClaimWriter: %v", err)
			}
			command, err := store.PrepareCheckpoint(ctx)
			if err != nil {
				t.Fatalf("PrepareCheckpoint: %v", err)
			}
			test.mutate(t, ctx, store, &command)
			assertCheckpointRejected(t, ctx, store, command)
		})
	}
}

func TestWriterRejectsCheckpointReplayFloorRegression(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("checkpoint-floor-regression")
	defer blob.Close()

	store := NewStoreWithStorage(NewBlobStoreBackend(blob))
	if _, err := store.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	if _, err := store.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "sst-1", Level: 0}); err != nil {
		t.Fatalf("AppendAddSSTableWithFence: %v", err)
	}
	prepareAndApplyCheckpointForTest(t, ctx, store)

	current, err := store.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.LogSeqStart == 0 {
		t.Fatal("checkpoint did not advance replay floor")
	}
	command, err := store.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("PrepareCheckpoint: %v", err)
	}
	command.SnapshotNextSeq = current.LogSeqStart - 1
	assertCheckpointRejected(t, ctx, store, command)
}

func TestCheckpointSnapshotReadFailureIsRetryable(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("checkpoint-read-failure")
	defer blob.Close()

	storage := &checkpointReadErrorStorage{BlobStoreBackend: NewBlobStoreBackend(blob)}
	store := NewStoreWithStorage(storage)
	if _, err := store.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	command, err := store.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("PrepareCheckpoint: %v", err)
	}
	token, err := store.ClaimMaintenance(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}
	if _, err := store.StageMaintenance(ctx, MaintenanceCommand{
		ID:         "checkpoint-read-failure",
		Kind:       MaintenanceCommandCheckpoint,
		Checkpoint: &command,
	}, token); err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}

	wantErr := errors.New("temporary snapshot read failure")
	storage.err = wantErr
	if _, err := store.ApplyPendingMaintenance(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("ApplyPendingMaintenance error=%v, want %v", err, wantErr)
	}
	current, err := store.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.Snapshot != nil || current.MaintenanceReceipt != nil {
		t.Fatalf("transient read failure changed CURRENT: snapshot=%+v receipt=%+v", current.Snapshot, current.MaintenanceReceipt)
	}

	storage.err = nil
	result, err := store.ApplyPendingMaintenance(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingMaintenance(retry): %v", err)
	}
	if result.Status != MaintenanceStatusApplied || !result.Changed {
		t.Fatalf("retry result=%+v, want changed applied", result)
	}
}

func assertCheckpointRejected(t *testing.T, ctx context.Context, store *Store, command CheckpointCommand) {
	t.Helper()
	before, err := store.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(before): %v", err)
	}
	token, err := store.ClaimMaintenance(ctx, "maintenance-reject-test")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}
	staged, err := store.StageMaintenance(ctx, MaintenanceCommand{
		ID:         "checkpoint-reject-" + ksuid.New().String(),
		Kind:       MaintenanceCommandCheckpoint,
		Checkpoint: &command,
	}, token)
	if err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}
	result, err := store.ApplyPendingMaintenance(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingMaintenance: %v", err)
	}
	if result.Status != MaintenanceStatusRejected || !result.Changed {
		t.Fatalf("result=%+v, want changed rejected", result)
	}
	after, err := store.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(after): %v", err)
	}
	if !objectRefsEqual(after.Snapshot, before.Snapshot) || after.LogSeqStart != before.LogSeqStart {
		t.Fatalf("rejected checkpoint changed base: before=%+v after=%+v", before.Snapshot, after.Snapshot)
	}
	if after.MaintenanceScheduler != before.MaintenanceScheduler {
		t.Fatalf("rejected checkpoint changed scheduler: before=%+v after=%+v",
			before.MaintenanceScheduler, after.MaintenanceScheduler)
	}
	if !receiptMatchesCommand(after.MaintenanceReceipt, staged.Pending) || after.MaintenanceReceipt.Status != MaintenanceStatusRejected {
		t.Fatalf("rejection receipt=%+v pending=%+v", after.MaintenanceReceipt, staged.Pending)
	}
}
