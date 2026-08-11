package manifest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestAdvanceMaintenanceSchedulerTracksAppliedPrimaryWork(t *testing.T) {
	current := &Current{}
	advanceMaintenanceScheduler(current, &MaintenanceCommand{
		Kind: MaintenanceCommandCompaction,
		Scheduling: MaintenanceScheduling{
			WorkUnits: 3,
		},
		Compaction: &CompactionCommand{Payload: CompactionLogPayload{SourceLevel: 0, DestinationLevel: 1}},
	})
	if current.MaintenanceScheduler.LastPrimary != MaintenanceCommandCompaction ||
		current.MaintenanceScheduler.CompactionUnitsSinceCheckpoint != 3 ||
		current.MaintenanceScheduler.L0UnitsSinceLower != 3 {
		t.Fatalf("after L0=%+v", current.MaintenanceScheduler)
	}

	advanceMaintenanceScheduler(current, &MaintenanceCommand{
		Kind: MaintenanceCommandCompaction,
		Scheduling: MaintenanceScheduling{
			WorkUnits: 1,
		},
		Compaction: &CompactionCommand{Payload: CompactionLogPayload{SourceLevel: 2, DestinationLevel: 3}},
	})
	if current.MaintenanceScheduler.L0UnitsSinceLower != 0 || current.MaintenanceScheduler.NextLowerLevel != 3 {
		t.Fatalf("after lower=%+v", current.MaintenanceScheduler)
	}

	advanceMaintenanceScheduler(current, &MaintenanceCommand{Kind: MaintenanceCommandCheckpoint})
	if current.MaintenanceScheduler.LastPrimary != MaintenanceCommandCheckpoint ||
		current.MaintenanceScheduler.CompactionUnitsSinceCheckpoint != 0 {
		t.Fatalf("after checkpoint=%+v", current.MaintenanceScheduler)
	}
}

func TestMaintenanceHeadClaimStageAndClear(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-head")
	defer store.Close()

	manifestStore := NewStoreWithStorage(NewBlobStoreBackend(store))
	token, err := manifestStore.ClaimMaintenance(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}
	head, err := manifestStore.StageMaintenance(ctx, MaintenanceCommand{
		ID:   "checkpoint-1",
		Kind: MaintenanceCommandCheckpoint,
		Checkpoint: &CheckpointCommand{
			Snapshot:          testObjectRef("manifest/snapshots/checkpoint-1.manifest.zst"),
			SnapshotNextSeq:   100,
			FoldedReplayPages: 64,
		},
	}, token)
	if err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}
	if head.Pending == nil || head.Pending.Epoch != token.Epoch || head.Pending.Generation != head.Generation {
		t.Fatalf("pending=%+v head=%+v", head.Pending, head)
	}
	if _, err := manifestStore.StageMaintenance(ctx, MaintenanceCommand{
		ID:         "checkpoint-2",
		Kind:       MaintenanceCommandCheckpoint,
		Checkpoint: &CheckpointCommand{Snapshot: testObjectRef("unused")},
	}, token); !errors.Is(err, ErrMaintenanceCommandPending) {
		t.Fatalf("StageMaintenance(second) error=%v, want %v", err, ErrMaintenanceCommandPending)
	}

	cleared, err := manifestStore.ClearMaintenance(ctx, head.Pending.ID, head.Pending.Epoch, head.Pending.Generation, token)
	if err != nil {
		t.Fatalf("ClearMaintenance: %v", err)
	}
	if cleared.Pending != nil {
		t.Fatalf("pending after clear=%+v, want nil", cleared.Pending)
	}
}

func TestMaintenanceClaimPreservesPendingAndFencesOldOwner(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-head-fence")
	defer store.Close()

	manifestStore := NewStoreWithStorage(NewBlobStoreBackend(store))
	first, err := manifestStore.ClaimMaintenance(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimMaintenance(first): %v", err)
	}
	staged, err := manifestStore.StageMaintenance(ctx, MaintenanceCommand{
		ID:              "floor-1",
		Kind:            MaintenanceCommandChangeFeedFloor,
		ChangeFeedFloor: &AdvanceFloorCommand{Floor: 42},
	}, first)
	if err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}

	second, err := manifestStore.ClaimMaintenance(ctx, "maintenance-2")
	if err != nil {
		t.Fatalf("ClaimMaintenance(second): %v", err)
	}
	head, _, err := manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("ReadMaintenanceHead: %v", err)
	}
	if head.Pending == nil || head.Pending.ID != staged.Pending.ID {
		t.Fatalf("pending after new claim=%+v, want %q", head.Pending, staged.Pending.ID)
	}
	if _, err := manifestStore.ClearMaintenance(ctx, staged.Pending.ID, staged.Pending.Epoch, staged.Pending.Generation, first); !errors.Is(err, ErrFenced) {
		t.Fatalf("ClearMaintenance(old owner) error=%v, want %v", err, ErrFenced)
	}
	if _, err := manifestStore.ClearMaintenance(ctx, staged.Pending.ID, staged.Pending.Epoch, staged.Pending.Generation, second); err != nil {
		t.Fatalf("ClearMaintenance(new owner): %v", err)
	}
}

func TestMaintenanceCommandRejectsMultiplePayloads(t *testing.T) {
	command := &MaintenanceCommand{
		ID:              "invalid",
		Epoch:           1,
		Generation:      1,
		Kind:            MaintenanceCommandCheckpoint,
		CreatedAt:       time.Now().UTC(),
		Checkpoint:      &CheckpointCommand{Snapshot: testObjectRef("snapshot")},
		ChangeFeedFloor: &AdvanceFloorCommand{Floor: 1},
	}
	if err := command.Validate(); !errors.Is(err, ErrInvalidMaintenanceCommand) {
		t.Fatalf("Validate error=%v, want %v", err, ErrInvalidMaintenanceCommand)
	}
}

func TestMaintenanceCheckpointPreservesEnabledChangeFeedFloor(t *testing.T) {
	current := &Current{
		Snapshot:           testObjectRefPtr("old-snapshot"),
		LogSeqStart:        0,
		NextSeq:            10,
		ChangeFeedEnabled:  true,
		ChangeFeedPayload:  ChangeFeedPayloadFullValues,
		ChangeFeedLogStart: 0,
		StateReplayPages:   2,
		StateReplayBytes:   1024,
	}
	command := &MaintenanceCommand{
		Kind: MaintenanceCommandCheckpoint,
		Checkpoint: &CheckpointCommand{
			Snapshot:          testObjectRef("new-snapshot"),
			BaseSnapshot:      testObjectRefPtr("old-snapshot"),
			BaseLogSeqStart:   0,
			SnapshotNextSeq:   8,
			FoldedReplayPages: 2,
			FoldedReplayBytes: 1024,
		},
	}

	verification := &checkpointSnapshotVerification{done: true}
	if err := (&Store{}).applyMaintenanceCommand(context.Background(), current, command, verification); err != nil {
		t.Fatalf("apply checkpoint: %v", err)
	}
	if current.ChangeFeedLogStart != 0 {
		t.Fatalf("enabled change-feed floor=%d want=0", current.ChangeFeedLogStart)
	}
	if current.LogSeqStart != 8 {
		t.Fatalf("state replay floor=%d want=8", current.LogSeqStart)
	}
}

func TestWriterAppliesPendingCompactionAndReceiptAtomically(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-apply-compaction")
	defer store.Close()

	manifestStore := NewStoreWithStorage(NewBlobStoreBackend(store))
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for _, id := range []string{"a", "b"} {
		if _, err := manifestStore.AppendAddSSTableWithFence(ctx, SSTMeta{ID: id, Level: 0}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%s): %v", id, err)
		}
	}
	token, err := manifestStore.ClaimMaintenance(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}
	staged, err := manifestStore.StageMaintenance(ctx, MaintenanceCommand{
		ID:   "compact-1",
		Kind: MaintenanceCommandCompaction,
		Compaction: &CompactionCommand{
			Payload: CompactionLogPayload{
				RemoveSSTableIDs: []string{"a", "b"},
				SourceLevel:      0,
				DestinationLevel: 1,
				AddSSTables:      []SSTMeta{{ID: "c", Level: 1}},
			},
			RetiredObjects: []RetiredObject{
				{Kind: RetiredObjectSST, ID: "a", Key: "sstable/a"},
				{Kind: RetiredObjectSST, ID: "b", Key: "sstable/b"},
			},
		},
	}, token)
	if err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}

	result, err := manifestStore.ApplyPendingMaintenance(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingMaintenance: %v", err)
	}
	if !result.Changed || result.Status != MaintenanceStatusApplied {
		t.Fatalf("result=%+v, want changed applied", result)
	}
	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if !receiptMatchesCommand(current.MaintenanceReceipt, staged.Pending) {
		t.Fatalf("receipt=%+v pending=%+v", current.MaintenanceReceipt, staged.Pending)
	}
	replayed, err := NewStoreWithStorage(NewBlobStoreBackend(store)).Replay(ctx)
	if err != nil {
		t.Fatalf("Replay after compaction: %v", err)
	}
	if replayed.LookupSST("a") != nil || replayed.LookupSST("b") != nil || replayed.LookupSST("c") == nil {
		t.Fatalf("unexpected replayed topology: a=%v b=%v c=%v",
			replayed.LookupSST("a"), replayed.LookupSST("b"), replayed.LookupSST("c"))
	}

	repeated, err := manifestStore.ApplyPendingMaintenance(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingMaintenance(retry): %v", err)
	}
	if repeated.Changed {
		t.Fatalf("retry changed CURRENT: %+v", repeated)
	}
}

func TestWriterRejectsStaleCheckpointWithDurableReceipt(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-reject-checkpoint")
	defer store.Close()

	manifestStore := NewStoreWithStorage(NewBlobStoreBackend(store))
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	token, err := manifestStore.ClaimMaintenance(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}
	if _, err := manifestStore.StageMaintenance(ctx, MaintenanceCommand{
		ID:   "stale-checkpoint",
		Kind: MaintenanceCommandCheckpoint,
		Checkpoint: &CheckpointCommand{
			Snapshot:        testObjectRef("manifest/snapshots/stale.manifest.zst"),
			BaseSnapshot:    testObjectRefPtr("different-snapshot"),
			SnapshotNextSeq: 1,
		},
	}, token); err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}

	result, err := manifestStore.ApplyPendingMaintenance(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingMaintenance: %v", err)
	}
	if result.Status != MaintenanceStatusRejected || !result.Changed {
		t.Fatalf("result=%+v, want changed rejected", result)
	}
	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.Snapshot != nil || current.MaintenanceReceipt == nil || current.MaintenanceReceipt.Status != MaintenanceStatusRejected {
		t.Fatalf("current=%+v", current)
	}
}
