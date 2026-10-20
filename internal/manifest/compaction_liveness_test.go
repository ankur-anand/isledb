package manifest

import (
	"context"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

// TestCompactionCommandOverMergedInputsIsRejected stages a drain planned
// before the writer merged two of its inputs away. Applying it would add an
// L1 output holding data the merged L0 file still holds, and retire the
// inputs twice; the writer rejects it instead, and maintenance plans again.
func TestCompactionCommandOverMergedInputsIsRejected(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("compaction-liveness")
	defer blob.Close()
	store := NewStore(blob)
	writerWithFlushes(t, store) // L0: c, b, a
	token, err := store.ClaimMaintenance(ctx, "maintenance")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}

	// Maintenance plans a drain of a and b into L1...
	drain := MaintenanceCommand{ID: "drain-ab", Kind: MaintenanceCommandCompaction,
		Compaction: &CompactionCommand{
			Payload: CompactionLogPayload{
				RemoveSSTableIDs: []string{"b", "a"},
				SourceLevel:      0,
				DestinationLevel: 1,
				AddSSTables:      []SSTMeta{{ID: "l1-ab", SeqLo: 1, SeqHi: 20, MinKey: []byte("a"), MaxKey: []byte("z"), Level: 1, Size: 200}},
			},
			RetiredObjects: retiredSSTs("b", "a"),
		}}
	head, err := store.StageMaintenance(ctx, drain, token)
	if err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}
	// ...while the writer merges b and a itself.
	if _, err := store.AppendWriterL0Merge(ctx, WriterL0Merge{
		ID: "merge-ba", InputIDs: []string{"b", "a"}, Output: flushSST("compacted-ba", 1, 20), Retired: retiredSSTs("b", "a"),
	}); err != nil {
		t.Fatalf("AppendWriterL0Merge: %v", err)
	}
	before := currentOf(t, store)

	result, err := store.ApplyMaintenanceCommand(ctx, *head.Pending)
	if err != nil {
		t.Fatalf("ApplyMaintenanceCommand: %v", err)
	}
	if result.Status != MaintenanceStatusRejected {
		t.Fatalf("status %q, want rejected", result.Status)
	}
	after := currentOf(t, store)
	if after.NextSeq != before.NextSeq {
		t.Fatalf("rejected command appended an entry: next %d -> %d", before.NextSeq, after.NextSeq)
	}
	if after.MaintenanceReceipt == nil || after.MaintenanceReceipt.CommandID != drain.ID ||
		after.MaintenanceReceipt.Status != MaintenanceStatusRejected {
		t.Fatalf("receipt=%+v", after.MaintenanceReceipt)
	}
	m, err := store.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if got := l0IDs(m); got != "c,compacted-ba" {
		t.Fatalf("L0 = %s, want c,compacted-ba", got)
	}
	if len(m.Levels) != 0 {
		t.Fatalf("L1 created by a rejected drain: %+v", m.Levels)
	}

	// A drain over live inputs still applies.
	if _, err := store.ClearMaintenance(ctx, head.Pending.ID, head.Pending.Epoch, head.Pending.Generation, token); err != nil {
		t.Fatalf("ClearMaintenance: %v", err)
	}
	drain.ID = "drain-merged"
	drain.Compaction.Payload.RemoveSSTableIDs = []string{"compacted-ba"}
	drain.Compaction.RetiredObjects = retiredSSTs("compacted-ba")
	head, err = store.StageMaintenance(ctx, drain, token)
	if err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}
	result, err = store.ApplyMaintenanceCommand(ctx, *head.Pending)
	if err != nil {
		t.Fatalf("ApplyMaintenanceCommand: %v", err)
	}
	if result.Status != MaintenanceStatusApplied {
		t.Fatalf("status %q, want applied", result.Status)
	}
}
