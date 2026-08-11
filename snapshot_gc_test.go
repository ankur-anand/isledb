package isledb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestSnapshotCleanerMarksCheckpointOutcome(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("snapshot-cleaner-outcome")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "snapshot-outcome-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}

	base, err := db.manifestStore.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("PrepareCheckpoint(base): %v", err)
	}
	candidate, err := db.manifestStore.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("PrepareCheckpoint(candidate): %v", err)
	}

	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	cleaner := newSnapshotCleaner(store, db.manifestStore, snapshotCleanerOptions{
		Now:          func() time.Time { return now },
		SafetyMargin: 2 * time.Minute,
	})
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	current.MaxPinnedViewAge = 10 * time.Minute

	tests := []struct {
		name       string
		status     manifest.MaintenanceStatus
		wantMarked manifest.ObjectRef
	}{
		{name: "applied retires base", status: manifest.MaintenanceStatusApplied, wantMarked: base.Snapshot},
		{name: "rejected retires candidate", status: manifest.MaintenanceStatusRejected, wantMarked: candidate.Snapshot},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := &manifest.MaintenanceCommand{
				ID:         test.name,
				Epoch:      7,
				Generation: uint64(index + 1),
				Kind:       manifest.MaintenanceCommandCheckpoint,
				Checkpoint: &manifest.CheckpointCommand{
					Snapshot:     candidate.Snapshot,
					BaseSnapshot: &base.Snapshot,
				},
			}
			receipt := &manifest.MaintenanceReceipt{
				CommandID:  command.ID,
				Epoch:      command.Epoch,
				Generation: command.Generation,
				Status:     test.status,
				AppliedAt:  now,
			}
			marked, err := cleaner.markCheckpointOutcome(ctx, current, command, receipt)
			if err != nil {
				t.Fatalf("markCheckpointOutcome: %v", err)
			}
			if !marked {
				t.Fatal("markCheckpointOutcome did not create a marker")
			}
			mark, err := cleaner.readMark(ctx, snapshotRetirementMarkerPath(store, test.wantMarked.Path))
			if err != nil {
				t.Fatalf("readMark: %v", err)
			}
			if mark.Path != test.wantMarked.Path {
				t.Fatalf("marked path=%q, want %q", mark.Path, test.wantMarked.Path)
			}
			wantNotBefore := now.Add(12 * time.Minute)
			if !mark.NotBefore.Equal(wantNotBefore) {
				t.Fatalf("NotBefore=%s, want %s", mark.NotBefore, wantNotBefore)
			}
		})
	}
}

func TestSnapshotCleanerExtendsExistingRetirementDeadline(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("snapshot-cleaner-extend-deadline")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	cleaner := newSnapshotCleaner(store, db.manifestStore, snapshotCleanerOptions{
		SafetyMargin: time.Minute,
		Now:          func() time.Time { return now },
	})
	ref := manifest.ObjectRef{Path: store.ManifestSnapshotPath("extend-deadline")}
	if _, err := store.Write(ctx, ref.Path, []byte("snapshot")); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if marked, err := cleaner.mark(ctx, ref, now, 5*time.Minute, "initial_orphan"); err != nil || !marked {
		t.Fatalf("initial mark=(%v, %v), want (true, nil)", marked, err)
	}
	later := now.Add(30 * time.Minute)
	if marked, err := cleaner.mark(ctx, ref, later, 5*time.Minute, "checkpoint_replaced"); err != nil || !marked {
		t.Fatalf("extended mark=(%v, %v), want (true, nil)", marked, err)
	}
	mark, err := cleaner.readMark(ctx, snapshotRetirementMarkerPath(store, ref.Path))
	if err != nil {
		t.Fatalf("readMark: %v", err)
	}
	wantNotBefore := later.Add(6 * time.Minute)
	if !mark.NotBefore.Equal(wantNotBefore) || mark.Reason != "checkpoint_replaced" {
		t.Fatalf("updated marker=%+v, want deadline=%s and latest reason", mark, wantNotBefore)
	}
}

func TestSnapshotCleanerRetiresRejectedCheckpointCandidate(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("snapshot-cleaner-rejected-checkpoint")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "snapshot-rejection-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	checkpoint, err := db.manifestStore.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("PrepareCheckpoint: %v", err)
	}
	// Keep the command structurally valid but make its ObjectRef fail the
	// writer's content verification.
	invalidCheckpoint := checkpoint
	invalidCheckpoint.Snapshot.Checksum = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if err := maintenance.stageCommand(ctx, manifest.MaintenanceCommand{
		Kind:       manifest.MaintenanceCommandCheckpoint,
		Checkpoint: &invalidCheckpoint,
	}); err != nil {
		t.Fatalf("stage invalid checkpoint: %v", err)
	}

	result, err := db.manifestStore.ApplyPendingMaintenance(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingMaintenance: %v", err)
	}
	if result.Status != manifest.MaintenanceStatusRejected {
		t.Fatalf("checkpoint status=%s, want rejected", result.Status)
	}
	reconcileSnapshotCheckpoint(t, ctx, maintenance)

	markerPath := snapshotRetirementMarkerPath(store, checkpoint.Snapshot.Path)
	mark, err := maintenance.snapshotGC.readMark(ctx, markerPath)
	if err != nil {
		t.Fatalf("read rejected-candidate marker: %v", err)
	}
	if mark.Path != checkpoint.Snapshot.Path || mark.Reason != "checkpoint_rejected" {
		t.Fatalf("rejected-candidate marker=%+v", mark)
	}
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.Snapshot != nil {
		t.Fatalf("rejected checkpoint published snapshot=%+v", current.Snapshot)
	}
	requireSnapshotObject(t, ctx, store, checkpoint.Snapshot.Path, true)
	stats, err := maintenance.snapshotGC.sweep(ctx, mark.NotBefore)
	if err != nil {
		t.Fatalf("sweep rejected candidate: %v", err)
	}
	if stats.SnapshotsDeleted != 1 {
		t.Fatalf("sweep stats=%+v, want candidate deletion", stats)
	}
	requireSnapshotObject(t, ctx, store, checkpoint.Snapshot.Path, false)
}

func TestSnapshotCleanerRetirementSurvivesMaintenanceRestart(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("snapshot-cleaner-restart")
	defer store.Close()

	const pinnedAge = 5 * time.Minute
	db, err := openDB(ctx, store, dbOpenOptions{storePolicy: StorePolicy{MaxPinnedViewAge: pinnedAge}})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	writerOptions := DefaultWriterOptions()
	writerOptions.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, writerOptions)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := writer.Put(ctx, []byte("key-1"), []byte("value-1")); err != nil {
		t.Fatalf("Put(key-1): %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush(key-1): %v", err)
	}

	first, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance(first): %v", err)
	}
	firstCheckpoint := stageSnapshotCheckpoint(t, ctx, db.manifestStore, first)
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("apply first checkpoint: %v", err)
	}
	reconcileSnapshotCheckpoint(t, ctx, first)

	if err := writer.Put(ctx, []byte("key-2"), []byte("value-2")); err != nil {
		t.Fatalf("Put(key-2): %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush(key-2): %v", err)
	}
	secondCheckpoint := stageSnapshotCheckpoint(t, ctx, db.manifestStore, first)
	if secondCheckpoint.BaseSnapshot == nil || secondCheckpoint.BaseSnapshot.Path != firstCheckpoint.Snapshot.Path {
		t.Fatalf("second checkpoint base=%+v, want %q", secondCheckpoint.BaseSnapshot, firstCheckpoint.Snapshot.Path)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("apply second checkpoint: %v", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatalf("Close(first maintenance): %v", err)
	}

	// A corrupt pre-existing marker simulates a failed/partial durable cleanup
	// record. Reconciliation must not clear HEAD until that record is valid.
	markerPath := snapshotRetirementMarkerPath(store, firstCheckpoint.Snapshot.Path)
	if _, err := store.Write(ctx, markerPath, []byte("not-json")); err != nil {
		t.Fatalf("write corrupt marker: %v", err)
	}

	second, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance(second): %v", err)
	}
	second.snapshotGC.opts.SafetyMargin = 2 * time.Minute
	if _, err := second.RunOnce(ctx); err == nil {
		t.Fatal("RunOnce succeeded with a corrupt retirement marker")
	}
	head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("ReadMaintenanceHead: %v", err)
	}
	if head == nil || head.Pending == nil || head.Pending.Checkpoint == nil {
		t.Fatal("checkpoint command was cleared before retirement became durable")
	}
	if err := store.Delete(ctx, markerPath); err != nil {
		t.Fatalf("delete corrupt marker: %v", err)
	}

	stats, err := second.RunOnce(ctx)
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if stats.ManifestCleanup.Snapshots.SnapshotsMarked != 1 {
		t.Fatalf("SnapshotsMarked=%d, want 1", stats.ManifestCleanup.Snapshots.SnapshotsMarked)
	}
	head, _, err = db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("ReadMaintenanceHead(after retry): %v", err)
	}
	if head.Pending != nil && head.Pending.Checkpoint != nil &&
		head.Pending.Checkpoint.Snapshot.Path == secondCheckpoint.Snapshot.Path {
		t.Fatalf("reconciled checkpoint remained pending: %+v", head.Pending)
	}

	mark, err := second.snapshotGC.readMark(ctx, markerPath)
	if err != nil {
		t.Fatalf("read durable marker: %v", err)
	}
	if mark.Path != firstCheckpoint.Snapshot.Path {
		t.Fatalf("marker path=%q, want %q", mark.Path, firstCheckpoint.Snapshot.Path)
	}

	before, err := second.snapshotGC.sweep(ctx, mark.NotBefore.Add(-time.Nanosecond))
	if err != nil {
		t.Fatalf("sweep before grace: %v", err)
	}
	if before.Deferred != 1 || before.SnapshotsDeleted != 0 {
		t.Fatalf("before-grace stats=%+v", before)
	}
	requireSnapshotObject(t, ctx, store, firstCheckpoint.Snapshot.Path, true)

	after, err := second.snapshotGC.sweep(ctx, mark.NotBefore)
	if err != nil {
		t.Fatalf("sweep after grace: %v", err)
	}
	if after.SnapshotsDeleted != 1 || after.MarkersCleared != 1 {
		t.Fatalf("after-grace stats=%+v", after)
	}
	requireSnapshotObject(t, ctx, store, firstCheckpoint.Snapshot.Path, false)
	requireSnapshotObject(t, ctx, store, secondCheckpoint.Snapshot.Path, true)

	if err := second.Close(ctx); err != nil {
		t.Fatalf("Close(second maintenance): %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}

	reopened, err := openDB(ctx, store, dbOpenOptions{storePolicy: StorePolicy{MaxPinnedViewAge: pinnedAge}})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	defer reopened.Close()
	reader, err := reopened.OpenReader(ctx, DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	for key, want := range map[string]string{"key-1": "value-1", "key-2": "value-2"} {
		value, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if !found || string(value) != want {
			t.Fatalf("Get(%s)=(%q, %v), want (%q, true)", key, value, found, want)
		}
	}
}

func TestSnapshotCleanerOrphanAuditIsBoundedAndProtectsLiveSnapshots(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("snapshot-cleaner-orphans")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "snapshot-orphan-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	pending, err := db.manifestStore.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("PrepareCheckpoint(pending): %v", err)
	}
	if err := maintenance.stageCommand(ctx, manifest.MaintenanceCommand{
		Kind:       manifest.MaintenanceCommandCheckpoint,
		Checkpoint: &pending,
	}); err != nil {
		t.Fatalf("stage pending checkpoint: %v", err)
	}

	for _, id := range []string{"orphan-a", "orphan-b", "orphan-c"} {
		if _, err := store.Write(ctx, store.ManifestSnapshotPath(id), []byte("orphan")); err != nil {
			t.Fatalf("write %s: %v", id, err)
		}
	}
	now := time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC)
	cleaner := newSnapshotCleaner(store, db.manifestStore, snapshotCleanerOptions{
		OrphanScanLimit: 2,
		Now:             func() time.Time { return now },
		SafetyMargin:    time.Minute,
	})
	stats, err := cleaner.discoverOrphans(ctx, now)
	if err != nil {
		t.Fatalf("discoverOrphans: %v", err)
	}
	if stats.ObjectsScanned != 2 {
		t.Fatalf("ObjectsScanned=%d, want 2", stats.ObjectsScanned)
	}
	if stats.Protected != 1 {
		t.Fatalf("Protected=%d, want pending snapshot to be observed", stats.Protected)
	}
	if _, err := store.Attributes(ctx, snapshotRetirementMarkerPath(store, pending.Snapshot.Path)); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("pending snapshot marker error=%v, want not found", err)
	}
}

func TestSnapshotCleanerSweepHonorsDeleteBatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("snapshot-cleaner-delete-batch")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	now := time.Date(2026, 8, 10, 14, 0, 0, 0, time.UTC)
	cleaner := newSnapshotCleaner(store, db.manifestStore, snapshotCleanerOptions{
		DeleteBatchSize: 2,
		MarkerScanLimit: 10,
		SafetyMargin:    time.Nanosecond,
		Now:             func() time.Time { return now },
	})
	for _, id := range []string{"batch-a", "batch-b", "batch-c", "batch-d", "batch-e"} {
		ref := manifest.ObjectRef{Path: store.ManifestSnapshotPath(id)}
		if _, err := store.Write(ctx, ref.Path, []byte("snapshot")); err != nil {
			t.Fatalf("write %s: %v", id, err)
		}
		if _, err := cleaner.mark(ctx, ref, now.Add(-time.Hour), time.Minute, "test_batch"); err != nil {
			t.Fatalf("mark %s: %v", id, err)
		}
	}

	stats, err := cleaner.sweep(ctx, now)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if stats.DeleteAttempts != 2 || stats.SnapshotsDeleted != 2 {
		t.Fatalf("sweep stats=%+v, want exactly two deletions", stats)
	}
	remaining, err := store.List(ctx, blobstore.ListOptions{Prefix: "manifest/snapshots/"})
	if err != nil {
		t.Fatalf("List snapshots: %v", err)
	}
	if len(remaining.Objects) != 3 {
		t.Fatalf("remaining snapshots=%d, want 3", len(remaining.Objects))
	}
}

func TestSnapshotCleanerSweepKeepsCurrentSnapshot(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("snapshot-cleaner-current")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "snapshot-current-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	checkpoint := stageSnapshotCheckpoint(t, ctx, db.manifestStore, maintenance)
	if _, err := db.manifestStore.ApplyPendingMaintenance(ctx); err != nil {
		t.Fatalf("ApplyPendingMaintenance: %v", err)
	}
	if _, err := maintenance.reconcilePendingCommand(ctx); err != nil {
		t.Fatalf("reconcilePendingCommand: %v", err)
	}

	now := time.Now().UTC()
	cleaner := newSnapshotCleaner(store, db.manifestStore, snapshotCleanerOptions{Now: func() time.Time { return now }})
	if _, err := cleaner.mark(ctx, checkpoint.Snapshot, now.Add(-2*time.Hour), time.Minute, "test_live_snapshot"); err != nil {
		t.Fatalf("mark current snapshot: %v", err)
	}
	stats, err := cleaner.sweep(ctx, now)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if stats.Protected != 1 || stats.MarkersCleared != 1 || stats.SnapshotsDeleted != 0 {
		t.Fatalf("sweep stats=%+v", stats)
	}
	requireSnapshotObject(t, ctx, store, checkpoint.Snapshot.Path, true)
}

func stageSnapshotCheckpoint(
	t *testing.T,
	ctx context.Context,
	manifestStore *manifest.Store,
	maintenance *Maintenance,
) manifest.CheckpointCommand {
	t.Helper()
	checkpoint, err := manifestStore.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("PrepareCheckpoint: %v", err)
	}
	if err := maintenance.stageCommand(ctx, manifest.MaintenanceCommand{
		Kind:       manifest.MaintenanceCommandCheckpoint,
		Checkpoint: &checkpoint,
	}); err != nil {
		t.Fatalf("stage checkpoint: %v", err)
	}
	return checkpoint
}

func reconcileSnapshotCheckpoint(t *testing.T, ctx context.Context, maintenance *Maintenance) {
	t.Helper()
	waiting, err := maintenance.reconcilePendingCommand(ctx)
	if err != nil {
		t.Fatalf("reconcilePendingCommand: %v", err)
	}
	if waiting {
		t.Fatal("checkpoint still waiting after publication")
	}
	// This helper invokes the reconciliation phase directly instead of a full
	// RunOnce cycle, so mirror RunOnce's per-cycle command bookkeeping reset.
	maintenance.statsMu.Lock()
	maintenance.commandStaged = false
	maintenance.statsMu.Unlock()
}

func requireSnapshotObject(t *testing.T, ctx context.Context, store *blobstore.Store, path string, exists bool) {
	t.Helper()
	_, err := store.Attributes(ctx, path)
	if exists && err != nil {
		t.Fatalf("snapshot %q does not exist: %v", path, err)
	}
	if !exists && !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("snapshot %q existence error=%v, want not found", path, err)
	}
}
