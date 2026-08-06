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

func TestMaintenanceRetentionDefaultsUseExplicitUnits(t *testing.T) {
	maintenance := DefaultMaintenanceOptions()
	if maintenance.Checkpoint.MaxReplayPages != DefaultCheckpointReplayPages {
		t.Fatalf("MaxReplayPages=%d, want %d", maintenance.Checkpoint.MaxReplayPages, DefaultCheckpointReplayPages)
	}

	retention := DefaultRetentionPolicy()
	if retention.KeepAtLeastSSTs != 10 {
		t.Fatalf("KeepAtLeastSSTs=%d, want 10", retention.KeepAtLeastSSTs)
	}
	if retention.KeepAtLeastWindows != 1 {
		t.Fatalf("KeepAtLeastWindows=%d, want 1", retention.KeepAtLeastWindows)
	}

	changeFeed := DefaultChangeFeedRetentionPolicy()
	if changeFeed.KeepAtLeastManifestEntries != 1024 {
		t.Fatalf("KeepAtLeastManifestEntries=%d, want 1024", changeFeed.KeepAtLeastManifestEntries)
	}
}

func TestMaintenanceRunOnceCheckpointsAtReplayPageLimit(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-checkpoint")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	if _, err := db.manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := 0; i < 64; i++ {
		if _, err := db.manifestStore.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{
			ID:    fmt.Sprintf("sst-%03d", i),
			Level: 0,
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%d): %v", i, err)
		}
	}
	beforeMaintenance, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(before maintenance): %v", err)
	}
	if beforeMaintenance.StateReplayPages < 1 {
		t.Fatalf("StateReplayPages before maintenance=%d, want at least 1", beforeMaintenance.StateReplayPages)
	}

	opts := DefaultMaintenanceOptions()
	opts.Checkpoint.MaxReplayPages = 1
	opts.Compaction.L0SSTCount = 10_000
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)
	beforeRun, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(before run): %v", err)
	}
	if beforeRun.StateReplayPages < 1 {
		t.Fatalf("StateReplayPages before run=%d, want at least 1", beforeRun.StateReplayPages)
	}

	var stats MaintenanceStats
	for attempt := 0; attempt < 8; attempt++ {
		stats, err = maintenance.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce(%d): %v", attempt, err)
		}
		head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
		if err != nil {
			t.Fatalf("ReadMaintenanceHead(%d): %v", attempt, err)
		}
		if head != nil && head.Pending != nil {
			if _, err := db.manifestStore.ApplyPendingMaintenance(ctx); err != nil {
				t.Fatalf("ApplyPendingMaintenance(%d): %v", attempt, err)
			}
		}
		if stats.CheckpointStaged {
			break
		}
	}
	if !stats.CheckpointStaged {
		t.Fatal("maintenance did not stage a checkpoint")
	}
	if stats.CheckpointReplayPages < 1 || stats.CheckpointReplayBytes == 0 {
		t.Fatalf("checkpoint stats=(%d pages, %d bytes), want non-zero", stats.CheckpointReplayPages, stats.CheckpointReplayBytes)
	}
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.StateReplayPages != 0 || current.StateReplayBytes != 0 {
		t.Fatalf("replay accounting after maintenance=(%d pages, %d bytes), want zero",
			current.StateReplayPages, current.StateReplayBytes)
	}
	if current.Snapshot == "" {
		t.Fatal("maintenance did not publish a snapshot")
	}
}

func TestDBOpenMaintenanceRejectsSecondActiveHandle(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-single-owner")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	first, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance(first): %v", err)
	}
	if _, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions()); !errors.Is(err, ErrMaintenanceAlreadyOpen) {
		t.Fatalf("OpenMaintenance(second) error=%v, want %v", err, ErrMaintenanceAlreadyOpen)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	second, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance(after close): %v", err)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
}

func TestDBOpenMaintenanceConcurrentCallsAllowOneHandle(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-concurrent-owner")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	const callers = 16
	type result struct {
		maintenance *Maintenance
		err         error
	}
	start := make(chan struct{})
	results := make(chan result, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
			results <- result{maintenance: maintenance, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var opened *Maintenance
	for result := range results {
		switch {
		case result.err == nil:
			if opened != nil {
				t.Fatal("more than one concurrent OpenMaintenance call succeeded")
			}
			opened = result.maintenance
		case !errors.Is(result.err, ErrMaintenanceAlreadyOpen):
			t.Fatalf("OpenMaintenance error=%v, want %v", result.err, ErrMaintenanceAlreadyOpen)
		}
	}
	if opened == nil {
		t.Fatal("no concurrent OpenMaintenance call succeeded")
	}
	if err := opened.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDBOpenMaintenanceRejectsInvalidPolicyAndReleasesReservation(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-invalid-policy")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	retention := DefaultRetentionPolicy()
	retention.Mode = RetentionMode(255)
	opts := DefaultMaintenanceOptions()
	opts.Retention = &retention
	if _, err := db.OpenMaintenance(ctx, opts); !errors.Is(err, ErrInvalidMaintenanceOptions) {
		t.Fatalf("OpenMaintenance(invalid) error=%v, want %v", err, ErrInvalidMaintenanceOptions)
	}

	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance(after invalid policy): %v", err)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMaintenanceStagesShareOneMailboxClaim(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-shared-fence")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	retention := DefaultRetentionPolicy()
	changeFeed := DefaultChangeFeedRetentionPolicy()
	opts := DefaultMaintenanceOptions()
	opts.OwnerID = "maintenance-owner"
	opts.Retention = &retention
	opts.ChangeFeedRetention = &changeFeed

	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	want := maintenance.fenceToken
	assertFenceTokenEqual(t, maintenance.compactor.fenceToken, want)
	assertFenceTokenEqual(t, maintenance.retention.fenceToken, want)
	assertFenceTokenEqual(t, maintenance.changeFeed.fenceToken, want)

	head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("ReadMaintenanceHead: %v", err)
	}
	if head == nil || head.Epoch != want.Epoch || head.OwnerID != want.Owner || !head.ClaimedAt.Equal(want.ClaimedAt) {
		t.Fatalf("maintenance HEAD=%+v, token=%+v", head, want)
	}

	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current != nil && current.CompactorFence != nil {
		t.Fatalf("CURRENT compactor fence=%+v, want nil", current.CompactorFence)
	}

	seqs, err := db.manifestStore.ListEntries(ctx)
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	claims := 0
	for _, seq := range seqs {
		entry, err := db.manifestStore.ReadEntry(ctx, seq)
		if err != nil {
			t.Fatalf("ReadEntry(%d): %v", seq, err)
		}
		if entry.Op == manifest.LogOpFenceClaim && entry.Role == manifest.FenceRoleCompactor {
			claims++
		}
	}
	if claims != 0 {
		t.Fatalf("compactor fence claims=%d, want 0", claims)
	}
}

func TestPendingMaintenanceSurvivesWriterReplacement(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-writer-replacement")
	defer store.Close()

	firstDB, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB(first): %v", err)
	}
	defer firstDB.Close()
	secondDB, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB(second): %v", err)
	}
	defer secondDB.Close()

	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	firstWriter, err := firstDB.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("OpenWriter(first): %v", err)
	}
	maintenance, err := firstDB.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)
	if err := maintenance.stageCommand(ctx, manifest.MaintenanceCommand{
		Kind:            manifest.MaintenanceCommandRetirementFloor,
		RetirementFloor: &manifest.AdvanceFloorCommand{Floor: 1},
	}); err != nil {
		t.Fatalf("stageCommand: %v", err)
	}

	secondWriter, err := secondDB.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("OpenWriter(second): %v", err)
	}
	defer secondWriter.Close(ctx)
	if err := secondWriter.Flush(ctx); err != nil {
		t.Fatalf("second writer Flush: %v", err)
	}
	waiting, err := maintenance.reconcilePendingCommand(ctx)
	if err != nil {
		t.Fatalf("reconcilePendingCommand: %v", err)
	}
	if waiting {
		t.Fatal("maintenance still waiting after replacement writer applied command")
	}
	head, _, err := firstDB.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("ReadMaintenanceHead: %v", err)
	}
	if head.Pending != nil {
		t.Fatalf("pending=%+v, want nil", head.Pending)
	}
	if err := firstWriter.Put(ctx, []byte("stale"), []byte("writer")); err != nil {
		t.Fatalf("old writer Put: %v", err)
	}
	if err := firstWriter.Flush(ctx); !errors.Is(err, manifest.ErrFenced) {
		t.Fatalf("old writer Flush error=%v, want %v", err, manifest.ErrFenced)
	}
}

func TestNewMaintenanceOwnerClearsReceiptAfterPreviousOwnerStops(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-receipt-recovery")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer writer.Close(ctx)

	first, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance(first): %v", err)
	}
	if err := first.stageCommand(ctx, manifest.MaintenanceCommand{
		Kind:            manifest.MaintenanceCommandRetirementFloor,
		RetirementFloor: &manifest.AdvanceFloorCommand{Floor: 1},
	}); err != nil {
		t.Fatalf("stageCommand: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("writer Flush: %v", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	secondOpts := DefaultMaintenanceOptions()
	secondOpts.OwnerID = "maintenance-replacement"
	second, err := db.OpenMaintenance(ctx, secondOpts)
	if err != nil {
		t.Fatalf("OpenMaintenance(second): %v", err)
	}
	defer second.Close(ctx)
	waiting, err := second.reconcilePendingCommand(ctx)
	if err != nil {
		t.Fatalf("reconcilePendingCommand: %v", err)
	}
	if waiting {
		t.Fatal("replacement maintenance owner did not observe durable receipt")
	}
	head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("ReadMaintenanceHead: %v", err)
	}
	if head.Pending != nil {
		t.Fatalf("pending=%+v, want nil", head.Pending)
	}
}

func TestMaintenanceRunStopsOnClose(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-run-close")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	opts := DefaultMaintenanceOptions()
	opts.Every = time.Hour
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- maintenance.Run(ctx) }()
	waitForCondition(t, time.Second, maintenance.running.Load, "maintenance Run did not start")

	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := maintenance.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error after Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after Close")
	}
	if _, err := maintenance.RunOnce(ctx); !errors.Is(err, ErrMaintenanceClosed) {
		t.Fatalf("RunOnce(after Close) error=%v, want %v", err, ErrMaintenanceClosed)
	}
}

func TestStaleMaintenanceCannotRunChangeFeedCleanup(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-stale-fence")
	defer store.Close()

	firstDB, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB(first): %v", err)
	}
	defer firstDB.Close()
	secondDB, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("OpenDB(second): %v", err)
	}
	defer secondDB.Close()

	changeFeed := DefaultChangeFeedRetentionPolicy()
	firstOpts := DefaultMaintenanceOptions()
	firstOpts.OwnerID = "maintenance-first"
	firstOpts.ChangeFeedRetention = &changeFeed
	first, err := firstDB.OpenMaintenance(ctx, firstOpts)
	if err != nil {
		t.Fatalf("OpenMaintenance(first): %v", err)
	}
	defer first.Close(ctx)

	secondOpts := DefaultMaintenanceOptions()
	secondOpts.OwnerID = "maintenance-second"
	second, err := secondDB.OpenMaintenance(ctx, secondOpts)
	if err != nil {
		t.Fatalf("OpenMaintenance(second): %v", err)
	}
	defer second.Close(ctx)

	stats, err := first.RunOnce(ctx)
	if !errors.Is(err, manifest.ErrFenced) {
		t.Fatalf("stale RunOnce error=%v, want %v", err, manifest.ErrFenced)
	}
	if stats.Duration <= 0 {
		t.Fatalf("stale RunOnce duration=%v, want partial cycle stats", stats.Duration)
	}
}

func assertFenceTokenEqual(t *testing.T, got, want *manifest.FenceToken) {
	t.Helper()
	if got == nil || want == nil {
		t.Fatalf("fence token got=%+v want=%+v", got, want)
	}
	if got.Epoch != want.Epoch || got.Owner != want.Owner || !got.ClaimedAt.Equal(want.ClaimedAt) {
		t.Fatalf("fence token got=%+v want=%+v", got, want)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(message)
}
