package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestMaintenanceDefaultsUseExplicitUnits(t *testing.T) {
	maintenance := DefaultMaintenanceOptions()
	normalized, err := normalizeMaintenanceOptions(maintenance)
	if err != nil {
		t.Fatalf("normalizeMaintenanceOptions: %v", err)
	}
	if normalized.manifestCheckpoint.TargetReplayPages != defaultCheckpointReplayPages {
		t.Fatalf("checkpointReplayPages=%d, want %d", normalized.manifestCheckpoint.TargetReplayPages, defaultCheckpointReplayPages)
	}
	if normalized.manifestCheckpoint.TargetReplayBytes != defaultCheckpointReplayBytes {
		t.Fatalf("checkpointReplayBytes=%d, want %d", normalized.manifestCheckpoint.TargetReplayBytes, defaultCheckpointReplayBytes)
	}

	changeFeed := DefaultChangeFeedRetentionOptions()
	if changeFeed.RetainFor != 7*24*time.Hour {
		t.Fatalf("RetainFor=%v, want 7 days", changeFeed.RetainFor)
	}
}

func TestNormalizeMaintenanceSSTCompactionOptions(t *testing.T) {
	opts := MaintenanceOptions{
		SSTCompaction: SSTCompactionOptions{L0TriggerSSTs: 12},
	}
	normalized, err := normalizeMaintenanceOptions(opts)
	if err != nil {
		t.Fatalf("normalizeMaintenanceOptions: %v", err)
	}
	defaults := DefaultMaintenanceOptions()
	if normalized.sstCompaction.L0TriggerSSTs != 12 {
		t.Fatalf("L0TriggerSSTs=%d, want 12", normalized.sstCompaction.L0TriggerSSTs)
	}
	if normalized.sstCompaction.ReadConcurrency != defaults.SSTCompaction.ReadConcurrency ||
		normalized.sstCompaction.LevelGrowthFactor != defaults.SSTCompaction.LevelGrowthFactor {
		t.Fatalf("zero fields did not inherit defaults: %+v", normalized.sstCompaction)
	}

	tests := []struct {
		name string
		opts SSTCompactionOptions
	}{
		{name: "negative", opts: SSTCompactionOptions{ReadConcurrency: -1}},
		{name: "negative input bytes", opts: SSTCompactionOptions{MaxInputBytesPerJob: -1}},
		{name: "invalid growth", opts: SSTCompactionOptions{LevelGrowthFactor: 1}},
		{name: "too many inputs", opts: SSTCompactionOptions{MaxInputSSTsPerJob: manifest.MaxRetiredObjectsPerEntry + 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizeMaintenanceOptions(MaintenanceOptions{SSTCompaction: test.opts})
			if !errors.Is(err, ErrInvalidMaintenanceOptions) {
				t.Fatalf("normalize error=%v, want %v", err, ErrInvalidMaintenanceOptions)
			}
		})
	}
}

func TestNormalizeMaintenanceCheckpointOptions(t *testing.T) {
	opts := MaintenanceOptions{
		ManifestCheckpoint: ManifestCheckpointOptions{
			TargetReplayPages: 17,
			TargetReplayBytes: 1234,
		},
	}
	normalized, err := normalizeMaintenanceOptions(opts)
	if err != nil {
		t.Fatalf("normalizeMaintenanceOptions: %v", err)
	}
	if normalized.manifestCheckpoint != opts.ManifestCheckpoint {
		t.Fatalf("checkpoint=%+v, want %+v", normalized.manifestCheckpoint, opts.ManifestCheckpoint)
	}
}

func TestMaintenanceStateString(t *testing.T) {
	for state, want := range map[MaintenanceState]string{
		MaintenanceIdle:             "idle",
		MaintenanceWaitingForWriter: "waiting_for_writer",
		MaintenanceState(255):       "MaintenanceState(255)",
	} {
		if got := state.String(); got != want {
			t.Errorf("state %d String()=%q, want %q", state, got, want)
		}
	}
}

func TestMaintenanceTaskString(t *testing.T) {
	for task, want := range map[MaintenanceTask]string{
		MaintenanceTaskNone:               "none",
		MaintenanceTaskSSTCompaction:      "sst_compaction",
		MaintenanceTaskManifestCheckpoint: "manifest_checkpoint",
		MaintenanceTask(255):              "MaintenanceTask(255)",
	} {
		if got := task.String(); got != want {
			t.Errorf("task %d String()=%q, want %q", task, got, want)
		}
	}
}

func TestMaintenanceRecordsPublicChangeFeedCleanupStats(t *testing.T) {
	cycle := MaintenanceStats{}
	maintenance := &Maintenance{
		currentStats: &cycle,
	}
	want := ChangeFeedCleanupStats{
		EntriesRetired:  7,
		BatchesPlanned:  6,
		BatchesDeleted:  5,
		BlockedRetained: 4,
		FailedDeletes:   3,
		Duration:        time.Second,
	}

	maintenance.recordChangeFeed(want)
	if cycle.ChangeFeedRetention != want {
		t.Fatalf("cycle stats=%+v want=%+v", cycle.ChangeFeedRetention, want)
	}
}

func TestMaintenanceRunOnceCheckpointsAtReplayPageLimit(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-checkpoint")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
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
	opts.SSTCompaction.L0TriggerSSTs = 10_000
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	maintenance.opts.manifestCheckpoint.TargetReplayPages = 1
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
		if stats.ManifestCheckpoint.Staged {
			break
		}
	}
	if !stats.ManifestCheckpoint.Staged {
		t.Fatal("maintenance did not stage a checkpoint")
	}
	if stats.State != MaintenanceWaitingForWriter {
		t.Fatalf("checkpoint cycle state=%v, want waiting for writer", stats.State)
	}
	if stats.ManifestCheckpoint.ReplayPages < 1 || stats.ManifestCheckpoint.ReplayBytes == 0 {
		t.Fatalf("checkpoint stats=(%d pages, %d bytes), want non-zero", stats.ManifestCheckpoint.ReplayPages, stats.ManifestCheckpoint.ReplayBytes)
	}
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.StateReplayPages != 0 || current.StateReplayBytes != 0 {
		t.Fatalf("replay accounting after maintenance=(%d pages, %d bytes), want zero",
			current.StateReplayPages, current.StateReplayBytes)
	}
	if current.Snapshot == nil {
		t.Fatal("maintenance did not publish a snapshot")
	}
}

func TestMaintenanceSchedulerPersistsCheckpointAndCompactionTurns(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-scheduler-persistence")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "writer-scheduler"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := 0; i < 64; i++ {
		if _, err := db.manifestStore.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{
			ID:     fmt.Sprintf("scheduler-sst-%02d", i),
			Level:  0,
			MinKey: []byte(fmt.Sprintf("key-%02d", i)),
			MaxKey: []byte(fmt.Sprintf("key-%02d", i)),
			Size:   1,
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%d): %v", i, err)
		}
	}

	opts := DefaultMaintenanceOptions()
	opts.SSTCompaction.L0TriggerSSTs = 64
	opts.ManifestCheckpoint.TargetReplayPages = 1
	opts.ManifestCheckpoint.TargetReplayBytes = ^uint64(0)
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}

	checkpointStats, err := maintenance.RunOnce(ctx)
	if err != nil {
		t.Fatalf("checkpoint RunOnce: %v", err)
	}
	if checkpointStats.Scheduling.Selected != MaintenanceTaskManifestCheckpoint {
		t.Fatalf("selected=%v, want checkpoint: %+v", checkpointStats.Scheduling.Selected, checkpointStats)
	}
	if _, err := db.manifestStore.ApplyPendingMaintenance(ctx); err != nil {
		t.Fatalf("apply checkpoint: %v", err)
	}
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(after checkpoint): %v", err)
	}
	if current.MaintenanceScheduler.LastPrimary != manifest.MaintenanceCommandCheckpoint {
		t.Fatalf("last primary=%q, want checkpoint", current.MaintenanceScheduler.LastPrimary)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("close first maintenance owner: %v", err)
	}
	maintenance, err = db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("reopen maintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	compactionStats, err := maintenance.RunOnce(ctx)
	if err != nil {
		t.Fatalf("compaction RunOnce: %v", err)
	}
	if compactionStats.Scheduling.Selected != MaintenanceTaskSSTCompaction ||
		compactionStats.Scheduling.CompactionSourceLevel != 0 {
		t.Fatalf("scheduling=%+v, want L0 compaction", compactionStats.Scheduling)
	}
	if _, err := db.manifestStore.ApplyPendingMaintenance(ctx); err != nil {
		t.Fatalf("apply compaction: %v", err)
	}
	current, err = db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(after compaction): %v", err)
	}
	if current.MaintenanceScheduler.LastPrimary != manifest.MaintenanceCommandCompaction ||
		current.MaintenanceScheduler.CompactionUnitsSinceCheckpoint != 1 ||
		current.MaintenanceScheduler.L0UnitsSinceLower != 1 {
		t.Fatalf("scheduler state after compaction=%+v", current.MaintenanceScheduler)
	}
}

func TestDBOpenMaintenanceRejectsSecondActiveHandle(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-single-owner")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
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

	db, err := openDB(ctx, store, dbOpenOptions{})
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

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	opts := DefaultMaintenanceOptions()
	opts.IdleInterval = -time.Second
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

	changeFeed := DefaultChangeFeedRetentionOptions()
	db, err := openDB(ctx, store, dbOpenOptions{changeFeedPayload: manifest.ChangeFeedPayloadFullValues})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	opts := DefaultMaintenanceOptions()
	opts.ChangeFeedRetention = &changeFeed

	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	want := maintenance.fenceToken
	assertFenceTokenEqual(t, maintenance.compactor.fenceToken, want)
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

	firstDB, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("OpenDB(first): %v", err)
	}
	defer firstDB.Close()
	secondDB, err := openDB(ctx, store, dbOpenOptions{})
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
		Kind:            manifest.MaintenanceCommandChangeFeedFloor,
		ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: 1},
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
	cycle := MaintenanceStats{State: MaintenanceWaitingForWriter}
	maintenance.currentStats = &cycle
	waiting, err := maintenance.reconcilePendingCommand(ctx)
	maintenance.currentStats = nil
	if err != nil {
		t.Fatalf("reconcilePendingCommand: %v", err)
	}
	if waiting {
		t.Fatal("maintenance still waiting after replacement writer applied command")
	}
	if cycle.State != MaintenanceIdle {
		t.Fatalf("reconciled state=%v, want idle", cycle.State)
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

	db, err := openDB(ctx, store, dbOpenOptions{})
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
		Kind:            manifest.MaintenanceCommandChangeFeedFloor,
		ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: 1},
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

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	opts := DefaultMaintenanceOptions()
	opts.IdleInterval = time.Hour
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

	firstDB, err := openDB(ctx, store, dbOpenOptions{changeFeedPayload: manifest.ChangeFeedPayloadFullValues})
	if err != nil {
		t.Fatalf("OpenDB(first): %v", err)
	}
	defer firstDB.Close()
	secondDB, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("OpenDB(second): %v", err)
	}
	defer secondDB.Close()

	changeFeed := DefaultChangeFeedRetentionOptions()
	firstOpts := DefaultMaintenanceOptions()
	firstOpts.ChangeFeedRetention = &changeFeed
	first, err := firstDB.OpenMaintenance(ctx, firstOpts)
	if err != nil {
		t.Fatalf("OpenMaintenance(first): %v", err)
	}
	defer first.Close(ctx)

	secondOpts := DefaultMaintenanceOptions()
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
