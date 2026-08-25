package isledb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestNormalizeReclamationOptions(t *testing.T) {
	defaults := DefaultMaintenanceOptions().Reclamation
	normalized, err := normalizeMaintenanceOptions(MaintenanceOptions{})
	if err != nil {
		t.Fatalf("normalize defaults: %v", err)
	}
	if normalized.reclamation != defaults {
		t.Fatalf("reclamation defaults=%+v want=%+v", normalized.reclamation, defaults)
	}

	custom := ReclamationOptions{
		MaxConcurrentDeletes: 7,
		SST:                  DeleterOptions{PollInterval: 11 * time.Millisecond, MaxObjectsPerPass: 13},
		ChangeFeed:           DeleterOptions{PollInterval: 17 * time.Millisecond, MaxObjectsPerPass: 19},
		Manifest: ManifestDeleterOptions{
			DeleterOptions: DeleterOptions{PollInterval: 23 * time.Millisecond, MaxObjectsPerPass: 29},
			AuditInterval:  31 * time.Millisecond,
		},
	}
	normalized, err = normalizeMaintenanceOptions(MaintenanceOptions{Reclamation: custom})
	if err != nil {
		t.Fatalf("normalize custom: %v", err)
	}
	if normalized.reclamation != custom {
		t.Fatalf("reclamation=%+v want=%+v", normalized.reclamation, custom)
	}

	tests := []ReclamationOptions{
		{MaxConcurrentDeletes: -1},
		{MaxConcurrentDeletes: maxReclaimDeleteConcurrency + 1},
		{SST: DeleterOptions{PollInterval: -1}},
		{ChangeFeed: DeleterOptions{MaxObjectsPerPass: -1}},
		{Manifest: ManifestDeleterOptions{AuditInterval: -1}},
		{Manifest: ManifestDeleterOptions{DeleterOptions: DeleterOptions{MaxObjectsPerPass: maxReclaimObjectsPerPass + 1}}},
	}
	for i, opts := range tests {
		if _, err := normalizeMaintenanceOptions(MaintenanceOptions{Reclamation: opts}); !errors.Is(err, ErrInvalidMaintenanceOptions) {
			t.Errorf("case %d error=%v want=%v", i, err, ErrInvalidMaintenanceOptions)
		}
	}
}

type concurrencyTrackingDeleter struct {
	active atomic.Int64
	max    atomic.Int64
}

func (d *concurrencyTrackingDeleter) Delete(ctx context.Context, _ string) error {
	return d.operation(ctx)
}

func (d *concurrencyTrackingDeleter) BatchDelete(ctx context.Context, _ []string) error {
	return d.operation(ctx)
}

func (d *concurrencyTrackingDeleter) operation(ctx context.Context) error {
	active := d.active.Add(1)
	for {
		maximum := d.max.Load()
		if active <= maximum || d.max.CompareAndSwap(maximum, active) {
			break
		}
	}
	defer d.active.Add(-1)
	timer := time.NewTimer(5 * time.Millisecond)
	defer stopMaintenanceTimer(timer)
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestLimitedObjectDeleterSharesOneConcurrencyBound(t *testing.T) {
	ctx := context.Background()
	base := &concurrencyTrackingDeleter{}
	deleter := newLimitedObjectDeleterFor(base, 2)

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				if err := deleter.Delete(ctx, fmt.Sprintf("object-%d", i)); err != nil {
					t.Errorf("Delete: %v", err)
				}
				return
			}
			if err := deleter.BatchDelete(ctx, []string{fmt.Sprintf("object-%d", i)}); err != nil {
				t.Errorf("BatchDelete: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if got := base.max.Load(); got != 2 {
		t.Fatalf("maximum concurrent provider deletes=%d want=2", got)
	}
}

type blockingObjectDeleter struct {
	started chan struct{}
	once    sync.Once
}

func (d *blockingObjectDeleter) Delete(context.Context, string) error { return nil }

func (d *blockingObjectDeleter) BatchDelete(ctx context.Context, _ []string) error {
	d.once.Do(func() { close(d.started) })
	<-ctx.Done()
	return ctx.Err()
}

type cancelingObjectDeleter struct {
	cancel      context.CancelFunc
	deleteCalls int
	batchCalls  int
}

func (d *cancelingObjectDeleter) Delete(ctx context.Context, _ string) error {
	d.deleteCalls++
	d.cancel()
	return ctx.Err()
}

func (d *cancelingObjectDeleter) BatchDelete(ctx context.Context, _ []string) error {
	d.batchCalls++
	d.cancel()
	return ctx.Err()
}

func TestMaintenanceRunControlLaneProgressesWhileSSTDeleteIsBlocked(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-independent-reclaim-lanes")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	cycles := make(chan MaintenanceStats, 16)
	opts := DefaultMaintenanceOptions()
	opts.IdleInterval = 5 * time.Millisecond
	opts.SSTCompaction.L0TriggerSSTs = 1 << 20
	opts.ManifestCheckpoint.TargetReplayPages = ^uint64(0)
	opts.ManifestCheckpoint.TargetReplayBytes = ^uint64(0)
	opts.Reclamation.SST.PollInterval = time.Millisecond
	opts.Reclamation.Manifest.PollInterval = time.Hour
	opts.Reclamation.ChangeFeed.PollInterval = time.Hour
	opts.OnCycle = func(stats MaintenanceStats) { cycles <- stats }
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(context.Background())

	current, command, receipt, _ := writeSSTDeletionPlanFixture(t, ctx, store, "blocked-lane")
	receipt.AppliedAt = time.Now().UTC().Add(-3 * time.Hour)
	plan, payload, err := buildSSTDeletionPlan(
		store, current, command, receipt, command.Compaction.RetiredObjects,
		time.Now().UTC().Add(-2*time.Hour), 0)
	if err != nil {
		t.Fatalf("build due plan: %v", err)
	}
	if created, err := storeSSTDeletionPlan(ctx, store, *plan, payload); err != nil || !created {
		t.Fatalf("store due plan created=%v error=%v", created, err)
	}

	blocked := &blockingObjectDeleter{started: make(chan struct{})}
	maintenance.sstGC.delete = blocked

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- maintenance.Run(runCtx) }()
	select {
	case <-blocked.started:
	case <-time.After(2 * time.Second):
		t.Fatal("SST reclamation did not start")
	}
	// Discard a control cycle that may have completed before the delete
	// reached its blocking point, then require another one while blocked.
	select {
	case <-cycles:
	default:
	}
	select {
	case <-cycles:
	case <-time.After(2 * time.Second):
		t.Fatal("control lane stopped while SST delete was blocked")
	}

	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error=%v want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not cancel blocked reclamation")
	}
}

func TestSSTPlanHandoffDoesNotWaitForBlockedReclaim(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("sst-plan-handoff-independent-of-reclaim")
	defer store.Close()

	current, command, receipt, _ := writeSSTDeletionPlanFixture(t, ctx, store, "blocked-reclaim")
	now := time.Now().UTC()
	receipt.AppliedAt = now.Add(-3 * time.Hour)
	plan, payload, err := buildSSTDeletionPlan(
		store,
		current,
		command,
		receipt,
		command.Compaction.RetiredObjects,
		now.Add(-2*time.Hour),
		0,
	)
	if err != nil {
		t.Fatalf("build due deletion plan: %v", err)
	}
	if created, err := storeSSTDeletionPlan(ctx, store, *plan, payload); err != nil || !created {
		t.Fatalf("store due deletion plan created=%v error=%v", created, err)
	}

	blocked := &blockingObjectDeleter{started: make(chan struct{})}
	cleaner := newSSTCleaner(store, sstCleanerOptions{
		SafetyMargin: -1,
		Now:          func() time.Time { return now },
		Deleter:      blocked,
	})
	reclaimCtx, cancelReclaim := context.WithCancel(ctx)
	reclaimDone := make(chan error, 1)
	go func() {
		_, err := cleaner.runOnce(reclaimCtx)
		reclaimDone <- err
	}()
	select {
	case <-blocked.started:
	case <-time.After(2 * time.Second):
		cancelReclaim()
		t.Fatal("SST reclamation did not reach the blocking delete")
	}

	// Reconcile a different applied compaction while the reclaim lane is stuck
	// in provider I/O. Publishing its durable handoff must not need the mutex
	// protecting the reclaimer's iterator.
	nextCurrent, nextCommand, nextReceipt, _ := writeSSTDeletionPlanFixture(t, ctx, store, "new-handoff")
	handoffDone := make(chan error, 1)
	go func() {
		_, err := cleaner.markCommandOutcome(ctx, nextCurrent, nextCommand, nextReceipt)
		handoffDone <- err
	}()

	blockedControl := false
	select {
	case err := <-handoffDone:
		if err != nil {
			cancelReclaim()
			<-reclaimDone
			t.Fatalf("mark command outcome: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		blockedControl = true
	}

	// Always release the intentionally hung provider call before reporting the
	// failed assertion, so the test cannot leak either goroutine.
	cancelReclaim()
	select {
	case <-reclaimDone:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked SST reclamation did not stop after cancellation")
	}
	if blockedControl {
		select {
		case err := <-handoffDone:
			if err != nil {
				t.Fatalf("mark command outcome after reclaim cancellation: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("SST plan handoff remained blocked after reclaim cancellation")
		}
		t.Fatal("SST plan handoff blocked on an unrelated reclaim operation")
	}
}

func TestMaintenanceSSTReclamationErrorsBackOff(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-sst-reclaim-error-backoff")
	defer store.Close()
	malformedPath := storeKey(
		store,
		sstDeletionPlanPrefix,
		deletionPlanReadyName(time.Now().UTC().Add(-time.Hour), strings.Repeat("a", deletionPlanSHA256HexBytes)),
	)
	if _, err := store.Write(ctx, malformedPath, []byte(`{"not":"a deletion plan"}`)); err != nil {
		t.Fatalf("write malformed ready record: %v", err)
	}

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	errorTimes := make(chan time.Time, 32)
	opts := DefaultMaintenanceOptions()
	opts.IdleInterval = time.Hour
	opts.SSTCompaction.L0TriggerSSTs = 1 << 20
	opts.ManifestCheckpoint.TargetReplayPages = ^uint64(0)
	opts.ManifestCheckpoint.TargetReplayBytes = ^uint64(0)
	opts.Reclamation.SST.PollInterval = 10 * time.Millisecond
	opts.Reclamation.ChangeFeed.PollInterval = time.Hour
	opts.Reclamation.Manifest.PollInterval = time.Hour
	opts.OnError = func(err error) {
		if strings.Contains(err.Error(), "sst reclamation") {
			errorTimes <- time.Now()
		}
	}
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(context.Background())

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- maintenance.Run(runCtx) }()

	count := 0
	select {
	case <-errorTimes:
		count++
	case <-time.After(2 * time.Second):
		cancel()
		<-runDone
		t.Fatal("SST reclamation did not report the malformed plan")
	}
	window := time.NewTimer(180 * time.Millisecond)
	for {
		select {
		case <-errorTimes:
			count++
		case <-window.C:
			cancel()
			if err := <-runDone; !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error=%v want context canceled", err)
			}
			// At a fixed 10 ms retry cadence this produces about 18 reports.
			// Exponential retry produces attempts near 0, 10, 30, 70, and
			// 150 ms while keeping the malformed record durable.
			if count < 3 || count > 7 {
				t.Fatalf("SST reclamation errors=%d in backoff window, want 3..7", count)
			}
			requireObjectExists(t, ctx, store, malformedPath, true)
			return
		}
	}
}

func TestMaintenanceSSTPlanWakeBypassesIdleBackoff(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-sst-reclaim-wake")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	cycles := make(chan ReclamationCycleStats, 32)
	opts := DefaultMaintenanceOptions()
	opts.SSTCompaction.L0TriggerSSTs = 1 << 20
	opts.ManifestCheckpoint.TargetReplayPages = ^uint64(0)
	opts.ManifestCheckpoint.TargetReplayBytes = ^uint64(0)
	opts.Reclamation.SST.PollInterval = time.Hour
	opts.Reclamation.ChangeFeed.PollInterval = time.Hour
	opts.Reclamation.Manifest.PollInterval = time.Hour
	opts.OnReclamationCycle = func(stats ReclamationCycleStats) { cycles <- stats }
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(context.Background())

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- maintenance.Run(runCtx) }()

	// Wait until the SST lane has observed the empty prefix and entered its
	// hour-long idle wait.
	for {
		select {
		case stats := <-cycles:
			if stats.Family == ReclamationSST {
				goto idle
			}
		case <-time.After(2 * time.Second):
			t.Fatal("SST reclamation did not perform its initial scan")
		}
	}

idle:
	current, command, receipt, target := writeSSTDeletionPlanFixture(t, ctx, store, "wake")
	now := time.Now().UTC()
	receipt.AppliedAt = now.Add(-3 * time.Hour)
	plan, payload, err := buildSSTDeletionPlan(
		store, current, command, receipt, command.Compaction.RetiredObjects,
		now.Add(-2*time.Hour), maintenance.sstGC.opts.SafetyMargin)
	if err != nil {
		t.Fatalf("build due wake plan: %v", err)
	}
	if created, err := storeSSTDeletionPlan(ctx, store, *plan, payload); err != nil || !created {
		t.Fatalf("store due wake plan created=%v error=%v", created, err)
	}
	maintenance.sstGC.planAvailable()
	plans := listSSTDeletionPlans(t, ctx, store)
	if len(plans) != 1 || plans[0].NotBefore.After(now) {
		t.Fatalf("wake plan is not due: now=%s plans=%+v", now, plans)
	}
	maintenance.notifyReclamation(ReclamationSST)

	for {
		select {
		case stats := <-cycles:
			if stats.Family == ReclamationSST && stats.SST.SSTsDeleted == 1 {
				requireObjectExists(t, ctx, store, target.Key, false)
				cancel()
				if err := <-runDone; !errors.Is(err, context.Canceled) {
					t.Fatalf("Run error=%v want context canceled", err)
				}
				return
			}
		case <-time.After(2 * time.Second):
			select {
			case runErr := <-runDone:
				t.Fatalf("maintenance stopped before wake was handled: %v", runErr)
			default:
			}
			cancel()
			<-runDone
			t.Fatal("new SST deletion plan did not wake idle reclamation")
		}
	}
}
