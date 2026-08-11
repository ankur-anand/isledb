package isledb

import (
	"context"
	"errors"
	"fmt"
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
