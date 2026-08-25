package isledb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestManifestPageDeferredPlanDoesNotStarveOrphanAudit(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-deferred-plan-audit")
	defer store.Close()

	now := time.Unix(10_000, 0).UTC()
	page := writeStandaloneManifestPageRange(t, ctx, store,
		"h00000000000000000050-l00000000000000000050-deferred-audit", 50, 50, now.Add(-2*time.Hour))
	writeManifestFloorForPageCleanerTest(t, ctx, store, 100)
	manifestLog := manifest.NewStore(store)
	current, err := manifestLog.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	command := &manifest.MaintenanceCommand{
		ID: "page-deferred-audit", Epoch: 1, Generation: 1,
		Kind: manifest.MaintenanceCommandChangeFeedFloor, CreatedAt: now,
		ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: 100},
	}
	receipt := &manifest.MaintenanceReceipt{
		CommandID: command.ID, Epoch: command.Epoch, Generation: command.Generation,
		Status: manifest.MaintenanceStatusApplied, AppliedAt: now,
	}
	cleaner := newManifestPageCleaner(store, manifestLog, manifestPageCleanerOptions{
		OrphanAuditEvery: time.Hour,
		OrphanGrace:      -1,
		SafetyMargin:     -1,
		Now:              func() time.Time { return now },
	})
	prepared, err := cleaner.markCommandOutcome(ctx, current, command, receipt)
	if err != nil || prepared.PlansPrepared != 1 {
		t.Fatalf("prepare page plan stats=%+v error=%v", prepared, err)
	}
	plan := listManifestPageDeletionPlans(t, ctx, store)[0]
	if !now.Before(plan.NotBefore) {
		t.Fatalf("plan deadline=%s must be after audit time=%s", plan.NotBefore, now)
	}

	stats, err := cleaner.runOnce(ctx)
	if err != nil {
		t.Fatalf("run cleaner with deferred plan: %v", err)
	}
	if stats.Deferred < 1 || stats.PagesMarked != 1 {
		t.Fatalf("cleanup stats=%+v want a deferred plan and one audited page", stats)
	}
	if cleaner.nextAudit.IsZero() {
		t.Fatal("successful orphan audit did not schedule its next run")
	}
	requireObjectExists(t, ctx, store, manifestPageRetirementMarkerPath(store, page.Path), true)
}

func TestManifestPageRangePlanMissingCurrentIsNotFloorRegression(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-range-plan-missing-current")
	defer store.Close()

	now := time.Unix(20_000, 0).UTC()
	page := writeStandaloneManifestPageRange(t, ctx, store,
		"h00000000000000000050-l00000000000000000050-missing-current", 50, 50, now)
	writeManifestFloorForPageCleanerTest(t, ctx, store, 100)
	manifestLog := manifest.NewStore(store)
	current, err := manifestLog.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	command := &manifest.MaintenanceCommand{
		ID: "page-missing-current", Epoch: 1, Generation: 1,
		Kind: manifest.MaintenanceCommandChangeFeedFloor, CreatedAt: now,
		ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: 100},
	}
	receipt := &manifest.MaintenanceReceipt{
		CommandID: command.ID, Epoch: command.Epoch, Generation: command.Generation,
		Status: manifest.MaintenanceStatusApplied, AppliedAt: now,
	}
	cleaner := newManifestPageCleaner(store, manifestLog, manifestPageCleanerOptions{
		OrphanGrace: -1, SafetyMargin: -1, Now: func() time.Time { return now },
	})
	prepared, err := cleaner.markCommandOutcome(ctx, current, command, receipt)
	if err != nil || prepared.PlansPrepared != 1 {
		t.Fatalf("prepare page plan stats=%+v error=%v", prepared, err)
	}
	plan := listManifestPageDeletionPlans(t, ctx, store)[0]
	if err := store.Delete(ctx, store.ManifestPath()); err != nil {
		t.Fatalf("delete CURRENT: %v", err)
	}

	_, err = cleaner.reclaimPlans(ctx, plan.NotBefore)
	if err == nil {
		t.Fatal("missing CURRENT did not defer page reclamation")
	}
	if strings.Contains(err.Error(), "regressed") || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("missing CURRENT diagnostic=%q want unavailable, not regression", err)
	}
	requireObjectExists(t, ctx, store, page.Path, true)
	requireObjectExists(t, ctx, store, manifestPageDeletionPlanReadyPath(store, plan.NotBefore, plan.PlanID), true)
}

func TestManifestPageRescanRemainsPendingWhilePlanActive(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-active-plan-rescan")
	defer store.Close()
	writeManifestFloorForPageCleanerTest(t, ctx, store, 100)

	cleaner := newManifestPageCleaner(store, manifest.NewStore(store), manifestPageCleanerOptions{})
	cleaner.activePlan = &manifestPageDeletionPlan{
		PlanID: "active-plan", Floor: 100, MaxLevel: 0,
	}
	cleaner.activeReadyKey = "manifest/gc/pages/ready/active-plan.json"
	cleaner.planIter = store.NewListIterator(blobstore.ListOptions{Prefix: manifestPageDeletionPlanReadyPrefix + "/"})
	cleaner.planAvailable()
	wantGeneration := cleaner.rescan.Load()

	if _, err := cleaner.reclaimPlans(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("complete active page plan: %v", err)
	}
	if cleaner.activePlan != nil {
		t.Fatal("active plan did not complete")
	}
	if cleaner.seenRescan == wantGeneration {
		t.Fatalf("rescan generation=%d was consumed while the active plan prevented invalidation", wantGeneration)
	}
}

func TestManifestPageRangePlanTreatsListedThenDeletedPageAsAlreadyReclaimed(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-listed-then-deleted")
	defer store.Close()

	now := time.Unix(30_000, 0).UTC()
	first := writeStandaloneManifestPageRange(t, ctx, store,
		"h00000000000000000010-l00000000000000000010-first", 10, 10, now)
	alreadyGone := writeStandaloneManifestPageRange(t, ctx, store,
		"h00000000000000000020-l00000000000000000020-gone", 20, 20, now)
	writeManifestFloorForPageCleanerTest(t, ctx, store, 100)

	iter := store.NewListIterator(blobstore.ListOptions{Prefix: manifestPageObjectPrefix + "/l00/"})
	listed, err := iter.Next(ctx)
	if err != nil || listed.Key != first.Path {
		t.Fatalf("prime page listing: object=%+v error=%v want=%q", listed, err, first.Path)
	}
	if err := store.Delete(ctx, alreadyGone.Path); err != nil {
		t.Fatalf("delete listed page: %v", err)
	}

	cleaner := newManifestPageCleaner(store, manifest.NewStore(store), manifestPageCleanerOptions{
		PageScanLimit: 1,
	})
	cleaner.activePlan = &manifestPageDeletionPlan{
		PlanID: strings.Repeat("a", deletionPlanSHA256HexBytes),
		Floor:  100,
	}
	cleaner.activeReadyKey = manifestPageDeletionPlanReadyPath(
		store, now, cleaner.activePlan.PlanID)
	cleaner.activePageIter = iter

	stats, err := cleaner.reclaimPlans(ctx, now)
	if err != nil {
		t.Fatalf("reclaim page deleted after listing: %v", err)
	}
	if stats.Failures != 0 || stats.ObjectsScanned != 1 || stats.PagesDeleted != 0 {
		t.Fatalf("already-reclaimed page stats=%+v", stats)
	}
}

func TestManifestPagePlanRequiresEffectiveFloorAdvance(t *testing.T) {
	now := time.Unix(40_000, 0).UTC()
	tests := []struct {
		name    string
		current *manifest.Current
		command *manifest.MaintenanceCommand
	}{
		{
			name: "checkpoint-change-feed-floor-remains-binding",
			current: &manifest.Current{
				LogSeqStart: 200, ChangeFeedEnabled: true, ChangeFeedLogStart: 100,
			},
			command: &manifest.MaintenanceCommand{
				ID: "checkpoint-nonbinding", Epoch: 1, Generation: 1,
				Kind: manifest.MaintenanceCommandCheckpoint, CreatedAt: now,
				Checkpoint: &manifest.CheckpointCommand{
					BaseLogSeqStart: 150, SnapshotNextSeq: 200, FoldedReplayPages: 1,
				},
			},
		},
		{
			name: "change-feed-log-floor-remains-binding",
			current: &manifest.Current{
				LogSeqStart: 100, ChangeFeedEnabled: true, ChangeFeedLogStart: 200,
			},
			command: &manifest.MaintenanceCommand{
				ID: "change-feed-nonbinding", Epoch: 1, Generation: 1,
				Kind: manifest.MaintenanceCommandChangeFeedFloor, CreatedAt: now,
				ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: 200},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := blobstore.NewMemory("manifest-page-plan-floor-" + test.name)
			defer store.Close()
			cleaner := newManifestPageCleaner(store, manifest.NewStore(store), manifestPageCleanerOptions{
				OrphanGrace: -1, SafetyMargin: -1, Now: func() time.Time { return now },
			})
			receipt := &manifest.MaintenanceReceipt{
				CommandID: test.command.ID, Epoch: test.command.Epoch, Generation: test.command.Generation,
				Status: manifest.MaintenanceStatusApplied, AppliedAt: now,
			}
			stats, err := cleaner.markCommandOutcome(ctx, test.current, test.command, receipt)
			if err != nil {
				t.Fatalf("mark command outcome: %v", err)
			}
			if stats.PlansPrepared != 0 || len(listManifestPageDeletionPlans(t, ctx, store)) != 0 {
				t.Fatalf("unchanged effective floor created a page plan: stats=%+v", stats)
			}
		})
	}
}

func TestManifestPagePlanRetryAdoptsFirstMaximumLevel(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-plan-retry-max-level")
	defer store.Close()
	now := time.Unix(50_000, 0).UTC()
	current := &manifest.Current{
		LogSeqStart: 100, ChangeFeedEnabled: true, ChangeFeedLogStart: 100,
		ManifestPageMaxLevel: 1,
	}
	command := &manifest.MaintenanceCommand{
		ID: "page-plan-retry-max-level", Epoch: 1, Generation: 1,
		Kind: manifest.MaintenanceCommandChangeFeedFloor, CreatedAt: now,
		ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: 100},
	}
	receipt := &manifest.MaintenanceReceipt{
		CommandID: command.ID, Epoch: command.Epoch, Generation: command.Generation,
		Status: manifest.MaintenanceStatusApplied, AppliedAt: now,
	}
	cleaner := newManifestPageCleaner(store, manifest.NewStore(store), manifestPageCleanerOptions{
		OrphanGrace: -1, SafetyMargin: -1, Now: func() time.Time { return now },
	})
	first, err := cleaner.markCommandOutcome(ctx, current, command, receipt)
	if err != nil || first.PlansPrepared != 1 {
		t.Fatalf("first plan stats=%+v error=%v", first, err)
	}

	retryCurrent := current.Clone()
	retryCurrent.ManifestPageMaxLevel = 3
	retry, err := cleaner.markCommandOutcome(ctx, retryCurrent, command, receipt)
	if err != nil {
		t.Fatalf("retry plan: %v", err)
	}
	plans := listManifestPageDeletionPlans(t, ctx, store)
	if retry.PlansPrepared != 0 || len(plans) != 1 || plans[0].MaxLevel != 1 {
		t.Fatalf("retry did not adopt first durable coverage: stats=%+v plans=%+v", retry, plans)
	}
}

func TestManifestReclamationLaneSchedulesDeferredPagePlan(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-plan-lane-schedule")
	defer store.Close()
	now := time.Unix(60_000, 0).UTC()
	writeManifestFloorForPageCleanerTest(t, ctx, store, 100)
	manifestLog := manifest.NewStore(store)
	current, err := manifestLog.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	command := &manifest.MaintenanceCommand{
		ID: "page-plan-lane-schedule", Epoch: 1, Generation: 1,
		Kind: manifest.MaintenanceCommandChangeFeedFloor, CreatedAt: now,
		ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: 100},
	}
	receipt := &manifest.MaintenanceReceipt{
		CommandID: command.ID, Epoch: command.Epoch, Generation: command.Generation,
		Status: manifest.MaintenanceStatusApplied, AppliedAt: now,
	}
	cleaner := newManifestPageCleaner(store, manifestLog, manifestPageCleanerOptions{
		OrphanAuditEvery: 2 * time.Hour,
		OrphanGrace:      -1,
		SafetyMargin:     -1,
		Now:              func() time.Time { return now },
	})
	prepared, err := cleaner.markCommandOutcome(ctx, current, command, receipt)
	if err != nil || prepared.PlansPrepared != 1 {
		t.Fatalf("prepare page plan stats=%+v error=%v", prepared, err)
	}
	plan := listManifestPageDeletionPlans(t, ctx, store)[0]
	maintenance := &Maintenance{
		pageGC: cleaner,
		reclaimGates: map[ReclamationFamily]chan struct{}{
			ReclamationManifest: make(chan struct{}, 1),
		},
	}

	stats, schedule, err := maintenance.runReclamationPass(ctx, ReclamationManifest)
	if err != nil {
		t.Fatalf("run manifest reclamation: %v", err)
	}
	if stats.Manifest.Pages.Deferred < 1 || !schedule.observedAt.Equal(now) ||
		!schedule.nextDue.Equal(plan.NotBefore) || !schedule.idle {
		t.Fatalf("stats=%+v schedule=%+v want now=%s due=%s idle", stats.Manifest.Pages, schedule, now, plan.NotBefore)
	}
}

func TestManifestPageRangePlanCheckpointHandoffAndReclaim(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-range-plan-checkpoint")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{storePolicy: StorePolicy{MaxPinnedViewAge: time.Millisecond}})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "page-range-plan-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := 0; i < 65; i++ {
		if _, err := db.manifestStore.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{
			ID: fmt.Sprintf("page-range-plan-sst-%03d", i),
		}); err != nil {
			t.Fatalf("append SST %d: %v", i, err)
		}
	}
	before, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil || len(before.IndexFrontier) != 1 {
		t.Fatalf("CURRENT before checkpoint=%+v error=%v", before, err)
	}
	retiredPage := before.IndexFrontier[0]

	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)
	stageSnapshotCheckpoint(t, ctx, db.manifestStore, maintenance)
	head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil || head == nil || head.Pending == nil {
		t.Fatalf("staged HEAD=%+v error=%v", head, err)
	}
	command := *head.Pending
	if _, err := db.manifestStore.ApplyPendingMaintenance(ctx); err != nil {
		t.Fatalf("ApplyPendingMaintenance: %v", err)
	}
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil || current.MaintenanceReceipt == nil {
		t.Fatalf("CURRENT after checkpoint=%+v error=%v", current, err)
	}
	receipt := current.MaintenanceReceipt.Clone()
	reconcileSnapshotCheckpoint(t, ctx, maintenance)

	plans := listManifestPageDeletionPlans(t, ctx, store)
	if len(plans) != 1 {
		t.Fatalf("ready page plans=%+v want one", plans)
	}
	plan := plans[0]
	if plan.Source.CommandID != command.ID || plan.Floor != current.LogSeqStart ||
		plan.MaxLevel != current.ManifestPageMaxLevel {
		t.Fatalf("page plan=%+v command=%+v current=%+v", plan, command, current)
	}
	reconciledHead, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil || reconciledHead == nil || reconciledHead.Pending != nil {
		t.Fatalf("HEAD was not cleared after durable plan: head=%+v error=%v", reconciledHead, err)
	}
	requireObjectExists(t, ctx, store, manifestPageDeletionPlanCanonicalPath(store, plan.PlanID), true)
	requireObjectExists(t, ctx, store, retiredPage.Path, true)
	retry, err := maintenance.pageGC.markCommandOutcome(ctx, current, &command, receipt)
	if err != nil {
		t.Fatalf("retry page plan handoff: %v", err)
	}
	if retry.PlansPrepared != 0 || len(listManifestPageDeletionPlans(t, ctx, store)) != 1 {
		t.Fatalf("retry did not adopt durable page plan: stats=%+v", retry)
	}
	scheduled := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{
		DeleteBatchSize: 1,
		OrphanGrace:     -1,
		SafetyMargin:    -1,
		Now:             func() time.Time { return plan.NotBefore.Add(-time.Nanosecond) },
	})
	if _, err := scheduled.reclaimPlans(ctx, plan.NotBefore.Add(-time.Nanosecond)); err != nil {
		t.Fatalf("scheduled pre-deadline pass: %v", err)
	}
	markers, err := store.List(ctx, blobstore.ListOptions{Prefix: manifestPageRetirementPrefix + "/"})
	if err != nil || len(markers.Objects) != 0 {
		t.Fatalf("plan-only reclaim created per-page markers: markers=%+v error=%v", markers, err)
	}

	cleaner := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{
		DeleteBatchSize: 1,
		OrphanGrace:     -1,
		SafetyMargin:    -1,
	})
	deferred, err := cleaner.reclaimPlans(ctx, plan.NotBefore.Add(-time.Nanosecond))
	if err != nil {
		t.Fatalf("reclaim before deadline: %v", err)
	}
	if deferred.Deferred != 1 || deferred.PagesDeleted != 0 {
		t.Fatalf("pre-deadline stats=%+v", deferred)
	}
	requireObjectExists(t, ctx, store, retiredPage.Path, true)

	due, err := cleaner.reclaimPlans(ctx, plan.NotBefore)
	if err != nil {
		t.Fatalf("reclaim at deadline: %v", err)
	}
	completed, err := cleaner.reclaimPlans(ctx, plan.NotBefore)
	if err != nil {
		t.Fatalf("complete plan after bounded deletion: %v", err)
	}
	mergeManifestPageCleanupStats(&due, completed)
	if due.PagesDeleted != 1 || due.PlansCompleted != 1 {
		t.Fatalf("due stats=%+v", due)
	}
	requireObjectExists(t, ctx, store, retiredPage.Path, false)
	requireObjectExists(t, ctx, store, manifestPageDeletionPlanCanonicalPath(store, plan.PlanID), false)
	requireObjectExists(t, ctx, store, manifestPageDeletionPlanReadyPath(store, plan.NotBefore, plan.PlanID), false)

	replayed, err := manifest.NewStore(store).Replay(ctx)
	if err != nil {
		t.Fatalf("Replay after page range reclamation: %v", err)
	}
	for i := 0; i < 65; i++ {
		if replayed.LookupSST(fmt.Sprintf("page-range-plan-sst-%03d", i)) == nil {
			t.Fatalf("SST %d missing after page range reclamation", i)
		}
	}
}

func TestManifestPageRangePlanFailsClosedOnFloorRegression(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-range-plan-floor-regression")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	page := writeStandaloneManifestPage(t, ctx, store,
		"h00000000000000000050-l00000000000000000050-floor-regression", 50, now)
	writeManifestFloorForPageCleanerTest(t, ctx, store, 100)
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	command := &manifest.MaintenanceCommand{
		ID:         "page-floor-regression",
		Epoch:      1,
		Generation: 1,
		Kind:       manifest.MaintenanceCommandChangeFeedFloor,
		CreatedAt:  now,
		ChangeFeedFloor: &manifest.AdvanceFloorCommand{
			Floor: 100,
		},
	}
	receipt := &manifest.MaintenanceReceipt{
		CommandID: command.ID, Epoch: command.Epoch, Generation: command.Generation,
		Status: manifest.MaintenanceStatusApplied, AppliedAt: now,
	}
	cleaner := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{
		OrphanGrace: -1, SafetyMargin: -1, Now: func() time.Time { return now },
	})
	created, err := cleaner.markCommandOutcome(ctx, current, command, receipt)
	if err != nil || created.PlansPrepared != 1 {
		t.Fatalf("mark floor plan stats=%+v error=%v", created, err)
	}
	plan := listManifestPageDeletionPlans(t, ctx, store)[0]

	writeManifestFloorForPageCleanerTest(t, ctx, store, 40)
	_, err = cleaner.reclaimPlans(ctx, plan.NotBefore)
	if err == nil {
		t.Fatal("floor regression did not fail closed")
	}
	requireObjectExists(t, ctx, store, page.Path, true)
	requireObjectExists(t, ctx, store, manifestPageDeletionPlanReadyPath(store, plan.NotBefore, plan.PlanID), true)
}

func TestManifestPageRangePlanOrdersByEndAndValidatesPayload(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-range-plan-key-hints")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	lying := writeStandaloneManifestPageRange(t, ctx, store,
		"h00000000000000000020-l00000000000000000000-lying", 0, 120, now)
	narrow := writeStandaloneManifestPageRange(t, ctx, store,
		"h00000000000000000080-l00000000000000000050-narrow", 50, 80, now)
	wide := writeStandaloneManifestPageRange(t, ctx, store,
		"h00000000000000000150-l00000000000000000000-wide", 0, 150, now)
	writeManifestFloorForPageCleanerTest(t, ctx, store, 100)
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	command := &manifest.MaintenanceCommand{
		ID: "page-key-hints", Epoch: 1, Generation: 1,
		Kind: manifest.MaintenanceCommandChangeFeedFloor, CreatedAt: now,
		ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: 100},
	}
	receipt := &manifest.MaintenanceReceipt{
		CommandID: command.ID, Epoch: command.Epoch, Generation: command.Generation,
		Status: manifest.MaintenanceStatusApplied, AppliedAt: now,
	}
	cleaner := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{
		DeleteBatchSize: 10, OrphanGrace: -1, SafetyMargin: -1, Now: func() time.Time { return now },
	})
	prepared, err := cleaner.markCommandOutcome(ctx, current, command, receipt)
	if err != nil || prepared.PlansPrepared != 1 {
		t.Fatalf("prepare page plan stats=%+v error=%v", prepared, err)
	}
	plan := listManifestPageDeletionPlans(t, ctx, store)[0]
	stats, err := cleaner.reclaimPlans(ctx, plan.NotBefore)
	if err != nil {
		t.Fatalf("reclaim page range: %v", err)
	}
	if stats.PagesDeleted != 1 || stats.PlansCompleted != 1 || stats.Failures != 1 {
		t.Fatalf("reclaim stats=%+v", stats)
	}
	requireObjectExists(t, ctx, store, narrow.Path, false)
	requireObjectExists(t, ctx, store, wide.Path, true)
	requireObjectExists(t, ctx, store, lying.Path, true)
}

func listManifestPageDeletionPlans(
	t testing.TB,
	ctx context.Context,
	store *blobstore.Store,
) []manifestPageDeletionPlan {
	t.Helper()
	listed, err := store.List(ctx, blobstore.ListOptions{Prefix: manifestPageDeletionPlanReadyPrefix + "/"})
	if err != nil {
		t.Fatalf("list manifest page deletion plans: %v", err)
	}
	plans := make([]manifestPageDeletionPlan, 0, len(listed.Objects))
	for _, object := range listed.Objects {
		if object.IsDir {
			continue
		}
		payload, _, err := store.Read(ctx, object.Key)
		if err != nil {
			t.Fatalf("read manifest page deletion plan %q: %v", object.Key, err)
		}
		plan, err := decodeManifestPageDeletionPlan(store, object.Key, payload)
		if err != nil {
			t.Fatalf("decode manifest page deletion plan %q: %v", object.Key, err)
		}
		plans = append(plans, plan)
	}
	return plans
}
