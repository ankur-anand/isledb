package isledb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestSSTDeletionPlanEndToEndUsesOneWriterPublication(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("sst-deletion-plan-compaction-e2e")
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		store.Close()
		t.Fatalf("open DB: %v", err)
	}
	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "sst-deletion-plan-compaction-writer"
	writerOpts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		db.Close()
		store.Close()
		t.Fatalf("open writer: %v", err)
	}
	t.Cleanup(func() {
		_ = writer.Close(context.Background())
		_ = db.Close()
		_ = store.Close()
	})

	writeAndFlush := func(key, value string) {
		t.Helper()
		if err := writer.Put(ctx, []byte(key), []byte(value)); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush %q: %v", key, err)
		}
	}
	writeAndFlush("a", "one")
	writeAndFlush("a", "two")

	before := replayManifestForTest(t, ctx, store)
	if before.L0SSTCount() != 2 {
		t.Fatalf("L0 SSTs before compaction=%d want=2", before.L0SSTCount())
	}
	oldIDs := make(map[string]struct{}, before.L0SSTCount())
	for _, sst := range before.L0SSTs {
		oldIDs[sst.ID] = struct{}{}
	}

	pinnedReader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open pinned reader: %v", err)
	}
	defer pinnedReader.Close()
	assertReaderValue(t, ctx, pinnedReader, "a", "two", true)

	opts := DefaultMaintenanceOptions()
	opts.SSTCompaction.L0TriggerSSTs = 2
	opts.SSTCompaction.BaseLevelBytes = 1 << 60
	opts.ManifestCheckpoint.TargetReplayPages = ^uint64(0)
	opts.ManifestCheckpoint.TargetReplayBytes = ^uint64(0)
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	stagedStats, err := maintenance.RunOnce(ctx)
	if err != nil {
		t.Fatalf("stage compaction: %v", err)
	}
	if stagedStats.Scheduling.Selected != MaintenanceTaskSSTCompaction || stagedStats.SSTCompaction.Jobs != 1 {
		t.Fatalf("compaction stats=%+v", stagedStats)
	}
	head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("read staged compaction HEAD: %v", err)
	}
	if head == nil || head.Pending == nil || head.Pending.Kind != manifest.MaintenanceCommandCompaction || head.Pending.Compaction == nil {
		t.Fatalf("staged compaction command=%+v", head)
	}
	command := *head.Pending
	if got := len(command.Compaction.RetiredObjects); got != len(oldIDs) {
		t.Fatalf("staged retired SSTs=%d want=%d", got, len(oldIDs))
	}
	for _, retired := range command.Compaction.RetiredObjects {
		if _, ok := oldIDs[retired.ID]; !ok || retired.Key != store.SSTPath(retired.ID) {
			t.Fatalf("invalid staged retirement target=%+v", retired)
		}
	}

	beforePublication, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read CURRENT before publication: %v", err)
	}
	if got := replayManifestForTest(t, ctx, store).L0SSTCount(); got != 2 {
		t.Fatalf("topology changed before writer publication: l0=%d", got)
	}

	// This is the sole writer-owned CURRENT CAS for the compaction. It installs
	// the topology and matching receipt together.
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("writer publish compaction: %v", err)
	}
	afterPublication, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read CURRENT after publication: %v", err)
	}
	if afterPublication.NextSeq != beforePublication.NextSeq+1 {
		t.Fatalf("manifest head after compaction=%d want=%d", afterPublication.NextSeq, beforePublication.NextSeq+1)
	}
	if afterPublication.MaintenanceReceipt == nil || !afterPublication.MaintenanceReceipt.Matches(&command) ||
		afterPublication.MaintenanceReceipt.Status != manifest.MaintenanceStatusApplied {
		t.Fatalf("compaction receipt=%+v command=%+v", afterPublication.MaintenanceReceipt, command)
	}
	if got := replayManifestForTest(t, ctx, store).L0SSTCount(); got != 0 {
		t.Fatalf("L0 SSTs after compaction=%d want=0", got)
	}
	assertReaderValue(t, ctx, pinnedReader, "a", "two", true)

	// Receipt reconciliation must persist the immutable deletion plan before it
	// clears HEAD. It does not stage another writer command.
	reconciledStats, err := maintenance.RunOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile compaction: %v", err)
	}
	if reconciledStats.SSTCleanup.SSTsPlanned != len(oldIDs) || reconciledStats.SSTCleanup.PlansPrepared != 1 {
		t.Fatalf("SST cleanup stats=%+v", reconciledStats.SSTCleanup)
	}
	head, _, err = db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("read reconciled HEAD: %v", err)
	}
	if head == nil || head.Pending != nil {
		t.Fatalf("HEAD after receipt reconciliation=%+v", head)
	}
	afterReconcile, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read CURRENT after reconciliation: %v", err)
	}
	if afterReconcile.NextSeq != afterPublication.NextSeq {
		t.Fatalf("reconciliation performed a second manifest write: next_seq=%d want=%d",
			afterReconcile.NextSeq, afterPublication.NextSeq)
	}
	if afterReconcile.MaintenanceReceipt == nil || !afterReconcile.MaintenanceReceipt.Matches(&command) {
		t.Fatalf("compaction receipt was replaced: %+v", afterReconcile.MaintenanceReceipt)
	}

	plans := listSSTDeletionPlans(t, ctx, store)
	if len(plans) != 1 || plans[0].TargetCount != len(oldIDs) {
		t.Fatalf("SST deletion plans=%+v", plans)
	}
	plan := plans[0]
	if plan.Source.CommandID != command.ID || plan.Source.Epoch != command.Epoch || plan.Source.Generation != command.Generation {
		t.Fatalf("plan source=%+v command=%+v", plan.Source, command)
	}
	wantDeadlineFloor := afterPublication.MaintenanceReceipt.AppliedAt.Add(afterPublication.PinnedViewAge())
	if plan.NotBefore.Before(wantDeadlineFloor) {
		t.Fatalf("unsafe plan deadline=%s want at least %s", plan.NotBefore, wantDeadlineFloor)
	}
	for _, target := range plan.Targets {
		if _, old := oldIDs[target.ID]; !old {
			t.Fatalf("plan contains live or unknown target=%+v", target)
		}
		if _, _, err := store.Read(ctx, target.Key); err != nil {
			t.Fatalf("retired SST was deleted before deadline: %v", err)
		}
	}

	freshReader := openReaderFromDBForTest(t, ctx, store, DefaultReaderOpenOptions(t.TempDir()))
	defer freshReader.Close()
	assertReaderValue(t, ctx, freshReader, "a", "two", true)
}

func TestSSTDeletionPlanReceiptHandoffIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store, current, command, receipt, target := newSSTDeletionPlanFixture(t, ctx, "idempotent")
	defer store.Close()

	firstNow := receipt.AppliedAt.Add(time.Minute)
	cleaner := newSSTCleaner(store, sstCleanerOptions{Now: func() time.Time { return firstNow }})
	first, err := cleaner.markCommandOutcome(ctx, current, command, receipt)
	if err != nil {
		t.Fatalf("first handoff: %v", err)
	}
	if first.PlansPrepared != 1 || first.TargetsPlanned != 1 {
		t.Fatalf("first handoff stats=%+v", first)
	}
	firstPlan := listSSTDeletionPlans(t, ctx, store)[0]

	cleaner.opts.Now = func() time.Time { return firstNow.Add(time.Hour) }
	second, err := cleaner.markCommandOutcome(ctx, current, command, receipt)
	if err != nil {
		t.Fatalf("retry handoff: %v", err)
	}
	if second.PlansPrepared != 0 || second.TargetsPlanned != 1 {
		t.Fatalf("retry handoff stats=%+v", second)
	}
	plans := listSSTDeletionPlans(t, ctx, store)
	if len(plans) != 1 || plans[0].PlanID != firstPlan.PlanID || !plans[0].NotBefore.Equal(firstPlan.NotBefore) {
		t.Fatalf("retry replaced immutable plan: first=%+v plans=%+v", firstPlan, plans)
	}
	if _, _, err := store.Read(ctx, target.Key); err != nil {
		t.Fatalf("target changed during handoff retry: %v", err)
	}

	changedPolicy := current.Clone()
	changedPolicy.MaxPinnedViewAge += time.Minute
	if _, err := cleaner.markCommandOutcome(ctx, changedPolicy, command, receipt); err == nil {
		t.Fatal("existing plan with a different pinned-view policy was accepted")
	}
}

func TestSSTDeletionPlanRejectedReceiptDoesNotCreatePlan(t *testing.T) {
	ctx := context.Background()
	store, current, command, receipt, _ := newSSTDeletionPlanFixture(t, ctx, "rejected")
	defer store.Close()
	receipt.Status = manifest.MaintenanceStatusRejected

	stats, err := newSSTCleaner(store, sstCleanerOptions{}).markCommandOutcome(ctx, current, command, receipt)
	if err != nil {
		t.Fatalf("rejected handoff: %v", err)
	}
	if stats != (sstCleanupWorkStats{}) || len(listSSTDeletionPlans(t, ctx, store)) != 0 {
		t.Fatalf("rejected command created deletion work: %+v", stats)
	}
}

func TestSSTDeletionPlanReclaimerHonorsDeadlineAndBoundsWork(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("sst-plan-reclaimer")
	defer store.Close()
	now := time.Now().UTC()

	var targets []sstDeletionTarget
	for i := 0; i < 2; i++ {
		current, command, receipt, target := writeSSTDeletionPlanFixture(t, ctx, store, fmt.Sprintf("bounded-%d", i))
		receipt.AppliedAt = now.Add(-3 * time.Hour)
		plan, payload, err := buildSSTDeletionPlan(store, current, command, receipt,
			command.Compaction.RetiredObjects, now.Add(-2*time.Hour), 0)
		if err != nil {
			t.Fatalf("build plan %d: %v", i, err)
		}
		if created, err := storeSSTDeletionPlan(ctx, store, *plan, payload); err != nil || !created {
			t.Fatalf("store plan %d created=%v err=%v", i, created, err)
		}
		targets = append(targets, target)
	}

	stats, err := runSSTDeletionPlanReclaimer(ctx, store, 1, 1024, now)
	if err != nil {
		t.Fatalf("bounded reclaim: %v", err)
	}
	if stats.Attempted != 1 || stats.Deleted != 1 || stats.PlansDeleted != 1 {
		t.Fatalf("bounded reclaim stats=%+v", stats)
	}
	if got := len(listSSTDeletionPlans(t, ctx, store)); got != 1 {
		t.Fatalf("remaining plans=%d want=1", got)
	}
	missing := 0
	for _, target := range targets {
		if _, _, err := store.Read(ctx, target.Key); errors.Is(err, blobstore.ErrNotFound) {
			missing++
		} else if err != nil {
			t.Fatalf("read target %q: %v", target.Key, err)
		}
	}
	if missing != 1 {
		t.Fatalf("deleted targets=%d want=1", missing)
	}
}

func TestSSTDeletionPlanCleanerRetainsListCursorPastDeferredPlan(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("sst-plan-cached-list-cursor")
	defer store.Close()
	now := time.Now().UTC()

	type builtPlan struct {
		plan    *sstDeletionPlan
		payload []byte
		target  sstDeletionTarget
	}
	built := make([]builtPlan, 0, 6)
	for i := 0; i < 6; i++ {
		current, command, receipt, target := writeSSTDeletionPlanFixture(t, ctx, store, fmt.Sprintf("cursor-%d", i))
		receipt.AppliedAt = now.Add(-4 * time.Hour)
		plan, payload, err := buildSSTDeletionPlan(
			store, current, command, receipt, command.Compaction.RetiredObjects,
			now.Add(-3*time.Hour), 0)
		if err != nil {
			t.Fatalf("build plan %d: %v", i, err)
		}
		built = append(built, builtPlan{plan: plan, payload: payload, target: target})
	}
	sort.Slice(built, func(i, j int) bool {
		return sstDeletionPlanPath(store, built[i].plan.PlanID) < sstDeletionPlanPath(store, built[j].plan.PlanID)
	})
	// Make the lexicographically first plan not due. A stateless prefix scan
	// with a one-plan budget would rediscover it forever.
	deferred := built[0].plan
	deferred.ObservedAt = now
	deferred.NotBefore = now.Add(deferred.PinnedViewAge).Add(deferred.SafetyMargin)
	deferred.Checksum = sstDeletionPlanChecksum(*deferred)
	var err error
	built[0].payload, err = encodeSSTDeletionPlan(store, *deferred)
	if err != nil {
		t.Fatalf("encode deferred plan: %v", err)
	}
	for i := range built {
		if created, err := storeSSTDeletionPlan(ctx, store, *built[i].plan, built[i].payload); err != nil || !created {
			t.Fatalf("store plan %d created=%v error=%v", i, created, err)
		}
	}

	cleaner := newSSTCleaner(store, sstCleanerOptions{
		DeleteBatchSize: 1,
		PlanScanLimit:   1,
		Now:             func() time.Time { return now },
	})
	first, err := cleaner.runOnce(ctx)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if first.Deferred != 1 || first.Deleted != 0 {
		t.Fatalf("first stats=%+v", first)
	}
	second, err := cleaner.runOnce(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if second.Deleted != 1 || second.PlansDeleted != 1 {
		t.Fatalf("second stats=%+v; iterator restarted at deferred plan", second)
	}
	requireObjectExists(t, ctx, store, built[0].target.Key, true)
}

func TestSSTDeletionPlanCleanerCarriesBudgetDeferredPlan(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("sst-plan-budget-carry")
	defer store.Close()
	now := time.Now().UTC()

	type builtPlan struct {
		targets []sstDeletionTarget
		path    string
	}
	buildPlan := func(label string, targetCount int) builtPlan {
		t.Helper()
		current, command, receipt, firstTarget := writeSSTDeletionPlanFixture(t, ctx, store, label)
		retired := append([]manifest.RetiredObject(nil), command.Compaction.RetiredObjects...)
		targets := []sstDeletionTarget{firstTarget}
		for i := 1; i < targetCount; i++ {
			id := fmt.Sprintf("retired-%s-%03d.sst", label, i)
			key := store.SSTPath(id)
			payload := []byte(fmt.Sprintf("%s-target-%03d", label, i))
			if _, err := store.Write(ctx, key, payload); err != nil {
				t.Fatalf("write %s target %d: %v", label, i, err)
			}
			retired = append(retired, manifest.RetiredObject{
				Kind: manifest.RetiredObjectSST, ID: id, Key: key, Size: int64(len(payload)),
			})
			targets = append(targets, sstDeletionTarget{ID: id, Key: key, Size: int64(len(payload))})
		}
		receipt.AppliedAt = now.Add(-4 * time.Hour)
		plan, payload, err := buildSSTDeletionPlan(
			store, current, command, receipt, retired, now.Add(-3*time.Hour), 0)
		if err != nil {
			t.Fatalf("build %s plan: %v", label, err)
		}
		if created, err := storeSSTDeletionPlan(ctx, store, *plan, payload); err != nil || !created {
			t.Fatalf("store %s plan created=%v error=%v", label, created, err)
		}
		return builtPlan{targets: targets, path: sstDeletionPlanPath(store, plan.PlanID)}
	}
	left := buildPlan("budget-left", 64)
	right := buildPlan("budget-right", 65)
	firstPlan, deferredPlan := left, right
	if right.path < left.path {
		firstPlan, deferredPlan = right, left
	}
	deleteBudget := len(firstPlan.targets) + 1

	cleaner := newSSTCleaner(store, sstCleanerOptions{
		DeleteBatchSize: deleteBudget,
		PlanScanLimit:   defaultSSTDeletionPlanScanLimit,
		Now:             func() time.Time { return now },
	})
	first, err := cleaner.runOnce(ctx)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if first.Attempted != len(firstPlan.targets) || first.Deleted != len(firstPlan.targets) ||
		first.PlansDeleted != 1 || first.Deferred != 1 {
		t.Fatalf("first run stats=%+v", first)
	}
	if cleaner.pendingPlanKey != deferredPlan.path {
		t.Fatalf("pending plan=%q want=%q", cleaner.pendingPlanKey, deferredPlan.path)
	}
	if cleaner.planIter == nil {
		t.Fatal("list iterator was discarded while carrying a plan")
	}

	second, err := cleaner.runOnce(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if second.Attempted != len(deferredPlan.targets) || second.Deleted != len(deferredPlan.targets) || second.PlansDeleted != 1 {
		t.Fatalf("second run stats=%+v", second)
	}
	if cleaner.pendingPlanKey != "" {
		t.Fatalf("pending plan after reclaim=%q", cleaner.pendingPlanKey)
	}
	for _, built := range []builtPlan{left, right} {
		for _, target := range built.targets {
			requireObjectExists(t, ctx, store, target.Key, false)
		}
	}
	if plans := listSSTDeletionPlans(t, ctx, store); len(plans) != 0 {
		t.Fatalf("remaining plans=%d want=0", len(plans))
	}
}

func TestSSTDeletionPlanReclaimerStopsOnCancellation(t *testing.T) {
	baseCtx := context.Background()
	store, current, command, receipt, target := newSSTDeletionPlanFixture(t, baseCtx, "cancel")
	defer store.Close()
	now := time.Now().UTC()
	receipt.AppliedAt = now.Add(-4 * time.Hour)
	plan, payload, err := buildSSTDeletionPlan(
		store, current, command, receipt, command.Compaction.RetiredObjects,
		now.Add(-3*time.Hour), 0)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	if created, err := storeSSTDeletionPlan(baseCtx, store, *plan, payload); err != nil || !created {
		t.Fatalf("store plan created=%v error=%v", created, err)
	}

	ctx, cancel := context.WithCancel(baseCtx)
	deleter := &cancelingObjectDeleter{cancel: cancel}
	cleaner := newSSTCleaner(store, sstCleanerOptions{
		Now: func() time.Time { return now }, Deleter: deleter,
	})
	stats, err := cleaner.runOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error=%v want context canceled", err)
	}
	if stats.Attempted != 1 || stats.Deleted != 0 || stats.Failed != 0 || deleter.batchCalls != 1 {
		t.Fatalf("canceled reclaim stats=%+v batch_calls=%d", stats, deleter.batchCalls)
	}
	if cleaner.planIter != nil {
		t.Fatal("canceled reclaim retained a terminal list iterator")
	}
	requireObjectExists(t, baseCtx, store, target.Key, true)
}

func TestSSTDeletionPlanCorruptionFailsClosed(t *testing.T) {
	ctx := context.Background()
	store, current, command, receipt, target := newSSTDeletionPlanFixture(t, ctx, "corrupt")
	defer store.Close()
	cleaner := newSSTCleaner(store, sstCleanerOptions{})
	if _, err := cleaner.markCommandOutcome(ctx, current, command, receipt); err != nil {
		t.Fatalf("prepare plan: %v", err)
	}
	objects, err := store.List(ctx, blobstore.ListOptions{Prefix: sstDeletionPlanPrefix + "/"})
	if err != nil || len(objects.Objects) != 1 {
		t.Fatalf("list plans=%+v err=%v", objects, err)
	}
	if _, err := store.Write(ctx, objects.Objects[0].Key, []byte(`{"version":1,"kind":"sst_retirement"}`)); err != nil {
		t.Fatalf("corrupt plan: %v", err)
	}

	stats, err := runSSTDeletionPlanReclaimer(ctx, store, 128, 1024, time.Now().UTC().Add(24*time.Hour))
	if err == nil {
		t.Fatal("corrupt plan was accepted")
	}
	if stats.Deleted != 0 || stats.Failed != 1 {
		t.Fatalf("corrupt plan stats=%+v", stats)
	}
	if _, _, err := store.Read(ctx, target.Key); err != nil {
		t.Fatalf("corrupt plan deleted its target: %v", err)
	}
}

func TestMaintenanceKeepsCompactionPendingWhenPlanHandoffFails(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("sst-plan-handoff-failure")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	defer db.Close()
	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close(ctx)
	if err := writer.Put(ctx, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	live := replayManifestForTest(t, ctx, store)
	if len(live.L0SSTs) != 1 {
		t.Fatalf("live L0 SSTs=%d want=1", len(live.L0SSTs))
	}
	sst := live.L0SSTs[0]

	opts := DefaultMaintenanceOptions()
	opts.SSTCompaction.L0TriggerSSTs = 1 << 20
	opts.ManifestCheckpoint.TargetReplayPages = ^uint64(0)
	opts.ManifestCheckpoint.TargetReplayBytes = ^uint64(0)
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	defer maintenance.Close(ctx)
	if err := maintenance.stageCommand(ctx, manifest.MaintenanceCommand{
		Kind: manifest.MaintenanceCommandRemoveSSTables,
		RemoveSSTables: &manifest.RemoveSSTablesCommand{
			SSTableIDs: []string{sst.ID},
			RetiredObjects: []manifest.RetiredObject{{
				Kind: manifest.RetiredObjectSST,
				ID:   sst.ID,
				Key:  store.SSTPath(sst.ID),
				Size: sst.Size,
			}},
		},
	}); err != nil {
		t.Fatalf("stage removal: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("publish removal: %v", err)
	}

	head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil || head == nil || head.Pending == nil {
		t.Fatalf("read pending command head=%+v err=%v", head, err)
	}
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read CURRENT: %v", err)
	}
	retired, ok := retiredObjectsFromMaintenanceCommand(head.Pending)
	if !ok {
		t.Fatalf("pending command has no retired targets: %+v", head.Pending)
	}
	plan, _, err := buildSSTDeletionPlan(store, current, head.Pending, current.MaintenanceReceipt,
		retired, maintenance.sstGC.opts.Now().UTC(), maintenance.sstGC.opts.SafetyMargin)
	if err != nil {
		t.Fatalf("build expected plan: %v", err)
	}
	if _, err := store.Write(ctx, sstDeletionPlanPath(store, plan.PlanID), []byte(`{"corrupt":true}`)); err != nil {
		t.Fatalf("write corrupt plan collision: %v", err)
	}

	if _, err := maintenance.RunOnce(ctx); err == nil {
		t.Fatal("maintenance cleared a command whose deletion plan could not be validated")
	}
	head, _, err = db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("read HEAD after failed handoff: %v", err)
	}
	if head == nil || head.Pending == nil || !current.MaintenanceReceipt.Matches(head.Pending) {
		t.Fatalf("pending command was not preserved after failed handoff: %+v", head)
	}
	if _, _, err := store.Read(ctx, store.SSTPath(sst.ID)); err != nil {
		t.Fatalf("failed handoff deleted retired target: %v", err)
	}
}

func TestManifestRejectsIncompleteRetirementBatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("retirement-validation")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := manifestStore.ClaimCompactor(ctx, "gc-validation"); err != nil {
		t.Fatalf("claim compactor: %v", err)
	}
	if _, err := manifestStore.AppendRemoveSSTablesWithFence(ctx, []string{"sst-a"}, nil); !errors.Is(err, manifest.ErrInvalidRetirement) {
		t.Fatalf("missing retirement error=%v want=%v", err, manifest.ErrInvalidRetirement)
	}

	retired := make([]manifest.RetiredObject, manifest.MaxRetiredObjectsPerEntry+1)
	for i := range retired {
		id := fmt.Sprintf("sst-%03d", i)
		retired[i] = manifest.RetiredObject{Kind: manifest.RetiredObjectSST, ID: id, Key: store.SSTPath(id)}
	}
	if _, err := manifestStore.AppendRemoveSSTablesWithFence(ctx, []string{"sst-000"}, retired); !errors.Is(err, manifest.ErrInvalidRetirement) {
		t.Fatalf("oversized retirement error=%v want=%v", err, manifest.ErrInvalidRetirement)
	}
}

func newSSTDeletionPlanFixture(
	t *testing.T,
	ctx context.Context,
	id string,
) (*blobstore.Store, *manifest.Current, *manifest.MaintenanceCommand, *manifest.MaintenanceReceipt, sstDeletionTarget) {
	t.Helper()
	store := blobstore.NewMemory("sst-plan-" + id)
	current, command, receipt, target := writeSSTDeletionPlanFixture(t, ctx, store, id)
	return store, current, command, receipt, target
}

func writeSSTDeletionPlanFixture(
	t *testing.T,
	ctx context.Context,
	store *blobstore.Store,
	id string,
) (*manifest.Current, *manifest.MaintenanceCommand, *manifest.MaintenanceReceipt, sstDeletionTarget) {
	t.Helper()
	sstID := "retired-" + id + ".sst"
	key := store.SSTPath(sstID)
	payload := []byte("sst-" + id)
	if _, err := store.Write(ctx, key, payload); err != nil {
		t.Fatalf("write target: %v", err)
	}
	appliedAt := time.Now().UTC().Add(-time.Minute)
	retired := manifest.RetiredObject{Kind: manifest.RetiredObjectSST, ID: sstID, Key: key, Size: int64(len(payload))}
	command := &manifest.MaintenanceCommand{
		ID:         "command-" + id,
		Epoch:      7,
		Generation: 11,
		Kind:       manifest.MaintenanceCommandCompaction,
		CreatedAt:  appliedAt.Add(-time.Second),
		Compaction: &manifest.CompactionCommand{
			Payload: manifest.CompactionLogPayload{
				RemoveSSTableIDs: []string{sstID},
				SourceLevel:      0,
				DestinationLevel: 1,
			},
			RetiredObjects: []manifest.RetiredObject{retired},
		},
	}
	receipt := &manifest.MaintenanceReceipt{
		CommandID:  command.ID,
		Epoch:      command.Epoch,
		Generation: command.Generation,
		Status:     manifest.MaintenanceStatusApplied,
		AppliedAt:  appliedAt,
	}
	current := &manifest.Current{MaxPinnedViewAge: 10 * time.Minute}
	return current, command, receipt, sstDeletionTarget{ID: sstID, Key: key, Size: int64(len(payload))}
}

func listSSTDeletionPlans(t *testing.T, ctx context.Context, store *blobstore.Store) []sstDeletionPlan {
	t.Helper()
	objects, err := store.List(ctx, blobstore.ListOptions{Prefix: sstDeletionPlanPrefix + "/"})
	if err != nil {
		t.Fatalf("list SST deletion plans: %v", err)
	}
	plans := make([]sstDeletionPlan, 0, len(objects.Objects))
	for _, object := range objects.Objects {
		if object.IsDir {
			continue
		}
		payload, _, err := store.Read(ctx, object.Key)
		if err != nil {
			t.Fatalf("read SST deletion plan %q: %v", object.Key, err)
		}
		plan, err := decodeSSTDeletionPlan(store, object.Key, payload)
		if err != nil {
			t.Fatalf("decode SST deletion plan %q: %v", object.Key, err)
		}
		plans = append(plans, plan)
	}
	return plans
}
