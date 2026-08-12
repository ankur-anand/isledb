package isledb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestChangeFeedDeletionPlanDeadlineUsesLocalObservationAcrossWriterClockSkew(t *testing.T) {
	store := blobstore.NewMemory("feed-clock-skew")
	defer store.Close()
	observedAt := time.Now().UTC()
	localSafetyDeadline := observedAt.Add(2 * time.Minute)
	for name, writerAppliedAt := range map[string]time.Time{
		"writer ahead":  observedAt.Add(24 * time.Hour),
		"writer behind": observedAt.Add(-24 * time.Hour),
	} {
		t.Run(name, func(t *testing.T) {
			plan, _, err := buildChangeFeedDeletionPlan(store,
				[]changeBatchDeleteCandidate{{Path: store.ChangeBatchPath("batch"), ID: "batch", Seq: 1}},
				2, observedAt, time.Minute, writerAppliedAt, observedAt, time.Minute, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if !plan.FloorPublishedAt.Equal(writerAppliedAt) {
				t.Fatalf("publication metadata=%s want=%s", plan.FloorPublishedAt, writerAppliedAt)
			}
			if !plan.NotBefore.Equal(localSafetyDeadline) {
				t.Fatalf("deletion deadline=%s trusts writer timestamp=%s; local observation deadline=%s",
					plan.NotBefore, writerAppliedAt, localSafetyDeadline)
			}
		})
	}
}

type changeFeedReclaimerCurrentStorage struct {
	manifest.Storage
	current []byte
}

func (s *changeFeedReclaimerCurrentStorage) ReadCurrent(context.Context) ([]byte, string, error) {
	return append([]byte(nil), s.current...), "test", nil
}

func TestChangeFeedReclaimerRetainsCursorAfterUnreadablePlan(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("feed-cursor-reset")
	defer store.Close()
	current, err := manifest.EncodeCurrent(&manifest.Current{
		LayoutVersion:      manifest.LayoutVersion,
		NextEpoch:          1,
		NextSeq:            1,
		ChangeFeedEnabled:  true,
		ChangeFeedLogStart: 1,
		MaxPinnedViewAge:   time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestLog := manifest.NewStoreWithStorage(&changeFeedReclaimerCurrentStorage{
		Storage: manifest.NewBlobStoreBackend(store),
		current: current,
	})

	type candidatePlan struct {
		candidate changeBatchDeleteCandidate
		path      string
	}
	plans := make([]candidatePlan, defaultChangeFeedDeletionPlanScanLimit)
	now := time.Now().UTC()
	for i := range plans {
		id := fmt.Sprintf("cursor-%06d", i)
		candidate := changeBatchDeleteCandidate{
			Path: store.ChangeBatchPath(id), ID: id, Seq: 0, Size: 1,
		}
		plan, _, buildErr := buildChangeFeedDeletionPlan(store,
			[]changeBatchDeleteCandidate{candidate}, 1,
			now.Add(24*time.Hour), time.Minute, now.Add(24*time.Hour),
			now.Add(24*time.Hour), time.Nanosecond, 0)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		plans[i] = candidatePlan{candidate: candidate, path: changeFeedDeletionPlanPath(store, plan.PlanID)}
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].path < plans[j].path })

	// The lexically last valid plan is due. The 1023 valid plans before it are
	// deferred, so retaining the iterator would reach it on the second pass.
	ready := plans[len(plans)-1]
	for i, item := range plans {
		createdAt := now.Add(24 * time.Hour)
		floorPublishedAt := createdAt
		if i == len(plans)-1 {
			createdAt = now.Add(-time.Hour)
			floorPublishedAt = createdAt
			if _, err := store.Write(ctx, item.candidate.Path, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		plan, payload, buildErr := buildChangeFeedDeletionPlan(store,
			[]changeBatchDeleteCandidate{item.candidate}, 1,
			createdAt, time.Nanosecond, floorPublishedAt, createdAt, time.Nanosecond, 0)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		if _, err := storeChangeFeedDeletionPlan(ctx, store, *plan, payload); err != nil {
			t.Fatal(err)
		}
	}
	badPath := storeKey(store, changeFeedDeletionPlanPrefix, "000-unreadable.json")
	if _, err := store.Write(ctx, badPath, []byte("not-json")); err != nil {
		t.Fatal(err)
	}

	cleaner := &changeFeedCleaner{
		store: store, manifestLog: manifestLog,
		opts:      changeFeedCleanerOptions{SweepBatchSize: 1},
		planCache: newDeletionPlanCache[changeFeedDeletionPlan](),
	}
	if _, err := cleaner.runReclaimOnce(ctx); err == nil {
		t.Fatal("first pass did not report the unreadable plan")
	}
	if _, err := cleaner.runReclaimOnce(ctx); err != nil {
		t.Fatalf("second pass did not continue past the unreadable plan: %v", err)
	}
	if _, _, err := store.Read(ctx, ready.candidate.Path); err == nil {
		t.Fatalf("ready change-feed plan after scan boundary was starved across passes; target %q still exists",
			ready.candidate.Path)
	}
}

type changeFeedCleanerScanStorage struct {
	manifest.Storage
	manifest.PageStorage

	currentReads atomic.Int64
	pageReads    atomic.Int64
}

func (s *changeFeedCleanerScanStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	s.currentReads.Add(1)
	return s.Storage.ReadCurrent(ctx)
}

func (s *changeFeedCleanerScanStorage) ReadPage(ctx context.Context, path string) ([]byte, error) {
	s.pageReads.Add(1)
	return s.PageStorage.ReadPage(ctx, path)
}

func TestChangeFeedCleanerRetiresOldBatches(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriterWithPolicy(ctx, "writer-1", time.Nanosecond); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	now := time.Now().UTC()
	oldMeta := writeChangeBatchForCleanerTest(t, ctx, store, "old.chg", now.Add(-2*time.Hour))
	oldEntry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{ID: "old.sst", Epoch: oldMeta.Epoch, SeqLo: oldMeta.SeqLo, SeqHi: oldMeta.SeqHi, Level: 0, CreatedAt: oldMeta.CreatedAt}, &oldMeta)
	if err != nil {
		t.Fatalf("append old sst: %v", err)
	}
	recentMeta := writeChangeBatchForCleanerTest(t, ctx, store, "recent.chg", now)
	recentEntry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{ID: "recent.sst", Epoch: recentMeta.Epoch, SeqLo: recentMeta.SeqLo, SeqHi: recentMeta.SeqHi, Level: 0, CreatedAt: recentMeta.CreatedAt}, &recentMeta)
	if err != nil {
		t.Fatalf("append recent sst: %v", err)
	}

	cleaner, err := newChangeFeedCleaner(ctx, store, manifestStore, changeFeedCleanerOptions{
		RetentionPeriod:            time.Hour,
		KeepAtLeastManifestEntries: 1,
		SweepGracePeriod:           -1,
		DeletionSafetyMargin:       -1,
	})
	if err != nil {
		t.Fatalf("new change feed cleaner: %v", err)
	}
	if err := cleaner.RunOnce(ctx); err != nil {
		t.Fatalf("run cleaner: %v", err)
	}

	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if got, want := current.ChangeFeedLogStart, oldEntry.Seq+1; got != want {
		t.Fatalf("unexpected change-feed floor: got=%d want=%d", got, want)
	}
	if _, err := manifestStore.ReadEntry(ctx, oldEntry.Seq); err == nil {
		t.Fatalf("expected old manifest entry seq=%d to be retired", oldEntry.Seq)
	}
	if _, err := manifestStore.ReadEntry(ctx, recentEntry.Seq); err != nil {
		t.Fatalf("recent manifest entry should remain readable: %v", err)
	}
	if _, _, err := store.Read(ctx, oldMeta.Path); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("old change batch read error=%v, want ErrNotFound", err)
	}
	if _, _, err := store.Read(ctx, recentMeta.Path); err != nil {
		t.Fatalf("recent change batch should remain: %v", err)
	}
	plans, err := listChangeFeedDeletionPlans(ctx, store)
	if err != nil {
		t.Fatalf("list change-feed deletion plans: %v", err)
	}
	if len(plans) != 0 {
		t.Fatalf("expected change-feed deletion plans to be cleared after sweep, got=%+v", plans)
	}

	// Change-feed retention must not remove manifest entries still required to
	// rebuild current KV state after a restart.
	freshManifestStore := manifest.NewStore(store)
	replayed, err := freshManifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("fresh replay after change-feed cleanup: %v", err)
	}
	if replayed.LookupSST("old.sst") == nil || replayed.LookupSST("recent.sst") == nil {
		t.Fatalf("change-feed cleanup changed visible SST state: %+v", replayed.AllSSTIDs())
	}
}

func TestChangeFeedCleanerSeparatesLogicalRetentionFromPhysicalDelete(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-control-reclaim-separation")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriterWithPolicy(ctx, "change-separation-writer", time.Nanosecond); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}

	now := time.Now().UTC()
	old := writeChangeBatchForCleanerTest(t, ctx, store, "separated-old.chg", now.Add(-2*time.Hour))
	oldEntry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx,
		manifest.SSTMeta{ID: "separated-old.sst", Epoch: old.Epoch, SeqLo: old.SeqLo, SeqHi: old.SeqHi}, &old)
	if err != nil {
		t.Fatalf("append old batch: %v", err)
	}
	recent := writeChangeBatchForCleanerTest(t, ctx, store, "separated-recent.chg", now)
	if _, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx,
		manifest.SSTMeta{ID: "separated-recent.sst", Epoch: recent.Epoch, SeqLo: recent.SeqLo, SeqHi: recent.SeqHi}, &recent); err != nil {
		t.Fatalf("append recent batch: %v", err)
	}

	cleaner, err := newChangeFeedCleaner(ctx, store, manifestStore, changeFeedCleanerOptions{
		RetentionPeriod:            time.Hour,
		KeepAtLeastManifestEntries: 1,
		SweepGracePeriod:           -1,
		DeletionSafetyMargin:       -1,
	})
	if err != nil {
		t.Fatalf("newChangeFeedCleaner: %v", err)
	}
	control, err := cleaner.runControlOnce(ctx)
	if err != nil {
		t.Fatalf("runControlOnce: %v", err)
	}
	if control.EntriesRetired == 0 || control.BatchesPlanned != 1 || control.BatchesDeleted != 0 {
		t.Fatalf("control stats=%+v", control)
	}
	requireObjectExists(t, ctx, store, old.Path, true)
	plans, err := listChangeFeedDeletionPlans(ctx, store)
	if err != nil || len(plans) != 1 || len(plans[0].Targets) != 1 || plans[0].Targets[0].Seq != oldEntry.Seq {
		t.Fatalf("plans=%+v error=%v", plans, err)
	}

	reclaim, err := cleaner.runReclaimOnce(ctx)
	if err != nil {
		t.Fatalf("runReclaimOnce: %v", err)
	}
	if reclaim.BatchesDeleted != 1 || reclaim.EntriesRetired != 0 || reclaim.BatchesPlanned != 0 {
		t.Fatalf("reclaim stats=%+v", reclaim)
	}
	requireObjectExists(t, ctx, store, old.Path, false)
	requireObjectExists(t, ctx, store, recent.Path, true)
}

func TestChangeFeedCleanerPlansInPageBatchesFromOneView(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-cleaner-page-scan")
	defer store.Close()

	backend := manifest.NewBlobStoreBackend(store)
	storage := &changeFeedCleanerScanStorage{Storage: backend, PageStorage: backend}
	manifestStore := manifest.NewStoreWithStorage(storage)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	writerFence, err := manifestStore.ClaimWriter(ctx, "writer-1")
	if err != nil {
		t.Fatalf("claim writer: %v", err)
	}
	if err := manifestStore.EnableChangeFeed(ctx, manifest.ChangeFeedPayloadFullValues); err != nil {
		t.Fatalf("enable change feed: %v", err)
	}

	now := time.Now().UTC()
	for i := 0; i < 130; i++ {
		appendChangeFeedCleanerManifestEntry(t, ctx, manifestStore, writerFence.Epoch, i, now.Add(-2*time.Hour))
	}
	appendChangeFeedCleanerManifestEntry(t, ctx, manifestStore, writerFence.Epoch, 130, now)

	cleaner, err := newChangeFeedCleaner(ctx, store, manifestStore, changeFeedCleanerOptions{
		RetentionPeriod:            time.Hour,
		KeepAtLeastManifestEntries: 1,
	})
	if err != nil {
		t.Fatalf("new change feed cleaner: %v", err)
	}
	view, err := manifestStore.LoadChangeFeedView(ctx)
	if err != nil {
		t.Fatalf("load change-feed view: %v", err)
	}
	storage.currentReads.Store(0)
	storage.pageReads.Store(0)

	floor, candidates, err := cleaner.planRetentionFloor(ctx, view, now)
	if err != nil {
		t.Fatalf("plan retention floor: %v", err)
	}
	if floor != candidates[len(candidates)-1].Seq+1 {
		t.Fatalf("floor=%d last candidate seq=%d", floor, candidates[len(candidates)-1].Seq)
	}
	if len(candidates) != defaultChangeFeedSweepBatchSize {
		t.Fatalf("candidates=%d want=%d", len(candidates), defaultChangeFeedSweepBatchSize)
	}
	if got := storage.currentReads.Load(); got != 0 {
		t.Fatalf("CURRENT reads during pinned scan=%d want=0", got)
	}
	if got := storage.pageReads.Load(); got != 2 {
		t.Fatalf("page reads=%d want=2", got)
	}
}

func TestChangeFeedDeletionPlanDoesNotDeleteRetainedBatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	meta := writeChangeBatchForCleanerTest(t, ctx, store, "retained.chg", time.Now().Add(-2*time.Hour))
	entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{ID: "retained.sst", Epoch: 1, SeqLo: meta.SeqLo, SeqHi: meta.SeqHi, Level: 0, CreatedAt: meta.CreatedAt}, &meta)
	if err != nil {
		t.Fatalf("append retained sst: %v", err)
	}
	now := time.Now().UTC()
	plan, payload, err := buildDueChangeFeedDeletionPlanForTest(store, []changeBatchDeleteCandidate{{
		Path: meta.Path, ID: meta.ID, Seq: entry.Seq, Size: meta.Size, Checksum: meta.Checksum,
	}}, entry.Seq+1, now, 0)
	if err != nil {
		t.Fatalf("build deletion plan: %v", err)
	}
	if created, err := storeChangeFeedDeletionPlan(ctx, store, *plan, payload); err != nil || !created {
		t.Fatalf("store deletion plan created=%v error=%v", created, err)
	}

	stats, _, err := runChangeFeedDeletionPlanReclaimer(
		ctx, store, manifestStore, 10, 10, now, store,
		store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"}), nil)
	if err != nil {
		t.Fatalf("run sweeper: %v", err)
	}
	if stats.Deleted != 0 || stats.BlockedRetained != 1 {
		t.Fatalf("unexpected sweep stats before floor advance: %+v", stats)
	}
	if _, _, err := store.Read(ctx, meta.Path); err != nil {
		t.Fatalf("retained change batch should not be deleted: %v", err)
	}

	compactorToken, err := manifestStore.ClaimCompactor(ctx, "change-feed-test")
	if err != nil {
		t.Fatalf("claim compactor: %v", err)
	}
	if _, err := manifestStore.AdvanceChangeFeedLogStart(ctx, entry.Seq+1, compactorToken); err != nil {
		t.Fatalf("advance change-feed floor: %v", err)
	}
	stats, _, err = runChangeFeedDeletionPlanReclaimer(
		ctx, store, manifestStore, 10, 10, now, store,
		store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"}), nil)
	if err != nil {
		t.Fatalf("run sweeper after floor advance: %v", err)
	}
	if stats.Deleted != 1 {
		t.Fatalf("deleted=%d, want 1", stats.Deleted)
	}
	if _, _, err := store.Read(ctx, meta.Path); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("change batch read error=%v, want ErrNotFound", err)
	}
}

func TestChangeFeedDeletionPlanReclaimerCarriesBudgetDeferredPlan(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-plan-budget-carry")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "change-feed-budget-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}

	now := time.Now().UTC()
	candidates := make([]changeBatchDeleteCandidate, 5)
	for i := range candidates {
		meta := writeChangeBatchForCleanerTest(
			t, ctx, store, fmt.Sprintf("budget-%d.chg", i), now.Add(-time.Hour))
		entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx,
			manifest.SSTMeta{ID: fmt.Sprintf("budget-%d.sst", i), Epoch: meta.Epoch, SeqLo: meta.SeqLo, SeqHi: meta.SeqHi}, &meta)
		if err != nil {
			t.Fatalf("append batch %d: %v", i, err)
		}
		candidates[i] = changeBatchDeleteCandidate{
			Path: meta.Path, ID: meta.ID, Seq: entry.Seq, Size: meta.Size, Checksum: meta.Checksum,
		}
	}
	targetFloor := candidates[len(candidates)-1].Seq + 1
	compactor, err := manifestStore.ClaimCompactor(ctx, "change-feed-budget-compactor")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	if _, err := manifestStore.AdvanceChangeFeedLogStart(ctx, targetFloor, compactor); err != nil {
		t.Fatalf("AdvanceChangeFeedLogStart: %v", err)
	}

	type builtPlan struct {
		plan *changeFeedDeletionPlan
		path string
	}
	buildPlan := func(targets []changeBatchDeleteCandidate) builtPlan {
		t.Helper()
		plan, payload, err := buildDueChangeFeedDeletionPlanForTest(
			store, targets, targetFloor, now.Add(-time.Minute), 0)
		if err != nil {
			t.Fatalf("build plan: %v", err)
		}
		if created, err := storeChangeFeedDeletionPlan(ctx, store, *plan, payload); err != nil || !created {
			t.Fatalf("store plan created=%v error=%v", created, err)
		}
		return builtPlan{plan: plan, path: changeFeedDeletionPlanPath(store, plan.PlanID)}
	}
	left := buildPlan(candidates[:2])
	right := buildPlan(candidates[2:])
	firstPlan, deferredPlan := left, right
	if right.path < left.path {
		firstPlan, deferredPlan = right, left
	}
	deleteBudget := len(firstPlan.plan.Targets) + 1

	iter := store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"})
	cache := newDeletionPlanCache[changeFeedDeletionPlan]()
	pendingPlanKey := ""
	first, exhausted, _, err := reclaimChangeFeedDeletionPlans(
		ctx, store, manifestStore, deleteBudget, defaultChangeFeedDeletionPlanScanLimit,
		now, store, iter, cache, &pendingPlanKey)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if exhausted || first.Attempted != len(firstPlan.plan.Targets) || first.Deleted != len(firstPlan.plan.Targets) ||
		first.PlansDeleted != 1 || first.Deferred != len(deferredPlan.plan.Targets) {
		t.Fatalf("first run exhausted=%v stats=%+v", exhausted, first)
	}
	if pendingPlanKey != deferredPlan.path {
		t.Fatalf("pending plan=%q want=%q", pendingPlanKey, deferredPlan.path)
	}

	second, exhausted, _, err := reclaimChangeFeedDeletionPlans(
		ctx, store, manifestStore, deleteBudget, defaultChangeFeedDeletionPlanScanLimit,
		now, store, iter, cache, &pendingPlanKey)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if second.Attempted != len(deferredPlan.plan.Targets) ||
		second.Deleted != len(deferredPlan.plan.Targets) || second.PlansDeleted != 1 {
		t.Fatalf("second run exhausted=%v stats=%+v", exhausted, second)
	}
	if pendingPlanKey != "" {
		t.Fatalf("pending plan after reclaim=%q", pendingPlanKey)
	}
	for _, candidate := range candidates {
		requireObjectExists(t, ctx, store, candidate.Path, false)
	}
	if plans, err := listChangeFeedDeletionPlans(ctx, store); err != nil || len(plans) != 0 {
		t.Fatalf("remaining plans=%d error=%v", len(plans), err)
	}
}

func TestChangeFeedDeletionPlanReclaimerStopsOnCancellation(t *testing.T) {
	baseCtx := context.Background()
	store := blobstore.NewMemory("change-feed-plan-cancel")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(baseCtx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(baseCtx, "change-feed-cancel-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	now := time.Now().UTC()
	meta := writeChangeBatchForCleanerTest(t, baseCtx, store, "cancel.chg", now.Add(-time.Hour))
	entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(baseCtx,
		manifest.SSTMeta{ID: "cancel.sst", Epoch: meta.Epoch, SeqLo: meta.SeqLo, SeqHi: meta.SeqHi}, &meta)
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	targetFloor := entry.Seq + 1
	compactor, err := manifestStore.ClaimCompactor(baseCtx, "change-feed-cancel-compactor")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	if _, err := manifestStore.AdvanceChangeFeedLogStart(baseCtx, targetFloor, compactor); err != nil {
		t.Fatalf("AdvanceChangeFeedLogStart: %v", err)
	}
	candidate := changeBatchDeleteCandidate{
		Path: meta.Path, ID: meta.ID, Seq: entry.Seq, Size: meta.Size, Checksum: meta.Checksum,
	}
	plan, payload, err := buildDueChangeFeedDeletionPlanForTest(
		store, []changeBatchDeleteCandidate{candidate}, targetFloor, now.Add(-time.Minute), 0)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	if created, err := storeChangeFeedDeletionPlan(baseCtx, store, *plan, payload); err != nil || !created {
		t.Fatalf("store plan created=%v error=%v", created, err)
	}

	ctx, cancel := context.WithCancel(baseCtx)
	deleter := &cancelingObjectDeleter{cancel: cancel}
	stats, _, err := runChangeFeedDeletionPlanReclaimer(
		ctx, store, manifestStore, 128, defaultChangeFeedDeletionPlanScanLimit,
		now, deleter,
		store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"}), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error=%v want context canceled", err)
	}
	if stats.Attempted != 1 || stats.Deleted != 0 || stats.Failed != 0 || deleter.batchCalls != 1 {
		t.Fatalf("canceled reclaim stats=%+v batch_calls=%d", stats, deleter.batchCalls)
	}
	requireObjectExists(t, baseCtx, store, meta.Path, true)
}

func TestChangeFeedDeletionWaitsForPublishedFloorPinnedViews(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-published-floor-deadline")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	const pinnedViewAge = time.Hour
	if _, err := manifestStore.ClaimWriterWithPolicy(ctx, "change-deadline-writer", pinnedViewAge); err != nil {
		t.Fatalf("ClaimWriterWithPolicy: %v", err)
	}

	createdAt := time.Now().UTC().Add(-3 * time.Hour)
	meta := writeChangeBatchForCleanerTest(t, ctx, store, "deadline.chg", createdAt)
	entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx,
		manifest.SSTMeta{ID: "deadline.sst", Epoch: meta.Epoch, SeqLo: meta.SeqLo, SeqHi: meta.SeqHi}, &meta)
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	candidates := []changeBatchDeleteCandidate{{
		Path: meta.Path, ID: meta.ID, Seq: entry.Seq, Size: meta.Size, Checksum: meta.Checksum,
	}}
	command := &manifest.MaintenanceCommand{
		ID:         "change-feed-deadline-command",
		Epoch:      1,
		Generation: 1,
		CreatedAt:  createdAt,
		Kind:       manifest.MaintenanceCommandChangeFeedFloor,
		ChangeFeedFloor: &manifest.AdvanceFloorCommand{
			Floor:           entry.Seq + 1,
			GracePeriod:     10 * time.Minute,
			DeletionTargets: changeFeedDeleteTargetsForManifest(candidates),
		},
	}
	if err := command.Validate(); err != nil {
		t.Fatalf("validate floor command: %v", err)
	}

	// The pending maintenance command is the only durable pre-publication
	// record. No reclaimable plan exists before the writer publishes its floor.
	before, _, err := runChangeFeedDeletionPlanReclaimer(
		ctx, store, manifestStore, 10, 10, time.Now().UTC().Add(24*time.Hour), store,
		store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"}), nil)
	if err != nil || before.PlansScanned != 0 || before.Deleted != 0 {
		t.Fatalf("reclaim before floor publication stats=%+v error=%v", before, err)
	}
	requireObjectExists(t, ctx, store, meta.Path, true)

	compactor, err := manifestStore.ClaimCompactor(ctx, "change-deadline-compactor")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	publishedCurrent, err := manifestStore.AdvanceChangeFeedLogStart(ctx, entry.Seq+1, compactor)
	if err != nil {
		t.Fatalf("AdvanceChangeFeedLogStart: %v", err)
	}
	publishedAt := time.Now().UTC()
	observedAt := publishedAt.Add(30 * time.Second)
	const safetyMargin = time.Minute
	receipt := &manifest.MaintenanceReceipt{
		CommandID: command.ID, Epoch: command.Epoch, Generation: command.Generation,
		Status: manifest.MaintenanceStatusApplied, AppliedAt: publishedAt,
	}
	created, err := recordChangeFeedDeletionPlan(
		ctx, store, publishedCurrent, command, receipt, observedAt, safetyMargin)
	if err != nil || !created {
		t.Fatalf("record deletion plan created=%v error=%v", created, err)
	}
	plans, err := listChangeFeedDeletionPlans(ctx, store)
	if err != nil || len(plans) != 1 {
		t.Fatalf("ready plans=%+v error=%v", plans, err)
	}
	wantNotBefore := observedAt.Add(pinnedViewAge).Add(safetyMargin)
	if !plans[0].FloorPublishedAt.Equal(publishedAt) ||
		!plans[0].ObservedAt.Equal(observedAt) ||
		plans[0].PinnedViewAge != pinnedViewAge ||
		!plans[0].NotBefore.Equal(wantNotBefore) {
		t.Fatalf("ready plan timing=%+v want_not_before=%s", plans[0], wantNotBefore)
	}

	deferred, _, err := runChangeFeedDeletionPlanReclaimer(
		ctx, store, manifestStore, 10, 10, wantNotBefore.Add(-time.Nanosecond), store,
		store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"}), nil)
	if err != nil || deferred.Deferred != 1 || deferred.Deleted != 0 {
		t.Fatalf("reclaim before pinned deadline stats=%+v error=%v", deferred, err)
	}
	requireObjectExists(t, ctx, store, meta.Path, true)

	reclaimed, _, err := runChangeFeedDeletionPlanReclaimer(
		ctx, store, manifestStore, 10, 10, wantNotBefore, store,
		store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"}), nil)
	if err != nil || reclaimed.Deleted != 1 || reclaimed.PlansDeleted != 1 {
		t.Fatalf("reclaim at pinned deadline stats=%+v error=%v", reclaimed, err)
	}
	requireObjectExists(t, ctx, store, meta.Path, false)
}

func TestChangeFeedRetentionCreatesIndependentBoundedPlans(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-independent-plans")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriterWithPolicy(ctx, "change-independent-writer", time.Nanosecond); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}

	now := time.Now().UTC()
	const oldBatches = 70
	oldPaths := make([]string, 0, oldBatches)
	for i := 0; i < oldBatches; i++ {
		id := fmt.Sprintf("independent-%03d.chg", i)
		meta := writeChangeBatchForCleanerTest(t, ctx, store, id, now.Add(-2*time.Hour))
		if _, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx,
			manifest.SSTMeta{ID: fmt.Sprintf("independent-%03d.sst", i), Epoch: meta.Epoch, SeqLo: meta.SeqLo, SeqHi: meta.SeqHi}, &meta); err != nil {
			t.Fatalf("append old batch %d: %v", i, err)
		}
		oldPaths = append(oldPaths, meta.Path)
	}
	recent := writeChangeBatchForCleanerTest(t, ctx, store, "independent-recent.chg", now)
	if _, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx,
		manifest.SSTMeta{ID: "independent-recent.sst", Epoch: recent.Epoch, SeqLo: recent.SeqLo, SeqHi: recent.SeqHi}, &recent); err != nil {
		t.Fatalf("append recent batch: %v", err)
	}

	cleaner, err := newChangeFeedCleaner(ctx, store, manifestStore, changeFeedCleanerOptions{
		RetentionPeriod:            time.Hour,
		KeepAtLeastManifestEntries: 1,
		SweepBatchSize:             16,
		SweepGracePeriod:           -1,
		DeletionSafetyMargin:       -1,
	})
	if err != nil {
		t.Fatalf("newChangeFeedCleaner: %v", err)
	}
	planned := 0
	for attempt := 0; attempt < 16; attempt++ {
		stats, err := cleaner.runControlOnce(ctx)
		if err != nil {
			t.Fatalf("control pass %d: %v", attempt, err)
		}
		planned += stats.BatchesPlanned
		if stats.BatchesPlanned == 0 {
			break
		}
	}
	if planned != oldBatches {
		t.Fatalf("planned batches=%d want=%d", planned, oldBatches)
	}
	plans, err := listChangeFeedDeletionPlans(ctx, store)
	if err != nil {
		t.Fatalf("list plans: %v", err)
	}
	if len(plans) != 5 {
		t.Fatalf("plan objects=%d want=5", len(plans))
	}
	seenIDs := make(map[string]struct{}, len(plans))
	for _, plan := range plans {
		if plan.TargetCount <= 0 || plan.TargetCount > 16 {
			t.Fatalf("unbounded plan target count=%d", plan.TargetCount)
		}
		if _, duplicate := seenIDs[plan.PlanID]; duplicate {
			t.Fatalf("duplicate plan ID=%q", plan.PlanID)
		}
		seenIDs[plan.PlanID] = struct{}{}
	}
	for _, path := range oldPaths {
		requireObjectExists(t, ctx, store, path, true)
	}

	deleted := 0
	for attempt := 0; attempt < 16; attempt++ {
		stats, err := cleaner.runReclaimOnce(ctx)
		if err != nil {
			t.Fatalf("reclaim pass %d: %v", attempt, err)
		}
		deleted += stats.BatchesDeleted
		remaining, err := listChangeFeedDeletionPlans(ctx, store)
		if err != nil {
			t.Fatalf("list remaining plans: %v", err)
		}
		if len(remaining) == 0 {
			break
		}
	}
	if deleted != oldBatches {
		t.Fatalf("deleted batches=%d want=%d", deleted, oldBatches)
	}
	for _, path := range oldPaths {
		requireObjectExists(t, ctx, store, path, false)
	}
	requireObjectExists(t, ctx, store, recent.Path, true)
}

func TestChangeFeedDeletionPlanIsIdempotentAndCorruptionFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-plan-integrity")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "change-integrity-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	meta := writeChangeBatchForCleanerTest(t, ctx, store, "integrity.chg", time.Now().UTC().Add(-time.Hour))
	entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx,
		manifest.SSTMeta{ID: "integrity.sst", Epoch: meta.Epoch, SeqLo: meta.SeqLo, SeqHi: meta.SeqHi}, &meta)
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	now := time.Now().UTC()
	plan, payload, err := buildDueChangeFeedDeletionPlanForTest(store, []changeBatchDeleteCandidate{{
		Path: meta.Path, ID: meta.ID, Seq: entry.Seq, Size: meta.Size, Checksum: meta.Checksum,
	}}, entry.Seq+1, now, 0)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	created, err := storeChangeFeedDeletionPlan(ctx, store, *plan, payload)
	if err != nil || !created {
		t.Fatalf("first store created=%v error=%v", created, err)
	}
	created, err = storeChangeFeedDeletionPlan(ctx, store, *plan, payload)
	if err != nil || created {
		t.Fatalf("idempotent store created=%v error=%v", created, err)
	}
	planPath := changeFeedDeletionPlanPath(store, plan.PlanID)
	if _, err := store.Write(ctx, planPath, []byte(`{"version":1}`)); err != nil {
		t.Fatalf("corrupt plan: %v", err)
	}
	compactor, err := manifestStore.ClaimCompactor(ctx, "change-integrity-compactor")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	if _, err := manifestStore.AdvanceChangeFeedLogStart(ctx, plan.TargetFloor, compactor); err != nil {
		t.Fatalf("AdvanceChangeFeedLogStart: %v", err)
	}
	stats, _, err := runChangeFeedDeletionPlanReclaimer(
		ctx, store, manifestStore, 10, 10, now, store,
		store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"}), nil)
	if err == nil || stats.Failed != 1 || stats.Deleted != 0 {
		t.Fatalf("corrupt reclaim stats=%+v error=%v", stats, err)
	}
	requireObjectExists(t, ctx, store, meta.Path, true)
}

type partialChangeDeleteDeleter struct {
	base      objectDeleter
	failKey   string
	remaining int
}

func (d *partialChangeDeleteDeleter) Delete(ctx context.Context, key string) error {
	return d.base.Delete(ctx, key)
}

func (d *partialChangeDeleteDeleter) BatchDelete(ctx context.Context, keys []string) error {
	failed := make(map[string]error)
	for _, key := range keys {
		if key == d.failKey && d.remaining > 0 {
			d.remaining--
			failed[key] = errors.New("injected partial delete")
			continue
		}
		if err := d.base.Delete(ctx, key); err != nil {
			failed[key] = err
		}
	}
	if len(failed) > 0 {
		return &blobstore.BatchDeleteError{Failed: failed}
	}
	return nil
}

func TestChangeFeedDeletionPlanPartialFailureRetriesIndependently(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-plan-partial-delete")
	defer store.Close()
	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "change-partial-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	now := time.Now().UTC()
	metas := make([]manifest.ChangeBatchMeta, 2)
	entries := make([]*manifest.ManifestLogEntry, 2)
	for i := range metas {
		metas[i] = writeChangeBatchForCleanerTest(t, ctx, store, fmt.Sprintf("partial-%d.chg", i), now.Add(-time.Hour))
		entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx,
			manifest.SSTMeta{ID: fmt.Sprintf("partial-%d.sst", i), Epoch: metas[i].Epoch, SeqLo: metas[i].SeqLo, SeqHi: metas[i].SeqHi}, &metas[i])
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		entries[i] = entry
	}
	candidates := []changeBatchDeleteCandidate{
		{Path: metas[0].Path, ID: metas[0].ID, Seq: entries[0].Seq, Size: metas[0].Size},
		{Path: metas[1].Path, ID: metas[1].ID, Seq: entries[1].Seq, Size: metas[1].Size},
	}
	plan, payload, err := buildDueChangeFeedDeletionPlanForTest(store, candidates, entries[1].Seq+1, now, 0)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	if created, err := storeChangeFeedDeletionPlan(ctx, store, *plan, payload); err != nil || !created {
		t.Fatalf("store plan created=%v error=%v", created, err)
	}
	compactor, err := manifestStore.ClaimCompactor(ctx, "change-partial-compactor")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	if _, err := manifestStore.AdvanceChangeFeedLogStart(ctx, plan.TargetFloor, compactor); err != nil {
		t.Fatalf("AdvanceChangeFeedLogStart: %v", err)
	}
	deleter := &partialChangeDeleteDeleter{base: store, failKey: metas[1].Path, remaining: 1}
	first, _, err := runChangeFeedDeletionPlanReclaimer(
		ctx, store, manifestStore, 10, 10, now, deleter,
		store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"}), nil)
	if err == nil || first.Deleted != 1 || first.Failed != 1 || first.PlansDeleted != 0 {
		t.Fatalf("partial reclaim stats=%+v error=%v", first, err)
	}
	requireObjectExists(t, ctx, store, metas[0].Path, false)
	requireObjectExists(t, ctx, store, metas[1].Path, true)
	if got, err := listChangeFeedDeletionPlans(ctx, store); err != nil || len(got) != 1 {
		t.Fatalf("plans after partial delete=%+v error=%v", got, err)
	}

	second, _, err := runChangeFeedDeletionPlanReclaimer(
		ctx, store, manifestStore, 10, 10, now, deleter,
		store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"}), nil)
	if err != nil || second.PlansDeleted != 1 {
		t.Fatalf("retry reclaim stats=%+v error=%v", second, err)
	}
	requireObjectExists(t, ctx, store, metas[1].Path, false)
	if got, err := listChangeFeedDeletionPlans(ctx, store); err != nil || len(got) != 0 {
		t.Fatalf("plans after retry=%+v error=%v", got, err)
	}
}

func listChangeFeedDeletionPlans(ctx context.Context, store *blobstore.Store) ([]changeFeedDeletionPlan, error) {
	objects, err := store.List(ctx, blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"})
	if err != nil {
		return nil, err
	}
	plans := make([]changeFeedDeletionPlan, 0, len(objects.Objects))
	for _, object := range objects.Objects {
		if object.IsDir {
			continue
		}
		payload, _, err := store.Read(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		plan, err := decodeChangeFeedDeletionPlan(store, object.Key, payload)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func buildDueChangeFeedDeletionPlanForTest(
	store *blobstore.Store,
	candidates []changeBatchDeleteCandidate,
	targetFloor uint64,
	createdAt time.Time,
	gracePeriod time.Duration,
) (*changeFeedDeletionPlan, []byte, error) {
	publicationTime := createdAt.Add(-time.Second)
	observedAt := publicationTime
	return buildChangeFeedDeletionPlan(
		store, candidates, targetFloor, createdAt, gracePeriod,
		publicationTime, observedAt, time.Nanosecond, 0)
}

func TestChangeFeedCleanerRunRejectsLostFence(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-cleaner-fenced")
	defer store.Close()

	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}

	cleaner, err := newChangeFeedCleaner(ctx, store, manifestStore, changeFeedCleanerOptions{})
	if err != nil {
		t.Fatalf("new change feed cleaner: %v", err)
	}
	defer cleaner.Close(ctx)

	competingStore := manifest.NewStore(store)
	if _, err := competingStore.Replay(ctx); err != nil {
		t.Fatalf("competing replay: %v", err)
	}
	if _, err := competingStore.ClaimCompactor(ctx, "change-feed-cleaner-other"); err != nil {
		t.Fatalf("competing compactor claim: %v", err)
	}

	if err := cleaner.RunOnce(ctx); !errors.Is(err, manifest.ErrFenced) {
		t.Fatalf("RunOnce after fence loss error=%v, want %v", err, manifest.ErrFenced)
	}
}

func writeChangeBatchForCleanerTest(t *testing.T, ctx context.Context, store *blobstore.Store, id string, createdAt time.Time) manifest.ChangeBatchMeta {
	t.Helper()
	path := store.ChangeBatchPath(id)
	body := []byte("change-batch:" + id)
	if _, err := store.Write(ctx, path, body); err != nil {
		t.Fatalf("write change batch %s: %v", id, err)
	}
	return manifest.ChangeBatchMeta{
		ID:        id,
		Path:      path,
		Epoch:     1,
		SeqLo:     1,
		SeqHi:     1,
		Count:     1,
		Size:      int64(len(body)),
		Checksum:  "sha256:test",
		CreatedAt: createdAt,
		Version:   1,
		Payload:   manifest.ChangeFeedPayloadFullValues,
	}
}

func appendChangeFeedCleanerManifestEntry(
	t *testing.T,
	ctx context.Context,
	manifestStore *manifest.Store,
	epoch uint64,
	index int,
	createdAt time.Time,
) *manifest.ManifestLogEntry {
	t.Helper()
	seq := uint64(index + 1)
	change := manifest.ChangeBatchMeta{
		ID:        fmt.Sprintf("change-%03d", index),
		Path:      fmt.Sprintf("changes/change-%03d.batch", index),
		Epoch:     epoch,
		SeqLo:     seq,
		SeqHi:     seq,
		Count:     1,
		Size:      128,
		RawSize:   256,
		Checksum:  "sha256:test",
		CreatedAt: createdAt,
		Payload:   manifest.ChangeFeedPayloadFullValues,
	}
	entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{
		ID:        fmt.Sprintf("sst-%03d", index),
		Epoch:     epoch,
		SeqLo:     seq,
		SeqHi:     seq,
		CreatedAt: createdAt,
	}, &change)
	if err != nil {
		t.Fatalf("append entry %d: %v", index, err)
	}
	return entry
}
