package isledb

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestPlanPendingSSTSweep_ClearsLiveAndStopsAtNotDueBoundary(t *testing.T) {
	now := time.Now().UTC()
	byID := map[string]pendingSSTDeleteMark{
		"live-1": {
			SSTID: "live-1",
			DueAt: now.Add(-time.Minute),
		},
		"due-1": {
			SSTID: "due-1",
			DueAt: now.Add(-time.Minute),
		},
		"not-due-1": {
			SSTID: "not-due-1",
			DueAt: now.Add(time.Minute),
		},
	}
	liveSet := map[string]struct{}{
		"live-1": {},
	}

	plan := planPendingSSTSweep(byID, liveSet, now, time.Hour, 10)
	if plan.clearedLive != 1 {
		t.Fatalf("clearedLive mismatch: got=%d want=1", plan.clearedLive)
	}
	if !plan.changed {
		t.Fatalf("expected plan.changed=true when live marks are cleared")
	}
	if _, exists := byID["live-1"]; exists {
		t.Fatalf("expected live mark to be removed from map")
	}
	wantDeleteIDs := []string{"due-1"}
	if !reflect.DeepEqual(plan.deleteIDs, wantDeleteIDs) {
		t.Fatalf("delete ids mismatch: got=%v want=%v", plan.deleteIDs, wantDeleteIDs)
	}
}

func TestPlanPendingSSTSweep_RespectsBatchSizeAndOrder(t *testing.T) {
	now := time.Now().UTC()
	byID := map[string]pendingSSTDeleteMark{
		"c": {SSTID: "c", DueAt: now.Add(-30 * time.Second)},
		"a": {SSTID: "a", DueAt: now.Add(-time.Minute)},
		"b": {SSTID: "b", DueAt: now.Add(-time.Minute)},
	}

	plan := planPendingSSTSweep(byID, map[string]struct{}{}, now, 0, 2)
	wantDeleteIDs := []string{"a", "b"}
	if !reflect.DeepEqual(plan.deleteIDs, wantDeleteIDs) {
		t.Fatalf("delete ids mismatch: got=%v want=%v", plan.deleteIDs, wantDeleteIDs)
	}
	if plan.clearedLive != 0 {
		t.Fatalf("unexpected live clears: got=%d want=0", plan.clearedLive)
	}
	if plan.changed {
		t.Fatalf("expected plan.changed=false without live clears")
	}
}

func TestRunPendingSSTSweeper_DeletesDueOrphan(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)

	orphanID := "sweep-orphan-1"
	if _, err := store.Write(ctx, store.SSTPath(orphanID), []byte("orphan-data")); err != nil {
		t.Fatalf("write orphan sst: %v", err)
	}

	now := time.Now().UTC()
	if err := storePendingSSTDeleteMarkSet(ctx, store, &pendingSSTDeleteMarkSet{
		Marks: []pendingSSTDeleteMark{
			{
				Version:                 gcMarkSchemaVersion,
				SSTID:                   orphanID,
				FirstSeenUnreferencedAt: now.Add(-2 * time.Hour),
				LastSeenUnreferencedAt:  now.Add(-2 * time.Hour),
				DueAt:                   now.Add(-time.Minute),
			},
		},
	}); err != nil {
		t.Fatalf("store pending marks: %v", err)
	}

	stats, err := runPendingSSTSweeper(ctx, store, manifestStore, 10, time.Hour)
	if err != nil {
		t.Fatalf("run sweeper: %v", err)
	}
	if stats.Deleted == 0 {
		t.Fatalf("expected orphan sst delete to be counted")
	}

	exists, err := store.Exists(ctx, store.SSTPath(orphanID))
	if err != nil {
		t.Fatalf("exists orphan sst: %v", err)
	}
	if exists {
		t.Fatalf("expected orphan sst to be deleted")
	}

	found, err := hasPendingSSTDeleteMark(ctx, store, orphanID)
	if err != nil {
		t.Fatalf("lookup orphan mark: %v", err)
	}
	if found {
		t.Fatalf("expected orphan mark to be removed after sweep")
	}
}

func TestRunPendingSSTSweeper_ClearsLiveMarkWithoutDelete(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	if err := w.put([]byte("live-key"), []byte("value")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	m, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	if len(m.L0SSTs) == 0 {
		t.Fatalf("expected live L0 sst")
	}
	liveID := m.L0SSTs[0].ID

	now := time.Now().UTC()
	if err := storePendingSSTDeleteMarkSet(ctx, store, &pendingSSTDeleteMarkSet{
		Marks: []pendingSSTDeleteMark{
			{
				Version:                 gcMarkSchemaVersion,
				SSTID:                   liveID,
				FirstSeenUnreferencedAt: now.Add(-2 * time.Hour),
				LastSeenUnreferencedAt:  now.Add(-2 * time.Hour),
				DueAt:                   now.Add(-time.Minute),
			},
		},
	}); err != nil {
		t.Fatalf("store pending marks: %v", err)
	}

	stats, err := runPendingSSTSweeper(ctx, store, manifestStore, 10, time.Hour)
	if err != nil {
		t.Fatalf("run sweeper: %v", err)
	}
	if stats.ClearedLive == 0 {
		t.Fatalf("expected live mark clear to be counted")
	}

	exists, err := store.Exists(ctx, store.SSTPath(liveID))
	if err != nil {
		t.Fatalf("exists live sst: %v", err)
	}
	if !exists {
		t.Fatalf("live sst should not be deleted")
	}

	found, err := hasPendingSSTDeleteMark(ctx, store, liveID)
	if err != nil {
		t.Fatalf("lookup live mark: %v", err)
	}
	if found {
		t.Fatalf("expected live mark to be removed")
	}
}

func TestRunPendingSSTSweeper_KeepsNotDueMark(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)

	orphanID := fmt.Sprintf("sweep-not-due-%d", time.Now().UnixNano())
	if _, err := store.Write(ctx, store.SSTPath(orphanID), []byte("orphan-data")); err != nil {
		t.Fatalf("write orphan sst: %v", err)
	}

	now := time.Now().UTC()
	if err := storePendingSSTDeleteMarkSet(ctx, store, &pendingSSTDeleteMarkSet{
		Marks: []pendingSSTDeleteMark{
			{
				Version:                 gcMarkSchemaVersion,
				SSTID:                   orphanID,
				FirstSeenUnreferencedAt: now,
				LastSeenUnreferencedAt:  now,
			},
		},
	}); err != nil {
		t.Fatalf("store pending marks: %v", err)
	}

	stats, err := runPendingSSTSweeper(ctx, store, manifestStore, 10, time.Hour)
	if err != nil {
		t.Fatalf("run sweeper: %v", err)
	}
	if stats.Attempted != 0 {
		t.Fatalf("expected zero delete attempts for not-due marks, got %d", stats.Attempted)
	}

	exists, err := store.Exists(ctx, store.SSTPath(orphanID))
	if err != nil {
		t.Fatalf("exists orphan sst: %v", err)
	}
	if !exists {
		t.Fatalf("not-due orphan should not be deleted")
	}

	found, err := hasPendingSSTDeleteMark(ctx, store, orphanID)
	if err != nil {
		t.Fatalf("lookup not-due mark: %v", err)
	}
	if !found {
		t.Fatalf("expected not-due mark to remain")
	}
}

func TestRunPendingSSTSweeper_ClearsLiveMarksPastNotDueBoundary(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	if err := w.put([]byte("live-boundary-key"), []byte("value")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	m, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	if len(m.L0SSTs) == 0 {
		t.Fatalf("expected live L0 sst")
	}
	liveID := m.L0SSTs[0].ID

	orphanID := "sweep-boundary-orphan"
	if _, err := store.Write(ctx, store.SSTPath(orphanID), []byte("orphan-data")); err != nil {
		t.Fatalf("write orphan sst: %v", err)
	}

	now := time.Now().UTC()
	if err := storePendingSSTDeleteMarkSet(ctx, store, &pendingSSTDeleteMarkSet{
		Marks: []pendingSSTDeleteMark{
			{
				Version:                 gcMarkSchemaVersion,
				SSTID:                   orphanID,
				FirstSeenUnreferencedAt: now,
				LastSeenUnreferencedAt:  now,
			},
			{
				Version:                 gcMarkSchemaVersion,
				SSTID:                   liveID,
				FirstSeenUnreferencedAt: now.Add(-2 * time.Hour),
				LastSeenUnreferencedAt:  now.Add(-2 * time.Hour),
				DueAt:                   now.Add(time.Hour),
			},
		},
	}); err != nil {
		t.Fatalf("store pending marks: %v", err)
	}

	stats, err := runPendingSSTSweeper(ctx, store, manifestStore, 10, time.Hour)
	if err != nil {
		t.Fatalf("run sweeper: %v", err)
	}
	if stats.ClearedLive == 0 {
		t.Fatalf("expected live mark to be cleared")
	}
	if stats.Attempted != 0 {
		t.Fatalf("expected no deletes when earliest orphan mark is not due, got attempted=%d", stats.Attempted)
	}

	foundLive, err := hasPendingSSTDeleteMark(ctx, store, liveID)
	if err != nil {
		t.Fatalf("lookup live mark: %v", err)
	}
	if foundLive {
		t.Fatalf("expected live mark to be cleared")
	}

	foundOrphan, err := hasPendingSSTDeleteMark(ctx, store, orphanID)
	if err != nil {
		t.Fatalf("lookup orphan mark: %v", err)
	}
	if !foundOrphan {
		t.Fatalf("expected not-due orphan mark to remain")
	}
}
