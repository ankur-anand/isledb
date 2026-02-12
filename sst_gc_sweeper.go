package isledb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

const (
	defaultSSTSweepBatchSize   = 128
	defaultSSTSweepGracePeriod = 10 * time.Minute
)

type sstSweepStats struct {
	Attempted   int
	Deleted     int
	ClearedLive int
	Failed      int
}

type sstSweepPlan struct {
	deleteIDs   []string
	clearedLive int
	changed     bool
}

func runPendingSSTSweeper(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, batchSize int, gracePeriod time.Duration) (sstSweepStats, error) {
	return runPendingSSTSweeperWithStorage(ctx, store, manifestLog, newGCMarkStorage(store), batchSize, gracePeriod)
}

func runPendingSSTSweeperWithStorage(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, markStorage manifest.GCMarkStorage, batchSize int, gracePeriod time.Duration) (sstSweepStats, error) {
	stats := sstSweepStats{}
	if batchSize <= 0 {
		batchSize = defaultSSTSweepBatchSize
	}
	if gracePeriod < 0 {
		gracePeriod = 0
	}
	if markStorage == nil {
		return stats, errors.New("nil gc mark storage")
	}

	liveSet, err := currentLiveSSTSet(ctx, manifestLog)
	if err != nil {
		return stats, err
	}

	var lastErr error
	for attempt := 0; attempt < gcCASMaxRetries; attempt++ {
		stats = sstSweepStats{}

		set, matchToken, exists, err := loadPendingSSTDeleteMarkSetWithStorageCAS(ctx, markStorage)
		if err != nil {
			return stats, err
		}
		if len(set.Marks) == 0 {
			return stats, nil
		}

		byID := pendingMarkMapFromSet(set)
		now := time.Now().UTC()
		plan := planPendingSSTSweep(byID, liveSet, now, gracePeriod, batchSize)
		stats.ClearedLive = plan.clearedLive

		deleteKeys := deleteKeysForSSTIDs(store, plan.deleteIDs)
		stats.Attempted = len(plan.deleteIDs)
		deleted, failed, deleteChanged := applySweepDeleteBatch(ctx, store, byID, plan.deleteIDs, deleteKeys)
		stats.Deleted = deleted
		stats.Failed = failed

		changed := plan.changed || deleteChanged
		if !changed {
			return stats, nil
		}

		pendingMarkMapToSet(set, byID)
		if err := storePendingSSTDeleteMarkSetWithStorageCAS(ctx, markStorage, set, matchToken, exists); err != nil {
			if isGCMarkCASConflict(err) {
				lastErr = err
				continue
			}
			return stats, fmt.Errorf("store pending sst delete marks after sweep: %w", err)
		}
		return stats, nil
	}

	if lastErr != nil {
		return stats, fmt.Errorf("store pending sst delete marks after retries: %w", lastErr)
	}
	return stats, fmt.Errorf("store pending sst delete marks exceeded retries")
}

func planPendingSSTSweep(byID map[string]pendingSSTDeleteMark, liveSet map[string]struct{}, now time.Time, gracePeriod time.Duration, batchSize int) sstSweepPlan {
	deleteCap := 0
	if batchSize > 0 {
		deleteCap = batchSize
	}
	plan := sstSweepPlan{
		deleteIDs: make([]string, 0, deleteCap),
	}

	liveIDs := make([]string, 0, len(liveSet))
	for id := range liveSet {
		liveIDs = append(liveIDs, id)
	}
	beforeLiveClear := len(byID)
	if applyPendingSSTDeleteMarkClears(byID, liveIDs) {
		plan.changed = true
		plan.clearedLive = beforeLiveClear - len(byID)
	}

	if batchSize <= 0 {
		return plan
	}

	ids := sortedPendingMarkIDs(byID, now, gracePeriod)
	for _, id := range ids {
		if len(plan.deleteIDs) >= batchSize {
			break
		}
		mark, ok := byID[id]
		if !ok {
			continue
		}
		if now.Before(pendingMarkDueAt(mark, now, gracePeriod)) {
			break
		}
		plan.deleteIDs = append(plan.deleteIDs, id)
	}

	return plan
}

func deleteKeysForSSTIDs(store *blobstore.Store, ids []string) []string {
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, store.SSTPath(id))
	}
	return keys
}

func applySweepDeleteBatch(ctx context.Context, store *blobstore.Store, byID map[string]pendingSSTDeleteMark, deleteIDs, deleteKeys []string) (deleted, failed int, changed bool) {
	if len(deleteIDs) == 0 {
		return 0, 0, false
	}

	failedByKey := map[string]error{}
	if err := store.BatchDelete(ctx, deleteKeys); err != nil {
		var batchErr *blobstore.BatchDeleteError
		if errors.As(err, &batchErr) {
			failedByKey = batchErr.Failed
		} else {
			for _, key := range deleteKeys {
				failedByKey[key] = err
			}
		}
	}

	successfulDeleteIDs := make([]string, 0, len(deleteIDs))
	for i, id := range deleteIDs {
		key := deleteKeys[i]
		if _, hadFailure := failedByKey[key]; hadFailure {
			failed++
			continue
		}
		successfulDeleteIDs = append(successfulDeleteIDs, id)
	}
	changed = applyPendingSSTDeleteMarkClears(byID, successfulDeleteIDs)
	deleted = len(successfulDeleteIDs)
	return deleted, failed, changed
}

func currentLiveSSTSet(ctx context.Context, manifestLog *manifest.Store) (map[string]struct{}, error) {
	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	liveSet := make(map[string]struct{}, len(m.L0SSTs))
	for _, sst := range m.L0SSTs {
		if sst.ID == "" {
			continue
		}
		liveSet[sst.ID] = struct{}{}
	}
	for _, sr := range m.SortedRuns {
		for _, sst := range sr.SSTs {
			if sst.ID == "" {
				continue
			}
			liveSet[sst.ID] = struct{}{}
		}
	}
	return liveSet, nil
}

func sortedPendingMarkIDs(byID map[string]pendingSSTDeleteMark, now time.Time, gracePeriod time.Duration) []string {
	ids := make([]string, 0, len(byID))
	for id := range byID {
		if id == "" {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		li := pendingMarkDueAt(byID[ids[i]], now, gracePeriod)
		lj := pendingMarkDueAt(byID[ids[j]], now, gracePeriod)
		if li.Equal(lj) {
			return ids[i] < ids[j]
		}
		return li.Before(lj)
	})
	return ids
}

func pendingMarkDueAt(mark pendingSSTDeleteMark, now time.Time, gracePeriod time.Duration) time.Time {
	if !mark.DueAt.IsZero() {
		return mark.DueAt
	}
	if !mark.FirstSeenUnreferencedAt.IsZero() {
		return mark.FirstSeenUnreferencedAt.Add(gracePeriod)
	}
	if !mark.LastSeenUnreferencedAt.IsZero() {
		return mark.LastSeenUnreferencedAt.Add(gracePeriod)
	}
	return now.Add(gracePeriod)
}
