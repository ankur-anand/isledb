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

func runPendingSSTSweeper(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, batchSize int, gracePeriod time.Duration) (sstSweepStats, error) {
	stats := sstSweepStats{}
	if batchSize <= 0 {
		batchSize = defaultSSTSweepBatchSize
	}
	if gracePeriod < 0 {
		gracePeriod = 0
	}

	liveSet, err := currentLiveSSTSet(ctx, manifestLog)
	if err != nil {
		return stats, err
	}

	var lastErr error
	for attempt := 0; attempt < gcCASMaxRetries; attempt++ {
		stats = sstSweepStats{}

		set, matchToken, exists, err := loadPendingSSTDeleteMarkSetWithCAS(ctx, store)
		if err != nil {
			return stats, err
		}
		if len(set.Marks) == 0 {
			return stats, nil
		}

		byID := pendingMarkMapFromSet(set)
		changed := false
		now := time.Now().UTC()

		for id := range liveSet {
			if _, ok := byID[id]; !ok {
				continue
			}
			delete(byID, id)
			stats.ClearedLive++
			changed = true
		}

		ids := sortedPendingMarkIDs(byID, now, gracePeriod)
		processed := 0
		deleteIDs := make([]string, 0, batchSize)
		deleteKeys := make([]string, 0, batchSize)
		for _, id := range ids {
			if processed >= batchSize {
				break
			}
			mark, ok := byID[id]
			if !ok {
				continue
			}

			dueAt := pendingMarkDueAt(mark, now, gracePeriod)
			if now.Before(dueAt) {
				break
			}
			processed++
			deleteIDs = append(deleteIDs, id)
			deleteKeys = append(deleteKeys, store.SSTPath(id))
		}

		if len(deleteIDs) > 0 {
			stats.Attempted += len(deleteIDs)

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

			for i, id := range deleteIDs {
				key := deleteKeys[i]
				if _, failed := failedByKey[key]; failed {
					stats.Failed++
					continue
				}
				delete(byID, id)
				stats.Deleted++
				changed = true
			}
		}

		if !changed {
			return stats, nil
		}

		pendingMarkMapToSet(set, byID)
		if err := storePendingSSTDeleteMarkSetWithCAS(ctx, store, set, matchToken, exists); err != nil {
			if errors.Is(err, blobstore.ErrPreconditionFailed) {
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
