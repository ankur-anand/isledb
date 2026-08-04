package isledb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

const (
	defaultSSTSweepBatchSize   = 128
	defaultSSTSweepGracePeriod = 10 * time.Minute
	defaultRetirementReadLimit = 128
)

type sstSweepStats struct {
	Attempted       int
	Deleted         int
	Failed          int
	EntriesAdvanced uint64
	NextManifestSeq uint64
	NextObjectIndex uint32
}

type retirementSweepPlan struct {
	keys             []string
	nextManifestSeq  uint64
	nextObjectIndex  uint32
	entriesAdvanced  uint64
	blockedNotBefore bool
}

func runRetirementSweeper(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, storage manifest.GCCursorStorage, fence *manifest.FenceToken, batchSize int) (sstSweepStats, error) {
	stats := sstSweepStats{}
	if batchSize <= 0 {
		batchSize = defaultSSTSweepBatchSize
	}
	if storage == nil {
		return stats, errors.New("nil gc cursor storage")
	}
	if fence == nil {
		return stats, manifest.ErrFenced
	}

	var lastErr error
	for attempt := 0; attempt < gcCASMaxRetries; attempt++ {
		stats = sstSweepStats{}
		cursor, matchToken, exists, err := loadGCCursorWithCAS(ctx, storage)
		if err != nil {
			return stats, err
		}
		if !exists {
			current, err := manifestLog.ReadCurrentData(ctx)
			if err != nil {
				return stats, fmt.Errorf("read current for gc cursor: %w", err)
			}
			if current != nil {
				cursor.NextManifestSeq = current.RetirementLogStart
			}
			if err := storeGCCursorCAS(ctx, storage, cursor, matchToken, false); err != nil {
				if isGCMarkCASConflict(err) {
					lastErr = err
					continue
				}
				return stats, fmt.Errorf("initialize gc cursor: %w", err)
			}
			continue
		}

		entries, _, err := manifestLog.ReadRetirementEntries(ctx, cursor.NextManifestSeq, defaultRetirementReadLimit)
		if err != nil {
			return stats, err
		}
		plan, err := planRetirementSweep(entries, cursor, time.Now().UTC(), batchSize)
		if err != nil {
			return stats, err
		}
		stats.Attempted = len(plan.keys)
		stats.EntriesAdvanced = plan.entriesAdvanced
		stats.NextManifestSeq = plan.nextManifestSeq
		stats.NextObjectIndex = plan.nextObjectIndex

		if len(plan.keys) > 0 {
			if err := store.BatchDelete(ctx, plan.keys); err != nil {
				stats.Failed = len(plan.keys)
				return stats, fmt.Errorf("delete retired objects: %w", err)
			}
			stats.Deleted = len(plan.keys)
		}

		advanced := plan.nextManifestSeq != cursor.NextManifestSeq || plan.nextObjectIndex != cursor.NextObjectIndex
		if !advanced {
			if cursor.NextObjectIndex == 0 {
				if _, err := manifestLog.AdvanceRetirementLogStart(ctx, cursor.NextManifestSeq, fence); err != nil {
					return stats, fmt.Errorf("sync retirement floor: %w", err)
				}
			}
			return stats, nil
		}

		next := &gcCursor{
			Version:         gcMarkSchemaVersion,
			NextManifestSeq: plan.nextManifestSeq,
			NextObjectIndex: plan.nextObjectIndex,
		}
		if err := storeGCCursorCAS(ctx, storage, next, matchToken, true); err != nil {
			if isGCMarkCASConflict(err) {
				lastErr = err
				continue
			}
			return stats, fmt.Errorf("advance gc cursor: %w", err)
		}

		if next.NextObjectIndex == 0 {
			if _, err := manifestLog.AdvanceRetirementLogStart(ctx, next.NextManifestSeq, fence); err != nil {
				return stats, fmt.Errorf("advance retirement floor: %w", err)
			}
		}
		return stats, nil
	}

	if lastErr != nil {
		return stats, fmt.Errorf("advance gc cursor after retries: %w", lastErr)
	}
	return stats, errors.New("advance gc cursor exceeded retries")
}

func planRetirementSweep(entries []*manifest.ManifestLogEntry, cursor *gcCursor, now time.Time, batchSize int) (retirementSweepPlan, error) {
	plan := retirementSweepPlan{
		keys:            make([]string, 0, batchSize),
		nextManifestSeq: cursor.NextManifestSeq,
		nextObjectIndex: cursor.NextObjectIndex,
	}
	if batchSize <= 0 {
		return plan, nil
	}

	expectedSeq := cursor.NextManifestSeq
	objectIndex := int(cursor.NextObjectIndex)
	for _, entry := range entries {
		if entry == nil || entry.Seq != expectedSeq {
			return retirementSweepPlan{}, fmt.Errorf("%w: expected seq=%d", manifest.ErrRetirementHistory, expectedSeq)
		}
		if objectIndex > len(entry.RetiredObjects) {
			return retirementSweepPlan{}, fmt.Errorf("invalid gc cursor object index=%d count=%d seq=%d", objectIndex, len(entry.RetiredObjects), entry.Seq)
		}

		for objectIndex < len(entry.RetiredObjects) {
			retired := entry.RetiredObjects[objectIndex]
			if now.Before(retired.NotBefore) {
				plan.blockedNotBefore = true
				plan.nextManifestSeq = expectedSeq
				plan.nextObjectIndex = uint32(objectIndex)
				return plan, nil
			}
			plan.keys = append(plan.keys, retired.Key)
			objectIndex++
			if len(plan.keys) >= batchSize {
				if objectIndex == len(entry.RetiredObjects) {
					plan.nextManifestSeq = expectedSeq + 1
					plan.nextObjectIndex = 0
					plan.entriesAdvanced++
				} else {
					plan.nextManifestSeq = expectedSeq
					plan.nextObjectIndex = uint32(objectIndex)
				}
				return plan, nil
			}
		}

		expectedSeq++
		objectIndex = 0
		plan.nextManifestSeq = expectedSeq
		plan.nextObjectIndex = 0
		plan.entriesAdvanced++
	}
	return plan, nil
}
