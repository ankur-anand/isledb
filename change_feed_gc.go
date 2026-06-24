package isledb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

const (
	pendingChangeBatchDeleteSetObject = "manifest/gc/pending-change-batch/pending.json"
	defaultChangeFeedSweepBatchSize   = 128
	defaultChangeFeedSweepGracePeriod = 10 * time.Minute
)

type pendingChangeBatchDeleteMark struct {
	Version int `json:"version,omitempty"`

	Path string `json:"path"`
	ID   string `json:"id,omitempty"`
	Seq  uint64 `json:"seq"`

	Size     int64  `json:"size,omitempty"`
	Checksum string `json:"checksum,omitempty"`

	FirstSeenUnreferencedAt time.Time `json:"first_seen_unreferenced_at,omitempty"`
	LastSeenUnreferencedAt  time.Time `json:"last_seen_unreferenced_at,omitempty"`
	FirstReason             string    `json:"first_reason,omitempty"`
	LastReason              string    `json:"last_reason,omitempty"`
	DueAt                   time.Time `json:"due_at,omitempty"`
}

type pendingChangeBatchDeleteMarkSet struct {
	Version int                            `json:"version,omitempty"`
	Marks   []pendingChangeBatchDeleteMark `json:"marks,omitempty"`
}

type changeBatchDeleteCandidate struct {
	Path     string
	ID       string
	Seq      uint64
	Size     int64
	Checksum string
}

type changeFeedSweepStats struct {
	Attempted       int
	Deleted         int
	BlockedRetained int
	Failed          int
}

func enqueuePendingChangeBatchDeleteMarks(ctx context.Context, store *blobstore.Store, candidates []changeBatchDeleteCandidate, reason string) error {
	now := time.Now().UTC()
	candidates = uniqueChangeBatchDeleteCandidates(candidates)
	if len(candidates) == 0 {
		return nil
	}

	return withGCMarkCASRetries("store pending change-batch mark set", func() error {
		set, matchToken, exists, err := loadPendingChangeBatchDeleteMarkSetWithCAS(ctx, store)
		if err != nil {
			return err
		}

		byPath := pendingChangeBatchMarkMapFromSet(set)
		if !applyPendingChangeBatchDeleteMarkUpserts(byPath, candidates, reason, now) {
			return nil
		}
		pendingChangeBatchMarkMapToSet(set, byPath)
		return storePendingChangeBatchDeleteMarkSetWithCAS(ctx, store, set, matchToken, exists)
	})
}

func runPendingChangeBatchSweeper(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, batchSize int, gracePeriod time.Duration) (changeFeedSweepStats, error) {
	stats := changeFeedSweepStats{}
	if batchSize <= 0 {
		batchSize = defaultChangeFeedSweepBatchSize
	}
	if gracePeriod < 0 {
		gracePeriod = 0
	}

	current, err := manifestLog.ReadCurrentData(ctx)
	if err != nil {
		return stats, err
	}
	var retainedFloor uint64
	if current != nil {
		retainedFloor = current.ChangeFeedLogStart
	}

	var lastErr error
	for attempt := 0; attempt < gcCASMaxRetries; attempt++ {
		stats = changeFeedSweepStats{}

		set, matchToken, exists, err := loadPendingChangeBatchDeleteMarkSetWithCAS(ctx, store)
		if err != nil {
			return stats, err
		}
		if len(set.Marks) == 0 {
			return stats, nil
		}

		byPath := pendingChangeBatchMarkMapFromSet(set)
		now := time.Now().UTC()
		deletePaths, blocked := planPendingChangeBatchDeletes(byPath, retainedFloor, now, gracePeriod, batchSize)
		stats.BlockedRetained = blocked
		stats.Attempted = len(deletePaths)

		deleted, failed, changed := applyChangeBatchDeleteBatch(ctx, store, byPath, deletePaths)
		stats.Deleted = deleted
		stats.Failed = failed
		if !changed {
			return stats, nil
		}

		pendingChangeBatchMarkMapToSet(set, byPath)
		if err := storePendingChangeBatchDeleteMarkSetWithCAS(ctx, store, set, matchToken, exists); err != nil {
			if isGCMarkCASConflict(err) {
				lastErr = err
				continue
			}
			return stats, fmt.Errorf("store pending change-batch delete marks after sweep: %w", err)
		}
		return stats, nil
	}

	if lastErr != nil {
		return stats, fmt.Errorf("store pending change-batch delete marks after retries: %w", lastErr)
	}
	return stats, fmt.Errorf("store pending change-batch delete marks exceeded retries")
}

func planPendingChangeBatchDeletes(byPath map[string]pendingChangeBatchDeleteMark, retainedFloor uint64, now time.Time, gracePeriod time.Duration, batchSize int) ([]string, int) {
	paths := sortedPendingChangeBatchPaths(byPath, now, gracePeriod)
	deletePaths := make([]string, 0, batchSize)
	blockedRetained := 0
	for _, p := range paths {
		if len(deletePaths) >= batchSize {
			break
		}
		mark, ok := byPath[p]
		if !ok {
			continue
		}
		if mark.Path == "" {
			delete(byPath, p)
			continue
		}
		if mark.Seq >= retainedFloor {
			blockedRetained++
			continue
		}
		if now.Before(pendingChangeBatchDueAt(mark, now, gracePeriod)) {
			break
		}
		deletePaths = append(deletePaths, p)
	}
	return deletePaths, blockedRetained
}

func applyChangeBatchDeleteBatch(ctx context.Context, store *blobstore.Store, byPath map[string]pendingChangeBatchDeleteMark, deletePaths []string) (deleted, failed int, changed bool) {
	if len(deletePaths) == 0 {
		return 0, 0, false
	}

	failedByKey := map[string]error{}
	if err := store.BatchDelete(ctx, deletePaths); err != nil {
		var batchErr *blobstore.BatchDeleteError
		if errors.As(err, &batchErr) {
			failedByKey = batchErr.Failed
		} else {
			for _, key := range deletePaths {
				failedByKey[key] = err
			}
		}
	}

	for _, p := range deletePaths {
		if _, hadFailure := failedByKey[p]; hadFailure {
			failed++
			continue
		}
		delete(byPath, p)
		deleted++
		changed = true
	}
	return deleted, failed, changed
}

func loadPendingChangeBatchDeleteMarks(ctx context.Context, store *blobstore.Store) ([]pendingChangeBatchDeleteMark, error) {
	set, err := loadPendingChangeBatchDeleteMarkSet(ctx, store)
	if err != nil {
		return nil, err
	}
	out := make([]pendingChangeBatchDeleteMark, len(set.Marks))
	copy(out, set.Marks)
	return out, nil
}

func loadPendingChangeBatchDeleteMarkSet(ctx context.Context, store *blobstore.Store) (*pendingChangeBatchDeleteMarkSet, error) {
	set, _, _, err := loadPendingChangeBatchDeleteMarkSetWithCAS(ctx, store)
	return set, err
}

func loadPendingChangeBatchDeleteMarkSetWithCAS(ctx context.Context, store *blobstore.Store) (*pendingChangeBatchDeleteMarkSet, string, bool, error) {
	data, matchToken, exists, err := readObjectWithCAS(ctx, store, pendingChangeBatchDeleteSetPath(store))
	if err != nil {
		return nil, "", false, err
	}
	if !exists {
		return &pendingChangeBatchDeleteMarkSet{Version: gcMarkSchemaVersion}, "", false, nil
	}

	var set pendingChangeBatchDeleteMarkSet
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, "", false, err
	}
	if set.Version == 0 {
		set.Version = gcMarkSchemaVersion
	}
	return &set, matchToken, true, nil
}

func storePendingChangeBatchDeleteMarkSetWithCAS(ctx context.Context, store *blobstore.Store, set *pendingChangeBatchDeleteMarkSet, matchToken string, exists bool) error {
	if set == nil {
		return errors.New("nil pending change-batch delete mark set")
	}
	set.Version = gcMarkSchemaVersion
	payload, err := json.Marshal(set)
	if err != nil {
		return err
	}
	return writeObjectCAS(ctx, store, pendingChangeBatchDeleteSetPath(store), payload, matchToken, exists)
}

func pendingChangeBatchDeleteSetPath(store *blobstore.Store) string {
	return storeKey(store, pendingChangeBatchDeleteSetObject)
}

func pendingChangeBatchMarkMapFromSet(set *pendingChangeBatchDeleteMarkSet) map[string]pendingChangeBatchDeleteMark {
	byPath := make(map[string]pendingChangeBatchDeleteMark)
	if set == nil {
		return byPath
	}
	for _, mark := range set.Marks {
		if mark.Path == "" {
			continue
		}
		byPath[mark.Path] = mark
	}
	return byPath
}

func pendingChangeBatchMarkMapToSet(set *pendingChangeBatchDeleteMarkSet, byPath map[string]pendingChangeBatchDeleteMark) {
	set.Marks = set.Marks[:0]
	for _, path := range sortedChangeBatchPaths(byPath) {
		mark := byPath[path]
		mark.Version = gcMarkSchemaVersion
		set.Marks = append(set.Marks, mark)
	}
}

func applyPendingChangeBatchDeleteMarkUpserts(byPath map[string]pendingChangeBatchDeleteMark, candidates []changeBatchDeleteCandidate, reason string, now time.Time) bool {
	changed := false
	for _, candidate := range candidates {
		if candidate.Path == "" {
			continue
		}
		mark, exists := byPath[candidate.Path]
		if !exists {
			mark = pendingChangeBatchDeleteMark{
				Version:                 gcMarkSchemaVersion,
				Path:                    candidate.Path,
				ID:                      candidate.ID,
				Seq:                     candidate.Seq,
				Size:                    candidate.Size,
				Checksum:                candidate.Checksum,
				FirstSeenUnreferencedAt: now,
				FirstReason:             reason,
			}
			changed = true
		}
		if candidate.Seq > mark.Seq {
			mark.Seq = candidate.Seq
			changed = true
		}
		if mark.ID == "" && candidate.ID != "" {
			mark.ID = candidate.ID
			changed = true
		}
		if mark.Size == 0 && candidate.Size != 0 {
			mark.Size = candidate.Size
			changed = true
		}
		if mark.Checksum == "" && candidate.Checksum != "" {
			mark.Checksum = candidate.Checksum
			changed = true
		}
		mark.LastSeenUnreferencedAt = now
		mark.LastReason = reason
		byPath[candidate.Path] = mark
	}
	return changed
}

func uniqueChangeBatchDeleteCandidates(candidates []changeBatchDeleteCandidate) []changeBatchDeleteCandidate {
	if len(candidates) == 0 {
		return nil
	}
	byPath := make(map[string]changeBatchDeleteCandidate, len(candidates))
	for _, candidate := range candidates {
		if candidate.Path == "" {
			continue
		}
		if existing, ok := byPath[candidate.Path]; ok && existing.Seq >= candidate.Seq {
			continue
		}
		byPath[candidate.Path] = candidate
	}
	out := make([]changeBatchDeleteCandidate, 0, len(byPath))
	for _, path := range sortedChangeBatchCandidates(byPath) {
		out = append(out, byPath[path])
	}
	return out
}

func sortedPendingChangeBatchPaths(byPath map[string]pendingChangeBatchDeleteMark, now time.Time, gracePeriod time.Duration) []string {
	paths := sortedChangeBatchPaths(byPath)
	sort.Slice(paths, func(i, j int) bool {
		li := pendingChangeBatchDueAt(byPath[paths[i]], now, gracePeriod)
		lj := pendingChangeBatchDueAt(byPath[paths[j]], now, gracePeriod)
		if li.Equal(lj) {
			return paths[i] < paths[j]
		}
		return li.Before(lj)
	})
	return paths
}

func sortedChangeBatchPaths(byPath map[string]pendingChangeBatchDeleteMark) []string {
	paths := make([]string, 0, len(byPath))
	for p := range byPath {
		if p != "" {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	return paths
}

func sortedChangeBatchCandidates(byPath map[string]changeBatchDeleteCandidate) []string {
	paths := make([]string, 0, len(byPath))
	for p := range byPath {
		if p != "" {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	return paths
}

func pendingChangeBatchDueAt(mark pendingChangeBatchDeleteMark, now time.Time, gracePeriod time.Duration) time.Time {
	if !mark.DueAt.IsZero() {
		return mark.DueAt
	}
	if !mark.FirstSeenUnreferencedAt.IsZero() {
		return mark.FirstSeenUnreferencedAt.Add(gracePeriod)
	}
	if !mark.LastSeenUnreferencedAt.IsZero() {
		return mark.LastSeenUnreferencedAt.Add(gracePeriod)
	}
	return now
}
