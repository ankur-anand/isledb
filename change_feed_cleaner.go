package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	changeFeedCleanerScanBatchSize     = 1024
	changeFeedCleanerMaxEntriesPerPass = 1024
)

type changeFeedCleanerOptions struct {
	// RetentionPeriod is the minimum age retained for change-feed entries.
	// Entries with change batches older than this can be retired, subject to
	// KeepAtLeastManifestEntries. Zero uses the default.
	RetentionPeriod time.Duration

	// KeepAtLeastManifestEntries is the minimum number of newest manifest
	// entries retained for change-feed readers. Zero uses the default.
	KeepAtLeastManifestEntries uint64

	// SweepBatchSize bounds both the targets placed in one immutable deletion
	// plan and normal physical deletes in one reclaim pass.
	SweepBatchSize int

	// SweepGracePeriod is persisted in each plan as its physical deletion
	// delay. The pinned-view deadline starts only after CURRENT reaches the
	// plan's target floor.
	SweepGracePeriod time.Duration

	// DeletionSafetyMargin is added after CURRENT.MaxPinnedViewAge. A negative
	// value disables the margin in tests; zero selects the production default.
	DeletionSafetyMargin time.Duration

	OnCleanup func(ChangeFeedCleanupStats)
}

func defaultChangeFeedCleanerOptions() changeFeedCleanerOptions {
	return changeFeedCleanerOptions{
		RetentionPeriod:            7 * 24 * time.Hour,
		KeepAtLeastManifestEntries: 1024,
		SweepBatchSize:             defaultChangeFeedSweepBatchSize,
		SweepGracePeriod:           defaultChangeFeedSweepGracePeriod,
		DeletionSafetyMargin:       defaultChangeFeedDeletionSafetyMargin,
	}
}

type changeFeedCleaner struct {
	store        *blobstore.Store
	manifestLog  *manifest.Store
	opts         changeFeedCleanerOptions
	fenceToken   *manifest.FenceToken
	stageCommand maintenanceCommandStager

	reclaimMu sync.Mutex
	planIter  *blobstore.ListIterator
	planCache map[string]changeFeedDeletionPlan

	closed atomic.Bool
}

func newChangeFeedCleaner(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts changeFeedCleanerOptions) (*changeFeedCleaner, error) {
	return newChangeFeedCleanerWithFence(ctx, store, manifestLog, opts, nil)
}

func newChangeFeedCleanerWithFence(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts changeFeedCleanerOptions, fence *manifest.FenceToken) (*changeFeedCleaner, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	defaults := defaultChangeFeedCleanerOptions()
	if opts.RetentionPeriod <= 0 {
		opts.RetentionPeriod = defaults.RetentionPeriod
	}
	if opts.KeepAtLeastManifestEntries == 0 {
		opts.KeepAtLeastManifestEntries = defaults.KeepAtLeastManifestEntries
	}
	if opts.SweepBatchSize <= 0 {
		opts.SweepBatchSize = defaults.SweepBatchSize
	}
	if opts.SweepBatchSize > manifest.MaxChangeFeedDeleteTargetsPerCommand {
		opts.SweepBatchSize = manifest.MaxChangeFeedDeleteTargetsPerCommand
	}
	if opts.SweepGracePeriod == 0 {
		opts.SweepGracePeriod = defaults.SweepGracePeriod
	}
	if opts.DeletionSafetyMargin < 0 {
		opts.DeletionSafetyMargin = 0
	} else if opts.DeletionSafetyMargin == 0 {
		opts.DeletionSafetyMargin = defaults.DeletionSafetyMargin
	}
	if fence == nil {
		ownerID := fmt.Sprintf("change-feed-cleaner-%d", time.Now().UnixNano())
		token, err := manifestLog.ClaimCompactor(ctx, ownerID)
		if err != nil {
			return nil, fmt.Errorf("claim compactor fence: %w", err)
		}
		fence = token
	}
	token := *fence
	return &changeFeedCleaner{
		store: store, manifestLog: manifestLog, opts: opts, fenceToken: &token,
		planCache: make(map[string]changeFeedDeletionPlan),
	}, nil
}

func (c *changeFeedCleaner) Close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	c.closed.Store(true)
	return nil
}

func (c *changeFeedCleaner) closeDB() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.Close(ctx)
}

func (c *changeFeedCleaner) RunOnce(ctx context.Context) error {
	control, err := c.runControlOnce(ctx)
	if err != nil {
		return err
	}
	reclaim, err := c.runReclaimOnce(ctx)
	stats := mergeChangeFeedCleanupStats(control, reclaim)
	if c.opts.OnCleanup != nil && hasChangeFeedCleanupWork(stats) {
		c.opts.OnCleanup(stats)
	}
	return err
}

// runControlOnce performs only logical retention work: it discovers retired
// batches and stages the exact targets with the new feed floor. A matching
// writer receipt turns that command into one reclaimable plan. This method
// never waits for physical object deletion.
func (c *changeFeedCleaner) runControlOnce(ctx context.Context) (stats ChangeFeedCleanupStats, err error) {
	start := time.Now()
	defer func() { stats.Duration = time.Since(start) }()
	if err := checkContext(ctx); err != nil {
		return stats, err
	}
	if c.closed.Load() {
		return stats, errors.New("change feed cleaner closed")
	}
	if c.stageCommand == nil {
		if err := c.manifestLog.CheckCompactorFenceToken(ctx, c.fenceToken); err != nil {
			return stats, err
		}
	}

	view, err := c.manifestLog.LoadChangeFeedView(ctx)
	if err != nil {
		return stats, fmt.Errorf("load change-feed view: %w", err)
	}
	if view.Head() <= view.RetainedFrom() {
		return stats, nil
	}

	now := time.Now().UTC()
	floor, candidates, err := c.planRetentionFloor(ctx, view, now)
	if err != nil {
		return stats, err
	}

	var entriesRetired int
	if floor > view.RetainedFrom() {
		var err error
		if c.stageCommand != nil {
			var gracePeriod time.Duration
			if len(candidates) > 0 {
				gracePeriod = max(c.opts.SweepGracePeriod, 0)
			}
			err = c.stageCommand(ctx, manifest.MaintenanceCommand{
				Kind: manifest.MaintenanceCommandChangeFeedFloor,
				ChangeFeedFloor: &manifest.AdvanceFloorCommand{
					Floor:           floor,
					GracePeriod:     gracePeriod,
					DeletionTargets: changeFeedDeleteTargetsForManifest(candidates),
				},
			})
		} else {
			var updated *manifest.Current
			updated, err = c.manifestLog.AdvanceChangeFeedLogStart(ctx, floor, c.fenceToken)
			if err == nil && len(candidates) > 0 {
				observedAt := time.Now().UTC()
				if updated == nil || updated.ChangeFeedLogStart < floor {
					err = errors.New("change-feed floor publication was not observable")
				} else {
					var plan *changeFeedDeletionPlan
					var payload []byte
					plan, payload, err = buildChangeFeedDeletionPlan(
						c.store, candidates, floor, now, c.opts.SweepGracePeriod,
						observedAt, updated.PinnedViewAge(), c.opts.DeletionSafetyMargin)
					if err == nil {
						_, err = storeChangeFeedDeletionPlan(ctx, c.store, *plan, payload)
					}
				}
			}
		}
		if err != nil {
			return stats, fmt.Errorf("advance change-feed floor: %w", err)
		}
		entriesRetired = int(floor - view.RetainedFrom())
	}
	stats = ChangeFeedCleanupStats{
		EntriesRetired: entriesRetired,
		BatchesPlanned: len(candidates),
	}
	return stats, nil
}

// runReclaimOnce performs only bounded physical deletion from durable plans.
// It is intentionally fence-independent: CURRENT remains the authority that
// proves whether every planned change batch is below the committed feed floor.
func (c *changeFeedCleaner) runReclaimOnce(ctx context.Context, deleter ...objectDeleter) (stats ChangeFeedCleanupStats, err error) {
	start := time.Now()
	defer func() { stats.Duration = time.Since(start) }()
	if err := checkContext(ctx); err != nil {
		return stats, err
	}
	if c.closed.Load() {
		return stats, errors.New("change feed cleaner closed")
	}
	deleteObjects := objectDeleter(c.store)
	if len(deleter) > 0 && deleter[0] != nil {
		deleteObjects = deleter[0]
	}
	c.reclaimMu.Lock()
	defer c.reclaimMu.Unlock()
	if c.planIter == nil {
		c.planIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: changeFeedDeletionPlanPrefix + "/"})
	}
	sweep, exhausted, sweepErr := runChangeFeedDeletionPlanReclaimer(
		ctx, c.store, c.manifestLog, c.opts.SweepBatchSize,
		defaultChangeFeedDeletionPlanScanLimit, time.Now().UTC(), deleteObjects, c.planIter, c.planCache)
	if exhausted || sweepErr != nil {
		c.planIter = nil
	}
	stats.BatchesDeleted = sweep.Deleted
	stats.BlockedRetained = sweep.BlockedRetained
	stats.FailedDeletes = sweep.Failed
	if sweepErr != nil {
		return stats, fmt.Errorf("reclaim change-feed deletion plans: %w", sweepErr)
	}
	return stats, nil
}

func mergeChangeFeedCleanupStats(a, b ChangeFeedCleanupStats) ChangeFeedCleanupStats {
	return ChangeFeedCleanupStats{
		EntriesRetired:  a.EntriesRetired + b.EntriesRetired,
		BatchesPlanned:  a.BatchesPlanned + b.BatchesPlanned,
		BatchesDeleted:  a.BatchesDeleted + b.BatchesDeleted,
		BlockedRetained: a.BlockedRetained + b.BlockedRetained,
		FailedDeletes:   a.FailedDeletes + b.FailedDeletes,
		Duration:        a.Duration + b.Duration,
	}
}

func hasChangeFeedCleanupWork(stats ChangeFeedCleanupStats) bool {
	return stats.EntriesRetired > 0 || stats.BatchesPlanned > 0 || stats.BatchesDeleted > 0 || stats.FailedDeletes > 0
}

func (c *changeFeedCleaner) planRetentionFloor(ctx context.Context, view *manifest.ChangeFeedView, now time.Time) (uint64, []changeBatchDeleteCandidate, error) {
	if view == nil || view.Head() <= view.RetainedFrom() {
		if view == nil {
			return 0, nil, nil
		}
		return view.Head(), nil, nil
	}

	start := view.RetainedFrom()
	end := view.Head()
	maxFloor := end
	if c.opts.KeepAtLeastManifestEntries > 0 && end-start > c.opts.KeepAtLeastManifestEntries {
		maxFloor = end - c.opts.KeepAtLeastManifestEntries
	} else if c.opts.KeepAtLeastManifestEntries > 0 {
		maxFloor = start
	}

	cutoff := now.Add(-c.opts.RetentionPeriod)
	floor := start
	candidates := make([]changeBatchDeleteCandidate, 0)
	entriesScanned := 0
	for floor < maxFloor && entriesScanned < changeFeedCleanerMaxEntriesPerPass {
		limit := changeFeedCleanerScanBatchSize
		if remaining := maxFloor - floor; remaining < uint64(limit) {
			limit = int(remaining)
		}
		if remaining := changeFeedCleanerMaxEntriesPerPass - entriesScanned; limit > remaining {
			limit = remaining
		}
		entries, err := c.manifestLog.ReadChangeEntriesFromView(ctx, view, floor, false, limit)
		if err != nil {
			return floor, nil, fmt.Errorf("read manifest entries from seq=%d: %w", floor, err)
		}
		if len(entries) == 0 {
			return floor, nil, fmt.Errorf("manifest scan made no progress at seq=%d", floor)
		}
		for _, entry := range entries {
			entriesScanned++
			if entry == nil {
				return floor, nil, fmt.Errorf("manifest scan returned nil entry at seq=%d", floor)
			}
			if entry.ChangeBatch != nil {
				createdAt := entry.ChangeBatch.CreatedAt
				if createdAt.IsZero() {
					createdAt = entry.Timestamp
				}
				if !createdAt.IsZero() && !createdAt.Before(cutoff) {
					return floor, candidates, nil
				}
				candidates = append(candidates, changeBatchDeleteCandidate{
					Path:     entry.ChangeBatch.Path,
					ID:       entry.ChangeBatch.ID,
					Seq:      entry.Seq,
					Size:     entry.ChangeBatch.Size,
					Checksum: entry.ChangeBatch.Checksum,
				})
			}
			floor = entry.Seq + 1
			if len(candidates) >= c.opts.SweepBatchSize || entriesScanned >= changeFeedCleanerMaxEntriesPerPass {
				return floor, candidates, nil
			}
		}
	}

	return floor, candidates, nil
}
