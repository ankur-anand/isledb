package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const changeFeedCleanerScanBatchSize = 1024

type changeFeedCleanerOptions struct {
	// RetentionPeriod is the minimum age retained for change-feed entries.
	// Entries with change batches older than this can be retired, subject to
	// KeepAtLeastManifestEntries. Zero uses the default.
	RetentionPeriod time.Duration

	// KeepAtLeastManifestEntries is the minimum number of newest manifest
	// entries retained for change-feed readers. Zero uses the default.
	KeepAtLeastManifestEntries uint64

	// SweepBatchSize limits physical change-batch deletes per RunOnce.
	SweepBatchSize int

	// SweepGracePeriod is the delay between first marking a change batch and
	// physically deleting it. Physical delete still requires CURRENT to have
	// advanced beyond the marked manifest seq.
	SweepGracePeriod time.Duration

	OnCleanup func(ChangeFeedCleanupStats)
}

func defaultChangeFeedCleanerOptions() changeFeedCleanerOptions {
	return changeFeedCleanerOptions{
		RetentionPeriod:            7 * 24 * time.Hour,
		KeepAtLeastManifestEntries: 1024,
		SweepBatchSize:             defaultChangeFeedSweepBatchSize,
		SweepGracePeriod:           defaultChangeFeedSweepGracePeriod,
	}
}

type changeFeedCleaner struct {
	store        *blobstore.Store
	manifestLog  *manifest.Store
	opts         changeFeedCleanerOptions
	fenceToken   *manifest.FenceToken
	stageCommand maintenanceCommandStager

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
	if opts.SweepGracePeriod == 0 {
		opts.SweepGracePeriod = defaults.SweepGracePeriod
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
	return &changeFeedCleaner{store: store, manifestLog: manifestLog, opts: opts, fenceToken: &token}, nil
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
	if err := checkContext(ctx); err != nil {
		return err
	}
	if c.closed.Load() {
		return errors.New("change feed cleaner closed")
	}
	if c.stageCommand == nil {
		if err := c.manifestLog.CheckCompactorFenceToken(ctx, c.fenceToken); err != nil {
			return err
		}
	}
	start := time.Now()

	view, err := c.manifestLog.LoadChangeFeedView(ctx)
	if err != nil {
		return fmt.Errorf("load change-feed view: %w", err)
	}
	if view.Head() <= view.RetainedFrom() {
		stats, err := runPendingChangeBatchSweeper(ctx, c.store, c.manifestLog, c.opts.SweepBatchSize, c.opts.SweepGracePeriod)
		if err != nil {
			return err
		}
		if c.opts.OnCleanup != nil && stats.Deleted > 0 {
			c.opts.OnCleanup(ChangeFeedCleanupStats{BatchesDeleted: stats.Deleted, BlockedRetained: stats.BlockedRetained, FailedDeletes: stats.Failed, Duration: time.Since(start)})
		}
		return nil
	}

	now := time.Now().UTC()
	floor, candidates, err := c.planRetentionFloor(ctx, view, now)
	if err != nil {
		return err
	}

	if len(candidates) > 0 {
		if err := enqueuePendingChangeBatchDeleteMarks(ctx, c.store, candidates, "change_feed_retention"); err != nil {
			return fmt.Errorf("enqueue change-batch delete marks: %w", err)
		}
	}

	var entriesRetired int
	if floor > view.RetainedFrom() {
		var err error
		if c.stageCommand != nil {
			err = c.stageCommand(ctx, manifest.MaintenanceCommand{
				Kind:            manifest.MaintenanceCommandChangeFeedFloor,
				ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: floor},
			})
		} else {
			_, err = c.manifestLog.AdvanceChangeFeedLogStart(ctx, floor, c.fenceToken)
		}
		if err != nil {
			return fmt.Errorf("advance change-feed floor: %w", err)
		}
		entriesRetired = int(floor - view.RetainedFrom())
	}

	sweepStats, err := runPendingChangeBatchSweeper(ctx, c.store, c.manifestLog, c.opts.SweepBatchSize, c.opts.SweepGracePeriod)
	if err != nil {
		return fmt.Errorf("sweep pending change batches: %w", err)
	}

	stats := ChangeFeedCleanupStats{
		EntriesRetired:  entriesRetired,
		BatchesMarked:   len(candidates),
		BatchesDeleted:  sweepStats.Deleted,
		BlockedRetained: sweepStats.BlockedRetained,
		FailedDeletes:   sweepStats.Failed,
		Duration:        time.Since(start),
	}
	if c.opts.OnCleanup != nil && (stats.EntriesRetired > 0 || stats.BatchesMarked > 0 || stats.BatchesDeleted > 0 || stats.FailedDeletes > 0) {
		c.opts.OnCleanup(stats)
	}
	return nil
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
	for floor < maxFloor {
		limit := changeFeedCleanerScanBatchSize
		if remaining := maxFloor - floor; remaining < uint64(limit) {
			limit = int(remaining)
		}
		entries, err := c.manifestLog.ReadChangeEntriesFromView(ctx, view, floor, false, limit)
		if err != nil {
			return floor, nil, fmt.Errorf("read manifest entries from seq=%d: %w", floor, err)
		}
		if len(entries) == 0 {
			return floor, nil, fmt.Errorf("manifest scan made no progress at seq=%d", floor)
		}
		for _, entry := range entries {
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
		}
	}

	return floor, candidates, nil
}
