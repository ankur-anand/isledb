package isledb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

type ChangeFeedCleanerOptions struct {
	// RetentionPeriod is the minimum age retained for change-feed entries.
	// Entries with change batches older than this can be retired, subject to
	// RetentionCount. Zero uses the default.
	RetentionPeriod time.Duration

	// RetentionCount is the minimum number of newest manifest entries retained
	// for change-feed readers. Zero uses the default.
	RetentionCount uint64

	// CheckInterval is the background cleaner cadence used by Start.
	CheckInterval time.Duration

	// SweepBatchSize limits physical change-batch deletes per RunOnce.
	SweepBatchSize int

	// SweepGracePeriod is the delay between first marking a change batch and
	// physically deleting it. Physical delete still requires CURRENT to have
	// advanced beyond the marked manifest seq.
	SweepGracePeriod time.Duration

	OnCleanup      func(ChangeFeedCleanupStats)
	OnCleanupError func(error)
}

type ChangeFeedCleanupStats struct {
	EntriesRetired  int
	BatchesMarked   int
	BatchesDeleted  int
	BlockedRetained int
	FailedDeletes   int
	Duration        time.Duration
}

func DefaultChangeFeedCleanerOptions() ChangeFeedCleanerOptions {
	return ChangeFeedCleanerOptions{
		RetentionPeriod:  7 * 24 * time.Hour,
		RetentionCount:   1024,
		CheckInterval:    time.Minute,
		SweepBatchSize:   defaultChangeFeedSweepBatchSize,
		SweepGracePeriod: defaultChangeFeedSweepGracePeriod,
	}
}

type ChangeFeedCleaner struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        ChangeFeedCleanerOptions

	lifecycleMu sync.Mutex
	ticker      *time.Ticker
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	running atomic.Bool
	closed  atomic.Bool
}

func newChangeFeedCleaner(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts ChangeFeedCleanerOptions) (*ChangeFeedCleaner, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	defaults := DefaultChangeFeedCleanerOptions()
	if opts.RetentionPeriod <= 0 {
		opts.RetentionPeriod = defaults.RetentionPeriod
	}
	if opts.RetentionCount == 0 {
		opts.RetentionCount = defaults.RetentionCount
	}
	if opts.CheckInterval <= 0 {
		opts.CheckInterval = defaults.CheckInterval
	}
	if opts.SweepBatchSize <= 0 {
		opts.SweepBatchSize = defaults.SweepBatchSize
	}
	if opts.SweepGracePeriod == 0 {
		opts.SweepGracePeriod = defaults.SweepGracePeriod
	}
	return &ChangeFeedCleaner{store: store, manifestLog: manifestLog, opts: opts}, nil
}

func (c *ChangeFeedCleaner) Start(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.closed.Load() {
		return errors.New("change feed cleaner closed")
	}
	if !c.running.CompareAndSwap(false, true) {
		return nil
	}

	loopCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.ticker = time.NewTicker(c.opts.CheckInterval)
	c.wg.Add(1)
	go c.cleanupLoop(loopCtx, c.ticker)
	return nil
}

func (c *ChangeFeedCleaner) stopLoop() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	if c.ticker != nil {
		c.ticker.Stop()
		c.ticker = nil
	}
	c.running.Store(false)
}

func (c *ChangeFeedCleaner) Close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if c.closed.CompareAndSwap(false, true) {
		c.stopLoop()
	}
	return waitGroupContext(ctx, &c.wg)
}

func (c *ChangeFeedCleaner) closeDB() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.Close(ctx)
}

func (c *ChangeFeedCleaner) cleanupLoop(ctx context.Context, ticker *time.Ticker) {
	defer c.wg.Done()
	defer func() {
		ticker.Stop()
		c.lifecycleMu.Lock()
		if c.ticker == ticker {
			c.ticker = nil
			c.cancel = nil
		}
		c.lifecycleMu.Unlock()
		c.running.Store(false)
	}()

	for {
		select {
		case <-ticker.C:
			if err := c.RunOnce(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if errors.Is(err, manifest.ErrFenceConflict) {
					slog.Debug("isledb: change-feed cleanup skipped after concurrent manifest update")
					continue
				}
				if c.opts.OnCleanupError != nil {
					c.opts.OnCleanupError(err)
				} else {
					slog.Error("isledb: change-feed cleanup error", "error", err)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *ChangeFeedCleaner) RunOnce(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	start := time.Now()

	current, err := c.manifestLog.ReadCurrentData(ctx)
	if err != nil {
		return fmt.Errorf("read current: %w", err)
	}
	if current == nil || current.NextSeq <= current.ChangeFeedLogStart {
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
	floor, candidates, err := c.planRetentionFloor(ctx, current, now)
	if err != nil {
		return err
	}

	if len(candidates) > 0 {
		if err := enqueuePendingChangeBatchDeleteMarks(ctx, c.store, candidates, "change_feed_retention"); err != nil {
			return fmt.Errorf("enqueue change-batch delete marks: %w", err)
		}
	}

	var entriesRetired int
	if floor > current.ChangeFeedLogStart {
		if _, err := c.manifestLog.AdvanceChangeFeedLogStart(ctx, floor); err != nil {
			return fmt.Errorf("advance change-feed floor: %w", err)
		}
		entriesRetired = int(floor - current.ChangeFeedLogStart)
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

func (c *ChangeFeedCleaner) planRetentionFloor(ctx context.Context, current *manifest.Current, now time.Time) (uint64, []changeBatchDeleteCandidate, error) {
	if current == nil || current.NextSeq <= current.ChangeFeedLogStart {
		return currentNextSeqForCleaner(current), nil, nil
	}

	start := current.ChangeFeedLogStart
	end := current.NextSeq
	maxFloor := end
	if c.opts.RetentionCount > 0 && end-start > c.opts.RetentionCount {
		maxFloor = end - c.opts.RetentionCount
	} else if c.opts.RetentionCount > 0 {
		maxFloor = start
	}

	cutoff := now.Add(-c.opts.RetentionPeriod)
	floor := start
	candidates := make([]changeBatchDeleteCandidate, 0)
	for seq := start; seq < maxFloor; seq++ {
		entry, err := c.manifestLog.ReadEntry(ctx, seq)
		if err != nil {
			return floor, nil, fmt.Errorf("read manifest entry seq=%d: %w", seq, err)
		}
		if entry == nil {
			return floor, nil, fmt.Errorf("manifest entry seq=%d not found", seq)
		}
		if entry.ChangeBatch != nil {
			createdAt := entry.ChangeBatch.CreatedAt
			if createdAt.IsZero() {
				createdAt = entry.Timestamp
			}
			if !createdAt.IsZero() && !createdAt.Before(cutoff) {
				break
			}
			candidates = append(candidates, changeBatchDeleteCandidate{
				Path:     entry.ChangeBatch.Path,
				ID:       entry.ChangeBatch.ID,
				Seq:      entry.Seq,
				Size:     entry.ChangeBatch.Size,
				Checksum: entry.ChangeBatch.Checksum,
			})
		}
		floor = seq + 1
	}

	return floor, candidates, nil
}

func currentNextSeqForCleaner(current *manifest.Current) uint64 {
	if current == nil {
		return 0
	}
	return current.NextSeq
}
