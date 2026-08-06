package isledb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

type retentionCompactorMode int

const (
	compactByAge retentionCompactorMode = iota

	compactByTimeWindow
)

var errRetentionCompactorClosed = errors.New("retention compactor closed")

type retentionCompactorOptions struct {
	Mode retentionCompactorMode

	RetentionPeriod time.Duration

	KeepAtLeastSSTs int

	KeepAtLeastWindows int

	CheckInterval time.Duration

	SegmentDuration time.Duration

	OnCleanup func(CleanupStats)

	OnCleanupError    func(error)
	GCCursorStorage   manifest.GCCursorStorage
	GCDeleteBatchSize int
	GCGracePeriod     time.Duration
}

type CleanupStats struct {
	SSTsDeleted    int
	BytesReclaimed int64
	Duration       time.Duration
}

func defaultRetentionCompactorOptions() retentionCompactorOptions {
	return retentionCompactorOptions{
		Mode:               compactByAge,
		RetentionPeriod:    7 * 24 * time.Hour,
		KeepAtLeastSSTs:    10,
		KeepAtLeastWindows: 1,
		CheckInterval:      time.Minute,
		SegmentDuration:    time.Hour,
		GCDeleteBatchSize:  defaultSSTSweepBatchSize,
		GCGracePeriod:      defaultSSTSweepGracePeriod,
	}
}

type retentionCompactor struct {
	store         *blobstore.Store
	manifestLog   *manifest.Store
	gcCursorStore manifest.GCCursorStorage
	opts          retentionCompactorOptions
	stageCommand  maintenanceCommandStager

	lifecycleMu sync.Mutex
	mu          sync.Mutex
	manifest    *Manifest

	ticker     *time.Ticker
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	activeRuns sync.WaitGroup
	runGate    chan struct{}

	fenced     atomic.Bool
	fenceToken *manifest.FenceToken
	running    atomic.Bool
	closed     atomic.Bool
}

func newRetentionCompactor(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts retentionCompactorOptions) (*retentionCompactor, error) {
	return newRetentionCompactorWithFence(ctx, store, manifestLog, opts, nil)
}

func newRetentionCompactorWithFence(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts retentionCompactorOptions, fence *manifest.FenceToken) (*retentionCompactor, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	defaults := defaultRetentionCompactorOptions()
	if opts.RetentionPeriod <= 0 {
		opts.RetentionPeriod = defaults.RetentionPeriod
	}
	if opts.KeepAtLeastSSTs == 0 {
		opts.KeepAtLeastSSTs = defaults.KeepAtLeastSSTs
	}
	if opts.KeepAtLeastWindows == 0 {
		opts.KeepAtLeastWindows = defaults.KeepAtLeastWindows
	}
	if opts.CheckInterval <= 0 {
		opts.CheckInterval = defaults.CheckInterval
	}
	if opts.SegmentDuration <= 0 {
		opts.SegmentDuration = defaults.SegmentDuration
	}
	if opts.GCCursorStorage == nil {
		opts.GCCursorStorage = newGCCursorStorage(store)
	}
	if opts.GCDeleteBatchSize <= 0 {
		opts.GCDeleteBatchSize = defaults.GCDeleteBatchSize
	}
	if opts.GCGracePeriod == 0 {
		opts.GCGracePeriod = defaults.GCGracePeriod
	}

	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	if fence == nil {
		ownerID := fmt.Sprintf("retention-compactor-%d-%d", time.Now().UnixNano(), m.NextEpoch)
		token, err := manifestLog.ClaimCompactor(ctx, ownerID)
		if err != nil {
			return nil, fmt.Errorf("claim compactor fence: %w", err)
		}
		fence = token
	}
	token := *fence

	return &retentionCompactor{
		store:         store,
		manifestLog:   manifestLog,
		gcCursorStore: opts.GCCursorStorage,
		opts:          opts,
		manifest:      m,
		runGate:       make(chan struct{}, 1),
		fenceToken:    &token,
	}, nil
}

func (c *retentionCompactor) Start(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.closed.Load() {
		return errRetentionCompactorClosed
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

func (c *retentionCompactor) stopLoop() {
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

func (c *retentionCompactor) Close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if c.closed.CompareAndSwap(false, true) {
		c.stopLoop()
	}
	if err := waitGroupContext(ctx, &c.wg); err != nil {
		return err
	}
	return waitGroupContext(ctx, &c.activeRuns)
}

func (c *retentionCompactor) closeDB() error {
	return c.closeWithTimeout(30 * time.Second)
}

func (c *retentionCompactor) closeWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Close(ctx)
}

func (c *retentionCompactor) refresh(ctx context.Context) error {
	m, err := c.manifestLog.Replay(ctx)
	if err != nil {
		return err
	}
	c.setManifest(m)
	return nil
}

func (c *retentionCompactor) setManifest(m *Manifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m == nil {
		c.manifest = nil
		return
	}
	c.manifest = m.Clone()
}

func (c *retentionCompactor) cleanupLoop(ctx context.Context, ticker *time.Ticker) {
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
				if isFenceError(err) {
					if c.opts.OnCleanupError != nil {
						c.opts.OnCleanupError(err)
					} else {
						slog.Error("isledb: retention compactor fenced, stopping background cleanup", "error", err)
					}
					return
				}
				if errors.Is(err, manifest.ErrFenceConflict) {
					slog.Debug("isledb: retention cleanup skipped after concurrent manifest update")
					continue
				}
				if c.opts.OnCleanupError != nil {
					c.opts.OnCleanupError(err)
				} else {
					slog.Error("isledb: retention cleanup error", "error", err)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *retentionCompactor) RunOnce(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := c.beginRun(ctx); err != nil {
		return err
	}
	defer c.finishRun()

	start := time.Now()
	if c.fenced.Load() {
		return manifest.ErrFenced
	}
	if c.stageCommand == nil {
		if err := c.manifestLog.CheckCompactorFence(ctx); err != nil {
			if isFenceError(err) {
				c.fenced.Store(true)
			}
			return err
		}
	}

	if err := c.refresh(ctx); err != nil {
		return fmt.Errorf("refresh manifest: %w", err)
	}

	c.mu.Lock()
	m := c.manifest.Clone()
	c.mu.Unlock()

	var stats CleanupStats

	switch c.opts.Mode {
	case compactByAge:
		deleted, bytes, err := c.cleanupFIFO(ctx, m)
		if err != nil {
			if isFenceError(err) {
				c.fenced.Store(true)
			}
			return err
		}
		stats.SSTsDeleted = deleted
		stats.BytesReclaimed = bytes

	case compactByTimeWindow:
		deleted, bytes, err := c.cleanupSegmented(ctx, m)
		if err != nil {
			if isFenceError(err) {
				c.fenced.Store(true)
			}
			return err
		}
		stats.SSTsDeleted = deleted
		stats.BytesReclaimed = bytes
	}

	stats.Duration = time.Since(start)

	if c.opts.OnCleanup != nil && stats.SSTsDeleted > 0 {
		c.opts.OnCleanup(stats)
	}

	c.runSSTSweeperBestEffort(ctx)

	return nil
}

func (c *retentionCompactor) beginRun(ctx context.Context) error {
	if c.closed.Load() {
		return errRetentionCompactorClosed
	}
	if err := c.acquireRun(ctx); err != nil {
		return err
	}

	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		c.releaseRun()
		return errRetentionCompactorClosed
	}
	c.activeRuns.Add(1)
	c.lifecycleMu.Unlock()
	return nil
}

func (c *retentionCompactor) finishRun() {
	c.activeRuns.Done()
	c.releaseRun()
}

func (c *retentionCompactor) acquireRun(ctx context.Context) error {
	select {
	case c.runGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *retentionCompactor) releaseRun() {
	select {
	case <-c.runGate:
	default:
	}
}

func (c *retentionCompactor) runSSTSweeperBestEffort(ctx context.Context) {
	if c.stageCommand == nil {
		if err := c.manifestLog.CheckCompactorFence(ctx); err != nil {
			return
		}
	}
	if _, err := runRetirementSweeperWithStager(ctx, c.store, c.manifestLog, c.gcCursorStore, c.fenceToken, c.opts.GCDeleteBatchSize, c.stageCommand); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		slog.Warn("isledb: retention sst sweep failed", "error", err)
	}
}

func (c *retentionCompactor) cleanupFIFO(ctx context.Context, m *Manifest) (int, int64, error) {
	cutoff := time.Now().Add(-c.opts.RetentionPeriod)

	type sstAge struct {
		id        string
		createdAt time.Time
		size      int64
	}

	var allSSTs []sstAge

	for _, sst := range m.L0SSTs {
		allSSTs = append(allSSTs, sstAge{
			id:        sst.ID,
			createdAt: sst.CreatedAt,
			size:      sst.Size,
		})
	}

	for _, level := range m.Levels {
		for _, sst := range level.SSTs {
			allSSTs = append(allSSTs, sstAge{
				id:        sst.ID,
				createdAt: sst.CreatedAt,
				size:      sst.Size,
			})
		}
	}

	sort.Slice(allSSTs, func(i, j int) bool {
		return allSSTs[i].createdAt.Before(allSSTs[j].createdAt)
	})

	keepCount := len(allSSTs)
	if keepCount > c.opts.KeepAtLeastSSTs {
		keepCount = c.opts.KeepAtLeastSSTs
	}

	var toDelete []string
	var bytesReclaimed int64

	for i, sst := range allSSTs {

		remaining := len(allSSTs) - i
		if remaining <= keepCount {
			break
		}

		if sst.createdAt.Before(cutoff) {
			toDelete = append(toDelete, sst.id)
			bytesReclaimed += sst.size
			if len(toDelete) >= manifest.MaxRetiredObjectsPerEntry {
				break
			}
		}
	}

	if len(toDelete) == 0 {
		return 0, 0, nil
	}

	retired, err := retiredSSTObjects(c.store, m, toDelete, c.opts.GCGracePeriod)
	if err != nil {
		return 0, 0, fmt.Errorf("build retirement records: %w", err)
	}
	if c.stageCommand != nil {
		err = c.stageCommand(ctx, manifest.MaintenanceCommand{
			Kind: manifest.MaintenanceCommandRemoveSSTables,
			RemoveSSTables: &manifest.RemoveSSTablesCommand{
				SSTableIDs:     append([]string(nil), toDelete...),
				RetiredObjects: retired,
			},
		})
	} else {
		_, err = c.manifestLog.AppendRemoveSSTablesWithFence(ctx, toDelete, retired)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("update manifest: %w", err)
	}
	updated := m.Clone()
	updated.RemoveSSTables(toDelete)
	c.setManifest(updated)

	return len(toDelete), bytesReclaimed, nil
}

func (c *retentionCompactor) cleanupSegmented(ctx context.Context, m *Manifest) (int, int64, error) {
	cutoff := time.Now().Add(-c.opts.RetentionPeriod)

	type segment struct {
		start time.Time
		ssts  []SSTMeta
		size  int64
	}

	segments := make(map[int64]*segment)

	segmentFor := func(t time.Time) int64 {
		return t.Truncate(c.opts.SegmentDuration).Unix()
	}

	for _, sst := range m.L0SSTs {
		key := segmentFor(sst.CreatedAt)
		if segments[key] == nil {
			segments[key] = &segment{
				start: time.Unix(key, 0),
			}
		}
		segments[key].ssts = append(segments[key].ssts, sst)
		segments[key].size += sst.Size
	}

	for _, level := range m.Levels {
		for _, sst := range level.SSTs {
			key := segmentFor(sst.CreatedAt)
			if segments[key] == nil {
				segments[key] = &segment{
					start: time.Unix(key, 0),
				}
			}
			segments[key].ssts = append(segments[key].ssts, sst)
			segments[key].size += sst.Size
		}
	}

	var sortedSegments []*segment
	for _, seg := range segments {
		sortedSegments = append(sortedSegments, seg)
	}
	sort.Slice(sortedSegments, func(i, j int) bool {
		return sortedSegments[i].start.Before(sortedSegments[j].start)
	})

	minSegments := c.opts.KeepAtLeastWindows

	var toDelete []string
	var bytesReclaimed int64
	deletedSegments := 0

segmentLoop:
	for i, seg := range sortedSegments {

		remaining := len(sortedSegments) - i
		if remaining <= minSegments {
			break
		}

		segmentEnd := seg.start.Add(c.opts.SegmentDuration)
		if segmentEnd.Before(cutoff) {
			for _, sst := range seg.ssts {
				toDelete = append(toDelete, sst.ID)
				bytesReclaimed += sst.Size
				if len(toDelete) >= manifest.MaxRetiredObjectsPerEntry {
					break segmentLoop
				}
			}
			deletedSegments++
		}
	}

	if len(toDelete) == 0 {
		return 0, 0, nil
	}

	retired, err := retiredSSTObjects(c.store, m, toDelete, c.opts.GCGracePeriod)
	if err != nil {
		return 0, 0, fmt.Errorf("build retirement records: %w", err)
	}
	if c.stageCommand != nil {
		err = c.stageCommand(ctx, manifest.MaintenanceCommand{
			Kind: manifest.MaintenanceCommandRemoveSSTables,
			RemoveSSTables: &manifest.RemoveSSTablesCommand{
				SSTableIDs:     append([]string(nil), toDelete...),
				RetiredObjects: retired,
			},
		})
	} else {
		_, err = c.manifestLog.AppendRemoveSSTablesWithFence(ctx, toDelete, retired)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("update manifest: %w", err)
	}
	updated := m.Clone()
	updated.RemoveSSTables(toDelete)
	c.setManifest(updated)

	return len(toDelete), bytesReclaimed, nil
}

func (c *retentionCompactor) Stats() retentionCompactorStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats := retentionCompactorStats{
		Mode:            c.opts.Mode,
		RetentionPeriod: c.opts.RetentionPeriod,
	}

	if c.manifest != nil {
		stats.L0SSTCount = len(c.manifest.L0SSTs)
		stats.LevelCount = len(c.manifest.Levels)

		for _, sst := range c.manifest.L0SSTs {
			stats.TotalSize += sst.Size
		}
		for _, level := range c.manifest.Levels {
			for _, sst := range level.SSTs {
				stats.TotalSize += sst.Size
			}
		}

		var oldest time.Time
		foundOldest := false
		for _, sst := range c.manifest.L0SSTs {
			if !foundOldest || sst.CreatedAt.Before(oldest) {
				oldest = sst.CreatedAt
				foundOldest = true
			}
		}
		for _, level := range c.manifest.Levels {
			for _, sst := range level.SSTs {
				if !foundOldest || sst.CreatedAt.Before(oldest) {
					oldest = sst.CreatedAt
					foundOldest = true
				}
			}
		}
		if foundOldest {
			stats.OldestSST = oldest
		}
	}

	return stats
}

func (c *retentionCompactor) IsFenced() bool {
	return c.fenced.Load()
}

type retentionCompactorStats struct {
	Mode            retentionCompactorMode
	RetentionPeriod time.Duration
	L0SSTCount      int
	LevelCount      int
	TotalSize       int64
	OldestSST       time.Time
}
