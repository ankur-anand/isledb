package isledb

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

type RetentionCompactorMode int

const (
	CompactByAge RetentionCompactorMode = iota

	CompactByTimeWindow
)

type RetentionCompactorOptions struct {
	Mode RetentionCompactorMode

	RetentionPeriod time.Duration

	RetentionCount int

	CheckInterval time.Duration

	SegmentDuration time.Duration

	OnCleanup func(CleanupStats)

	OnCleanupError func(error)
}

type CleanupStats struct {
	SSTsDeleted    int
	BytesReclaimed int64
	Duration       time.Duration
}

func DefaultRetentionCompactorOptions() RetentionCompactorOptions {
	return RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: 7 * 24 * time.Hour,
		RetentionCount:  10,
		CheckInterval:   time.Minute,
		SegmentDuration: time.Hour,
	}
}

type RetentionCompactor struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        RetentionCompactorOptions

	startStopMu sync.Mutex
	mu          sync.Mutex
	manifest    *Manifest

	ticker *time.Ticker
	stopCh chan struct{}
	wg     sync.WaitGroup

	fenced  atomic.Bool
	running atomic.Bool
	closed  atomic.Bool
}

func newRetentionCompactor(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts RetentionCompactorOptions) (*RetentionCompactor, error) {

	defaults := DefaultRetentionCompactorOptions()
	if opts.RetentionPeriod <= 0 {
		opts.RetentionPeriod = defaults.RetentionPeriod
	}
	if opts.RetentionCount == 0 {
		opts.RetentionCount = defaults.RetentionCount
	}
	if opts.CheckInterval <= 0 {
		opts.CheckInterval = defaults.CheckInterval
	}
	if opts.SegmentDuration <= 0 {
		opts.SegmentDuration = defaults.SegmentDuration
	}

	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	ownerID := fmt.Sprintf("retention-compactor-%d-%d", time.Now().UnixNano(), m.NextEpoch)
	if _, err := manifestLog.ClaimCompactor(ctx, ownerID); err != nil {
		return nil, fmt.Errorf("claim compactor fence: %w", err)
	}

	return &RetentionCompactor{
		store:       store,
		manifestLog: manifestLog,
		opts:        opts,
		manifest:    m,
		stopCh:      make(chan struct{}),
	}, nil
}

func (c *RetentionCompactor) Start() {
	c.startStopMu.Lock()
	defer c.startStopMu.Unlock()

	if c.running.Swap(true) {
		return
	}

	c.stopCh = make(chan struct{})
	c.ticker = time.NewTicker(c.opts.CheckInterval)
	c.wg.Add(1)
	stopCh := c.stopCh
	ticks := c.ticker.C
	go c.cleanupLoop(stopCh, ticks)
}

func (c *RetentionCompactor) Stop() {
	c.startStopMu.Lock()
	defer c.startStopMu.Unlock()

	if !c.running.Swap(false) {
		return
	}

	if c.stopCh != nil {
		close(c.stopCh)
		c.stopCh = nil
	}
	if c.ticker != nil {
		c.ticker.Stop()
		c.ticker = nil
	}
	c.wg.Wait()
}

func (c *RetentionCompactor) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.Stop()
	return nil
}

func (c *RetentionCompactor) Refresh(ctx context.Context) error {
	m, err := c.manifestLog.Replay(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.manifest = m
	c.mu.Unlock()
	return nil
}

func (c *RetentionCompactor) cleanupLoop(stopCh <-chan struct{}, ticks <-chan time.Time) {
	defer c.wg.Done()

	for {
		select {
		case <-ticks:
			if err := c.RunCleanup(context.Background()); err != nil {
				if isFenceError(err) {
					if c.opts.OnCleanupError != nil {
						c.opts.OnCleanupError(err)
					} else {
						slog.Error("isledb: retention compactor fenced, stopping background cleanup", "error", err)
					}
					return
				}
				if c.opts.OnCleanupError != nil {
					c.opts.OnCleanupError(err)
				} else {
					slog.Error("isledb: retention cleanup error", "error", err)
				}
			}
		case <-stopCh:
			return
		}
	}
}

func (c *RetentionCompactor) RunCleanup(ctx context.Context) error {
	start := time.Now()
	if c.fenced.Load() {
		return manifest.ErrFenced
	}
	if err := c.manifestLog.CheckCompactorFence(ctx); err != nil {
		if isFenceError(err) {
			c.fenced.Store(true)
		}
		return err
	}

	if err := c.Refresh(ctx); err != nil {
		return fmt.Errorf("refresh manifest: %w", err)
	}

	c.mu.Lock()
	m := c.manifest.Clone()
	c.mu.Unlock()

	var stats CleanupStats

	switch c.opts.Mode {
	case CompactByAge:
		deleted, bytes, err := c.cleanupFIFO(ctx, m)
		if err != nil {
			if isFenceError(err) {
				c.fenced.Store(true)
			}
			return err
		}
		stats.SSTsDeleted = deleted
		stats.BytesReclaimed = bytes

	case CompactByTimeWindow:
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

	return nil
}

func (c *RetentionCompactor) cleanupFIFO(ctx context.Context, m *Manifest) (int, int64, error) {
	cutoff := time.Now().Add(-c.opts.RetentionPeriod)

	type sstAge struct {
		id        string
		createdAt time.Time
		size      int64
		isL0      bool
	}

	var allSSTs []sstAge

	for _, sst := range m.L0SSTs {
		allSSTs = append(allSSTs, sstAge{
			id:        sst.ID,
			createdAt: sst.CreatedAt,
			size:      sst.Size,
			isL0:      true,
		})
	}

	for _, sr := range m.SortedRuns {
		for _, sst := range sr.SSTs {
			allSSTs = append(allSSTs, sstAge{
				id:        sst.ID,
				createdAt: sst.CreatedAt,
				size:      sst.Size,
				isL0:      false,
			})
		}
	}

	sort.Slice(allSSTs, func(i, j int) bool {
		return allSSTs[i].createdAt.Before(allSSTs[j].createdAt)
	})

	keepCount := len(allSSTs)
	if keepCount > c.opts.RetentionCount {
		keepCount = c.opts.RetentionCount
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
		}
	}

	if len(toDelete) == 0 {
		return 0, 0, nil
	}

	_, err := c.manifestLog.AppendRemoveSSTablesWithFence(ctx, toDelete)
	if err != nil {
		return 0, 0, fmt.Errorf("update manifest: %w", err)
	}

	for _, id := range toDelete {
		path := c.store.SSTPath(id)
		if err := c.store.Delete(ctx, path); err != nil {

			slog.Error("isledb: failed to delete SST", "sst_id", id, "error", err)
		}
	}

	return len(toDelete), bytesReclaimed, nil
}

func (c *RetentionCompactor) cleanupSegmented(ctx context.Context, m *Manifest) (int, int64, error) {
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

	for _, sr := range m.SortedRuns {
		for _, sst := range sr.SSTs {
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

	minSegments := c.opts.RetentionCount / 10
	if minSegments < 1 {
		minSegments = 1
	}

	var toDelete []string
	var bytesReclaimed int64
	deletedSegments := 0

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
			}
			deletedSegments++
		}
	}

	if len(toDelete) == 0 {
		return 0, 0, nil
	}

	_, err := c.manifestLog.AppendRemoveSSTablesWithFence(ctx, toDelete)
	if err != nil {
		return 0, 0, fmt.Errorf("update manifest: %w", err)
	}

	for _, id := range toDelete {
		path := c.store.SSTPath(id)
		if err := c.store.Delete(ctx, path); err != nil {
			slog.Error("isledb: failed to delete SST", "sst_id", id, "error", err)
		}
	}

	return len(toDelete), bytesReclaimed, nil
}

func (c *RetentionCompactor) Stats() RetentionCompactorStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats := RetentionCompactorStats{
		Mode:            c.opts.Mode,
		RetentionPeriod: c.opts.RetentionPeriod,
	}

	if c.manifest != nil {
		stats.L0SSTCount = len(c.manifest.L0SSTs)
		stats.SortedRunCount = len(c.manifest.SortedRuns)

		for _, sst := range c.manifest.L0SSTs {
			stats.TotalSize += sst.Size
		}
		for _, sr := range c.manifest.SortedRuns {
			for _, sst := range sr.SSTs {
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
		for _, sr := range c.manifest.SortedRuns {
			for _, sst := range sr.SSTs {
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

func (c *RetentionCompactor) IsFenced() bool {
	return c.fenced.Load()
}

type RetentionCompactorStats struct {
	Mode            RetentionCompactorMode
	RetentionPeriod time.Duration
	L0SSTCount      int
	SortedRunCount  int
	TotalSize       int64
	OldestSST       time.Time
}
