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
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/segmentio/ksuid"
)

var (
	ErrMaintenanceAlreadyOpen    = errors.New("maintenance already open")
	ErrMaintenanceClosed         = errors.New("maintenance closed")
	ErrMaintenanceRunning        = errors.New("maintenance already running")
	ErrInvalidMaintenanceOptions = errors.New("invalid maintenance options")
)

// DefaultCheckpointReplayPages bounds cold state replay to roughly 64
// immutable page reads between snapshots.
const DefaultCheckpointReplayPages uint64 = 64

// RetentionMode selects how maintenance groups data for retention.
type RetentionMode uint8

const (
	RetentionByAge RetentionMode = iota
	RetentionByTimeWindow
)

// MaintenanceOptions configures one fenced maintenance owner for a DB.
type MaintenanceOptions struct {
	// OwnerID is stored in the maintenance fence. Empty generates a process-local ID.
	OwnerID string

	// Every is the delay between completed cycles when Run is used.
	Every time.Duration

	Compaction CompactionPolicy

	GarbageCollection GarbageCollectionPolicy

	// Checkpoint bounds manifest state replay between snapshots.
	Checkpoint CheckpointPolicy

	// Retention is nil by default because enabling it removes historical data.
	Retention *RetentionPolicy

	OnCycle func(MaintenanceStats)
	OnError func(error)
}

// CheckpointPolicy bounds the immutable manifest pages needed to rebuild the
// current KV state. MaxReplayPages defaults to DefaultCheckpointReplayPages.
type CheckpointPolicy struct {
	MaxReplayPages uint64
}

// CompactionPolicy controls L0 and leveled compaction.
type CompactionPolicy struct {
	InputReadParallelism        int
	L0SSTCount                  int
	MaxConsecutiveL0Compactions int
	BaseLevelBytes              int64
	LevelSizeMultiplier         int
	MaxInputSSTs                int
	TargetSSTBytes              int64
	BloomBitsPerKey             int
	BlockBytes                  int
	Compression                 string
	ValidateSSTChecksum         bool
	SSTHashVerifier             SSTHashVerifier
	OnCompactionStart           func(CompactionJob)
	OnCompactionEnd             func(CompactionJob, error)
}

// GarbageCollectionPolicy controls deterministic deletion of objects retired
// by committed manifest entries.
type GarbageCollectionPolicy struct {
	DeleteBatchSize int
}

// RetentionPolicy controls removal of old SSTs. It is enabled only when the
// containing MaintenanceOptions.Retention pointer is non-nil.
type RetentionPolicy struct {
	Mode RetentionMode

	KeepFor time.Duration

	// KeepAtLeastSSTs is the minimum number of newest SSTs retained in
	// RetentionByAge mode.
	KeepAtLeastSSTs int

	// KeepAtLeastWindows is the minimum number of newest windows retained in
	// RetentionByTimeWindow mode.
	KeepAtLeastWindows int

	Window    time.Duration
	OnCleanup func(CleanupStats)
}

type changeFeedRetentionPolicy struct {
	KeepFor time.Duration

	// KeepAtLeastManifestEntries is the minimum number of newest manifest
	// entries retained, including entries that do not contain a change batch.
	KeepAtLeastManifestEntries uint64

	DeleteBatchSize   int
	DeleteGracePeriod time.Duration
	OnCleanup         func(changeFeedCleanupStats)
}

// MaintenanceStats describes work performed by one bounded RunOnce cycle.
// Command-producing work is not visible until the writer applies it.
type MaintenanceStats struct {
	CompactionJobs        int
	CompactionInputSSTs   int
	CompactionOutputSSTs  int
	CompactionOutputBytes int64
	Retention             CleanupStats
	CheckpointStaged      bool
	CheckpointReplayPages uint64
	CheckpointReplayBytes uint64
	CommandPending        bool
	WaitingForWriter      bool
	CommandApplied        bool
	CommandRejected       bool
	Duration              time.Duration
}

// DefaultMaintenanceOptions returns safe defaults. Compaction, checkpoints,
// and SST sweeping are enabled; data retention remains disabled.
func DefaultMaintenanceOptions() MaintenanceOptions {
	compaction := defaultCompactorOptions()
	return MaintenanceOptions{
		Every: compaction.Trigger.CheckInterval,
		Compaction: CompactionPolicy{
			InputReadParallelism:        compaction.InputReadParallelism,
			L0SSTCount:                  compaction.Trigger.L0SSTCount,
			MaxConsecutiveL0Compactions: compaction.Trigger.MaxConsecutiveL0Compactions,
			BaseLevelBytes:              compaction.Trigger.BaseLevelBytes,
			LevelSizeMultiplier:         compaction.Trigger.LevelSizeMultiplier,
			MaxInputSSTs:                compaction.Trigger.MaxInputSSTs,
			TargetSSTBytes:              compaction.Output.TargetSSTBytes,
			BloomBitsPerKey:             compaction.Output.BloomBitsPerKey,
			BlockBytes:                  compaction.Output.BlockBytes,
			Compression:                 compaction.Output.Compression,
		},
		GarbageCollection: GarbageCollectionPolicy{
			DeleteBatchSize: compaction.GCDeleteBatchSize,
		},
		Checkpoint: CheckpointPolicy{MaxReplayPages: DefaultCheckpointReplayPages},
	}
}

func DefaultRetentionPolicy() RetentionPolicy {
	opts := defaultRetentionCompactorOptions()
	return RetentionPolicy{
		Mode:               RetentionByAge,
		KeepFor:            opts.RetentionPeriod,
		KeepAtLeastSSTs:    opts.KeepAtLeastSSTs,
		KeepAtLeastWindows: opts.KeepAtLeastWindows,
		Window:             opts.SegmentDuration,
	}
}

func defaultChangeFeedRetentionPolicy() changeFeedRetentionPolicy {
	opts := defaultChangeFeedCleanerOptions()
	return changeFeedRetentionPolicy{
		KeepFor:                    opts.RetentionPeriod,
		KeepAtLeastManifestEntries: opts.KeepAtLeastManifestEntries,
		DeleteBatchSize:            opts.SweepBatchSize,
		DeleteGracePeriod:          opts.SweepGracePeriod,
	}
}

// Maintenance owns compaction, checkpoints, retention, and garbage collection
// for one DB.
// It is safe to call Close concurrently with Run or RunOnce.
type Maintenance struct {
	manifestLog *manifest.Store
	opts        MaintenanceOptions
	compactor   *compactor
	retention   *retentionCompactor
	changeFeed  *changeFeedCleaner
	fenceToken  *manifest.FenceToken

	lifecycleMu   sync.Mutex
	closeMu       sync.Mutex
	runCancel     context.CancelFunc
	loopWG        sync.WaitGroup
	activeRuns    sync.WaitGroup
	runGate       chan struct{}
	enginesClosed bool

	statsMu       sync.Mutex
	currentStats  *MaintenanceStats
	commandStaged bool
	writerWake    chan<- struct{}

	running atomic.Bool
	closed  atomic.Bool

	releaseOnce sync.Once
	release     func()

	changeFeedRetention *changeFeedRetentionPolicy
}

func newMaintenance(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, gcCursor manifest.GCCursorStorage, opts MaintenanceOptions, changeFeedRetention *changeFeedRetentionPolicy) (*Maintenance, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	var err error
	opts, err = normalizeMaintenanceOptions(opts)
	if err != nil {
		return nil, err
	}

	state, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}
	ownerID := opts.OwnerID
	if ownerID == "" {
		ownerID = fmt.Sprintf("maintenance-%d-%d", time.Now().UnixNano(), state.NextEpoch)
	}
	token, err := manifestLog.ClaimMaintenance(ctx, ownerID)
	if err != nil {
		return nil, fmt.Errorf("claim maintenance fence: %w", err)
	}

	m := &Maintenance{
		manifestLog:         manifestLog,
		opts:                opts,
		fenceToken:          token,
		runGate:             make(chan struct{}, 1),
		changeFeedRetention: changeFeedRetention,
	}

	compactorOpts := m.compactorOptions(gcCursor)
	m.compactor, err = newCompactorWithFence(ctx, store, manifestLog, compactorOpts, token)
	if err != nil {
		return nil, fmt.Errorf("open compaction stage: %w", err)
	}
	m.compactor.stageCommand = m.stageCommand

	if opts.Retention != nil {
		retentionOpts := m.retentionOptions(gcCursor)
		m.retention, err = newRetentionCompactorWithFence(ctx, store, manifestLog, retentionOpts, token)
		if err != nil {
			_ = m.compactor.Close(ctx)
			return nil, fmt.Errorf("open retention stage: %w", err)
		}
		m.retention.stageCommand = m.stageCommand
	}

	if changeFeedRetention != nil {
		changeFeedOpts := m.changeFeedOptions()
		m.changeFeed, err = newChangeFeedCleanerWithFence(ctx, store, manifestLog, changeFeedOpts, token)
		if err != nil {
			if m.retention != nil {
				_ = m.retention.Close(ctx)
			}
			_ = m.compactor.Close(ctx)
			return nil, fmt.Errorf("open change-feed retention stage: %w", err)
		}
		m.changeFeed.stageCommand = m.stageCommand
	}

	return m, nil
}

func normalizeMaintenanceOptions(opts MaintenanceOptions) (MaintenanceOptions, error) {
	defaults := DefaultMaintenanceOptions()
	if opts.Every <= 0 {
		opts.Every = defaults.Every
	}
	if opts.Retention != nil && opts.Retention.Mode != RetentionByAge && opts.Retention.Mode != RetentionByTimeWindow {
		return MaintenanceOptions{}, fmt.Errorf("%w: retention mode=%d", ErrInvalidMaintenanceOptions, opts.Retention.Mode)
	}
	if opts.Retention != nil && (opts.Retention.KeepAtLeastSSTs < 0 || opts.Retention.KeepAtLeastWindows < 0) {
		return MaintenanceOptions{}, fmt.Errorf("%w: retention minimums must not be negative", ErrInvalidMaintenanceOptions)
	}
	if opts.GarbageCollection.DeleteBatchSize < 0 {
		return MaintenanceOptions{}, fmt.Errorf("%w: invalid garbage collection policy", ErrInvalidMaintenanceOptions)
	}
	if opts.GarbageCollection.DeleteBatchSize == 0 {
		opts.GarbageCollection.DeleteBatchSize = defaults.GarbageCollection.DeleteBatchSize
	}
	if opts.Checkpoint.MaxReplayPages == 0 {
		opts.Checkpoint.MaxReplayPages = defaults.Checkpoint.MaxReplayPages
	}
	return opts, nil
}

func (m *Maintenance) compactorOptions(gcCursor manifest.GCCursorStorage) compactorOptions {
	p := m.opts.Compaction
	return compactorOptions{
		OwnerID:              m.opts.OwnerID,
		InputReadParallelism: p.InputReadParallelism,
		Trigger: compactionTriggerOptions{
			CheckInterval:               m.opts.Every,
			L0SSTCount:                  p.L0SSTCount,
			MaxConsecutiveL0Compactions: p.MaxConsecutiveL0Compactions,
			BaseLevelBytes:              p.BaseLevelBytes,
			LevelSizeMultiplier:         p.LevelSizeMultiplier,
			MaxInputSSTs:                p.MaxInputSSTs,
		},
		Output: compactionOutputOptions{
			TargetSSTBytes:  p.TargetSSTBytes,
			BloomBitsPerKey: p.BloomBitsPerKey,
			BlockBytes:      p.BlockBytes,
			Compression:     p.Compression,
		},
		Safety: compactionSafetyOptions{
			ValidateSSTChecksum: p.ValidateSSTChecksum,
			SSTHashVerifier:     p.SSTHashVerifier,
		},
		OnCompactionStart: p.OnCompactionStart,
		OnCompactionEnd:   m.recordCompaction,
		GCCursorStorage:   gcCursor,
		GCDeleteBatchSize: m.opts.GarbageCollection.DeleteBatchSize,
	}
}

func (m *Maintenance) retentionOptions(gcCursor manifest.GCCursorStorage) retentionCompactorOptions {
	p := m.opts.Retention
	mode := compactByAge
	if p.Mode == RetentionByTimeWindow {
		mode = compactByTimeWindow
	}
	return retentionCompactorOptions{
		Mode:               mode,
		RetentionPeriod:    p.KeepFor,
		KeepAtLeastSSTs:    p.KeepAtLeastSSTs,
		KeepAtLeastWindows: p.KeepAtLeastWindows,
		CheckInterval:      m.opts.Every,
		SegmentDuration:    p.Window,
		OnCleanup:          m.recordRetention,
		GCCursorStorage:    gcCursor,
		GCDeleteBatchSize:  m.opts.GarbageCollection.DeleteBatchSize,
	}
}

func (m *Maintenance) changeFeedOptions() changeFeedCleanerOptions {
	p := m.changeFeedRetention
	return changeFeedCleanerOptions{
		RetentionPeriod:            p.KeepFor,
		KeepAtLeastManifestEntries: p.KeepAtLeastManifestEntries,
		CheckInterval:              m.opts.Every,
		SweepBatchSize:             p.DeleteBatchSize,
		SweepGracePeriod:           p.DeleteGracePeriod,
		OnCleanup:                  m.recordChangeFeed,
	}
}

// Run executes maintenance cycles until ctx is canceled, Close is called, or
// the maintenance fence is lost.
func (m *Maintenance) Run(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	m.lifecycleMu.Lock()
	if m.closed.Load() {
		m.lifecycleMu.Unlock()
		return ErrMaintenanceClosed
	}
	if !m.running.CompareAndSwap(false, true) {
		m.lifecycleMu.Unlock()
		return ErrMaintenanceRunning
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.runCancel = cancel
	m.loopWG.Add(1)
	m.lifecycleMu.Unlock()

	defer func() {
		cancel()
		m.lifecycleMu.Lock()
		m.runCancel = nil
		m.running.Store(false)
		m.lifecycleMu.Unlock()
		m.loopWG.Done()
	}()

	for {
		stats, err := m.RunOnce(runCtx)
		if err != nil {
			if m.closed.Load() && (errors.Is(err, context.Canceled) || errors.Is(err, ErrMaintenanceClosed)) {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if isFenceError(err) {
				return err
			}
			if errors.Is(err, manifest.ErrFenceConflict) {
				slog.Debug("isledb: maintenance cycle skipped after concurrent manifest update")
			} else if m.opts.OnError != nil {
				m.opts.OnError(err)
			} else {
				slog.Error("isledb: maintenance cycle failed", "error", err)
			}
		} else if m.opts.OnCycle != nil {
			m.opts.OnCycle(stats)
		}

		timer := time.NewTimer(m.opts.Every)
		select {
		case <-timer.C:
		case <-runCtx.Done():
			stopMaintenanceTimer(timer)
			if m.closed.Load() {
				return nil
			}
			return runCtx.Err()
		}
	}
}

// RunOnce performs one serialized maintenance cycle.
func (m *Maintenance) RunOnce(ctx context.Context) (MaintenanceStats, error) {
	if err := checkContext(ctx); err != nil {
		return MaintenanceStats{}, err
	}
	if err := m.beginCycle(ctx); err != nil {
		return MaintenanceStats{}, err
	}

	stats := MaintenanceStats{}
	start := time.Now()
	m.statsMu.Lock()
	m.currentStats = &stats
	m.commandStaged = false
	m.statsMu.Unlock()
	defer func() {
		m.statsMu.Lock()
		m.currentStats = nil
		m.statsMu.Unlock()
		m.finishCycle()
	}()

	waiting, err := m.reconcilePendingCommand(ctx)
	if err != nil {
		return m.completeCycleStats(start), fmt.Errorf("reconcile maintenance command: %w", err)
	}
	if waiting {
		return m.completeCycleStats(start), nil
	}

	if err := m.compactor.RunOnce(ctx); err != nil {
		return m.completeCycleStats(start), fmt.Errorf("compaction: %w", err)
	}
	if m.hasStagedCommand() {
		return m.completeCycleStats(start), nil
	}
	if m.retention != nil {
		if err := m.retention.RunOnce(ctx); err != nil {
			return m.completeCycleStats(start), fmt.Errorf("retention: %w", err)
		}
		if m.hasStagedCommand() {
			return m.completeCycleStats(start), nil
		}
	}
	if m.changeFeed != nil {
		if err := m.changeFeed.RunOnce(ctx); err != nil {
			return m.completeCycleStats(start), fmt.Errorf("change-feed retention: %w", err)
		}
		if m.hasStagedCommand() {
			return m.completeCycleStats(start), nil
		}
	}
	if err := m.checkpointIfNeeded(ctx); err != nil {
		return m.completeCycleStats(start), fmt.Errorf("checkpoint: %w", err)
	}

	return m.completeCycleStats(start), nil
}

func (m *Maintenance) checkpointIfNeeded(ctx context.Context) error {
	current, err := m.manifestLog.ReadCurrentData(ctx)
	if err != nil {
		return err
	}
	if current == nil || current.StateReplayPages < m.opts.Checkpoint.MaxReplayPages {
		return nil
	}

	checkpoint, err := m.manifestLog.PrepareCheckpoint(ctx)
	if err != nil {
		return err
	}
	if err := m.stageCommand(ctx, manifest.MaintenanceCommand{
		Kind:       manifest.MaintenanceCommandCheckpoint,
		Checkpoint: &checkpoint,
	}); err != nil {
		return err
	}

	m.statsMu.Lock()
	if m.currentStats != nil {
		m.currentStats.CheckpointStaged = true
		m.currentStats.CheckpointReplayPages = checkpoint.FoldedReplayPages
		m.currentStats.CheckpointReplayBytes = checkpoint.FoldedReplayBytes
	}
	m.statsMu.Unlock()
	return nil
}

func (m *Maintenance) stageCommand(ctx context.Context, command manifest.MaintenanceCommand) error {
	m.statsMu.Lock()
	if m.commandStaged {
		m.statsMu.Unlock()
		return manifest.ErrMaintenanceCommandPending
	}
	m.statsMu.Unlock()

	if command.ID == "" {
		command.ID = ksuid.New().String()
	}
	if _, err := m.manifestLog.StageMaintenance(ctx, command, m.fenceToken); err != nil {
		return err
	}
	m.notifyWriter()
	m.statsMu.Lock()
	m.commandStaged = true
	if m.currentStats != nil {
		m.currentStats.CommandPending = true
	}
	m.statsMu.Unlock()
	return nil
}

func (m *Maintenance) notifyWriter() {
	select {
	case m.writerWake <- struct{}{}:
	default:
	}
}

func (m *Maintenance) hasStagedCommand() bool {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	return m.commandStaged
}

func (m *Maintenance) reconcilePendingCommand(ctx context.Context) (bool, error) {
	head, _, err := m.manifestLog.ReadMaintenanceHead(ctx)
	if err != nil {
		return false, err
	}
	if head == nil || m.fenceToken == nil ||
		head.Epoch != m.fenceToken.Epoch ||
		head.OwnerID != m.fenceToken.Owner ||
		!head.ClaimedAt.Equal(m.fenceToken.ClaimedAt) {
		return false, manifest.ErrFenced
	}
	if head.Pending == nil {
		return false, nil
	}

	current, err := m.manifestLog.ReadCurrentData(ctx)
	if err != nil {
		return false, err
	}
	m.statsMu.Lock()
	if m.currentStats != nil {
		m.currentStats.CommandPending = true
	}
	m.statsMu.Unlock()
	if current == nil || !current.MaintenanceReceipt.Matches(head.Pending) {
		m.statsMu.Lock()
		if m.currentStats != nil {
			m.currentStats.WaitingForWriter = true
		}
		m.statsMu.Unlock()
		return true, nil
	}

	receipt := current.MaintenanceReceipt
	if _, err := m.manifestLog.ClearMaintenance(ctx, head.Pending.ID, head.Pending.Epoch, head.Pending.Generation, m.fenceToken); err != nil {
		return false, err
	}
	m.statsMu.Lock()
	if m.currentStats != nil {
		m.currentStats.CommandPending = false
		m.currentStats.CommandApplied = receipt.Status == manifest.MaintenanceStatusApplied
		m.currentStats.CommandRejected = receipt.Status == manifest.MaintenanceStatusRejected
	}
	m.statsMu.Unlock()
	return false, nil
}

func (m *Maintenance) completeCycleStats(start time.Time) MaintenanceStats {
	m.statsMu.Lock()
	if m.currentStats != nil {
		m.currentStats.Duration = time.Since(start)
	}
	result := *m.currentStats
	m.statsMu.Unlock()
	return result
}

func (m *Maintenance) beginCycle(ctx context.Context) error {
	if m.closed.Load() {
		return ErrMaintenanceClosed
	}
	select {
	case m.runGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	m.lifecycleMu.Lock()
	if m.closed.Load() {
		m.lifecycleMu.Unlock()
		<-m.runGate
		return ErrMaintenanceClosed
	}
	m.activeRuns.Add(1)
	m.lifecycleMu.Unlock()
	return nil
}

func (m *Maintenance) finishCycle() {
	m.activeRuns.Done()
	<-m.runGate
}

// Close stops scheduled maintenance, waits for active work, and releases the
// DB maintenance slot after all stages close successfully.
func (m *Maintenance) Close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	if m.closed.CompareAndSwap(false, true) {
		m.lifecycleMu.Lock()
		if m.runCancel != nil {
			m.runCancel()
		}
		m.lifecycleMu.Unlock()
	}
	if err := waitGroupContext(ctx, &m.loopWG); err != nil {
		return err
	}
	if err := waitGroupContext(ctx, &m.activeRuns); err != nil {
		return err
	}

	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	if m.enginesClosed {
		m.releaseMaintenance()
		return nil
	}
	if m.changeFeed != nil {
		if err := m.changeFeed.Close(ctx); err != nil {
			return err
		}
	}
	if m.retention != nil {
		if err := m.retention.Close(ctx); err != nil {
			return err
		}
	}
	if err := m.compactor.Close(ctx); err != nil {
		return err
	}
	m.enginesClosed = true
	m.releaseMaintenance()
	return nil
}

func (m *Maintenance) closeDB() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return m.Close(ctx)
}

func (m *Maintenance) releaseMaintenance() {
	if m == nil || m.release == nil {
		return
	}
	m.releaseOnce.Do(m.release)
}

func stopMaintenanceTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (m *Maintenance) recordCompaction(job CompactionJob, err error) {
	if err == nil {
		m.statsMu.Lock()
		if m.currentStats != nil {
			m.currentStats.CompactionJobs++
			m.currentStats.CompactionInputSSTs += len(job.InputSSTs)
			m.currentStats.CompactionOutputSSTs += len(job.OutputSSTs)
			for _, sst := range job.OutputSSTs {
				m.currentStats.CompactionOutputBytes += sst.Bytes
			}
		}
		m.statsMu.Unlock()
	}
	if callback := m.opts.Compaction.OnCompactionEnd; callback != nil {
		callback(job, err)
	}
}

func (m *Maintenance) recordRetention(stats CleanupStats) {
	m.statsMu.Lock()
	if m.currentStats != nil {
		m.currentStats.Retention = stats
	}
	m.statsMu.Unlock()
	if callback := m.opts.Retention.OnCleanup; callback != nil {
		callback(stats)
	}
}

func (m *Maintenance) recordChangeFeed(stats changeFeedCleanupStats) {
	m.statsMu.Lock()
	m.statsMu.Unlock()
	if callback := m.changeFeedRetention.OnCleanup; callback != nil {
		callback(stats)
	}
}
