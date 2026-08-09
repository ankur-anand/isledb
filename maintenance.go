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

// defaultCheckpointReplayPages bounds cold state replay to roughly 64
// immutable page reads between snapshots.
const defaultCheckpointReplayPages uint64 = 64

// MaintenanceOptions configures one fenced maintenance owner for a DB.
type MaintenanceOptions struct {
	// Interval is the delay between completed cycles when Run is used. Zero
	// selects the production default.
	Interval time.Duration

	// SSTCompaction controls L0 and leveled SST compaction. Zero fields select
	// production defaults.
	SSTCompaction SSTCompactionOptions

	// ChangeFeedRetention is nil by default, preserving change-feed history
	// indefinitely. When configured, maintenance retires and deletes old feed
	// batches without affecting KV state.
	ChangeFeedRetention *ChangeFeedRetentionOptions

	// OnCycle is called synchronously by Run after each successful cycle.
	OnCycle func(MaintenanceStats)
	// OnError is called synchronously by Run when a cycle fails with a
	// recoverable error.
	OnError func(error)
}

// SSTCompactionOptions controls the workload and resource tradeoffs of L0 and
// leveled SST compaction. Zero fields select production defaults.
type SSTCompactionOptions struct {
	// ReadConcurrency bounds concurrent input-SST reads within one job.
	ReadConcurrency int
	// L0TriggerSSTs starts L0 compaction at this many files.
	L0TriggerSSTs int
	// MaxConsecutiveL0Jobs prevents sustained L0 traffic from starving higher
	// levels.
	MaxConsecutiveL0Jobs int
	// BaseLevelBytes is the target size of L1.
	BaseLevelBytes int64
	// LevelGrowthFactor scales the target size of each successive level. It
	// must be at least 2.
	LevelGrowthFactor int
	// MaxInputSSTsPerJob bounds the inputs and retirement records in one job.
	MaxInputSSTsPerJob int
	// TargetSSTBytes is the approximate output-file size. Encoding settings
	// come from DBOptions.SSTOutput.
	TargetSSTBytes int64
}

// ChangeFeedRetentionOptions controls removal of old change-feed history. It is
// enabled only when MaintenanceOptions.ChangeFeedRetention is non-nil.
type ChangeFeedRetentionOptions struct {
	// RetainFor is the minimum age retained. Zero selects seven days.
	RetainFor time.Duration
}

type maintenanceOptions struct {
	interval                     time.Duration
	sstCompaction                SSTCompactionOptions
	changeFeedRetention          *changeFeedRetentionOptions
	onCycle                      func(MaintenanceStats)
	onError                      func(error)
	checkpointReplayPages        uint64
	retiredObjectDeletesPerCycle int
}

type changeFeedRetentionOptions struct {
	retainFor             time.Duration
	minimumHistoryEntries uint64
	deletesPerCycle       int
	deleteGracePeriod     time.Duration
}

// ChangeFeedCleanupStats describes change-feed retention work completed in one
// cleanup pass.
type ChangeFeedCleanupStats struct {
	EntriesRetired  int
	BatchesMarked   int
	BatchesDeleted  int
	BlockedRetained int
	FailedDeletes   int
	Duration        time.Duration
}

// MaintenanceState describes whether another maintenance step can make
// progress immediately.
type MaintenanceState uint8

const (
	// MaintenanceIdle means the cycle left no staged command waiting for
	// publication.
	MaintenanceIdle MaintenanceState = iota
	// MaintenanceWaitingForWriter means the active writer must publish or
	// reject the staged command before maintenance can continue.
	MaintenanceWaitingForWriter
)

func (state MaintenanceState) String() string {
	switch state {
	case MaintenanceIdle:
		return "idle"
	case MaintenanceWaitingForWriter:
		return "waiting_for_writer"
	default:
		return fmt.Sprintf("MaintenanceState(%d)", state)
	}
}

// SSTCompactionStats describes SST compaction work completed in one cycle.
type SSTCompactionStats struct {
	Jobs        int
	InputSSTs   int
	OutputSSTs  int
	OutputBytes int64
}

// ManifestCheckpointStats describes a manifest checkpoint staged in one cycle.
type ManifestCheckpointStats struct {
	Staged      bool
	ReplayPages uint64
	ReplayBytes uint64
}

// MaintenanceStats describes work performed by one bounded RunOnce cycle.
// Command-producing work is not visible until the writer applies it.
type MaintenanceStats struct {
	State               MaintenanceState
	SSTCompaction       SSTCompactionStats
	ChangeFeedRetention ChangeFeedCleanupStats
	ManifestCheckpoint  ManifestCheckpointStats
	Duration            time.Duration
}

// DefaultMaintenanceOptions returns safe defaults. Compaction, checkpoints,
// and SST sweeping are enabled; change-feed retention remains disabled.
func DefaultMaintenanceOptions() MaintenanceOptions {
	compaction := defaultCompactorOptions()
	return MaintenanceOptions{
		Interval: compaction.Trigger.CheckInterval,
		SSTCompaction: SSTCompactionOptions{
			ReadConcurrency:      compaction.InputReadParallelism,
			L0TriggerSSTs:        compaction.Trigger.L0SSTCount,
			MaxConsecutiveL0Jobs: compaction.Trigger.MaxConsecutiveL0Compactions,
			BaseLevelBytes:       compaction.Trigger.BaseLevelBytes,
			LevelGrowthFactor:    compaction.Trigger.LevelSizeMultiplier,
			MaxInputSSTsPerJob:   compaction.Trigger.MaxInputSSTs,
			TargetSSTBytes:       compaction.Output.TargetSSTBytes,
		},
	}
}

// DefaultChangeFeedRetentionOptions returns conservative change-feed retention
// defaults. Assigning these options to MaintenanceOptions.ChangeFeedRetention
// opts into cleanup; merely enabling a feed does not delete history.
func DefaultChangeFeedRetentionOptions() ChangeFeedRetentionOptions {
	opts := defaultChangeFeedCleanerOptions()
	return ChangeFeedRetentionOptions{
		RetainFor: opts.RetentionPeriod,
	}
}

// Maintenance owns compaction, checkpoints, optional change-feed retention,
// and garbage collection for one DB.
// It is safe to call Close concurrently with Run or RunOnce.
type Maintenance struct {
	manifestLog *manifest.Store
	opts        maintenanceOptions
	compactor   *compactor
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

	sstOutput SSTEncodingOptions
}

func newMaintenance(
	ctx context.Context,
	store *blobstore.Store,
	manifestLog *manifest.Store,
	gcCursor manifest.GCCursorStorage,
	opts MaintenanceOptions,
	sstOutput SSTEncodingOptions,
) (*Maintenance, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	normalized, err := normalizeMaintenanceOptions(opts)
	if err != nil {
		return nil, err
	}

	state, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}
	if normalized.changeFeedRetention != nil {
		view, err := manifestLog.LoadChangeFeedView(ctx)
		if err != nil {
			return nil, fmt.Errorf("load change-feed configuration: %w", err)
		}
		if !view.Enabled() {
			return nil, ErrChangeFeedDisabled
		}
	}
	ownerID := fmt.Sprintf("maintenance-%d-%d", time.Now().UnixNano(), state.NextEpoch)
	token, err := manifestLog.ClaimMaintenance(ctx, ownerID)
	if err != nil {
		return nil, fmt.Errorf("claim maintenance fence: %w", err)
	}

	m := &Maintenance{
		manifestLog: manifestLog,
		opts:        normalized,
		fenceToken:  token,
		runGate:     make(chan struct{}, 1),
		sstOutput:   sstOutput,
	}

	compactorOpts := m.compactorOptions(gcCursor)
	m.compactor, err = newCompactorWithFence(ctx, store, manifestLog, compactorOpts, token)
	if err != nil {
		return nil, fmt.Errorf("open compaction stage: %w", err)
	}
	m.compactor.stageCommand = m.stageCommand

	if normalized.changeFeedRetention != nil {
		changeFeedOpts := m.changeFeedOptions()
		m.changeFeed, err = newChangeFeedCleanerWithFence(ctx, store, manifestLog, changeFeedOpts, token)
		if err != nil {
			_ = m.compactor.Close(ctx)
			return nil, fmt.Errorf("open change-feed retention stage: %w", err)
		}
		m.changeFeed.stageCommand = m.stageCommand
	}

	return m, nil
}

func normalizeMaintenanceOptions(opts MaintenanceOptions) (maintenanceOptions, error) {
	defaults := DefaultMaintenanceOptions()
	if opts.Interval < 0 {
		return maintenanceOptions{}, fmt.Errorf("%w: negative interval", ErrInvalidMaintenanceOptions)
	}
	if opts.Interval == 0 {
		opts.Interval = defaults.Interval
	}
	compaction, err := normalizeSSTCompactionOptions(opts.SSTCompaction, defaults.SSTCompaction)
	if err != nil {
		return maintenanceOptions{}, err
	}
	var retention *changeFeedRetentionOptions
	if opts.ChangeFeedRetention != nil {
		normalizedRetention, err := normalizeChangeFeedRetentionOptions(*opts.ChangeFeedRetention)
		if err != nil {
			return maintenanceOptions{}, err
		}
		retention = &normalizedRetention
	}
	return maintenanceOptions{
		interval:                     opts.Interval,
		sstCompaction:                compaction,
		changeFeedRetention:          retention,
		onCycle:                      opts.OnCycle,
		onError:                      opts.OnError,
		checkpointReplayPages:        defaultCheckpointReplayPages,
		retiredObjectDeletesPerCycle: defaultCompactorOptions().GCDeleteBatchSize,
	}, nil
}

func normalizeSSTCompactionOptions(opts, defaults SSTCompactionOptions) (SSTCompactionOptions, error) {
	if opts.ReadConcurrency < 0 || opts.L0TriggerSSTs < 0 || opts.MaxConsecutiveL0Jobs < 0 ||
		opts.BaseLevelBytes < 0 || opts.LevelGrowthFactor < 0 || opts.MaxInputSSTsPerJob < 0 ||
		opts.TargetSSTBytes < 0 {
		return SSTCompactionOptions{}, fmt.Errorf("%w: negative SST compaction option", ErrInvalidMaintenanceOptions)
	}
	if opts.LevelGrowthFactor == 1 {
		return SSTCompactionOptions{}, fmt.Errorf("%w: level growth factor must be at least 2", ErrInvalidMaintenanceOptions)
	}
	if opts.MaxInputSSTsPerJob > manifest.MaxRetiredObjectsPerEntry {
		return SSTCompactionOptions{}, fmt.Errorf(
			"%w: max input SSTs per job=%d exceeds %d",
			ErrInvalidMaintenanceOptions, opts.MaxInputSSTsPerJob, manifest.MaxRetiredObjectsPerEntry)
	}
	if opts.ReadConcurrency == 0 {
		opts.ReadConcurrency = defaults.ReadConcurrency
	}
	if opts.L0TriggerSSTs == 0 {
		opts.L0TriggerSSTs = defaults.L0TriggerSSTs
	}
	if opts.MaxConsecutiveL0Jobs == 0 {
		opts.MaxConsecutiveL0Jobs = defaults.MaxConsecutiveL0Jobs
	}
	if opts.BaseLevelBytes == 0 {
		opts.BaseLevelBytes = defaults.BaseLevelBytes
	}
	if opts.LevelGrowthFactor == 0 {
		opts.LevelGrowthFactor = defaults.LevelGrowthFactor
	}
	if opts.MaxInputSSTsPerJob == 0 {
		opts.MaxInputSSTsPerJob = defaults.MaxInputSSTsPerJob
	}
	if opts.TargetSSTBytes == 0 {
		opts.TargetSSTBytes = defaults.TargetSSTBytes
	}
	return opts, nil
}

func normalizeChangeFeedRetentionOptions(opts ChangeFeedRetentionOptions) (changeFeedRetentionOptions, error) {
	if opts.RetainFor < 0 {
		return changeFeedRetentionOptions{}, fmt.Errorf("%w: negative change-feed retention period", ErrInvalidMaintenanceOptions)
	}
	defaults := defaultChangeFeedCleanerOptions()
	if opts.RetainFor == 0 {
		opts.RetainFor = defaults.RetentionPeriod
	}
	return changeFeedRetentionOptions{
		retainFor:             opts.RetainFor,
		minimumHistoryEntries: defaults.KeepAtLeastManifestEntries,
		deletesPerCycle:       defaults.SweepBatchSize,
		deleteGracePeriod:     defaults.SweepGracePeriod,
	}, nil
}

func (m *Maintenance) compactorOptions(gcCursor manifest.GCCursorStorage) compactorOptions {
	p := m.opts.sstCompaction
	return compactorOptions{
		OwnerID:              m.fenceToken.Owner,
		InputReadParallelism: p.ReadConcurrency,
		Trigger: compactionTriggerOptions{
			CheckInterval:               m.opts.interval,
			L0SSTCount:                  p.L0TriggerSSTs,
			MaxConsecutiveL0Compactions: p.MaxConsecutiveL0Jobs,
			BaseLevelBytes:              p.BaseLevelBytes,
			LevelSizeMultiplier:         p.LevelGrowthFactor,
			MaxInputSSTs:                p.MaxInputSSTsPerJob,
		},
		Output: compactionOutputOptions{
			TargetSSTBytes:  p.TargetSSTBytes,
			BloomBitsPerKey: m.sstOutput.BloomBitsPerKey,
			BlockBytes:      m.sstOutput.BlockBytes,
			Compression:     m.sstOutput.Compression,
		},
		OnCompactionEnd:   m.recordCompaction,
		GCCursorStorage:   gcCursor,
		GCDeleteBatchSize: m.opts.retiredObjectDeletesPerCycle,
	}
}

func (m *Maintenance) changeFeedOptions() changeFeedCleanerOptions {
	p := m.opts.changeFeedRetention
	return changeFeedCleanerOptions{
		RetentionPeriod:            p.retainFor,
		KeepAtLeastManifestEntries: p.minimumHistoryEntries,
		CheckInterval:              m.opts.interval,
		SweepBatchSize:             p.deletesPerCycle,
		SweepGracePeriod:           p.deleteGracePeriod,
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
			} else if m.opts.onError != nil {
				m.opts.onError(err)
			} else {
				slog.Error("isledb: maintenance cycle failed", "error", err)
			}
		} else if m.opts.onCycle != nil {
			m.opts.onCycle(stats)
		}

		timer := time.NewTimer(m.opts.interval)
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
	if current == nil || current.StateReplayPages < m.opts.checkpointReplayPages {
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
		m.currentStats.ManifestCheckpoint.Staged = true
		m.currentStats.ManifestCheckpoint.ReplayPages = checkpoint.FoldedReplayPages
		m.currentStats.ManifestCheckpoint.ReplayBytes = checkpoint.FoldedReplayBytes
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
		m.currentStats.State = MaintenanceWaitingForWriter
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
		m.currentStats.State = MaintenanceWaitingForWriter
	}
	m.statsMu.Unlock()
	if current == nil || !current.MaintenanceReceipt.Matches(head.Pending) {
		return true, nil
	}

	if _, err := m.manifestLog.ClearMaintenance(ctx, head.Pending.ID, head.Pending.Epoch, head.Pending.Generation, m.fenceToken); err != nil {
		return false, err
	}
	m.statsMu.Lock()
	if m.currentStats != nil {
		m.currentStats.State = MaintenanceIdle
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

func (m *Maintenance) recordCompaction(job compactionJob, err error) {
	if err == nil {
		m.statsMu.Lock()
		if m.currentStats != nil {
			m.currentStats.SSTCompaction.Jobs++
			m.currentStats.SSTCompaction.InputSSTs += len(job.InputSSTs)
			m.currentStats.SSTCompaction.OutputSSTs += len(job.OutputSSTs)
			for _, sst := range job.OutputSSTs {
				m.currentStats.SSTCompaction.OutputBytes += sst.Bytes
			}
		}
		m.statsMu.Unlock()
	}
}

func (m *Maintenance) recordChangeFeed(stats ChangeFeedCleanupStats) {
	m.statsMu.Lock()
	if m.currentStats != nil {
		m.currentStats.ChangeFeedRetention = stats
	}
	m.statsMu.Unlock()
}
