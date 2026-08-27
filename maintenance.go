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

const (
	// defaultCheckpointReplayPages bounds cold state replay to roughly 64
	// immutable page reads between snapshots.
	defaultCheckpointReplayPages uint64 = 64
	// defaultCheckpointReplayBytes also bounds unusually large replay pages.
	defaultCheckpointReplayBytes       uint64 = 32 << 20
	defaultMaintenanceIdleInterval            = 5 * time.Second
	maintenanceActiveInterval                 = 100 * time.Millisecond
	defaultReclaimDeleteConcurrency           = 4
	defaultSSTReclaimPollInterval             = time.Second
	defaultChangeReclaimPollInterval          = 5 * time.Second
	defaultManifestReclaimPollInterval        = time.Minute
	defaultReclaimIdleMaxInterval             = 5 * time.Minute
	maxReclaimDeleteConcurrency               = 256
	maxReclaimObjectsPerPass                  = 4096
)

// MaintenanceOptions configures one fenced maintenance owner for a DB.
type MaintenanceOptions struct {
	// IdleInterval is the delay between scans when no maintenance work is
	// immediately available. Known work and pending commands use a shorter
	// internal active cadence. Zero selects the production default.
	IdleInterval time.Duration

	// SSTCompaction controls L0 and leveled SST compaction. Zero fields select
	// production defaults.
	SSTCompaction SSTCompactionOptions

	// ManifestCheckpoint controls the amount of manifest replay work retained
	// between snapshots. Checkpointing is always enabled.
	ManifestCheckpoint ManifestCheckpointOptions

	// ChangeFeedRetention is nil by default, preserving change-feed history
	// indefinitely. When configured, maintenance retires and deletes old feed
	// batches without affecting KV state.
	ChangeFeedRetention *ChangeFeedRetentionOptions

	// Reclamation controls the independently scheduled SST, change-feed, and
	// manifest-metadata physical deletion lanes. Zero fields select defaults.
	Reclamation ReclamationOptions

	// OnCycle is called synchronously by Run after each successful cycle.
	OnCycle func(MaintenanceStats)
	// OnReclamationCycle is called independently after a successful bounded
	// physical deletion pass.
	OnReclamationCycle func(ReclamationCycleStats)
	// OnError is called synchronously by Run when a cycle fails with a
	// recoverable error.
	OnError func(error)
}

// ReclamationOptions controls physical deletion without changing logical
// visibility or retention floors.
type ReclamationOptions struct {
	// MaxConcurrentDeletes is shared by all physical deletion lanes.
	MaxConcurrentDeletes int

	SST        DeleterOptions
	ChangeFeed DeleterOptions
	Manifest   ManifestDeleterOptions
}

// DeleterOptions controls one independently paced physical deletion lane.
type DeleterOptions struct {
	// PollInterval is the active-work and initial error-retry cadence. Empty
	// lanes and repeated failures back off to a bounded internal safety
	// interval, while newly created plans and known deadlines can wake
	// reclamation earlier.
	PollInterval time.Duration
	// MaxObjectsPerPass bounds normal work. One immutable SST retirement plan
	// can exceed it because a plan is completed atomically and is independently
	// bounded by MaxRetiredObjectsPerEntry.
	MaxObjectsPerPass int
}

// ManifestDeleterOptions controls the shared snapshot and manifest-page lane.
type ManifestDeleterOptions struct {
	DeleterOptions
	// AuditInterval controls low-frequency orphan object discovery.
	AuditInterval time.Duration
}

// SSTCompactionOptions controls the workload and resource tradeoffs of L0 and
// leveled SST compaction. Zero fields select production defaults.
type SSTCompactionOptions struct {
	// ReadConcurrency bounds concurrent input-SST reads within one job.
	ReadConcurrency int
	// L0TriggerSSTs starts L0 compaction at this many files.
	L0TriggerSSTs int
	// BaseLevelBytes is the target size of L1.
	BaseLevelBytes int64
	// LevelGrowthFactor scales the target size of each successive level. It
	// must be at least 2.
	LevelGrowthFactor int
	// MaxInputSSTsPerJob bounds the inputs and retirement records in one job.
	MaxInputSSTsPerJob int
	// MaxInputBytesPerJob softly bounds source and destination bytes in one
	// job. One indivisible oversized plan may exceed this value.
	MaxInputBytesPerJob int64
	// TargetSSTBytes is the approximate output-file size. Encoding settings
	// come from DBOptions.SSTOutput.
	TargetSSTBytes int64
}

// ManifestCheckpointOptions controls when maintenance snapshots manifest
// state. A checkpoint becomes eligible when either target is reached. Zero
// fields select production defaults.
type ManifestCheckpointOptions struct {
	TargetReplayPages uint64
	TargetReplayBytes uint64
}

// ChangeFeedRetentionOptions controls removal of old change-feed history. It is
// enabled only when MaintenanceOptions.ChangeFeedRetention is non-nil.
type ChangeFeedRetentionOptions struct {
	// RetainFor is the minimum age retained. Zero selects seven days.
	RetainFor time.Duration
}

type maintenanceOptions struct {
	idleInterval        time.Duration
	sstCompaction       SSTCompactionOptions
	manifestCheckpoint  ManifestCheckpointOptions
	changeFeedRetention *changeFeedRetentionOptions
	onCycle             func(MaintenanceStats)
	onReclamationCycle  func(ReclamationCycleStats)
	onError             func(error)
	reclamation         ReclamationOptions
}

type changeFeedRetentionOptions struct {
	retainFor             time.Duration
	minimumHistoryEntries uint64
	deleteGracePeriod     time.Duration
}

// ChangeFeedCleanupStats describes change-feed retention work completed in one
// cleanup pass.
type ChangeFeedCleanupStats struct {
	EntriesRetired  int
	BatchesPlanned  int
	BatchesDeleted  int
	BlockedRetained int
	FailedDeletes   int
	Duration        time.Duration
}

// ReclamationFamily identifies one independently scheduled physical deletion
// lane.
type ReclamationFamily string

const (
	ReclamationSST        ReclamationFamily = "sst"
	ReclamationChangeFeed ReclamationFamily = "change_feed"
	ReclamationManifest   ReclamationFamily = "manifest"
)

// ReclamationCycleStats reports one bounded physical deletion pass.
type ReclamationCycleStats struct {
	Family     ReclamationFamily
	SST        SSTCleanupStats
	ChangeFeed ChangeFeedCleanupStats
	Manifest   ManifestCleanupStats
	Duration   time.Duration
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
	Jobs       int
	InputSSTs  int
	OutputSSTs int
	// OutputBytes is the total size of the SSTs the cycle's jobs published. It
	// includes files a metadata-only job relocated between levels, so it is not
	// the amount of data compaction actually moved through the object store.
	OutputBytes int64
	// MovedJobs and MovedBytes are the metadata-only subset. A plan whose
	// sources do not overlap each other and overlap nothing in the destination
	// publishes the same immutable files at a new level. They are never
	// rewritten or re-uploaded, but may be read for checksum verification; that
	// cost is included in ReadBytes.
	MovedJobs  int
	MovedBytes int64
	// ReadBytes is what compaction pulled from the object store: the inputs of
	// every rewrite, plus the sources of any move that was checksum-verified.
	// It is the GET side of the cost; RewrittenBytes is the PUT side.
	ReadBytes int64
	// RewrittenBytes is what compaction genuinely wrote, and the figure that
	// tracks object-store cost and write amplification. Reading OutputBytes
	// instead overstates both, by the whole of MovedBytes.
	RewrittenBytes int64
}

// SSTCleanupStats describes durable retirement handoff and bounded physical
// deletion work completed in one maintenance cycle.
type SSTCleanupStats struct {
	SSTsPlanned    int
	PlansPrepared  int
	PlansScanned   int
	PlansCompleted int
	DeleteAttempts int
	SSTsDeleted    int
	DeferredPlans  int
	Failures       int
}

// ManifestCheckpointStats describes a manifest checkpoint staged in one cycle.
type ManifestCheckpointStats struct {
	Staged      bool
	ReplayPages uint64
	ReplayBytes uint64
}

// ManifestSnapshotCleanupStats describes bounded snapshot-retirement work
// completed in one maintenance cycle. Snapshot deletion is delayed long enough
// for already-loaded manifest views to expire.
type ManifestSnapshotCleanupStats struct {
	SnapshotsMarked  int
	DeleteAttempts   int
	SnapshotsDeleted int
	Protected        int
	Deferred         int
	Failures         int
	MarkersScanned   int
	MarkersCleared   int
	ObjectsScanned   int
	Duration         time.Duration
}

// ManifestPageCleanupStats describes bounded page discovery, quarantine, and
// physical reclamation.
type ManifestPageCleanupStats struct {
	PlansPrepared    int
	PlansScanned     int
	PlansCompleted   int
	PagesMarked      int
	PagesDeleted     int
	Protected        int
	Deferred         int
	Failures         int
	MarkersScanned   int
	MarkersCleared   int
	ObjectsScanned   int
	DeleteAttempts   int
	ReachabilityGETs int
	Duration         time.Duration
}

// ManifestCleanupStats combines snapshot and manifest-page work from their
// shared physical deletion lane.
type ManifestCleanupStats struct {
	Snapshots ManifestSnapshotCleanupStats
	Pages     ManifestPageCleanupStats
}

// MaintenanceTask identifies the expensive primary work selected for a cycle.
type MaintenanceTask uint8

const (
	MaintenanceTaskNone MaintenanceTask = iota
	MaintenanceTaskSSTCompaction
	MaintenanceTaskManifestCheckpoint
)

func (task MaintenanceTask) String() string {
	switch task {
	case MaintenanceTaskNone:
		return "none"
	case MaintenanceTaskSSTCompaction:
		return "sst_compaction"
	case MaintenanceTaskManifestCheckpoint:
		return "manifest_checkpoint"
	default:
		return fmt.Sprintf("MaintenanceTask(%d)", task)
	}
}

// CompactionBlocked describes a level that has compaction work waiting but
// could not produce a plan. A blocked level makes no progress while the
// scheduler keeps finding work elsewhere, so it is reported explicitly rather
// than showing up as an absent candidate.
type CompactionBlocked struct {
	SourceLevel uint32
	SSTCount    int
	// Critical repeats L0's depth signal, which is a property of the level
	// rather than of whether it could be planned.
	Critical bool
	Reason   string
}

// MaintenanceScheduleStats explains the primary-work decision made in one
// cycle. CompactionSourceLevel is meaningful when Selected is compaction.
type MaintenanceScheduleStats struct {
	Selected              MaintenanceTask
	CompactionSourceLevel uint32
	CompactionWorkUnits   uint32
	CompactionCritical    bool
	CheckpointEligible    bool
	CheckpointUrgent      bool
	ReplayPages           uint64
	ReplayBytes           uint64
	// Blocked lists levels that had work but could not be planned this cycle.
	Blocked []CompactionBlocked
}

// MaintenanceStats describes work performed by one bounded RunOnce cycle.
// Command-producing work is not visible until the writer applies it.
type MaintenanceStats struct {
	State               MaintenanceState
	Scheduling          MaintenanceScheduleStats
	SSTCompaction       SSTCompactionStats
	SSTCleanup          SSTCleanupStats
	ChangeFeedRetention ChangeFeedCleanupStats
	ManifestCheckpoint  ManifestCheckpointStats
	ManifestCleanup     ManifestCleanupStats
	Duration            time.Duration
}

// DefaultMaintenanceOptions returns safe defaults. Compaction, checkpoints,
// and SST sweeping are enabled; change-feed retention remains disabled.
func DefaultMaintenanceOptions() MaintenanceOptions {
	compaction := defaultCompactorOptions()
	return MaintenanceOptions{
		IdleInterval: defaultMaintenanceIdleInterval,
		SSTCompaction: SSTCompactionOptions{
			ReadConcurrency:     compaction.InputReadParallelism,
			L0TriggerSSTs:       compaction.Trigger.L0SSTCount,
			BaseLevelBytes:      compaction.Trigger.BaseLevelBytes,
			LevelGrowthFactor:   compaction.Trigger.LevelSizeMultiplier,
			MaxInputSSTsPerJob:  compaction.Trigger.MaxInputSSTs,
			MaxInputBytesPerJob: compaction.Trigger.MaxInputBytes,
			TargetSSTBytes:      compaction.Output.TargetSSTBytes,
		},
		ManifestCheckpoint: ManifestCheckpointOptions{
			TargetReplayPages: defaultCheckpointReplayPages,
			TargetReplayBytes: defaultCheckpointReplayBytes,
		},
		Reclamation: ReclamationOptions{
			MaxConcurrentDeletes: defaultReclaimDeleteConcurrency,
			SST: DeleterOptions{
				PollInterval:      defaultSSTReclaimPollInterval,
				MaxObjectsPerPass: defaultSSTDeletionPlanBatchSize,
			},
			ChangeFeed: DeleterOptions{
				PollInterval:      defaultChangeReclaimPollInterval,
				MaxObjectsPerPass: defaultChangeFeedSweepBatchSize,
			},
			Manifest: ManifestDeleterOptions{
				DeleterOptions: DeleterOptions{
					PollInterval:      defaultManifestReclaimPollInterval,
					MaxObjectsPerPass: defaultSnapshotDeleteBatchSize,
				},
				AuditInterval: defaultSnapshotOrphanAuditEvery,
			},
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
	sstGC       *sstCleaner
	snapshotGC  *snapshotCleaner
	pageGC      *manifestPageCleaner
	deleter     *limitedObjectDeleter
	fenceToken  *manifest.FenceToken

	lifecycleMu   sync.Mutex
	closeMu       sync.Mutex
	runCancel     context.CancelFunc
	loopWG        sync.WaitGroup
	activeRuns    sync.WaitGroup
	runGate       chan struct{}
	reclaimGates  map[ReclamationFamily]chan struct{}
	reclaimWake   map[ReclamationFamily]chan struct{}
	enginesClosed bool
	callbackMu    sync.Mutex

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

	deleter := newLimitedObjectDeleter(store, normalized.reclamation.MaxConcurrentDeletes)
	m := &Maintenance{
		manifestLog: manifestLog,
		opts:        normalized,
		sstGC: newSSTCleaner(store, sstCleanerOptions{
			DeleteBatchSize: normalized.reclamation.SST.MaxObjectsPerPass,
			Deleter:         deleter,
		}),
		snapshotGC: newSnapshotCleaner(store, manifestLog, snapshotCleanerOptions{
			DeleteBatchSize:  normalized.reclamation.Manifest.MaxObjectsPerPass,
			SweepInterval:    normalized.reclamation.Manifest.PollInterval,
			OrphanAuditEvery: normalized.reclamation.Manifest.AuditInterval,
			Deleter:          deleter,
		}),
		pageGC: newManifestPageCleaner(store, manifestLog, manifestPageCleanerOptions{
			DeleteBatchSize:  normalized.reclamation.Manifest.MaxObjectsPerPass,
			SweepInterval:    normalized.reclamation.Manifest.PollInterval,
			OrphanAuditEvery: normalized.reclamation.Manifest.AuditInterval,
			Deleter:          deleter,
		}),
		deleter:    deleter,
		fenceToken: token,
		runGate:    make(chan struct{}, 1),
		reclaimGates: map[ReclamationFamily]chan struct{}{
			ReclamationSST:        make(chan struct{}, 1),
			ReclamationChangeFeed: make(chan struct{}, 1),
			ReclamationManifest:   make(chan struct{}, 1),
		},
		reclaimWake: map[ReclamationFamily]chan struct{}{
			ReclamationSST:        make(chan struct{}, 1),
			ReclamationChangeFeed: make(chan struct{}, 1),
			ReclamationManifest:   make(chan struct{}, 1),
		},
		sstOutput: sstOutput,
	}

	compactorOpts := m.compactorOptions()
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
	if opts.IdleInterval < 0 {
		return maintenanceOptions{}, fmt.Errorf("%w: negative idle interval", ErrInvalidMaintenanceOptions)
	}
	if opts.IdleInterval == 0 {
		opts.IdleInterval = defaults.IdleInterval
	}
	compaction, err := normalizeSSTCompactionOptions(opts.SSTCompaction, defaults.SSTCompaction)
	if err != nil {
		return maintenanceOptions{}, err
	}
	checkpoint := opts.ManifestCheckpoint
	if checkpoint.TargetReplayPages == 0 {
		checkpoint.TargetReplayPages = defaults.ManifestCheckpoint.TargetReplayPages
	}
	if checkpoint.TargetReplayBytes == 0 {
		checkpoint.TargetReplayBytes = defaults.ManifestCheckpoint.TargetReplayBytes
	}
	reclamation, err := normalizeReclamationOptions(opts.Reclamation, defaults.Reclamation)
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
		idleInterval:        opts.IdleInterval,
		sstCompaction:       compaction,
		manifestCheckpoint:  checkpoint,
		changeFeedRetention: retention,
		onCycle:             opts.OnCycle,
		onReclamationCycle:  opts.OnReclamationCycle,
		onError:             opts.OnError,
		reclamation:         reclamation,
	}, nil
}

func normalizeReclamationOptions(opts, defaults ReclamationOptions) (ReclamationOptions, error) {
	if opts.MaxConcurrentDeletes < 0 ||
		opts.SST.PollInterval < 0 || opts.SST.MaxObjectsPerPass < 0 ||
		opts.ChangeFeed.PollInterval < 0 || opts.ChangeFeed.MaxObjectsPerPass < 0 ||
		opts.Manifest.PollInterval < 0 || opts.Manifest.MaxObjectsPerPass < 0 || opts.Manifest.AuditInterval < 0 {
		return ReclamationOptions{}, fmt.Errorf("%w: negative reclamation option", ErrInvalidMaintenanceOptions)
	}
	if opts.MaxConcurrentDeletes > maxReclaimDeleteConcurrency {
		return ReclamationOptions{}, fmt.Errorf("%w: max concurrent deletes=%d exceeds %d",
			ErrInvalidMaintenanceOptions, opts.MaxConcurrentDeletes, maxReclaimDeleteConcurrency)
	}
	if opts.MaxConcurrentDeletes == 0 {
		opts.MaxConcurrentDeletes = defaults.MaxConcurrentDeletes
	}
	var err error
	if opts.SST, err = normalizeDeleterOptions("SST", opts.SST, defaults.SST); err != nil {
		return ReclamationOptions{}, err
	}
	if opts.ChangeFeed, err = normalizeDeleterOptions("change-feed", opts.ChangeFeed, defaults.ChangeFeed); err != nil {
		return ReclamationOptions{}, err
	}
	manifestOpts, err := normalizeDeleterOptions("manifest", opts.Manifest.DeleterOptions, defaults.Manifest.DeleterOptions)
	if err != nil {
		return ReclamationOptions{}, err
	}
	opts.Manifest.DeleterOptions = manifestOpts
	if opts.Manifest.AuditInterval == 0 {
		opts.Manifest.AuditInterval = defaults.Manifest.AuditInterval
	}
	return opts, nil
}

func normalizeDeleterOptions(name string, opts, defaults DeleterOptions) (DeleterOptions, error) {
	if opts.MaxObjectsPerPass > maxReclaimObjectsPerPass {
		return DeleterOptions{}, fmt.Errorf("%w: %s max objects per pass=%d exceeds %d",
			ErrInvalidMaintenanceOptions, name, opts.MaxObjectsPerPass, maxReclaimObjectsPerPass)
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = defaults.PollInterval
	}
	if opts.MaxObjectsPerPass == 0 {
		opts.MaxObjectsPerPass = defaults.MaxObjectsPerPass
	}
	return opts, nil
}

func normalizeSSTCompactionOptions(opts, defaults SSTCompactionOptions) (SSTCompactionOptions, error) {
	if opts.ReadConcurrency < 0 || opts.L0TriggerSSTs < 0 ||
		opts.BaseLevelBytes < 0 || opts.LevelGrowthFactor < 0 || opts.MaxInputSSTsPerJob < 0 || opts.MaxInputBytesPerJob < 0 ||
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
	if opts.BaseLevelBytes == 0 {
		opts.BaseLevelBytes = defaults.BaseLevelBytes
	}
	if opts.LevelGrowthFactor == 0 {
		opts.LevelGrowthFactor = defaults.LevelGrowthFactor
	}
	if opts.MaxInputSSTsPerJob == 0 {
		opts.MaxInputSSTsPerJob = defaults.MaxInputSSTsPerJob
	}
	if opts.MaxInputBytesPerJob == 0 {
		opts.MaxInputBytesPerJob = defaults.MaxInputBytesPerJob
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
		deleteGracePeriod:     defaults.SweepGracePeriod,
	}, nil
}

func (m *Maintenance) compactorOptions() compactorOptions {
	p := m.opts.sstCompaction
	return compactorOptions{
		OwnerID:              m.fenceToken.Owner,
		InputReadParallelism: p.ReadConcurrency,
		Trigger: compactionTriggerOptions{
			L0SSTCount:          p.L0TriggerSSTs,
			BaseLevelBytes:      p.BaseLevelBytes,
			LevelSizeMultiplier: p.LevelGrowthFactor,
			MaxInputSSTs:        p.MaxInputSSTsPerJob,
			MaxInputBytes:       p.MaxInputBytesPerJob,
		},
		Output: compactionOutputOptions{
			TargetSSTBytes:  p.TargetSSTBytes,
			BloomBitsPerKey: m.sstOutput.BloomBitsPerKey,
			BlockBytes:      m.sstOutput.BlockBytes,
			Compression:     m.sstOutput.Compression,
		},
		OnPlanningBlocked: m.recordPlanningBlocked,
		OnCompactionEnd:   m.recordCompaction,
	}
}

func (m *Maintenance) changeFeedOptions() changeFeedCleanerOptions {
	p := m.opts.changeFeedRetention
	return changeFeedCleanerOptions{
		RetentionPeriod:            p.retainFor,
		KeepAtLeastManifestEntries: p.minimumHistoryEntries,
		SweepBatchSize:             m.opts.reclamation.ChangeFeed.MaxObjectsPerPass,
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

	var reclaimWG sync.WaitGroup
	for _, family := range []ReclamationFamily{ReclamationSST, ReclamationChangeFeed, ReclamationManifest} {
		family := family
		reclaimWG.Add(1)
		m.loopWG.Add(1)
		go func() {
			defer reclaimWG.Done()
			defer m.loopWG.Done()
			m.runReclamationLoop(runCtx, family)
		}()
	}

	defer func() {
		cancel()
		reclaimWG.Wait()
		m.lifecycleMu.Lock()
		m.runCancel = nil
		m.running.Store(false)
		m.lifecycleMu.Unlock()
		m.loopWG.Done()
	}()

	for {
		stats, err := m.runControlOnce(runCtx)
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
				m.reportError(err)
			} else {
				slog.Error("isledb: maintenance cycle failed", "error", err)
			}
		} else if m.opts.onCycle != nil {
			m.reportControlCycle(stats)
		}

		delay := m.opts.idleInterval
		if stats.State == MaintenanceWaitingForWriter {
			delay = min(delay, maintenanceActiveInterval)
		}
		timer := time.NewTimer(delay)
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

// RunOnce performs one deterministic bounded control pass followed by one
// pass from every physical reclamation lane. Run uses the same operations but
// schedules the physical passes independently so slow deletes cannot block
// compaction, checkpointing, or logical feed retention.
func (m *Maintenance) RunOnce(ctx context.Context) (MaintenanceStats, error) {
	stats, err := m.runControlOnce(ctx)
	if err != nil {
		return stats, err
	}

	sst, sstErr := m.runReclamationOnce(ctx, ReclamationSST)
	mergeSSTCleanupStats(&stats.SSTCleanup, sst.SST)
	change, changeErr := m.runReclamationOnce(ctx, ReclamationChangeFeed)
	stats.ChangeFeedRetention = mergeChangeFeedCleanupStats(stats.ChangeFeedRetention, change.ChangeFeed)
	metadata, metadataErr := m.runReclamationOnce(ctx, ReclamationManifest)
	mergeManifestSnapshotCleanupStats(&stats.ManifestCleanup.Snapshots, metadata.Manifest.Snapshots)
	mergeManifestPageCleanupStats(&stats.ManifestCleanup.Pages, metadata.Manifest.Pages)
	stats.Duration += sst.Duration + change.Duration + metadata.Duration
	return stats, errors.Join(sstErr, changeErr, metadataErr)
}

// runControlOnce performs one serialized logical/control cycle.
func (m *Maintenance) runControlOnce(ctx context.Context) (MaintenanceStats, error) {
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

	decision := maintenanceDecision{}
	selected, err := m.compactor.runSelected(ctx, func(current *manifest.Current, candidates []compactionCandidate) *compactionCandidate {
		checkpoint := calculateCheckpointPressure(current, m.opts.manifestCheckpoint)
		schedulerState := manifest.MaintenanceSchedulerState{}
		if current != nil {
			schedulerState = current.MaintenanceScheduler
		}
		compaction := selectCompactionCandidate(candidates, schedulerState)
		decision = selectMaintenancePrimary(compaction, checkpoint, schedulerState)
		return decision.compaction
	})
	if err != nil {
		return m.completeCycleStats(start), fmt.Errorf("compaction planning: %w", err)
	}
	m.recordScheduling(decision)
	if selected != nil {
		return m.completeCycleStats(start), nil
	}
	if decision.task == MaintenanceTaskManifestCheckpoint {
		if err := m.checkpointIfNeeded(ctx); err != nil {
			return m.completeCycleStats(start), fmt.Errorf("checkpoint: %w", err)
		}
		if m.hasStagedCommand() {
			return m.completeCycleStats(start), nil
		}
	}
	if m.changeFeed != nil {
		cleanup, err := m.changeFeed.runControlOnce(ctx)
		m.recordChangeFeed(cleanup)
		if err != nil {
			return m.completeCycleStats(start), fmt.Errorf("change-feed retention: %w", err)
		}
		if m.hasStagedCommand() {
			return m.completeCycleStats(start), nil
		}
	}
	return m.completeCycleStats(start), nil
}

func (m *Maintenance) runReclamationLoop(ctx context.Context, family ReclamationFamily) {
	interval := m.reclamationInterval(family)
	idleDelay := interval
	errorDelay := interval
	for {
		stats, schedule, err := m.runReclamationPass(ctx, family)
		if err != nil {
			if ctx.Err() != nil || m.closed.Load() {
				return
			}
			m.reportError(fmt.Errorf("%s reclamation: %w", family, err))
		} else if m.opts.onReclamationCycle != nil {
			m.reportReclamationCycle(stats)
		}

		var delay time.Duration
		if err == nil {
			errorDelay = interval
			delay, idleDelay = nextReclamationDelay(
				interval, idleDelay, schedule.observedAt, schedule.nextDue, schedule.idle)
		} else {
			idleDelay = interval
			delay, errorDelay = nextReclamationErrorDelay(
				interval, errorDelay, schedule.observedAt, schedule.nextDue)
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-m.reclaimWake[family]:
			stopMaintenanceTimer(timer)
			idleDelay = interval
			errorDelay = interval
		case <-ctx.Done():
			stopMaintenanceTimer(timer)
			return
		}
	}
}

func (m *Maintenance) notifyReclamation(family ReclamationFamily) {
	if m == nil {
		return
	}
	wake := m.reclaimWake[family]
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (m *Maintenance) reclamationInterval(family ReclamationFamily) time.Duration {
	switch family {
	case ReclamationSST:
		return m.opts.reclamation.SST.PollInterval
	case ReclamationChangeFeed:
		return m.opts.reclamation.ChangeFeed.PollInterval
	case ReclamationManifest:
		return m.opts.reclamation.Manifest.PollInterval
	default:
		return m.opts.idleInterval
	}
}

func (m *Maintenance) runReclamationOnce(ctx context.Context, family ReclamationFamily) (stats ReclamationCycleStats, err error) {
	stats, _, err = m.runReclamationPass(ctx, family)
	return stats, err
}

func (m *Maintenance) runReclamationPass(
	ctx context.Context,
	family ReclamationFamily,
) (stats ReclamationCycleStats, schedule reclamationLaneSchedule, err error) {
	stats.Family = family
	start := time.Now()
	defer func() { stats.Duration = time.Since(start) }()
	if err := m.beginReclamation(ctx, family); err != nil {
		return stats, schedule, err
	}
	defer m.finishReclamation(family)

	switch family {
	case ReclamationSST:
		if m.sstGC == nil {
			return stats, reclamationLaneSchedule{observedAt: time.Now().UTC(), idle: true}, nil
		}
		work, schedule, err := m.sstGC.runScheduledOnce(ctx)
		stats.SST = publicSSTCleanupStats(work)
		return stats, schedule, err
	case ReclamationChangeFeed:
		if m.changeFeed == nil {
			return stats, reclamationLaneSchedule{observedAt: time.Now().UTC(), idle: true}, nil
		}
		stats.ChangeFeed, schedule, err = m.changeFeed.runScheduledReclaimOnce(ctx, m.deleter)
		return stats, schedule, err
	case ReclamationManifest:
		var snapshotErr, pageErr error
		schedule = reclamationLaneSchedule{idle: true}
		if m.snapshotGC != nil {
			var snapshotSchedule reclamationLaneSchedule
			stats.Manifest.Snapshots, snapshotSchedule, snapshotErr = m.snapshotGC.runScheduledOnce(ctx)
			schedule = mergeReclamationLaneSchedules(schedule, snapshotSchedule)
			if cancelErr := reclamationCancellation(ctx, snapshotErr); cancelErr != nil {
				schedule.idle = false
				return stats, schedule, cancelErr
			}
		}
		if m.pageGC != nil {
			var pageSchedule reclamationLaneSchedule
			stats.Manifest.Pages, pageSchedule, pageErr = m.pageGC.runScheduledOnce(ctx)
			schedule = mergeReclamationLaneSchedules(schedule, pageSchedule)
		}
		if schedule.observedAt.IsZero() {
			schedule.observedAt = time.Now().UTC()
		}
		manifestErr := errors.Join(snapshotErr, pageErr)
		if manifestErr != nil {
			schedule.idle = false
		}
		return stats, schedule, manifestErr
	default:
		return stats, schedule, fmt.Errorf("unknown reclamation family %q", family)
	}
}

func (m *Maintenance) beginReclamation(ctx context.Context, family ReclamationFamily) error {
	if m.closed.Load() {
		return ErrMaintenanceClosed
	}
	gate := m.reclaimGates[family]
	if gate == nil {
		return fmt.Errorf("unknown reclamation family %q", family)
	}
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.lifecycleMu.Lock()
	if m.closed.Load() {
		m.lifecycleMu.Unlock()
		<-gate
		return ErrMaintenanceClosed
	}
	m.activeRuns.Add(1)
	m.lifecycleMu.Unlock()
	return nil
}

func (m *Maintenance) finishReclamation(family ReclamationFamily) {
	m.activeRuns.Done()
	<-m.reclaimGates[family]
}

func (m *Maintenance) reportError(err error) {
	if m.opts.onError == nil {
		slog.Error("isledb: maintenance operation failed", "error", err)
		return
	}
	m.callbackMu.Lock()
	m.opts.onError(err)
	m.callbackMu.Unlock()
}

func (m *Maintenance) reportControlCycle(stats MaintenanceStats) {
	m.callbackMu.Lock()
	m.opts.onCycle(stats)
	m.callbackMu.Unlock()
}

func (m *Maintenance) reportReclamationCycle(stats ReclamationCycleStats) {
	m.callbackMu.Lock()
	m.opts.onReclamationCycle(stats)
	m.callbackMu.Unlock()
}

func publicSSTCleanupStats(stats sstCleanupWorkStats) SSTCleanupStats {
	return SSTCleanupStats{
		SSTsPlanned:    stats.TargetsPlanned,
		PlansPrepared:  stats.PlansPrepared,
		PlansScanned:   stats.PlansScanned,
		PlansCompleted: stats.PlansDeleted,
		DeleteAttempts: stats.Attempted,
		SSTsDeleted:    stats.Deleted,
		DeferredPlans:  stats.Deferred,
		Failures:       stats.Failed,
	}
}

func mergeSSTCleanupStats(dst *SSTCleanupStats, src SSTCleanupStats) {
	if dst == nil {
		return
	}
	dst.SSTsPlanned += src.SSTsPlanned
	dst.PlansPrepared += src.PlansPrepared
	dst.PlansScanned += src.PlansScanned
	dst.PlansCompleted += src.PlansCompleted
	dst.DeleteAttempts += src.DeleteAttempts
	dst.SSTsDeleted += src.SSTsDeleted
	dst.DeferredPlans += src.DeferredPlans
	dst.Failures += src.Failures
}

func (m *Maintenance) checkpointIfNeeded(ctx context.Context) error {
	current, err := m.manifestLog.ReadCurrentData(ctx)
	if err != nil {
		return err
	}
	if !calculateCheckpointPressure(current, m.opts.manifestCheckpoint).eligible {
		return nil
	}

	checkpoint, err := m.manifestLog.PrepareCheckpoint(ctx)
	if err != nil {
		return err
	}
	if err := m.stageCommand(ctx, manifest.MaintenanceCommand{
		Kind:       manifest.MaintenanceCommandCheckpoint,
		Scheduling: manifest.MaintenanceScheduling{},
		Checkpoint: &checkpoint,
	}); err != nil {
		if m.snapshotGC == nil {
			return err
		}
		marked, markErr := m.snapshotGC.markAbandonedCandidate(ctx, current, checkpoint.Snapshot, "checkpoint_stage_failed")
		if marked {
			m.recordManifestSnapshotMarked()
		}
		return errors.Join(err, markErr)
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

func (m *Maintenance) recordScheduling(decision maintenanceDecision) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	if m.currentStats == nil {
		return
	}
	stats := &m.currentStats.Scheduling
	stats.Selected = decision.task
	stats.CheckpointEligible = decision.checkpoint.eligible
	stats.CheckpointUrgent = decision.checkpoint.urgent
	stats.ReplayPages = decision.checkpoint.replayPages
	stats.ReplayBytes = decision.checkpoint.replayBytes
	if decision.compaction != nil {
		stats.CompactionSourceLevel = decision.compaction.plan.sourceLevel
		stats.CompactionWorkUnits = decision.compaction.workUnits
		stats.CompactionCritical = decision.compaction.critical
	}
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
	changeFeedSafetyMargin := defaultChangeFeedDeletionSafetyMargin
	if m.changeFeed != nil {
		changeFeedSafetyMargin = m.changeFeed.opts.DeletionSafetyMargin
	}
	_, err = recordChangeFeedDeletionPlan(
		ctx,
		m.compactor.store,
		current,
		head.Pending,
		current.MaintenanceReceipt,
		time.Now().UTC(),
		changeFeedSafetyMargin,
	)
	if err != nil {
		return false, fmt.Errorf("publish change-feed deletion plan: %w", err)
	}
	changeFeedPlanAvailable := head.Pending.Kind == manifest.MaintenanceCommandChangeFeedFloor &&
		head.Pending.ChangeFeedFloor != nil && len(head.Pending.ChangeFeedFloor.DeletionTargets) > 0 &&
		current.MaintenanceReceipt.Status == manifest.MaintenanceStatusApplied
	if changeFeedPlanAvailable && m.changeFeed != nil {
		m.changeFeed.planAvailable()
		m.notifyReclamation(ReclamationChangeFeed)
	}
	if m.snapshotGC != nil {
		marked, err := m.snapshotGC.markCheckpointOutcome(ctx, current, head.Pending, current.MaintenanceReceipt)
		if err != nil {
			return false, fmt.Errorf("record checkpoint snapshot retirement: %w", err)
		}
		if marked {
			m.recordManifestSnapshotMarked()
		}
	}
	if m.pageGC != nil {
		cleanup, err := m.pageGC.markCommandOutcome(ctx, current, head.Pending, current.MaintenanceReceipt)
		m.recordManifestPageCleanup(cleanup)
		if err != nil {
			return false, fmt.Errorf("record manifest page retirement plan: %w", err)
		}
		if cleanup.PlansPrepared > 0 {
			m.notifyReclamation(ReclamationManifest)
		}
	}
	if m.sstGC != nil {
		cleanup, err := m.sstGC.markCommandOutcome(ctx, current, head.Pending, current.MaintenanceReceipt)
		m.recordSSTCleanup(cleanup)
		if err != nil {
			return false, fmt.Errorf("record SST retirement plan: %w", err)
		}
		if cleanup.TargetsPlanned > 0 {
			m.notifyReclamation(ReclamationSST)
		}
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
	defer m.statsMu.Unlock()
	duration := time.Since(start)
	if m.currentStats == nil {
		return MaintenanceStats{Duration: duration}
	}
	m.currentStats.Duration = duration
	return *m.currentStats
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

func (m *Maintenance) recordPlanningBlocked(sourceLevel uint32, sstCount int, critical bool, err error) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	if m.currentStats == nil {
		return
	}
	m.currentStats.Scheduling.Blocked = append(m.currentStats.Scheduling.Blocked, CompactionBlocked{
		SourceLevel: sourceLevel,
		SSTCount:    sstCount,
		Critical:    critical,
		Reason:      err.Error(),
	})
}

func (m *Maintenance) recordCompaction(job compactionJob, err error) {
	if err != nil {
		return
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	if m.currentStats == nil {
		return
	}

	stats := &m.currentStats.SSTCompaction
	stats.Jobs++
	stats.InputSSTs += len(job.InputSSTs)
	stats.OutputSSTs += len(job.OutputSSTs)

	var bytes int64
	for _, sst := range job.OutputSSTs {
		bytes += sst.Bytes
	}
	stats.OutputBytes += bytes
	stats.ReadBytes += job.ReadBytes
	if job.MetadataOnly {
		stats.MovedJobs++
		stats.MovedBytes += bytes
		return
	}
	stats.RewrittenBytes += bytes
}

func (m *Maintenance) recordChangeFeed(stats ChangeFeedCleanupStats) {
	m.statsMu.Lock()
	if m.currentStats != nil {
		m.currentStats.ChangeFeedRetention = stats
	}
	m.statsMu.Unlock()
}

func (m *Maintenance) recordSSTCleanup(stats sstCleanupWorkStats) {
	m.statsMu.Lock()
	if m.currentStats != nil {
		cleanup := &m.currentStats.SSTCleanup
		cleanup.SSTsPlanned += stats.TargetsPlanned
		cleanup.PlansPrepared += stats.PlansPrepared
		cleanup.PlansScanned += stats.PlansScanned
		cleanup.PlansCompleted += stats.PlansDeleted
		cleanup.DeleteAttempts += stats.Attempted
		cleanup.SSTsDeleted += stats.Deleted
		cleanup.DeferredPlans += stats.Deferred
		cleanup.Failures += stats.Failed
	}
	m.statsMu.Unlock()
}

func (m *Maintenance) recordManifestSnapshotMarked() {
	m.statsMu.Lock()
	if m.currentStats != nil {
		m.currentStats.ManifestCleanup.Snapshots.SnapshotsMarked++
	}
	m.statsMu.Unlock()
}

func (m *Maintenance) recordManifestPageCleanup(stats ManifestPageCleanupStats) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	if m.currentStats != nil {
		mergeManifestPageCleanupStats(&m.currentStats.ManifestCleanup.Pages, stats)
	}
}
