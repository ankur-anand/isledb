package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	sstDeletionPlanPrefix           = "manifest/gc/sst/ready"
	sstDeletionPlanCanonicalPrefix  = "manifest/gc/sst/plans"
	sstDeletionPlanVersion          = 1
	sstDeletionPlanKind             = "sst_retirement"
	defaultSSTDeletionPlanScanLimit = 1024
	defaultSSTDeletionSafetyMargin  = time.Minute
	maxSSTDeletionPlanEncodedBytes  = 256 << 10
	defaultSSTDeletionPlanBatchSize = 128
	defaultSSTOrphanAuditEvery      = 24 * time.Hour
	defaultSSTOrphanGrace           = 24 * time.Hour
	defaultSSTOrphanScanLimit       = 1024
)

type sstCleanupWorkStats struct {
	Attempted            int
	Deleted              int
	Failed               int
	TargetsPlanned       int
	PlansPrepared        int
	PlansScanned         int
	PlansDeleted         int
	Deferred             int
	NextDue              time.Time
	OrphanPlansScanned   int
	OrphanObjectsScanned int
	OrphanCandidates     int
	OrphansDeleted       int
}

type sstDeletionPlanSource struct {
	CommandID  string `json:"command_id"`
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
}

type sstDeletionTarget struct {
	ID   string `json:"id"`
	Key  string `json:"key"`
	Size int64  `json:"size,omitempty"`
}

// sstDeletionPlan is the immutable handoff created while reconciling an
// applied compaction receipt. HEAD is not cleared until this object is durable.
type sstDeletionPlan struct {
	Version  int    `json:"version"`
	Kind     string `json:"kind"`
	PlanID   string `json:"plan_id"`
	Checksum string `json:"checksum"`

	Source sstDeletionPlanSource `json:"source"`

	AppliedAt     time.Time     `json:"applied_at"`
	ObservedAt    time.Time     `json:"observed_at"`
	PinnedViewAge time.Duration `json:"pinned_view_age_nanos"`
	SafetyMargin  time.Duration `json:"safety_margin_nanos"`
	NotBefore     time.Time     `json:"not_before"`

	TargetCount int                 `json:"target_count"`
	TargetBytes int64               `json:"target_bytes"`
	Targets     []sstDeletionTarget `json:"targets"`
}

type sstCleanerOptions struct {
	DeleteBatchSize  int
	PlanScanLimit    int
	SafetyMargin     time.Duration
	OrphanAuditEvery time.Duration
	OrphanGrace      time.Duration
	OrphanScanLimit  int
	ManifestLog      *manifest.Store
	Now              func() time.Time
	Deleter          objectDeleter
}

type sstCleaner struct {
	store  *blobstore.Store
	opts   sstCleanerOptions
	delete objectDeleter

	// Reclaim passes are serialized by Maintenance.reclaimGates. The iterator,
	// carry, cache, and seen generation are therefore owned by that lane.
	planIter       *blobstore.ListIterator
	pendingPlanKey string
	cache          *boundedPlanCache[sstDeletionPlan]
	seenRescan     uint64

	nextOrphanAudit time.Time
	orphanAudit     *sstOrphanAuditState

	// Control work only signals that a durable plan may have changed the first
	// ready key. It never waits for reclaim-lane network I/O. Missing or
	// coalescing this optimization is safe because periodic scans remain.
	rescan atomic.Uint64
}

type sstOrphanAuditState struct {
	protected  map[string]struct{}
	current    *manifest.Current
	planIter   *blobstore.ListIterator
	objectIter *blobstore.ListIterator
}

func defaultSSTCleanerOptions() sstCleanerOptions {
	return sstCleanerOptions{
		DeleteBatchSize:  defaultSSTDeletionPlanBatchSize,
		PlanScanLimit:    defaultSSTDeletionPlanScanLimit,
		SafetyMargin:     defaultSSTDeletionSafetyMargin,
		OrphanAuditEvery: defaultSSTOrphanAuditEvery,
		OrphanGrace:      defaultSSTOrphanGrace,
		OrphanScanLimit:  defaultSSTOrphanScanLimit,
		Now:              func() time.Time { return time.Now().UTC() },
	}
}

func newSSTCleaner(store *blobstore.Store, opts sstCleanerOptions) *sstCleaner {
	defaults := defaultSSTCleanerOptions()
	if opts.DeleteBatchSize <= 0 {
		opts.DeleteBatchSize = defaults.DeleteBatchSize
	}
	if opts.DeleteBatchSize > manifest.MaxRetiredObjectsPerEntry {
		opts.DeleteBatchSize = manifest.MaxRetiredObjectsPerEntry
	}
	if opts.PlanScanLimit <= 0 {
		opts.PlanScanLimit = defaults.PlanScanLimit
	}
	if opts.SafetyMargin < 0 {
		opts.SafetyMargin = 0
	} else if opts.SafetyMargin == 0 {
		opts.SafetyMargin = defaults.SafetyMargin
	}
	if opts.OrphanAuditEvery <= 0 {
		opts.OrphanAuditEvery = defaults.OrphanAuditEvery
	}
	if opts.OrphanGrace < 0 {
		opts.OrphanGrace = 0
	} else if opts.OrphanGrace == 0 {
		opts.OrphanGrace = defaults.OrphanGrace
	}
	if opts.OrphanScanLimit <= 0 {
		opts.OrphanScanLimit = defaults.OrphanScanLimit
	}
	if opts.Now == nil {
		opts.Now = defaults.Now
	}
	deleter := opts.Deleter
	if deleter == nil {
		deleter = store
	}
	return &sstCleaner{store: store, opts: opts, delete: deleter, cache: newDeletionPlanCache[sstDeletionPlan]()}
}

func (c *sstCleaner) markCommandOutcome(
	ctx context.Context,
	current *manifest.Current,
	command *manifest.MaintenanceCommand,
	receipt *manifest.MaintenanceReceipt,
) (sstCleanupWorkStats, error) {
	stats := sstCleanupWorkStats{}
	if current == nil || command == nil || receipt == nil || !receipt.Matches(command) ||
		receipt.Status != manifest.MaintenanceStatusApplied {
		return stats, nil
	}

	retired, ok := retiredObjectsFromMaintenanceCommand(command)
	if !ok || len(retired) == 0 {
		return stats, nil
	}
	plan, payload, err := buildSSTDeletionPlan(
		c.store,
		current,
		command,
		receipt,
		retired,
		c.opts.Now().UTC(),
		c.opts.SafetyMargin,
	)
	if err != nil {
		return stats, err
	}
	stats.TargetsPlanned = len(plan.Targets)
	created, err := storeSSTDeletionPlan(ctx, c.store, *plan, payload)
	if err != nil {
		return stats, err
	}
	c.planAvailable()
	if created {
		stats.PlansPrepared = 1
	}
	return stats, nil
}

func retiredObjectsFromMaintenanceCommand(command *manifest.MaintenanceCommand) ([]manifest.RetiredObject, bool) {
	if command == nil {
		return nil, false
	}
	switch command.Kind {
	case manifest.MaintenanceCommandCompaction:
		if command.Compaction == nil {
			return nil, false
		}
		return command.Compaction.RetiredObjects, true
	case manifest.MaintenanceCommandRemoveSSTables:
		if command.RemoveSSTables == nil {
			return nil, false
		}
		return command.RemoveSSTables.RetiredObjects, true
	default:
		return nil, false
	}
}

func buildSSTDeletionPlan(
	store *blobstore.Store,
	current *manifest.Current,
	command *manifest.MaintenanceCommand,
	receipt *manifest.MaintenanceReceipt,
	retired []manifest.RetiredObject,
	observedAt time.Time,
	safetyMargin time.Duration,
) (*sstDeletionPlan, []byte, error) {
	if current == nil || command == nil || receipt == nil || !receipt.Matches(command) ||
		receipt.Status != manifest.MaintenanceStatusApplied {
		return nil, nil, errors.New("SST deletion plan requires a matching applied receipt")
	}
	if len(retired) == 0 || len(retired) > manifest.MaxRetiredObjectsPerEntry {
		return nil, nil, fmt.Errorf("invalid SST deletion target count=%d", len(retired))
	}
	if observedAt.IsZero() || receipt.AppliedAt.IsZero() || safetyMargin < 0 {
		return nil, nil, errors.New("incomplete SST deletion timing")
	}

	plan := &sstDeletionPlan{
		Version: sstDeletionPlanVersion,
		Kind:    sstDeletionPlanKind,
		Source: sstDeletionPlanSource{
			CommandID:  command.ID,
			Epoch:      command.Epoch,
			Generation: command.Generation,
		},
		AppliedAt:     receipt.AppliedAt.UTC(),
		ObservedAt:    observedAt.UTC(),
		PinnedViewAge: current.PinnedViewAge(),
		SafetyMargin:  safetyMargin,
		TargetCount:   len(retired),
		Targets:       make([]sstDeletionTarget, len(retired)),
	}
	base := plan.AppliedAt
	if plan.ObservedAt.After(base) {
		base = plan.ObservedAt
	}
	plan.NotBefore = base.Add(plan.PinnedViewAge).Add(plan.SafetyMargin)
	for i, object := range retired {
		plan.Targets[i] = sstDeletionTarget{ID: object.ID, Key: object.Key, Size: object.Size}
		if object.Size > 0 && plan.TargetBytes > int64(^uint64(0)>>1)-object.Size {
			return nil, nil, errors.New("SST deletion target bytes overflow")
		}
		plan.TargetBytes += object.Size
	}
	plan.PlanID = sstDeletionPlanID(*plan)
	plan.Checksum = sstDeletionPlanChecksum(*plan)
	payload, err := encodeSSTDeletionPlan(store, *plan)
	if err != nil {
		return nil, nil, err
	}
	return plan, payload, nil
}

func encodeSSTDeletionPlan(store *blobstore.Store, plan sstDeletionPlan) ([]byte, error) {
	if err := validateSSTDeletionPlan(store, plan); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxSSTDeletionPlanEncodedBytes {
		return nil, fmt.Errorf("SST deletion plan bytes=%d max=%d", len(payload), maxSSTDeletionPlanEncodedBytes)
	}
	return payload, nil
}

func decodeSSTDeletionPlan(store *blobstore.Store, planPath string, payload []byte) (sstDeletionPlan, error) {
	if len(payload) == 0 || len(payload) > maxSSTDeletionPlanEncodedBytes {
		return sstDeletionPlan{}, fmt.Errorf("invalid SST deletion plan bytes=%d", len(payload))
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var plan sstDeletionPlan
	if err := decoder.Decode(&plan); err != nil {
		return sstDeletionPlan{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return sstDeletionPlan{}, errors.New("SST deletion plan has trailing JSON")
		}
		return sstDeletionPlan{}, err
	}
	if err := validateSSTDeletionPlan(store, plan); err != nil {
		return sstDeletionPlan{}, err
	}
	if store != nil {
		canonicalPath := sstDeletionPlanCanonicalPath(store, plan.PlanID)
		readyPath := sstDeletionPlanReadyPath(store, plan.NotBefore, plan.PlanID)
		if err := validateDeletionPlanObjectPath(planPath, canonicalPath, readyPath); err != nil {
			return sstDeletionPlan{}, fmt.Errorf("SST %w", err)
		}
	}
	return plan, nil
}

func validateSSTDeletionPlan(store *blobstore.Store, plan sstDeletionPlan) error {
	if plan.Version != sstDeletionPlanVersion || plan.Kind != sstDeletionPlanKind {
		return fmt.Errorf("unsupported SST deletion plan version=%d kind=%q", plan.Version, plan.Kind)
	}
	if plan.Source.CommandID == "" || plan.Source.Epoch == 0 || plan.Source.Generation == 0 {
		return errors.New("incomplete SST deletion plan source")
	}
	if plan.PlanID == "" || plan.PlanID != sstDeletionPlanID(plan) {
		return errors.New("SST deletion plan ID mismatch")
	}
	if plan.Checksum == "" || plan.Checksum != sstDeletionPlanChecksum(plan) {
		return errors.New("SST deletion plan checksum mismatch")
	}
	if plan.AppliedAt.IsZero() || plan.ObservedAt.IsZero() || plan.PinnedViewAge <= 0 || plan.SafetyMargin < 0 {
		return errors.New("incomplete SST deletion plan timing")
	}
	base := plan.AppliedAt
	if plan.ObservedAt.After(base) {
		base = plan.ObservedAt
	}
	wantNotBefore := base.Add(plan.PinnedViewAge).Add(plan.SafetyMargin)
	if !plan.NotBefore.Equal(wantNotBefore) {
		return errors.New("SST deletion plan deadline mismatch")
	}
	if plan.TargetCount != len(plan.Targets) || plan.TargetCount <= 0 ||
		plan.TargetCount > manifest.MaxRetiredObjectsPerEntry || plan.TargetBytes < 0 {
		return fmt.Errorf("invalid SST deletion plan target count=%d", plan.TargetCount)
	}

	seenIDs := make(map[string]struct{}, len(plan.Targets))
	seenKeys := make(map[string]struct{}, len(plan.Targets))
	var targetBytes int64
	for i, target := range plan.Targets {
		if target.ID == "" || target.Key == "" || target.Size < 0 {
			return fmt.Errorf("incomplete SST deletion target index=%d", i)
		}
		if store != nil && target.Key != store.SSTPath(target.ID) {
			return fmt.Errorf("SST deletion target path mismatch id=%q key=%q", target.ID, target.Key)
		}
		if _, ok := seenIDs[target.ID]; ok {
			return fmt.Errorf("duplicate SST deletion target id=%q", target.ID)
		}
		if _, ok := seenKeys[target.Key]; ok {
			return fmt.Errorf("duplicate SST deletion target key=%q", target.Key)
		}
		seenIDs[target.ID] = struct{}{}
		seenKeys[target.Key] = struct{}{}
		if target.Size > 0 && targetBytes > int64(^uint64(0)>>1)-target.Size {
			return errors.New("SST deletion target bytes overflow")
		}
		targetBytes += target.Size
	}
	if targetBytes != plan.TargetBytes {
		return errors.New("SST deletion plan byte accounting mismatch")
	}
	return nil
}

func sstDeletionPlanID(plan sstDeletionPlan) string {
	identity := struct {
		Version   int                   `json:"version"`
		Kind      string                `json:"kind"`
		Source    sstDeletionPlanSource `json:"source"`
		AppliedAt time.Time             `json:"applied_at"`
		Targets   []sstDeletionTarget   `json:"targets"`
	}{
		Version:   plan.Version,
		Kind:      plan.Kind,
		Source:    plan.Source,
		AppliedAt: plan.AppliedAt,
		Targets:   plan.Targets,
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		panic(fmt.Sprintf("marshal SST deletion plan identity: %v", err))
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func sstDeletionPlanChecksum(plan sstDeletionPlan) string {
	plan.Checksum = ""
	payload, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("marshal SST deletion plan checksum: %v", err))
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func sstDeletionPlanCanonicalPath(store *blobstore.Store, planID string) string {
	return storeKey(store, sstDeletionPlanCanonicalPrefix, planID+".json")
}

func sstDeletionPlanReadyPath(store *blobstore.Store, notBefore time.Time, planID string) string {
	return storeKey(store, sstDeletionPlanPrefix, deletionPlanReadyName(notBefore, planID))
}

func storeSSTDeletionPlan(ctx context.Context, store *blobstore.Store, plan sstDeletionPlan, payload []byte) (bool, error) {
	canonicalPath := sstDeletionPlanCanonicalPath(store, plan.PlanID)
	encoded, err := decodeSSTDeletionPlan(store, canonicalPath, payload)
	if err != nil {
		return false, fmt.Errorf("validate SST deletion plan payload: %w", err)
	}
	if encoded.Checksum != plan.Checksum {
		return false, fmt.Errorf("SST deletion plan payload mismatch id=%q", plan.PlanID)
	}

	storedPlan := encoded
	storedPayload := payload
	_, writeErr := store.WriteIfNotExist(ctx, canonicalPath, payload)
	if writeErr != nil {
		if !errors.Is(writeErr, blobstore.ErrPreconditionFailed) {
			return false, writeErr
		}
		existingPayload, _, err := store.Read(ctx, canonicalPath)
		if err != nil {
			return false, err
		}
		existing, err := decodeSSTDeletionPlan(store, canonicalPath, existingPayload)
		if err != nil {
			return false, fmt.Errorf("validate existing SST deletion plan: %w", err)
		}
		if err := validateSameSSTDeletionPlan(existing, plan); err != nil {
			return false, err
		}
		// The first durable observation owns NotBefore. A reconciliation retry
		// adopts it instead of extending the grace window or creating another
		// deadline-addressed ready record.
		storedPlan = existing
		storedPayload = existingPayload
	}

	readyPath := sstDeletionPlanReadyPath(store, storedPlan.NotBefore, storedPlan.PlanID)
	if _, err := store.WriteIfNotExist(ctx, readyPath, storedPayload); err == nil {
		return true, nil
	} else if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		return false, err
	}
	existingReady, _, err := store.Read(ctx, readyPath)
	if err != nil {
		return false, err
	}
	readyPlan, err := decodeSSTDeletionPlan(store, readyPath, existingReady)
	if err != nil {
		return false, fmt.Errorf("validate existing SST deletion ready record: %w", err)
	}
	if readyPlan.Checksum != storedPlan.Checksum {
		return false, fmt.Errorf("SST deletion ready record collision id=%q", storedPlan.PlanID)
	}
	return false, nil
}

func validateSameSSTDeletionPlan(existing, requested sstDeletionPlan) error {
	if existing.PlanID != requested.PlanID || existing.Source != requested.Source ||
		!existing.AppliedAt.Equal(requested.AppliedAt) ||
		existing.TargetCount != requested.TargetCount || existing.TargetBytes != requested.TargetBytes {
		return fmt.Errorf("SST deletion plan collision id=%q", requested.PlanID)
	}
	for i := range existing.Targets {
		if existing.Targets[i] != requested.Targets[i] {
			return fmt.Errorf("SST deletion plan target collision id=%q index=%d", requested.PlanID, i)
		}
	}
	return nil
}

func (c *sstCleaner) runOnce(ctx context.Context) (sstCleanupWorkStats, error) {
	stats, _, err := c.runScheduledOnce(ctx)
	return stats, err
}

func (c *sstCleaner) runScheduledOnce(
	ctx context.Context,
) (sstCleanupWorkStats, reclamationLaneSchedule, error) {
	if err := checkContext(ctx); err != nil {
		return sstCleanupWorkStats{}, reclamationLaneSchedule{}, err
	}
	if generation := c.rescan.Load(); generation != c.seenRescan {
		c.seenRescan = generation
		c.planIter = nil
		c.pendingPlanKey = ""
	}
	if c.planIter == nil {
		c.planIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: sstDeletionPlanPrefix + "/"})
	}
	passNow := c.opts.Now().UTC()
	stats, exhausted, restartIterator, err := reclaimSSTDeletionPlans(
		ctx, c.store, c.delete, c.opts.DeleteBatchSize, c.opts.PlanScanLimit,
		passNow, c.planIter, c.cache, &c.pendingPlanKey)
	if exhausted || restartIterator {
		c.planIter = nil
	}
	if exhausted {
		c.pendingPlanKey = ""
	}
	remainingDeletes := c.opts.DeleteBatchSize - stats.Attempted
	auditDue := c.orphanAudit != nil || c.nextOrphanAudit.IsZero() || !passNow.Before(c.nextOrphanAudit)
	if err == nil && c.opts.ManifestLog != nil && auditDue && remainingDeletes > 0 {
		auditStats, auditErr := c.runOrphanAudit(ctx, passNow, remainingDeletes)
		mergeSSTCleanupWorkStats(&stats, &auditStats)
		err = errors.Join(err, auditErr)
	}
	nextDue := stats.NextDue
	if c.orphanAudit == nil && !c.nextOrphanAudit.IsZero() {
		nextDue = earlierReclamationDeadline(nextDue, c.nextOrphanAudit)
	}
	schedule := reclamationLaneSchedule{
		observedAt: c.opts.Now().UTC(),
		nextDue:    nextDue,
		idle:       err == nil && exhausted && c.orphanAudit == nil && stats.NextDue.IsZero(),
	}
	return stats, schedule, err
}

func mergeSSTCleanupWorkStats(dst, src *sstCleanupWorkStats) {
	if dst == nil || src == nil {
		return
	}
	dst.Attempted += src.Attempted
	dst.Deleted += src.Deleted
	dst.Failed += src.Failed
	dst.OrphanPlansScanned += src.OrphanPlansScanned
	dst.OrphanObjectsScanned += src.OrphanObjectsScanned
	dst.OrphanCandidates += src.OrphanCandidates
	dst.OrphansDeleted += src.OrphansDeleted
}

func (c *sstCleaner) planAvailable() {
	if c == nil {
		return
	}
	c.rescan.Add(1)
}

// runOrphanAudit discovers immutable SST uploads that no manifest history or
// durable retirement plan owns. The proof is deliberately conservative:
// candidates must predate the current owner fence, outlive the orphan grace,
// and remain absent from a fresh manifest immediately before deletion.
func (c *sstCleaner) runOrphanAudit(
	ctx context.Context,
	now time.Time,
	deleteBudget int,
) (stats sstCleanupWorkStats, err error) {
	if c == nil || c.opts.ManifestLog == nil || deleteBudget <= 0 {
		return stats, nil
	}
	if c.orphanAudit == nil {
		state, err := c.startOrphanAudit(ctx)
		if err != nil {
			c.nextOrphanAudit = now.Add(c.opts.OrphanAuditEvery)
			return stats, err
		}
		// A brand-new empty database has no CURRENT and therefore no ownership
		// fence with which to prove an SST orphan. There is nothing safe to scan
		// yet; retry on the ordinary audit cadence after a writer initializes it.
		if state == nil {
			c.nextOrphanAudit = now.Add(c.opts.OrphanAuditEvery)
			return stats, nil
		}
		c.orphanAudit = state
	}
	state := c.orphanAudit

	// Build a complete protection set before listing SSTs. This phase is
	// resumable and bounded; deletion cannot begin from a partial plan scan.
	for state.objectIter == nil && stats.OrphanPlansScanned < c.opts.OrphanScanLimit {
		object, err := state.planIter.Next(ctx)
		if errors.Is(err, io.EOF) {
			state.planIter = nil
			state.objectIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: "sstable/"})
			break
		}
		if err != nil {
			c.resetOrphanAudit(now)
			return stats, err
		}
		if object.IsDir {
			continue
		}
		stats.OrphanPlansScanned++
		payload, _, err := c.store.Read(ctx, object.Key)
		if err != nil {
			c.resetOrphanAudit(now)
			return stats, fmt.Errorf("read SST plan while protecting orphan audit %q: %w", object.Key, err)
		}
		plan, err := decodeSSTDeletionPlan(c.store, object.Key, payload)
		if err != nil {
			c.resetOrphanAudit(now)
			return stats, fmt.Errorf("decode SST plan while protecting orphan audit %q: %w", object.Key, err)
		}
		for i := range plan.Targets {
			state.protected[plan.Targets[i].Key] = struct{}{}
		}
	}
	if state.objectIter == nil {
		return stats, nil
	}

	candidates := make([]string, 0, deleteBudget)
	for stats.OrphanObjectsScanned < c.opts.OrphanScanLimit && len(candidates) < deleteBudget {
		object, err := state.objectIter.Next(ctx)
		if errors.Is(err, io.EOF) {
			c.orphanAudit = nil
			c.nextOrphanAudit = now.Add(c.opts.OrphanAuditEvery)
			break
		}
		if err != nil {
			c.resetOrphanAudit(now)
			return stats, err
		}
		if object.IsDir {
			continue
		}
		stats.OrphanObjectsScanned++
		if _, protected := state.protected[object.Key]; protected {
			continue
		}
		id := path.Base(object.Key)
		if c.store.SSTPath(id) != object.Key || !orphanSSTPredatesCurrentFence(
			id, object.ModTime, now, state.current, c.opts.OrphanGrace, c.opts.SafetyMargin) {
			continue
		}
		stats.OrphanCandidates++
		candidates = append(candidates, object.Key)
	}
	if len(candidates) == 0 {
		return stats, nil
	}

	// Refresh the manifest and fences after candidate discovery. Normal
	// publication can only prepend current-fence objects, but this final check
	// also fails closed across ownership changes and administrative restores.
	live, current, err := c.opts.ManifestLog.ReplayWithCurrent(ctx)
	if err != nil {
		c.resetOrphanAudit(now)
		return stats, err
	}
	if !sameSSTAuditFences(state.current, current) {
		c.orphanAudit = nil
		c.nextOrphanAudit = time.Time{}
		return stats, nil
	}
	liveKeys := manifestSSTKeys(c.store, live)
	deleteKeys := candidates[:0]
	for _, key := range candidates {
		if _, live := liveKeys[key]; !live {
			deleteKeys = append(deleteKeys, key)
		}
	}
	if len(deleteKeys) == 0 {
		return stats, nil
	}
	stats.Attempted += len(deleteKeys)
	if err := c.delete.BatchDelete(ctx, deleteKeys); err != nil {
		if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
			return stats, cancelErr
		}
		failed := len(deleteKeys)
		var batchErr *blobstore.BatchDeleteError
		if errors.As(err, &batchErr) {
			failed = len(batchErr.Failed)
			stats.Deleted += len(deleteKeys) - failed
			stats.OrphansDeleted += len(deleteKeys) - failed
		}
		stats.Failed += failed
		return stats, fmt.Errorf("delete orphan SSTs: %w", err)
	}
	stats.Deleted += len(deleteKeys)
	stats.OrphansDeleted += len(deleteKeys)
	return stats, nil
}

func (c *sstCleaner) startOrphanAudit(ctx context.Context) (*sstOrphanAuditState, error) {
	live, current, err := c.opts.ManifestLog.ReplayWithCurrent(ctx)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, nil
	}
	protected := manifestSSTKeys(c.store, live)
	// Read HEAD after the manifest snapshot. If a retirement applied before the
	// replay, its pending command is visible here; if it applies afterwards,
	// the replay still protects the formerly-live input.
	head, _, err := c.opts.ManifestLog.ReadMaintenanceHead(ctx)
	if err != nil {
		return nil, err
	}
	if head != nil && head.Pending != nil {
		if retired, ok := retiredObjectsFromMaintenanceCommand(head.Pending); ok {
			for i := range retired {
				protected[retired[i].Key] = struct{}{}
			}
		}
	}
	return &sstOrphanAuditState{
		protected: protected,
		current:   current,
		planIter:  c.store.NewListIterator(blobstore.ListOptions{Prefix: sstDeletionPlanPrefix + "/"}),
	}, nil
}

func (c *sstCleaner) resetOrphanAudit(now time.Time) {
	c.orphanAudit = nil
	c.nextOrphanAudit = now.Add(c.opts.OrphanAuditEvery)
}

func manifestSSTKeys(store *blobstore.Store, m *manifest.Manifest) map[string]struct{} {
	keys := make(map[string]struct{})
	if store == nil || m == nil {
		return keys
	}
	for i := range m.L0SSTs {
		keys[store.SSTPath(m.L0SSTs[i].ID)] = struct{}{}
	}
	for i := range m.Levels {
		for j := range m.Levels[i].SSTs {
			keys[store.SSTPath(m.Levels[i].SSTs[j].ID)] = struct{}{}
		}
	}
	return keys
}

func orphanSSTPredatesCurrentFence(
	id string,
	modifiedAt, now time.Time,
	current *manifest.Current,
	orphanGrace, safetyMargin time.Duration,
) bool {
	if current == nil || modifiedAt.IsZero() || now.Before(modifiedAt.Add(orphanGrace)) {
		return false
	}
	if isCompactionSSTID(id) {
		fence := current.CompactorFence
		return fence != nil && !fence.ClaimedAt.IsZero() &&
			modifiedAt.Add(safetyMargin).Before(fence.ClaimedAt)
	}
	epoch, ok := writerSSTEpoch(id)
	if !ok || current.WriterFence == nil || epoch >= current.WriterFence.Epoch {
		return false
	}
	return true
}

func sameSSTAuditFences(a, b *manifest.Current) bool {
	if a == nil || b == nil {
		return false
	}
	return sameSSTAuditFence(a.WriterFence, b.WriterFence) &&
		sameSSTAuditFence(a.CompactorFence, b.CompactorFence)
}

func sameSSTAuditFence(a, b *manifest.FenceToken) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Epoch == b.Epoch && a.Owner == b.Owner && a.ClaimedAt.Equal(b.ClaimedAt)
}

func runSSTDeletionPlanReclaimer(
	ctx context.Context,
	store *blobstore.Store,
	deleteBatchSize int,
	scanLimit int,
	now time.Time,
	deleter ...objectDeleter,
) (sstCleanupWorkStats, error) {
	if deleteBatchSize <= 0 {
		deleteBatchSize = defaultSSTDeletionPlanBatchSize
	}
	if deleteBatchSize > manifest.MaxRetiredObjectsPerEntry {
		deleteBatchSize = manifest.MaxRetiredObjectsPerEntry
	}
	if scanLimit <= 0 {
		scanLimit = defaultSSTDeletionPlanScanLimit
	}
	deleteObjects := objectDeleter(store)
	if len(deleter) > 0 && deleter[0] != nil {
		deleteObjects = deleter[0]
	}

	iter := store.NewListIterator(blobstore.ListOptions{Prefix: sstDeletionPlanPrefix + "/"})
	stats, _, _, err := reclaimSSTDeletionPlans(ctx, store, deleteObjects, deleteBatchSize, scanLimit, now, iter, nil, nil)
	return stats, err
}

func reclaimSSTDeletionPlans(
	ctx context.Context,
	store *blobstore.Store,
	deleteObjects objectDeleter,
	deleteBatchSize int,
	scanLimit int,
	now time.Time,
	iter *blobstore.ListIterator,
	cache *boundedPlanCache[sstDeletionPlan],
	pendingPlanKey *string,
) (stats sstCleanupWorkStats, exhausted, restartIterator bool, err error) {
	// A bad plan is an object-level failure: Next already advanced past it, so
	// restartIterator stays false and the next pass keeps that progress. Only a
	// LIST failure or cancellation makes the provider cursor unsafe to reuse.
	remaining := deleteBatchSize
	var reclaimErr error
	for stats.PlansScanned < scanLimit && remaining > 0 {
		var object blobstore.ObjectInfo
		if pendingPlanKey != nil && *pendingPlanKey != "" {
			// Next already advanced the provider iterator past this plan in the
			// preceding pass. Consume the carried key before listing more work.
			object.Key = *pendingPlanKey
			*pendingPlanKey = ""
		} else {
			var err error
			object, err = iter.Next(ctx)
			if errors.Is(err, io.EOF) {
				return stats, true, false, reclaimErr
			}
			if err != nil {
				return stats, false, true, errors.Join(reclaimErr, err)
			}
		}
		if object.IsDir {
			continue
		}
		stats.PlansScanned++
		readyDeadline, _, pathErr := parseDeletionPlanReadyName(object.Key)
		if pathErr != nil {
			stats.Failed++
			reclaimErr = errors.Join(reclaimErr, fmt.Errorf("parse SST deletion plan path %q: %w", object.Key, pathErr))
			continue
		}
		if now.Before(readyDeadline) {
			stats.Deferred++
			stats.NextDue = readyDeadline
			if pendingPlanKey != nil {
				*pendingPlanKey = object.Key
			}
			// Ready-record names are ordered by NotBefore. Every valid key after
			// this one is also in the future, so no payload GET is necessary.
			return stats, false, false, reclaimErr
		}
		plan, ok := cache.get(object.Key)
		if !ok {
			payload, _, err := store.Read(ctx, object.Key)
			if err != nil {
				if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
					return stats, false, true, errors.Join(reclaimErr, cancelErr)
				}
				stats.Failed++
				reclaimErr = errors.Join(reclaimErr, fmt.Errorf("read SST deletion plan %q: %w", object.Key, err))
				continue
			}
			plan, err = decodeSSTDeletionPlan(store, object.Key, payload)
			if err != nil {
				stats.Failed++
				reclaimErr = errors.Join(reclaimErr, fmt.Errorf("decode SST deletion plan %q: %w", object.Key, err))
				continue
			}
			cache.put(object.Key, plan, len(payload))
		}
		if len(plan.Targets) > remaining && stats.Attempted > 0 {
			stats.Deferred++
			if pendingPlanKey != nil {
				// Preserve the item already consumed from the iterator. The next
				// pass can complete the independently bounded plan atomically.
				*pendingPlanKey = object.Key
			}
			return stats, false, false, reclaimErr
		}

		keys := make([]string, len(plan.Targets))
		for i := range plan.Targets {
			keys[i] = plan.Targets[i].Key
		}
		stats.Attempted += len(keys)
		if len(keys) >= remaining {
			remaining = 0
		} else {
			remaining -= len(keys)
		}
		if err := deleteObjects.BatchDelete(ctx, keys); err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				return stats, false, true, errors.Join(reclaimErr, cancelErr)
			}
			failed := len(keys)
			var batchErr *blobstore.BatchDeleteError
			if errors.As(err, &batchErr) {
				failed = len(batchErr.Failed)
				stats.Deleted += len(keys) - failed
			}
			stats.Failed += failed
			reclaimErr = errors.Join(reclaimErr, fmt.Errorf("delete targets for SST plan %q: %w", plan.PlanID, err))
			continue
		}
		stats.Deleted += len(keys)
		canonicalPath := sstDeletionPlanCanonicalPath(store, plan.PlanID)
		if err := deleteObjects.Delete(ctx, canonicalPath); err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				return stats, false, true, errors.Join(reclaimErr, cancelErr)
			}
			stats.Failed++
			reclaimErr = errors.Join(reclaimErr, fmt.Errorf("delete canonical SST plan %q: %w", plan.PlanID, err))
			continue
		}
		if err := deleteObjects.Delete(ctx, object.Key); err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				return stats, false, true, errors.Join(reclaimErr, cancelErr)
			}
			stats.Failed++
			reclaimErr = errors.Join(reclaimErr, fmt.Errorf("delete completed SST plan %q: %w", plan.PlanID, err))
			continue
		}
		cache.remove(object.Key)
		stats.PlansDeleted++
	}
	return stats, false, false, reclaimErr
}
