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
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	sstDeletionPlanPrefix           = "manifest/gc/sst/ready"
	sstDeletionPlanVersion          = 1
	sstDeletionPlanKind             = "sst_retirement"
	defaultSSTDeletionPlanScanLimit = 1024
	defaultSSTDeletionSafetyMargin  = time.Minute
	maxSSTDeletionPlanEncodedBytes  = 256 << 10
	defaultSSTDeletionPlanBatchSize = 128
)

type sstCleanupWorkStats struct {
	Attempted      int
	Deleted        int
	Failed         int
	TargetsPlanned int
	PlansPrepared  int
	PlansScanned   int
	PlansDeleted   int
	Deferred       int
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
	DeleteBatchSize int
	PlanScanLimit   int
	SafetyMargin    time.Duration
	Now             func() time.Time
	Deleter         objectDeleter
}

type sstCleaner struct {
	store  *blobstore.Store
	opts   sstCleanerOptions
	delete objectDeleter

	mu             sync.Mutex
	planIter       *blobstore.ListIterator
	pendingPlanKey string
	cache          *boundedPlanCache[sstDeletionPlan]
}

func defaultSSTCleanerOptions() sstCleanerOptions {
	return sstCleanerOptions{
		DeleteBatchSize: defaultSSTDeletionPlanBatchSize,
		PlanScanLimit:   defaultSSTDeletionPlanScanLimit,
		SafetyMargin:    defaultSSTDeletionSafetyMargin,
		Now:             func() time.Time { return time.Now().UTC() },
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
	if store != nil && planPath != sstDeletionPlanPath(store, plan.PlanID) {
		return sstDeletionPlan{}, fmt.Errorf("SST deletion plan path mismatch %q", planPath)
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

func sstDeletionPlanPath(store *blobstore.Store, planID string) string {
	return storeKey(store, sstDeletionPlanPrefix, planID+".json")
}

func storeSSTDeletionPlan(ctx context.Context, store *blobstore.Store, plan sstDeletionPlan, payload []byte) (bool, error) {
	path := sstDeletionPlanPath(store, plan.PlanID)
	encoded, err := decodeSSTDeletionPlan(store, path, payload)
	if err != nil {
		return false, fmt.Errorf("validate SST deletion plan payload: %w", err)
	}
	if encoded.Checksum != plan.Checksum {
		return false, fmt.Errorf("SST deletion plan payload mismatch id=%q", plan.PlanID)
	}
	if _, err := store.WriteIfNotExist(ctx, path, payload); err == nil {
		return true, nil
	} else if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		return false, err
	}

	existingPayload, _, err := store.Read(ctx, path)
	if err != nil {
		return false, err
	}
	existing, err := decodeSSTDeletionPlan(store, path, existingPayload)
	if err != nil {
		return false, fmt.Errorf("validate existing SST deletion plan: %w", err)
	}
	if existing.PlanID != plan.PlanID || existing.Source != plan.Source || !existing.AppliedAt.Equal(plan.AppliedAt) ||
		existing.PinnedViewAge != plan.PinnedViewAge || existing.SafetyMargin != plan.SafetyMargin ||
		existing.TargetCount != plan.TargetCount || existing.TargetBytes != plan.TargetBytes {
		return false, fmt.Errorf("SST deletion plan collision id=%q", plan.PlanID)
	}
	for i := range existing.Targets {
		if existing.Targets[i] != plan.Targets[i] {
			return false, fmt.Errorf("SST deletion plan target collision id=%q index=%d", plan.PlanID, i)
		}
	}
	return false, nil
}

func (c *sstCleaner) runOnce(ctx context.Context) (sstCleanupWorkStats, error) {
	if err := checkContext(ctx); err != nil {
		return sstCleanupWorkStats{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.planIter == nil {
		c.planIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: sstDeletionPlanPrefix + "/"})
	}
	stats, exhausted, err := reclaimSSTDeletionPlans(
		ctx, c.store, c.delete, c.opts.DeleteBatchSize, c.opts.PlanScanLimit,
		c.opts.Now().UTC(), c.planIter, c.cache, &c.pendingPlanKey)
	if exhausted || err != nil {
		c.planIter = nil
	}
	if exhausted {
		c.pendingPlanKey = ""
	}
	return stats, err
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
	stats, _, err := reclaimSSTDeletionPlans(ctx, store, deleteObjects, deleteBatchSize, scanLimit, now, iter, nil, nil)
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
) (sstCleanupWorkStats, bool, error) {
	stats := sstCleanupWorkStats{}
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
				return stats, true, reclaimErr
			}
			if err != nil {
				return stats, false, errors.Join(reclaimErr, err)
			}
		}
		if object.IsDir {
			continue
		}
		stats.PlansScanned++
		plan, ok := cache.get(object.Key)
		if !ok {
			payload, _, err := store.Read(ctx, object.Key)
			if err != nil {
				if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
					return stats, false, errors.Join(reclaimErr, cancelErr)
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
		if now.Before(plan.NotBefore) {
			stats.Deferred++
			continue
		}
		if len(plan.Targets) > remaining && stats.Attempted > 0 {
			stats.Deferred++
			if pendingPlanKey != nil {
				// Preserve the item already consumed from the iterator. The next
				// pass can complete the independently bounded plan atomically.
				*pendingPlanKey = object.Key
			}
			return stats, false, reclaimErr
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
				return stats, false, errors.Join(reclaimErr, cancelErr)
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
		if err := deleteObjects.Delete(ctx, object.Key); err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				return stats, false, errors.Join(reclaimErr, cancelErr)
			}
			stats.Failed++
			reclaimErr = errors.Join(reclaimErr, fmt.Errorf("delete completed SST plan %q: %w", plan.PlanID, err))
			continue
		}
		cache.remove(object.Key)
		stats.PlansDeleted++
	}
	return stats, false, reclaimErr
}
