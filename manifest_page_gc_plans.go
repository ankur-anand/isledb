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
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	manifestPageDeletionPlanReadyPrefix     = "manifest/gc/pages/ready"
	manifestPageDeletionPlanCanonicalPrefix = "manifest/gc/pages/plans"
	manifestPageDeletionPlanVersion         = 1
	manifestPageDeletionPlanKind            = "manifest_page_retirement"
	defaultManifestPagePlanScanLimit        = 1024
	maxManifestPageDeletionPlanBytes        = 32 << 10
)

type manifestPageDeletionPlanSource struct {
	CommandID  string `json:"command_id"`
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
}

// manifestPageDeletionPlan describes an ordered range rather than enumerating
// page paths. A page is eligible only when its validated payload has SeqHi
// below Floor. The range is constant-sized regardless of checkpoint size.
type manifestPageDeletionPlan struct {
	Version  int    `json:"version"`
	Kind     string `json:"kind"`
	PlanID   string `json:"plan_id"`
	Checksum string `json:"checksum"`

	Source manifestPageDeletionPlanSource `json:"source"`

	Floor    uint64 `json:"floor"`
	MaxLevel uint8  `json:"max_level"`

	AppliedAt     time.Time     `json:"applied_at"`
	ObservedAt    time.Time     `json:"observed_at"`
	PinnedViewAge time.Duration `json:"pinned_view_age_nanos"`
	SafetyMargin  time.Duration `json:"safety_margin_nanos"`
	NotBefore     time.Time     `json:"not_before"`
}

func (c *manifestPageCleaner) markCommandOutcome(
	ctx context.Context,
	current *manifest.Current,
	command *manifest.MaintenanceCommand,
	receipt *manifest.MaintenanceReceipt,
) (ManifestPageCleanupStats, error) {
	stats := ManifestPageCleanupStats{}
	if current == nil || command == nil || receipt == nil || !receipt.Matches(command) ||
		receipt.Status != manifest.MaintenanceStatusApplied {
		return stats, nil
	}

	switch command.Kind {
	case manifest.MaintenanceCommandCheckpoint:
		if command.Checkpoint == nil || command.Checkpoint.FoldedReplayPages == 0 {
			return stats, nil
		}
	case manifest.MaintenanceCommandChangeFeedFloor:
		if command.ChangeFeedFloor == nil {
			return stats, nil
		}
	default:
		return stats, nil
	}

	floor, ok := manifestPageRetentionFloor(current)
	if !ok {
		return stats, nil
	}
	switch command.Kind {
	case manifest.MaintenanceCommandCheckpoint:
		// Checkpoints advance LogSeqStart. If change-feed history remains the
		// lower bound, the effective page floor did not move.
		if current.LogSeqStart != floor {
			return stats, nil
		}
	case manifest.MaintenanceCommandChangeFeedFloor:
		// Feed retention advances ChangeFeedLogStart. If state replay remains
		// the lower bound, another page-range plan would be redundant.
		if current.ChangeFeedLogStart != floor {
			return stats, nil
		}
	}
	maxLevel := current.ManifestPageMaxLevel
	if command.Checkpoint != nil && command.Checkpoint.FoldedReplayMaxPageLevel > maxLevel {
		maxLevel = command.Checkpoint.FoldedReplayMaxPageLevel
	}
	plan, payload, err := buildManifestPageDeletionPlan(
		current, command, receipt, floor, maxLevel, c.opts.Now().UTC(), c.opts.SafetyMargin)
	if err != nil {
		return stats, err
	}
	created, err := storeManifestPageDeletionPlan(ctx, c.store, *plan, payload)
	if err != nil {
		return stats, err
	}
	c.planAvailable()
	if created {
		stats.PlansPrepared = 1
	}
	return stats, nil
}

func buildManifestPageDeletionPlan(
	current *manifest.Current,
	command *manifest.MaintenanceCommand,
	receipt *manifest.MaintenanceReceipt,
	floor uint64,
	maxLevel uint8,
	observedAt time.Time,
	safetyMargin time.Duration,
) (*manifestPageDeletionPlan, []byte, error) {
	if current == nil || command == nil || receipt == nil || !receipt.Matches(command) ||
		receipt.Status != manifest.MaintenanceStatusApplied {
		return nil, nil, errors.New("manifest page deletion plan requires a matching applied receipt")
	}
	if floor == 0 || observedAt.IsZero() || receipt.AppliedAt.IsZero() || safetyMargin < 0 {
		return nil, nil, errors.New("incomplete manifest page deletion plan")
	}
	plan := &manifestPageDeletionPlan{
		Version: manifestPageDeletionPlanVersion,
		Kind:    manifestPageDeletionPlanKind,
		Source: manifestPageDeletionPlanSource{
			CommandID:  command.ID,
			Epoch:      command.Epoch,
			Generation: command.Generation,
		},
		Floor:         floor,
		MaxLevel:      maxLevel,
		AppliedAt:     receipt.AppliedAt.UTC(),
		ObservedAt:    observedAt.UTC(),
		PinnedViewAge: current.PinnedViewAge(),
		SafetyMargin:  safetyMargin,
	}
	base := plan.AppliedAt
	if plan.ObservedAt.After(base) {
		base = plan.ObservedAt
	}
	plan.NotBefore = base.Add(plan.PinnedViewAge).Add(plan.SafetyMargin)
	plan.PlanID = manifestPageDeletionPlanID(*plan)
	plan.Checksum = manifestPageDeletionPlanChecksum(*plan)
	payload, err := encodeManifestPageDeletionPlan(*plan)
	if err != nil {
		return nil, nil, err
	}
	return plan, payload, nil
}

func encodeManifestPageDeletionPlan(plan manifestPageDeletionPlan) ([]byte, error) {
	if err := validateManifestPageDeletionPlan(plan); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxManifestPageDeletionPlanBytes {
		return nil, fmt.Errorf("manifest page deletion plan bytes=%d max=%d", len(payload), maxManifestPageDeletionPlanBytes)
	}
	return payload, nil
}

func decodeManifestPageDeletionPlan(
	store *blobstore.Store,
	planPath string,
	payload []byte,
) (manifestPageDeletionPlan, error) {
	if len(payload) == 0 || len(payload) > maxManifestPageDeletionPlanBytes {
		return manifestPageDeletionPlan{}, fmt.Errorf("invalid manifest page deletion plan bytes=%d", len(payload))
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var plan manifestPageDeletionPlan
	if err := decoder.Decode(&plan); err != nil {
		return manifestPageDeletionPlan{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return manifestPageDeletionPlan{}, errors.New("manifest page deletion plan has trailing JSON")
		}
		return manifestPageDeletionPlan{}, err
	}
	if err := validateManifestPageDeletionPlan(plan); err != nil {
		return manifestPageDeletionPlan{}, err
	}
	if store != nil {
		canonicalPath := manifestPageDeletionPlanCanonicalPath(store, plan.PlanID)
		readyPath := manifestPageDeletionPlanReadyPath(store, plan.NotBefore, plan.PlanID)
		if err := validateDeletionPlanObjectPath(planPath, canonicalPath, readyPath); err != nil {
			return manifestPageDeletionPlan{}, fmt.Errorf("manifest page %w", err)
		}
	}
	return plan, nil
}

func validateManifestPageDeletionPlan(plan manifestPageDeletionPlan) error {
	if plan.Version != manifestPageDeletionPlanVersion || plan.Kind != manifestPageDeletionPlanKind {
		return fmt.Errorf("unsupported manifest page deletion plan version=%d kind=%q", plan.Version, plan.Kind)
	}
	if plan.Source.CommandID == "" || plan.Source.Epoch == 0 || plan.Source.Generation == 0 || plan.Floor == 0 {
		return errors.New("incomplete manifest page deletion plan identity")
	}
	if plan.PlanID == "" || plan.PlanID != manifestPageDeletionPlanID(plan) {
		return errors.New("manifest page deletion plan ID mismatch")
	}
	if plan.Checksum == "" || plan.Checksum != manifestPageDeletionPlanChecksum(plan) {
		return errors.New("manifest page deletion plan checksum mismatch")
	}
	if plan.AppliedAt.IsZero() || plan.ObservedAt.IsZero() || plan.PinnedViewAge <= 0 || plan.SafetyMargin < 0 {
		return errors.New("incomplete manifest page deletion plan timing")
	}
	base := plan.AppliedAt
	if plan.ObservedAt.After(base) {
		base = plan.ObservedAt
	}
	if !plan.NotBefore.Equal(base.Add(plan.PinnedViewAge).Add(plan.SafetyMargin)) {
		return errors.New("manifest page deletion plan deadline mismatch")
	}
	return nil
}

func manifestPageDeletionPlanID(plan manifestPageDeletionPlan) string {
	identity := struct {
		Version   int                            `json:"version"`
		Kind      string                         `json:"kind"`
		Source    manifestPageDeletionPlanSource `json:"source"`
		Floor     uint64                         `json:"floor"`
		AppliedAt time.Time                      `json:"applied_at"`
	}{plan.Version, plan.Kind, plan.Source, plan.Floor, plan.AppliedAt}
	payload, err := json.Marshal(identity)
	if err != nil {
		panic(fmt.Sprintf("marshal manifest page deletion plan identity: %v", err))
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func manifestPageDeletionPlanChecksum(plan manifestPageDeletionPlan) string {
	plan.Checksum = ""
	payload, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("marshal manifest page deletion plan checksum: %v", err))
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func manifestPageDeletionPlanCanonicalPath(store *blobstore.Store, planID string) string {
	return storeKey(store, manifestPageDeletionPlanCanonicalPrefix, planID+".json")
}

func manifestPageDeletionPlanReadyPath(store *blobstore.Store, notBefore time.Time, planID string) string {
	return storeKey(store, manifestPageDeletionPlanReadyPrefix, deletionPlanReadyName(notBefore, planID))
}

func storeManifestPageDeletionPlan(
	ctx context.Context,
	store *blobstore.Store,
	plan manifestPageDeletionPlan,
	payload []byte,
) (bool, error) {
	canonicalPath := manifestPageDeletionPlanCanonicalPath(store, plan.PlanID)
	decoded, err := decodeManifestPageDeletionPlan(store, canonicalPath, payload)
	if err != nil {
		return false, fmt.Errorf("validate manifest page deletion plan payload: %w", err)
	}
	if decoded.Checksum != plan.Checksum {
		return false, fmt.Errorf("manifest page deletion plan payload mismatch id=%q", plan.PlanID)
	}

	storedPlan := decoded
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
		existing, err := decodeManifestPageDeletionPlan(store, canonicalPath, existingPayload)
		if err != nil {
			return false, fmt.Errorf("validate existing manifest page deletion plan: %w", err)
		}
		if err := validateSameManifestPageDeletionPlan(existing, plan); err != nil {
			return false, err
		}
		storedPlan = existing
		storedPayload = existingPayload
	}

	readyPath := manifestPageDeletionPlanReadyPath(store, storedPlan.NotBefore, storedPlan.PlanID)
	if _, err := store.WriteIfNotExist(ctx, readyPath, storedPayload); err == nil {
		return true, nil
	} else if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		return false, err
	}
	existingReady, _, err := store.Read(ctx, readyPath)
	if err != nil {
		return false, err
	}
	readyPlan, err := decodeManifestPageDeletionPlan(store, readyPath, existingReady)
	if err != nil {
		return false, fmt.Errorf("validate existing manifest page ready record: %w", err)
	}
	if readyPlan.Checksum != storedPlan.Checksum {
		return false, fmt.Errorf("manifest page deletion ready record collision id=%q", storedPlan.PlanID)
	}
	return false, nil
}

func validateSameManifestPageDeletionPlan(existing, requested manifestPageDeletionPlan) error {
	if existing.PlanID != requested.PlanID || existing.Source != requested.Source ||
		existing.Floor != requested.Floor ||
		!existing.AppliedAt.Equal(requested.AppliedAt) {
		return fmt.Errorf("manifest page deletion plan collision id=%q", requested.PlanID)
	}
	// MaxLevel is adopted from the first durable canonical record. A retry can
	// observe later page-level growth, but that growth is outside the range
	// retired by this already-applied command. A later floor plan and the orphan
	// audit cover subsequently created page objects.
	return nil
}

func (c *manifestPageCleaner) planAvailable() {
	if c != nil {
		c.rescan.Add(1)
	}
}

func (c *manifestPageCleaner) reclaimPlans(
	ctx context.Context,
	now time.Time,
) (stats ManifestPageCleanupStats, err error) {
	if err := checkContext(ctx); err != nil {
		return stats, err
	}
	if generation := c.rescan.Load(); generation != c.seenRescan {
		if c.activePlan == nil {
			// Do not acknowledge a rescan while an active plan prevents iterator
			// invalidation. Provider listings need not observe objects added after
			// iteration began, so the next pass must re-list after the plan drains.
			c.seenRescan = generation
			c.planIter = nil
			c.pendingPlanKey = ""
		}
	}
	if c.planIter == nil && c.activePlan == nil {
		c.planIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: manifestPageDeletionPlanReadyPrefix + "/"})
	}

	remaining := c.opts.DeleteBatchSize
	var currentFloor uint64
	currentFloorLoaded := false
	for stats.PlansScanned < c.opts.PlanScanLimit && remaining > 0 {
		if c.activePlan == nil {
			readyKey, exhausted, loadErr := c.nextManifestPageDeletionPlan(ctx, now, &stats)
			if loadErr != nil {
				return stats, loadErr
			}
			if exhausted || readyKey == "" {
				return stats, nil
			}
		}

		if !currentFloorLoaded {
			current, err := c.manifestLog.ReadCurrentData(ctx)
			if err != nil {
				return stats, err
			}
			var floorKnown bool
			currentFloor, floorKnown = manifestPageRetentionFloor(current)
			if !floorKnown {
				return stats, errors.New("manifest page retention floor unavailable")
			}
			currentFloorLoaded = true
		}
		completed, processErr := c.reclaimActiveManifestPagePlan(ctx, currentFloor, &stats, &remaining)
		if processErr != nil {
			return stats, processErr
		}
		if !completed {
			return stats, nil
		}
		if err := c.completeActiveManifestPagePlan(ctx); err != nil {
			return stats, err
		}
		stats.PlansCompleted++
		if c.planIter == nil {
			c.planIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: manifestPageDeletionPlanReadyPrefix + "/"})
		}
	}
	return stats, nil
}

func (c *manifestPageCleaner) nextManifestPageDeletionPlan(
	ctx context.Context,
	now time.Time,
	stats *ManifestPageCleanupStats,
) (readyKey string, exhausted bool, err error) {
	for stats.PlansScanned < c.opts.PlanScanLimit {
		var object blobstore.ObjectInfo
		if c.pendingPlanKey != "" {
			object.Key = c.pendingPlanKey
			c.pendingPlanKey = ""
		} else {
			object, err = c.planIter.Next(ctx)
			if errors.Is(err, io.EOF) {
				c.planIter = nil
				return "", true, nil
			}
			if err != nil {
				c.planIter = nil
				return "", false, err
			}
		}
		if object.IsDir {
			continue
		}
		stats.PlansScanned++
		deadline, _, err := parseDeletionPlanReadyName(object.Key)
		if err != nil {
			stats.Failures++
			continue
		}
		if now.Before(deadline) {
			stats.Deferred++
			c.pendingPlanKey = object.Key
			return "", false, nil
		}

		plan, ok := c.planCache.get(object.Key)
		if !ok {
			payload, _, err := c.store.Read(ctx, object.Key)
			if err != nil {
				return "", false, err
			}
			plan, err = decodeManifestPageDeletionPlan(c.store, object.Key, payload)
			if err != nil {
				stats.Failures++
				continue
			}
			c.planCache.put(object.Key, plan, len(payload))
		}
		c.activeReadyKey = object.Key
		c.activePlan = &plan
		c.activePageLevel = 0
		c.activePageIter = nil
		return object.Key, false, nil
	}
	return "", false, nil
}

func (c *manifestPageCleaner) reclaimActiveManifestPagePlan(
	ctx context.Context,
	currentFloor uint64,
	stats *ManifestPageCleanupStats,
	remaining *int,
) (bool, error) {
	plan := c.activePlan
	if plan == nil {
		return true, nil
	}
	if currentFloor < plan.Floor {
		return false, fmt.Errorf("manifest page retention floor regressed: current=%d plan=%d", currentFloor, plan.Floor)
	}

	for *remaining > 0 && stats.ObjectsScanned < c.opts.PageScanLimit {
		if c.activePageLevel > int(plan.MaxLevel) {
			return true, nil
		}
		if c.activePageIter == nil {
			prefix := fmt.Sprintf("%s/l%02d/", manifestPageObjectPrefix, c.activePageLevel)
			c.activePageIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: prefix})
		}
		object, err := c.activePageIter.Next(ctx)
		if errors.Is(err, io.EOF) {
			c.activePageIter = nil
			c.activePageLevel++
			continue
		}
		if err != nil {
			c.activePageIter = nil
			return false, err
		}
		if object.IsDir {
			continue
		}
		stats.ObjectsScanned++
		hint, ok := manifestPageKeyHintFromPath(c.store, object.Key)
		if !ok {
			stats.Failures++
			continue
		}
		if hint.HasRange && hint.SeqHi >= plan.Floor {
			// Keys are ordered by SeqHi within a level, so every later valid key
			// is protected by the same floor. The object payload remains the
			// authority for every key selected for deletion below.
			c.activePageIter = nil
			c.activePageLevel++
			continue
		}

		data, _, err := c.store.Read(ctx, object.Key)
		if err != nil {
			if errors.Is(err, blobstore.ErrNotFound) {
				// Another idempotent reclaimer may delete the immutable page after
				// this iterator listed it. Absence is already the desired end state.
				continue
			}
			c.activePageIter = nil
			return false, err
		}
		candidate, _, inspectErr := manifest.InspectCommitPage(object.Key, data)
		if inspectErr != nil || candidate.Level != hint.Level ||
			(hint.HasRange && (candidate.SeqLo != hint.SeqLo || candidate.SeqHi != hint.SeqHi)) {
			stats.Failures++
			continue
		}
		if candidate.SeqHi >= plan.Floor {
			stats.Protected++
			continue
		}
		stats.DeleteAttempts++
		if err := c.delete.Delete(ctx, object.Key); err != nil {
			c.activePageIter = nil
			return false, err
		}
		stats.PagesDeleted++
		(*remaining)--
	}
	return c.activePageLevel > int(plan.MaxLevel), nil
}

func (c *manifestPageCleaner) completeActiveManifestPagePlan(ctx context.Context) error {
	if c.activePlan == nil {
		return nil
	}
	canonicalPath := manifestPageDeletionPlanCanonicalPath(c.store, c.activePlan.PlanID)
	if err := c.delete.Delete(ctx, canonicalPath); err != nil {
		return fmt.Errorf("delete canonical manifest page plan %q: %w", c.activePlan.PlanID, err)
	}
	if err := c.delete.Delete(ctx, c.activeReadyKey); err != nil {
		return fmt.Errorf("delete completed manifest page plan %q: %w", c.activePlan.PlanID, err)
	}
	c.planCache.remove(c.activeReadyKey)
	c.activeReadyKey = ""
	c.activePlan = nil
	c.activePageLevel = 0
	c.activePageIter = nil
	return nil
}
