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
	"sort"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	changeFeedDeletionPlanPrefix           = "manifest/gc/change-feed/ready"
	changeFeedDeletionPlanVersion          = 1
	changeFeedDeletionPlanKind             = "change_feed_retention"
	defaultChangeFeedSweepBatchSize        = 128
	defaultChangeFeedSweepGracePeriod      = 10 * time.Minute
	defaultChangeFeedDeletionSafetyMargin  = time.Minute
	defaultChangeFeedDeletionPlanScanLimit = 1024
	maxChangeFeedDeletionPlanEncodedBytes  = 2 << 20
)

type changeBatchDeleteCandidate struct {
	Path     string `json:"path"`
	ID       string `json:"id"`
	Seq      uint64 `json:"seq"`
	Size     int64  `json:"size,omitempty"`
	Checksum string `json:"checksum,omitempty"`
}

// changeFeedDeletionPlan is the immutable handoff created only after CURRENT
// proves that the logical feed floor was published. Its deadline protects every
// manifest view that could have been loaded before that publication.
type changeFeedDeletionPlan struct {
	Version  int    `json:"version"`
	Kind     string `json:"kind"`
	PlanID   string `json:"plan_id"`
	Checksum string `json:"plan_checksum"`

	TargetFloor uint64        `json:"target_floor"`
	CreatedAt   time.Time     `json:"created_at"`
	GracePeriod time.Duration `json:"grace_period_nanos"`

	FloorPublishedAt time.Time     `json:"floor_published_at"`
	PinnedViewAge    time.Duration `json:"pinned_view_age_nanos"`
	SafetyMargin     time.Duration `json:"safety_margin_nanos"`
	NotBefore        time.Time     `json:"not_before"`

	TargetCount int                          `json:"target_count"`
	TargetBytes int64                        `json:"target_bytes"`
	Targets     []changeBatchDeleteCandidate `json:"targets"`
}

type changeFeedSweepStats struct {
	Attempted       int
	Deleted         int
	BlockedRetained int
	Failed          int
	PlansScanned    int
	PlansDeleted    int
	Deferred        int
}

func buildChangeFeedDeletionPlan(
	store *blobstore.Store,
	candidates []changeBatchDeleteCandidate,
	targetFloor uint64,
	createdAt time.Time,
	gracePeriod time.Duration,
	floorPublishedAt time.Time,
	pinnedViewAge time.Duration,
	safetyMargin time.Duration,
) (*changeFeedDeletionPlan, []byte, error) {
	candidates = uniqueChangeBatchDeleteCandidates(candidates)
	if len(candidates) == 0 || len(candidates) > manifest.MaxChangeFeedDeleteTargetsPerCommand {
		return nil, nil, fmt.Errorf("invalid change-feed deletion target count=%d", len(candidates))
	}
	if targetFloor == 0 || createdAt.IsZero() || floorPublishedAt.IsZero() ||
		pinnedViewAge <= 0 || safetyMargin < 0 {
		return nil, nil, errors.New("incomplete change-feed deletion timing")
	}
	if gracePeriod < 0 {
		gracePeriod = 0
	}
	plan := &changeFeedDeletionPlan{
		Version:          changeFeedDeletionPlanVersion,
		Kind:             changeFeedDeletionPlanKind,
		TargetFloor:      targetFloor,
		CreatedAt:        createdAt.UTC(),
		GracePeriod:      gracePeriod,
		FloorPublishedAt: floorPublishedAt.UTC(),
		PinnedViewAge:    pinnedViewAge,
		SafetyMargin:     safetyMargin,
		TargetCount:      len(candidates),
		Targets:          candidates,
	}
	for _, target := range plan.Targets {
		if target.Size > 0 && plan.TargetBytes > int64(^uint64(0)>>1)-target.Size {
			return nil, nil, errors.New("change-feed deletion target bytes overflow")
		}
		plan.TargetBytes += target.Size
	}
	plan.NotBefore = plan.CreatedAt.Add(plan.GracePeriod)
	if viewDeadline := plan.FloorPublishedAt.Add(plan.PinnedViewAge).Add(plan.SafetyMargin); viewDeadline.After(plan.NotBefore) {
		plan.NotBefore = viewDeadline
	}
	plan.PlanID = changeFeedDeletionPlanID(*plan)
	plan.Checksum = changeFeedDeletionPlanChecksum(*plan)
	payload, err := encodeChangeFeedDeletionPlan(store, *plan)
	if err != nil {
		return nil, nil, err
	}
	return plan, payload, nil
}

func encodeChangeFeedDeletionPlan(store *blobstore.Store, plan changeFeedDeletionPlan) ([]byte, error) {
	if err := validateChangeFeedDeletionPlan(store, plan); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxChangeFeedDeletionPlanEncodedBytes {
		return nil, fmt.Errorf("change-feed deletion plan bytes=%d max=%d", len(payload), maxChangeFeedDeletionPlanEncodedBytes)
	}
	return payload, nil
}

func decodeChangeFeedDeletionPlan(store *blobstore.Store, planPath string, payload []byte) (changeFeedDeletionPlan, error) {
	if len(payload) == 0 || len(payload) > maxChangeFeedDeletionPlanEncodedBytes {
		return changeFeedDeletionPlan{}, fmt.Errorf("invalid change-feed deletion plan bytes=%d", len(payload))
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var plan changeFeedDeletionPlan
	if err := decoder.Decode(&plan); err != nil {
		return changeFeedDeletionPlan{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return changeFeedDeletionPlan{}, errors.New("change-feed deletion plan has trailing JSON")
		}
		return changeFeedDeletionPlan{}, err
	}
	if err := validateChangeFeedDeletionPlan(store, plan); err != nil {
		return changeFeedDeletionPlan{}, err
	}
	if planPath != changeFeedDeletionPlanPath(store, plan.PlanID) {
		return changeFeedDeletionPlan{}, fmt.Errorf("change-feed deletion plan path mismatch %q", planPath)
	}
	return plan, nil
}

func validateChangeFeedDeletionPlan(store *blobstore.Store, plan changeFeedDeletionPlan) error {
	if plan.Version != changeFeedDeletionPlanVersion || plan.Kind != changeFeedDeletionPlanKind {
		return fmt.Errorf("unsupported change-feed deletion plan version=%d kind=%q", plan.Version, plan.Kind)
	}
	if plan.PlanID == "" || plan.PlanID != changeFeedDeletionPlanID(plan) {
		return errors.New("change-feed deletion plan ID mismatch")
	}
	if plan.Checksum == "" || plan.Checksum != changeFeedDeletionPlanChecksum(plan) {
		return errors.New("change-feed deletion plan checksum mismatch")
	}
	if plan.TargetFloor == 0 || plan.CreatedAt.IsZero() || plan.GracePeriod < 0 ||
		plan.FloorPublishedAt.IsZero() || plan.PinnedViewAge <= 0 || plan.SafetyMargin < 0 {
		return errors.New("invalid change-feed deletion plan timing")
	}
	wantNotBefore := plan.CreatedAt.Add(plan.GracePeriod)
	if viewDeadline := plan.FloorPublishedAt.Add(plan.PinnedViewAge).Add(plan.SafetyMargin); viewDeadline.After(wantNotBefore) {
		wantNotBefore = viewDeadline
	}
	if !plan.NotBefore.Equal(wantNotBefore) {
		return errors.New("change-feed deletion plan deadline mismatch")
	}
	return validateChangeFeedDeletionTargets(store, plan.TargetFloor, plan.TargetCount, plan.TargetBytes, plan.Targets)
}

func validateChangeFeedDeletionTargets(
	store *blobstore.Store,
	targetFloor uint64,
	targetCount int,
	wantTargetBytes int64,
	targets []changeBatchDeleteCandidate,
) error {
	if targetCount != len(targets) || targetCount <= 0 ||
		targetCount > manifest.MaxChangeFeedDeleteTargetsPerCommand || wantTargetBytes < 0 {
		return fmt.Errorf("invalid change-feed deletion target count=%d", targetCount)
	}
	seenPaths := make(map[string]struct{}, len(targets))
	seenIDs := make(map[string]struct{}, len(targets))
	var targetBytes int64
	var previousSeq uint64
	for i, target := range targets {
		if target.Path == "" || target.ID == "" || target.Size < 0 || target.Seq >= targetFloor {
			return fmt.Errorf("invalid change-feed deletion target index=%d", i)
		}
		if store != nil && target.Path != store.ChangeBatchPath(target.ID) {
			return fmt.Errorf("change-feed deletion target path mismatch id=%q path=%q", target.ID, target.Path)
		}
		if i > 0 && target.Seq <= previousSeq {
			return errors.New("change-feed deletion targets are not sequence ordered")
		}
		previousSeq = target.Seq
		if _, ok := seenPaths[target.Path]; ok {
			return fmt.Errorf("duplicate change-feed target path=%q", target.Path)
		}
		if _, ok := seenIDs[target.ID]; ok {
			return fmt.Errorf("duplicate change-feed target id=%q", target.ID)
		}
		seenPaths[target.Path] = struct{}{}
		seenIDs[target.ID] = struct{}{}
		if target.Size > 0 && targetBytes > int64(^uint64(0)>>1)-target.Size {
			return errors.New("change-feed deletion target bytes overflow")
		}
		targetBytes += target.Size
	}
	if targetBytes != wantTargetBytes {
		return errors.New("change-feed deletion target byte accounting mismatch")
	}
	return nil
}

func changeFeedDeletionPlanID(plan changeFeedDeletionPlan) string {
	identity := struct {
		Version     int                          `json:"version"`
		Kind        string                       `json:"kind"`
		TargetFloor uint64                       `json:"target_floor"`
		Targets     []changeBatchDeleteCandidate `json:"targets"`
	}{plan.Version, plan.Kind, plan.TargetFloor, plan.Targets}
	payload, err := json.Marshal(identity)
	if err != nil {
		panic(fmt.Sprintf("marshal change-feed deletion plan identity: %v", err))
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func changeFeedDeletionPlanChecksum(plan changeFeedDeletionPlan) string {
	plan.Checksum = ""
	payload, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("marshal change-feed deletion plan checksum: %v", err))
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func changeFeedDeletionPlanPath(store *blobstore.Store, planID string) string {
	return storeKey(store, changeFeedDeletionPlanPrefix, planID+".json")
}

func storeChangeFeedDeletionPlan(
	ctx context.Context,
	store *blobstore.Store,
	plan changeFeedDeletionPlan,
	payload []byte,
) (bool, error) {
	path := changeFeedDeletionPlanPath(store, plan.PlanID)
	decoded, err := decodeChangeFeedDeletionPlan(store, path, payload)
	if err != nil {
		return false, fmt.Errorf("validate change-feed deletion plan payload: %w", err)
	}
	if decoded.Checksum != plan.Checksum {
		return false, errors.New("validate change-feed deletion plan payload: checksum mismatch")
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
	existing, err := decodeChangeFeedDeletionPlan(store, path, existingPayload)
	if err != nil {
		return false, err
	}
	if existing.PlanID != plan.PlanID || existing.TargetFloor != plan.TargetFloor ||
		existing.TargetCount != plan.TargetCount || existing.TargetBytes != plan.TargetBytes {
		return false, fmt.Errorf("change-feed deletion plan collision id=%q", plan.PlanID)
	}
	for i := range existing.Targets {
		if existing.Targets[i] != plan.Targets[i] {
			return false, fmt.Errorf("change-feed deletion target collision id=%q index=%d", plan.PlanID, i)
		}
	}
	return false, nil
}

func recordChangeFeedDeletionPlan(
	ctx context.Context,
	store *blobstore.Store,
	current *manifest.Current,
	command *manifest.MaintenanceCommand,
	receipt *manifest.MaintenanceReceipt,
	safetyMargin time.Duration,
) (bool, error) {
	if command == nil || command.Kind != manifest.MaintenanceCommandChangeFeedFloor ||
		command.ChangeFeedFloor == nil || len(command.ChangeFeedFloor.DeletionTargets) == 0 {
		return false, nil
	}
	if receipt == nil || !receipt.Matches(command) {
		return false, errors.New("change-feed deletion plan requires a matching receipt")
	}
	if receipt.Status == manifest.MaintenanceStatusRejected {
		return false, nil
	}
	if receipt.Status != manifest.MaintenanceStatusApplied || current == nil ||
		current.ChangeFeedLogStart < command.ChangeFeedFloor.Floor {
		return false, errors.New("change-feed deletion plan lacks a committed retention floor")
	}
	plan, payload, err := buildChangeFeedDeletionPlan(
		store,
		changeFeedDeleteCandidatesFromManifest(command.ChangeFeedFloor.DeletionTargets),
		command.ChangeFeedFloor.Floor,
		command.CreatedAt,
		command.ChangeFeedFloor.GracePeriod,
		receipt.AppliedAt,
		current.PinnedViewAge(),
		safetyMargin,
	)
	if err != nil {
		return false, err
	}
	return storeChangeFeedDeletionPlan(ctx, store, *plan, payload)
}

func changeFeedDeleteTargetsForManifest(candidates []changeBatchDeleteCandidate) []manifest.ChangeFeedDeleteTarget {
	targets := make([]manifest.ChangeFeedDeleteTarget, len(candidates))
	for i, candidate := range candidates {
		targets[i] = manifest.ChangeFeedDeleteTarget{
			Path: candidate.Path, ID: candidate.ID, Seq: candidate.Seq,
			Size: candidate.Size, Checksum: candidate.Checksum,
		}
	}
	return targets
}

func changeFeedDeleteCandidatesFromManifest(targets []manifest.ChangeFeedDeleteTarget) []changeBatchDeleteCandidate {
	candidates := make([]changeBatchDeleteCandidate, len(targets))
	for i, target := range targets {
		candidates[i] = changeBatchDeleteCandidate{
			Path: target.Path, ID: target.ID, Seq: target.Seq,
			Size: target.Size, Checksum: target.Checksum,
		}
	}
	return candidates
}

func runChangeFeedDeletionPlanReclaimer(
	ctx context.Context,
	store *blobstore.Store,
	manifestLog *manifest.Store,
	deleteBatchSize int,
	scanLimit int,
	now time.Time,
	deleter objectDeleter,
	iter *blobstore.ListIterator,
	cache map[string]changeFeedDeletionPlan,
) (changeFeedSweepStats, bool, error) {
	stats := changeFeedSweepStats{}
	if deleteBatchSize <= 0 {
		deleteBatchSize = defaultChangeFeedSweepBatchSize
	}
	if scanLimit <= 0 {
		scanLimit = defaultChangeFeedDeletionPlanScanLimit
	}
	if deleter == nil {
		deleter = store
	}
	current, err := manifestLog.ReadCurrentData(ctx)
	if err != nil {
		return stats, false, err
	}
	var retainedFloor uint64
	if current != nil {
		retainedFloor = current.ChangeFeedLogStart
	}
	remaining := deleteBatchSize
	var reclaimErr error
	for stats.PlansScanned < scanLimit && remaining > 0 {
		object, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			return stats, true, reclaimErr
		}
		if err != nil {
			return stats, false, errors.Join(reclaimErr, err)
		}
		if object.IsDir {
			continue
		}
		stats.PlansScanned++
		plan, ok := cache[object.Key]
		if !ok {
			payload, _, err := store.Read(ctx, object.Key)
			if err != nil {
				stats.Failed++
				reclaimErr = errors.Join(reclaimErr, fmt.Errorf("read change-feed deletion plan %q: %w", object.Key, err))
				continue
			}
			plan, err = decodeChangeFeedDeletionPlan(store, object.Key, payload)
			if err != nil {
				stats.Failed++
				reclaimErr = errors.Join(reclaimErr, fmt.Errorf("decode change-feed deletion plan %q: %w", object.Key, err))
				continue
			}
			if cache != nil {
				cache[object.Key] = plan
			}
		}
		if retainedFloor < plan.TargetFloor {
			stats.BlockedRetained += len(plan.Targets)
			continue
		}
		if now.Before(plan.NotBefore) {
			stats.Deferred += len(plan.Targets)
			continue
		}
		if len(plan.Targets) > remaining && stats.Attempted > 0 {
			stats.Deferred += len(plan.Targets)
			continue
		}
		keys := make([]string, len(plan.Targets))
		for i := range plan.Targets {
			keys[i] = plan.Targets[i].Path
		}
		stats.Attempted += len(keys)
		if len(keys) >= remaining {
			remaining = 0
		} else {
			remaining -= len(keys)
		}
		if err := deleter.BatchDelete(ctx, keys); err != nil {
			failed := len(keys)
			var batchErr *blobstore.BatchDeleteError
			if errors.As(err, &batchErr) {
				failed = len(batchErr.Failed)
				stats.Deleted += len(keys) - failed
			}
			stats.Failed += failed
			reclaimErr = errors.Join(reclaimErr, fmt.Errorf("delete change-feed plan %q targets: %w", plan.PlanID, err))
			continue
		}
		stats.Deleted += len(keys)
		if err := deleter.Delete(ctx, object.Key); err != nil {
			stats.Failed++
			reclaimErr = errors.Join(reclaimErr, fmt.Errorf("delete completed change-feed plan %q: %w", plan.PlanID, err))
			continue
		}
		delete(cache, object.Key)
		stats.PlansDeleted++
	}
	return stats, false, reclaimErr
}

func uniqueChangeBatchDeleteCandidates(candidates []changeBatchDeleteCandidate) []changeBatchDeleteCandidate {
	if len(candidates) == 0 {
		return nil
	}
	byPath := make(map[string]changeBatchDeleteCandidate, len(candidates))
	for _, candidate := range candidates {
		if candidate.Path == "" {
			continue
		}
		if existing, ok := byPath[candidate.Path]; ok && existing.Seq >= candidate.Seq {
			continue
		}
		byPath[candidate.Path] = candidate
	}
	out := make([]changeBatchDeleteCandidate, 0, len(byPath))
	for _, candidate := range byPath {
		out = append(out, candidate)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Seq == out[j].Seq {
			return out[i].Path < out[j].Path
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}
