package isledb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// Deleting what a writer merge replaces goes through the deletion plans
// maintenance already reclaims. Before a merge does anything irreversible the
// writer records an intent naming the inputs and the output. Once the merge is
// settled it writes the plan: for the inputs when the merge replaced them, for
// the output when it did not. Then it marks the intent settled. An intent a
// dead writer left unsettled is resolved by maintenance from the manifest.

const (
	sstMergeIntentPrefix          = "manifest/gc/sst/intents"
	sstMergeIntentVersion         = 1
	sstMergeIntentKind            = "l0_merge_intent"
	maxSSTMergeIntentEncodedBytes = 64 << 10
)

// sstMergeIntent is the record a writer leaves before merging a run.
type sstMergeIntent struct {
	Version     int       `json:"version"`
	Kind        string    `json:"kind"`
	IntentID    string    `json:"intent_id"`
	WriterEpoch uint64    `json:"writer_epoch"`
	CreatedAt   time.Time `json:"created_at"`

	Inputs []sstDeletionTarget `json:"inputs"`
	Output sstDeletionTarget   `json:"output"`

	// Settled names the plan the writer wrote once the merge was decided,
	// and which side of the merge it deletes.
	SettledAt     time.Time `json:"settled_at,omitzero"`
	SettledPlanID string    `json:"settled_plan_id,omitempty"`
	Settled       string    `json:"settled,omitempty"`
}

func sstMergeIntentID(outputID string) string {
	sum := sha256.Sum256([]byte("l0-merge-intent:" + outputID))
	return hex.EncodeToString(sum[:])
}

func sstMergeIntentPath(store *blobstore.Store, intentID string) string {
	return storeKey(store, sstMergeIntentPrefix, intentID+".json")
}

func encodeSSTMergeIntent(intent sstMergeIntent) ([]byte, error) {
	if err := validateSSTMergeIntent(intent); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(intent)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxSSTMergeIntentEncodedBytes {
		return nil, fmt.Errorf("L0 merge intent bytes=%d max=%d", len(payload), maxSSTMergeIntentEncodedBytes)
	}
	return payload, nil
}

func decodeSSTMergeIntent(payload []byte) (sstMergeIntent, error) {
	var intent sstMergeIntent
	if len(payload) == 0 || len(payload) > maxSSTMergeIntentEncodedBytes {
		return intent, fmt.Errorf("invalid L0 merge intent bytes=%d", len(payload))
	}
	if err := json.Unmarshal(payload, &intent); err != nil {
		return intent, err
	}
	if err := validateSSTMergeIntent(intent); err != nil {
		return intent, err
	}
	return intent, nil
}

func validateSSTMergeIntent(intent sstMergeIntent) error {
	if intent.Version != sstMergeIntentVersion || intent.Kind != sstMergeIntentKind {
		return fmt.Errorf("unsupported L0 merge intent version=%d kind=%q", intent.Version, intent.Kind)
	}
	if intent.IntentID == "" || intent.IntentID != sstMergeIntentID(intent.Output.ID) {
		return errors.New("L0 merge intent ID mismatch")
	}
	if intent.WriterEpoch == 0 || intent.CreatedAt.IsZero() {
		return errors.New("incomplete L0 merge intent")
	}
	if len(intent.Inputs) == 0 || intent.Output.ID == "" || intent.Output.Key == "" {
		return errors.New("L0 merge intent names no inputs or no output")
	}
	for _, input := range intent.Inputs {
		if input.ID == "" || input.Key == "" {
			return errors.New("incomplete L0 merge intent input")
		}
	}
	switch intent.Settled {
	case "":
		if intent.SettledPlanID != "" || !intent.SettledAt.IsZero() {
			return errors.New("L0 merge intent settled without an outcome")
		}
	case sstDeletionOutcomeInputs, sstDeletionOutcomeOutput:
		if intent.SettledPlanID == "" || intent.SettledAt.IsZero() {
			return errors.New("L0 merge intent settled without a plan")
		}
	default:
		return fmt.Errorf("unknown L0 merge intent outcome %q", intent.Settled)
	}
	return nil
}

// writeL0MergeIntent records that the writer is about to merge inputs into
// output. The record's name is derived from the output's, so a retry of the
// same merge rewrites the same object.
func (m *l0Merger) writeL0MergeIntent(ctx context.Context, inputs []sstMetadata, outputID string) (sstMergeIntent, error) {
	store := m.w.store
	intent := sstMergeIntent{
		Version:     sstMergeIntentVersion,
		Kind:        sstMergeIntentKind,
		IntentID:    sstMergeIntentID(outputID),
		WriterEpoch: m.w.fenceToken.Epoch,
		CreatedAt:   time.Now().UTC(),
		Inputs:      sstDeletionTargets(store, inputs),
		Output:      sstDeletionTarget{ID: outputID, Key: store.SSTPath(outputID)},
	}
	payload, err := encodeSSTMergeIntent(intent)
	if err != nil {
		return intent, err
	}
	if _, err := store.Write(ctx, sstMergeIntentPath(store, intent.IntentID), payload); err != nil {
		return intent, fmt.Errorf("write l0 merge intent: %w", err)
	}
	return intent, nil
}

func sstDeletionTargets(store *blobstore.Store, ssts []sstMetadata) []sstDeletionTarget {
	targets := make([]sstDeletionTarget, len(ssts))
	for i, sst := range ssts {
		targets[i] = sstDeletionTarget{ID: sst.ID, Key: store.SSTPath(sst.ID), Size: sst.Size}
	}
	return targets
}

// l0MergeRetirement is one settled merge whose deletion plan is still to be
// written: the inputs of a merge that landed, at the time it landed, or the
// output of one that did not.
type l0MergeRetirement struct {
	intent    sstMergeIntent
	outcome   string
	appliedAt time.Time
	targets   []sstDeletionTarget
}

// retire queues the deletion plan for a settled merge. The committer writes
// it on its next pass.
func (m *l0Merger) retire(intent sstMergeIntent, outcome string, appliedAt time.Time, targets []sstDeletionTarget) {
	if m == nil || len(targets) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retirements = append(m.retirements, l0MergeRetirement{
		intent: intent, outcome: outcome, appliedAt: appliedAt.UTC(), targets: targets,
	})
}

func (m *l0Merger) nextRetirement() *l0MergeRetirement {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.retirements) == 0 {
		return nil
	}
	retirement := m.retirements[0]
	return &retirement
}

func (m *l0Merger) retirementDone(intentID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, retirement := range m.retirements {
		if retirement.intent.IntentID == intentID {
			m.retirements = append(m.retirements[:i], m.retirements[i+1:]...)
			return
		}
	}
}

// retireL0Merges writes the deletion plan for every settled merge, then marks
// its intent settled. Both writes are idempotent, so a failure ends the pass
// and the next pass does them again.
func (w *writer) retireL0Merges(ctx context.Context) error {
	for {
		retirement := w.l0Merge.nextRetirement()
		if retirement == nil {
			return nil
		}
		if err := w.writeL0MergeRetirement(ctx, retirement); err != nil {
			return fmt.Errorf("retire l0 merge %s: %w", retirement.outcome, err)
		}
		w.l0Merge.retirementDone(retirement.intent.IntentID)
	}
}

func (w *writer) writeL0MergeRetirement(ctx context.Context, retirement *l0MergeRetirement) error {
	intent := retirement.intent
	source := sstDeletionPlanSource{
		Kind:     sstDeletionSourceWriterL0Merge,
		IntentID: intent.IntentID,
		Outcome:  retirement.outcome,
		Epoch:    intent.WriterEpoch,
	}
	plan, payload, err := newSSTDeletionPlan(w.store, source, retirement.appliedAt, time.Now().UTC(),
		w.manifestLog.CurrentData().PinnedViewAge(), defaultSSTDeletionSafetyMargin, retirement.targets)
	if err != nil {
		return err
	}
	if _, err := storeSSTDeletionPlan(ctx, w.store, *plan, payload); err != nil {
		return err
	}
	intent.Settled = retirement.outcome
	intent.SettledPlanID = plan.PlanID
	intent.SettledAt = time.Now().UTC()
	settled, err := encodeSSTMergeIntent(intent)
	if err != nil {
		return err
	}
	if _, err := w.store.Write(ctx, sstMergeIntentPath(w.store, intent.IntentID), settled); err != nil {
		return fmt.Errorf("settle l0 merge intent: %w", err)
	}
	slog.Debug("isledb: l0 merge retirement planned", "component", "writer",
		"outcome", retirement.outcome, "targets", len(retirement.targets), "plan_id", plan.PlanID,
		"not_before", plan.NotBefore)
	return nil
}
