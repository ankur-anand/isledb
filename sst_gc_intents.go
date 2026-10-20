package isledb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// The writer records an intent before each L0 merge and settles it once the
// merge is decided and its deletion plan written. A settled intent has done
// its job and is removed here. An unsettled one belongs to its writer while
// that writer holds the fence; once a successor has claimed it, the merge can
// no longer be published or settled, and the audit resolves it from the
// manifest: whatever the intent names that the manifest no longer references,
// and that still exists, is planned for deletion.

// runIntentAudit looks at every writer L0 merge intent once per audit
// interval. A record that does not decode is left alone and reported.
func (c *sstCleaner) runIntentAudit(ctx context.Context, now time.Time) (stats sstCleanupWorkStats, err error) {
	if c == nil || c.opts.ManifestLog == nil {
		return stats, nil
	}
	iter := c.store.NewListIterator(blobstore.ListOptions{Prefix: sstMergeIntentPrefix + "/"})
	var unsettled []sstMergeIntent
	var auditErr error
	for stats.IntentsScanned < c.opts.OrphanScanLimit {
		object, listErr := iter.Next(ctx)
		if errors.Is(listErr, io.EOF) {
			break
		}
		if listErr != nil {
			return stats, errors.Join(auditErr, listErr)
		}
		if object.IsDir {
			continue
		}
		stats.IntentsScanned++
		payload, _, readErr := c.store.Read(ctx, object.Key)
		if readErr != nil {
			if cancelErr := reclamationCancellation(ctx, readErr); cancelErr != nil {
				return stats, errors.Join(auditErr, cancelErr)
			}
			stats.Failed++
			auditErr = errors.Join(auditErr, fmt.Errorf("read L0 merge intent %q: %w", object.Key, readErr))
			continue
		}
		intent, decodeErr := decodeSSTMergeIntent(payload)
		if decodeErr != nil {
			stats.Failed++
			auditErr = errors.Join(auditErr, fmt.Errorf("decode L0 merge intent %q: %w", object.Key, decodeErr))
			continue
		}
		if intent.Settled != "" {
			// The writer wrote the plan and said so: the record is done.
			if deleteErr := c.delete.Delete(ctx, object.Key); deleteErr != nil {
				if cancelErr := reclamationCancellation(ctx, deleteErr); cancelErr != nil {
					return stats, errors.Join(auditErr, cancelErr)
				}
				stats.Failed++
				auditErr = errors.Join(auditErr, fmt.Errorf("remove settled L0 merge intent %q: %w", object.Key, deleteErr))
				continue
			}
			stats.IntentsSettled++
			continue
		}
		unsettled = append(unsettled, intent)
	}
	if len(unsettled) == 0 {
		return stats, auditErr
	}

	live, current, replayErr := c.opts.ManifestLog.ReplayWithCurrent(ctx)
	if replayErr != nil {
		return stats, errors.Join(auditErr, replayErr)
	}
	if current == nil || current.WriterFence == nil {
		return stats, auditErr
	}
	for _, intent := range unsettled {
		if intent.WriterEpoch >= current.WriterFence.Epoch {
			// Its writer still holds the fence and may yet publish or settle.
			continue
		}
		if resolveErr := c.resolveAbandonedIntent(ctx, now, current.PinnedViewAge(), live, intent); resolveErr != nil {
			if cancelErr := reclamationCancellation(ctx, resolveErr); cancelErr != nil {
				return stats, errors.Join(auditErr, cancelErr)
			}
			stats.Failed++
			auditErr = errors.Join(auditErr, fmt.Errorf("resolve L0 merge intent %s: %w", intent.IntentID, resolveErr))
			continue
		}
		stats.IntentsResolved++
	}
	return stats, auditErr
}

// resolveAbandonedIntent plans the deletion of what an abandoned intent names
// and the manifest does not: the inputs when the merge replaced them, the
// output when it never did, and both when a drain has since taken the merged
// file as well. Only objects that still exist are planned, so a file another
// plan already deleted is not planned twice. Then the intent is removed.
func (c *sstCleaner) resolveAbandonedIntent(
	ctx context.Context,
	now time.Time,
	pinnedViewAge time.Duration,
	live liveSSTs,
	intent sstMergeIntent,
) error {
	unreferenced := func(targets []sstDeletionTarget) ([]sstDeletionTarget, error) {
		var out []sstDeletionTarget
		for _, target := range targets {
			if live.LookupSST(target.ID) != nil {
				continue
			}
			exists, err := c.store.Exists(ctx, target.Key)
			if err != nil {
				return nil, err
			}
			if exists {
				out = append(out, target)
			}
		}
		return out, nil
	}
	inputs, err := unreferenced(intent.Inputs)
	if err != nil {
		return err
	}
	outputs, err := unreferenced([]sstDeletionTarget{intent.Output})
	if err != nil {
		return err
	}
	for outcome, targets := range map[string][]sstDeletionTarget{
		sstDeletionOutcomeInputs: inputs,
		sstDeletionOutcomeOutput: outputs,
	} {
		if len(targets) == 0 {
			continue
		}
		source := sstDeletionPlanSource{
			Kind:     sstDeletionSourceWriterL0Merge,
			IntentID: intent.IntentID,
			Outcome:  outcome,
			Epoch:    intent.WriterEpoch,
		}
		// When the files stopped being referenced is not known; counting from
		// now is the conservative choice.
		plan, payload, err := newSSTDeletionPlan(c.store, source, now, now, pinnedViewAge, c.opts.SafetyMargin, targets)
		if err != nil {
			return err
		}
		if _, err := storeSSTDeletionPlan(ctx, c.store, *plan, payload); err != nil {
			return err
		}
		c.planAvailable()
		slog.Info("isledb: abandoned L0 merge intent resolved", "component", "maintenance",
			"intent", intent.IntentID, "writer_epoch", intent.WriterEpoch, "outcome", outcome,
			"targets", len(targets), "not_before", plan.NotBefore)
	}
	return c.delete.Delete(ctx, sstMergeIntentPath(c.store, intent.IntentID))
}

// liveSSTs is the part of a replayed manifest the audit needs.
type liveSSTs interface {
	LookupSST(id string) *sstMetadata
}
