package isledb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// How we Mark SST:
// 1. Read where we left off
// Load checkpoint.json (last_applied_seq, last_seen_log_seq_start).
//
// 2. Decide replay mode
// If manifest log_seq_start is same as checkpoint: do delta (only new logs).
// If different: do full replay start from current log_seq_start and run orphan scan.
//
// 3. Build marks in memory (not one-by-one writes)
// For each remove event in logs: mark SST as unreferenced.
// For each add event in logs: clear stale mark for that SST.
// Orphan scan:
// 	a. ist live SSTs from manifest,
//	b. list SST files in storage,
//  c. mark files not in manifest,
//  d. clear marks for live ones.
//
// 4. Save once
// 5. Move checkpoint forward
// Update last_applied_seq to processed end seq.
// Update last_seen_log_seq_start to current value.
//

const (
	pendingSSTDeletePrefix    = "manifest/gc/pending-sst"
	pendingSSTDeleteSetObject = pendingSSTDeletePrefix + "/pending.json"
	gcCheckpointObjectKey     = "manifest/gc/checkpoint.json"
	gcMarkSchemaVersion       = 1
	gcMarkCatchupBatchSize    = 100
	gcCASMaxRetries           = 8
)

type pendingSSTDeleteMark struct {
	Version int `json:"version,omitempty"`

	SSTID string `json:"sst_id"`

	FirstSeenUnreferencedAt time.Time `json:"first_seen_unreferenced_at,omitempty"`
	LastSeenUnreferencedAt  time.Time `json:"last_seen_unreferenced_at,omitempty"`
	FirstSeenSeq            uint64    `json:"first_seen_seq,omitempty"`
	LastSeenSeq             uint64    `json:"last_seen_seq,omitempty"`
	FirstReason             string    `json:"first_reason,omitempty"`
	LastReason              string    `json:"last_reason,omitempty"`
	HasBlobRefs             bool      `json:"has_blob_refs,omitempty"`
	DueAt                   time.Time `json:"due_at,omitempty"`
}

type pendingSSTDeleteMarkSet struct {
	Version int                    `json:"version,omitempty"`
	Marks   []pendingSSTDeleteMark `json:"marks,omitempty"`
}

type gcMarkCheckpoint struct {
	Version             int       `json:"version,omitempty"`
	LastAppliedSeq      uint64    `json:"last_applied_seq"`
	LastSeenLogSeqStart uint64    `json:"last_seen_log_seq_start,omitempty"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func enqueuePendingSSTDeleteMarks(ctx context.Context, store *blobstore.Store, sstIDs []string, reason string, seq uint64) error {
	now := time.Now().UTC()
	ids := uniqueSSTIDs(sstIDs)
	if len(ids) == 0 {
		return nil
	}

	var lastErr error
	for attempt := 0; attempt < gcCASMaxRetries; attempt++ {
		set, matchToken, exists, err := loadPendingSSTDeleteMarkSetWithCAS(ctx, store)
		if err != nil {
			return err
		}

		byID := make(map[string]pendingSSTDeleteMark, len(set.Marks))
		for _, mark := range set.Marks {
			if mark.SSTID == "" {
				continue
			}
			byID[mark.SSTID] = mark
		}

		for _, id := range ids {
			mark, found := byID[id]
			if !found {
				mark = pendingSSTDeleteMark{
					Version:                 gcMarkSchemaVersion,
					SSTID:                   id,
					FirstSeenUnreferencedAt: now,
					LastSeenUnreferencedAt:  now,
					FirstSeenSeq:            seq,
					LastSeenSeq:             seq,
					FirstReason:             reason,
					LastReason:              reason,
				}
				if seq == 0 {
					mark.LastSeenSeq = mark.FirstSeenSeq
				}
				byID[id] = mark
				continue
			}

			if mark.Version == 0 {
				mark.Version = gcMarkSchemaVersion
			}
			if mark.SSTID == "" {
				mark.SSTID = id
			}
			if mark.FirstSeenUnreferencedAt.IsZero() {
				mark.FirstSeenUnreferencedAt = now
			}
			if mark.FirstReason == "" {
				mark.FirstReason = reason
			}
			if mark.FirstSeenSeq == 0 && seq != 0 {
				mark.FirstSeenSeq = seq
			}

			mark.LastSeenUnreferencedAt = now
			mark.LastReason = reason
			if seq != 0 {
				mark.LastSeenSeq = seq
			} else if mark.LastSeenSeq == 0 {
				mark.LastSeenSeq = mark.FirstSeenSeq
			}

			byID[id] = mark
		}

		set.Marks = set.Marks[:0]
		for _, mark := range byID {
			set.Marks = append(set.Marks, mark)
		}
		sort.Slice(set.Marks, func(i, j int) bool {
			return set.Marks[i].SSTID < set.Marks[j].SSTID
		})

		err = storePendingSSTDeleteMarkSetWithCAS(ctx, store, set, matchToken, exists)
		if err == nil {
			return nil
		}
		if errors.Is(err, blobstore.ErrPreconditionFailed) {
			lastErr = err
			continue
		}
		return err
	}

	if lastErr != nil {
		return fmt.Errorf("store pending mark set after retries: %w", lastErr)
	}
	return fmt.Errorf("store pending mark set exceeded retries")
}

func clearPendingSSTDeleteMarks(ctx context.Context, store *blobstore.Store, sstIDs []string) error {
	ids := uniqueSSTIDs(sstIDs)
	if len(ids) == 0 {
		return nil
	}

	var lastErr error
	for attempt := 0; attempt < gcCASMaxRetries; attempt++ {
		set, matchToken, exists, err := loadPendingSSTDeleteMarkSetWithCAS(ctx, store)
		if err != nil {
			return err
		}

		removeSet := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			removeSet[id] = struct{}{}
		}

		filtered := set.Marks[:0]
		for _, mark := range set.Marks {
			if _, ok := removeSet[mark.SSTID]; ok {
				continue
			}
			filtered = append(filtered, mark)
		}
		set.Marks = filtered

		err = storePendingSSTDeleteMarkSetWithCAS(ctx, store, set, matchToken, exists)
		if err == nil {
			return nil
		}
		if errors.Is(err, blobstore.ErrPreconditionFailed) {
			lastErr = err
			continue
		}
		return err
	}

	if lastErr != nil {
		return fmt.Errorf("clear pending mark set after retries: %w", lastErr)
	}
	return fmt.Errorf("clear pending mark set exceeded retries")
}

func loadGCMarkCheckpoint(ctx context.Context, store *blobstore.Store) (*gcMarkCheckpoint, error) {
	checkpoint, _, _, err := loadGCMarkCheckpointWithCAS(ctx, store)
	return checkpoint, err
}

func loadGCMarkCheckpointWithCAS(ctx context.Context, store *blobstore.Store) (*gcMarkCheckpoint, string, bool, error) {
	data, _, err := store.Read(ctx, gcCheckpointPath(store))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return &gcMarkCheckpoint{Version: gcMarkSchemaVersion}, "", false, nil
		}
		return nil, "", false, err
	}

	var checkpoint gcMarkCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, "", false, err
	}
	if checkpoint.Version == 0 {
		checkpoint.Version = gcMarkSchemaVersion
	}
	matchToken, err := currentObjectMatchToken(ctx, store, gcCheckpointPath(store))
	if err != nil {
		return nil, "", false, err
	}
	return &checkpoint, matchToken, true, nil
}

func storeGCMarkCheckpoint(ctx context.Context, store *blobstore.Store, checkpoint *gcMarkCheckpoint) error {
	if checkpoint == nil {
		return errors.New("nil gc checkpoint")
	}

	var lastErr error
	for attempt := 0; attempt < gcCASMaxRetries; attempt++ {
		current, matchToken, exists, err := loadGCMarkCheckpointWithCAS(ctx, store)
		if err != nil {
			return err
		}

		merged := *checkpoint
		merged.Version = gcMarkSchemaVersion
		if current != nil {
			if merged.LastAppliedSeq < current.LastAppliedSeq {
				merged.LastAppliedSeq = current.LastAppliedSeq
			}
			if merged.LastSeenLogSeqStart < current.LastSeenLogSeqStart {
				merged.LastSeenLogSeqStart = current.LastSeenLogSeqStart
			}
		}
		merged.UpdatedAt = time.Now().UTC()

		payload, err := json.Marshal(merged)
		if err != nil {
			return err
		}
		err = writeObjectCAS(ctx, store, gcCheckpointPath(store), payload, matchToken, exists)
		if err == nil {
			return nil
		}
		if errors.Is(err, blobstore.ErrPreconditionFailed) {
			lastErr = err
			continue
		}
		return err
	}

	if lastErr != nil {
		return fmt.Errorf("store gc checkpoint after retries: %w", lastErr)
	}
	return fmt.Errorf("store gc checkpoint exceeded retries")
}

func loadPendingSSTDeleteMarks(ctx context.Context, store *blobstore.Store) ([]pendingSSTDeleteMark, error) {
	set, err := loadPendingSSTDeleteMarkSet(ctx, store)
	if err != nil {
		return nil, err
	}
	out := make([]pendingSSTDeleteMark, len(set.Marks))
	copy(out, set.Marks)
	return out, nil
}

func hasPendingSSTDeleteMark(ctx context.Context, store *blobstore.Store, sstID string) (bool, error) {
	set, err := loadPendingSSTDeleteMarkSet(ctx, store)
	if err != nil {
		return false, err
	}
	for _, mark := range set.Marks {
		if mark.SSTID == sstID {
			return true, nil
		}
	}
	return false, nil
}

func loadPendingSSTDeleteMarkSet(ctx context.Context, store *blobstore.Store) (*pendingSSTDeleteMarkSet, error) {
	set, _, _, err := loadPendingSSTDeleteMarkSetWithCAS(ctx, store)
	return set, err
}

func loadPendingSSTDeleteMarkSetWithCAS(ctx context.Context, store *blobstore.Store) (*pendingSSTDeleteMarkSet, string, bool, error) {
	path := pendingSSTDeleteSetPath(store)
	data, _, err := store.Read(ctx, path)
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return &pendingSSTDeleteMarkSet{Version: gcMarkSchemaVersion}, "", false, nil
		}
		return nil, "", false, err
	}

	var set pendingSSTDeleteMarkSet
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, "", false, err
	}
	if set.Version == 0 {
		set.Version = gcMarkSchemaVersion
	}
	matchToken, err := currentObjectMatchToken(ctx, store, path)
	if err != nil {
		return nil, "", false, err
	}
	return &set, matchToken, true, nil
}

func storePendingSSTDeleteMarkSetWithCAS(ctx context.Context, store *blobstore.Store, set *pendingSSTDeleteMarkSet, matchToken string, exists bool) error {
	if set == nil {
		return errors.New("nil pending sst delete mark set")
	}
	set.Version = gcMarkSchemaVersion

	payload, err := json.Marshal(set)
	if err != nil {
		return err
	}
	return writeObjectCAS(ctx, store, pendingSSTDeleteSetPath(store), payload, matchToken, exists)
}

func pendingSSTDeleteSetPath(store *blobstore.Store) string {
	return storeKey(store, pendingSSTDeleteSetObject)
}

func gcCheckpointPath(store *blobstore.Store) string {
	return storeKey(store, gcCheckpointObjectKey)
}

func storeKey(store *blobstore.Store, parts ...string) string {
	if store.Prefix() == "" {
		return path.Join(parts...)
	}
	return path.Join(append([]string{store.Prefix()}, parts...)...)
}

func uniqueSSTIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func currentObjectMatchToken(ctx context.Context, store *blobstore.Store, key string) (string, error) {
	attr, err := store.Attributes(ctx, key)
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	if attr.Generation > 0 {
		return fmt.Sprintf("%d", attr.Generation), nil
	}
	return attr.ETag, nil
}

func writeObjectCAS(ctx context.Context, store *blobstore.Store, key string, payload []byte, matchToken string, exists bool) error {
	if exists && matchToken == "" {
		_, err := store.Write(ctx, key, payload)
		return err
	}
	_, err := store.WriteIfMatch(ctx, key, payload, matchToken)
	return err
}
