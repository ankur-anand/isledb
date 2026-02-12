package isledb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
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

type gcMarkStorageAdapter struct {
	store *blobstore.Store
}

func newGCMarkStorage(store *blobstore.Store) manifest.GCMarkStorage {
	return gcMarkStorageAdapter{store: store}
}

func readObjectWithCAS(ctx context.Context, store *blobstore.Store, key string) ([]byte, string, bool, error) {
	data, attrs, err := store.Read(ctx, key)
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil, "", false, nil
		}
		return nil, "", false, err
	}
	return data, matchTokenFromAttrs(attrs), true, nil
}

func (s gcMarkStorageAdapter) LoadPendingDeleteMarks(ctx context.Context) ([]byte, string, bool, error) {
	return readObjectWithCAS(ctx, s.store, pendingSSTDeleteSetPath(s.store))
}

func (s gcMarkStorageAdapter) StorePendingDeleteMarks(ctx context.Context, data []byte, matchToken string, exists bool) error {
	return writeObjectCAS(ctx, s.store, pendingSSTDeleteSetPath(s.store), data, matchToken, exists)
}

func (s gcMarkStorageAdapter) LoadGCCheckpoint(ctx context.Context) ([]byte, string, bool, error) {
	return readObjectWithCAS(ctx, s.store, gcCheckpointPath(s.store))
}

func (s gcMarkStorageAdapter) StoreGCCheckpoint(ctx context.Context, data []byte, matchToken string, exists bool) error {
	return writeObjectCAS(ctx, s.store, gcCheckpointPath(s.store), data, matchToken, exists)
}

func enqueuePendingSSTDeleteMarks(ctx context.Context, store *blobstore.Store, sstIDs []string, reason string, seq uint64) error {
	return enqueuePendingSSTDeleteMarksWithStorage(ctx, newGCMarkStorage(store), sstIDs, reason, seq)
}

func enqueuePendingSSTDeleteMarksWithStorage(ctx context.Context, storage manifest.GCMarkStorage, sstIDs []string, reason string, seq uint64) error {
	now := time.Now().UTC()
	ids := uniqueSSTIDs(sstIDs)
	if len(ids) == 0 {
		return nil
	}
	if storage == nil {
		return errors.New("nil gc mark storage")
	}

	return withGCMarkCASRetries("store pending mark set", func() error {
		set, matchToken, exists, err := loadPendingSSTDeleteMarkSetWithStorageCAS(ctx, storage)
		if err != nil {
			return err
		}

		byID := pendingMarkMapFromSet(set)
		if !applyPendingSSTDeleteMarkUpserts(byID, ids, reason, seq, now) {
			return nil
		}
		pendingMarkMapToSet(set, byID)

		return storePendingSSTDeleteMarkSetWithStorageCAS(ctx, storage, set, matchToken, exists)
	})
}

func clearPendingSSTDeleteMarks(ctx context.Context, store *blobstore.Store, sstIDs []string) error {
	return clearPendingSSTDeleteMarksWithStorage(ctx, newGCMarkStorage(store), sstIDs)
}

func clearPendingSSTDeleteMarksWithStorage(ctx context.Context, storage manifest.GCMarkStorage, sstIDs []string) error {
	ids := uniqueSSTIDs(sstIDs)
	if len(ids) == 0 {
		return nil
	}
	if storage == nil {
		return errors.New("nil gc mark storage")
	}

	return withGCMarkCASRetries("clear pending mark set", func() error {
		set, matchToken, exists, err := loadPendingSSTDeleteMarkSetWithStorageCAS(ctx, storage)
		if err != nil {
			return err
		}

		byID := pendingMarkMapFromSet(set)
		if !applyPendingSSTDeleteMarkClears(byID, ids) {
			return nil
		}
		pendingMarkMapToSet(set, byID)

		return storePendingSSTDeleteMarkSetWithStorageCAS(ctx, storage, set, matchToken, exists)
	})
}

func loadGCMarkCheckpoint(ctx context.Context, store *blobstore.Store) (*gcMarkCheckpoint, error) {
	checkpoint, _, _, err := loadGCMarkCheckpointWithCAS(ctx, store)
	return checkpoint, err
}

func loadGCMarkCheckpointWithStorage(ctx context.Context, storage manifest.GCMarkStorage) (*gcMarkCheckpoint, error) {
	checkpoint, _, _, err := loadGCMarkCheckpointWithStorageCAS(ctx, storage)
	return checkpoint, err
}

func loadGCMarkCheckpointWithCAS(ctx context.Context, store *blobstore.Store) (*gcMarkCheckpoint, string, bool, error) {
	return loadGCMarkCheckpointWithStorageCAS(ctx, newGCMarkStorage(store))
}

func loadGCMarkCheckpointWithStorageCAS(ctx context.Context, storage manifest.GCMarkStorage) (*gcMarkCheckpoint, string, bool, error) {
	if storage == nil {
		return nil, "", false, errors.New("nil gc mark storage")
	}
	data, matchToken, exists, err := storage.LoadGCCheckpoint(ctx)
	if err != nil {
		return nil, "", false, err
	}
	if !exists {
		return &gcMarkCheckpoint{Version: gcMarkSchemaVersion}, "", false, nil
	}

	var checkpoint gcMarkCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, "", false, err
	}
	if checkpoint.Version == 0 {
		checkpoint.Version = gcMarkSchemaVersion
	}
	return &checkpoint, matchToken, true, nil
}

func storeGCMarkCheckpoint(ctx context.Context, store *blobstore.Store, checkpoint *gcMarkCheckpoint) error {
	return storeGCMarkCheckpointWithStorage(ctx, newGCMarkStorage(store), checkpoint)
}

func storeGCMarkCheckpointWithStorage(ctx context.Context, storage manifest.GCMarkStorage, checkpoint *gcMarkCheckpoint) error {
	if storage == nil {
		return errors.New("nil gc mark storage")
	}
	if checkpoint == nil {
		return errors.New("nil gc checkpoint")
	}

	return withGCMarkCASRetries("store gc checkpoint", func() error {
		current, matchToken, exists, err := loadGCMarkCheckpointWithStorageCAS(ctx, storage)
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
		err = storage.StoreGCCheckpoint(ctx, payload, matchToken, exists)
		return err
	})
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
	return loadPendingSSTDeleteMarkSetWithStorageCAS(ctx, newGCMarkStorage(store))
}

func loadPendingSSTDeleteMarkSetWithStorageCAS(ctx context.Context, storage manifest.GCMarkStorage) (*pendingSSTDeleteMarkSet, string, bool, error) {
	if storage == nil {
		return nil, "", false, errors.New("nil gc mark storage")
	}
	data, matchToken, exists, err := storage.LoadPendingDeleteMarks(ctx)
	if err != nil {
		return nil, "", false, err
	}
	if !exists {
		return &pendingSSTDeleteMarkSet{Version: gcMarkSchemaVersion}, "", false, nil
	}

	var set pendingSSTDeleteMarkSet
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, "", false, err
	}
	if set.Version == 0 {
		set.Version = gcMarkSchemaVersion
	}
	return &set, matchToken, true, nil
}

func storePendingSSTDeleteMarkSet(ctx context.Context, store *blobstore.Store, set *pendingSSTDeleteMarkSet) error {
	return storePendingSSTDeleteMarkSetWithCAS(ctx, store, set, "", false)
}

func storePendingSSTDeleteMarkSetWithCAS(ctx context.Context, store *blobstore.Store, set *pendingSSTDeleteMarkSet, matchToken string, exists bool) error {
	return storePendingSSTDeleteMarkSetWithStorageCAS(ctx, newGCMarkStorage(store), set, matchToken, exists)
}

func storePendingSSTDeleteMarkSetWithStorageCAS(ctx context.Context, storage manifest.GCMarkStorage, set *pendingSSTDeleteMarkSet, matchToken string, exists bool) error {
	if storage == nil {
		return errors.New("nil gc mark storage")
	}
	if set == nil {
		return errors.New("nil pending sst delete mark set")
	}
	set.Version = gcMarkSchemaVersion

	payload, err := json.Marshal(set)
	if err != nil {
		return err
	}
	return storage.StorePendingDeleteMarks(ctx, payload, matchToken, exists)
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

func matchTokenFromAttrs(attr blobstore.Attributes) string {
	if attr.Generation > 0 {
		return fmt.Sprintf("%d", attr.Generation)
	}
	return attr.ETag
}

func writeObjectCAS(ctx context.Context, store *blobstore.Store, key string, payload []byte, matchToken string, exists bool) error {
	if exists && matchToken == "" {
		_, err := store.Write(ctx, key, payload)
		return err
	}
	_, err := store.WriteIfMatch(ctx, key, payload, matchToken)
	return err
}

func isGCMarkCASConflict(err error) bool {
	return errors.Is(err, blobstore.ErrPreconditionFailed) || errors.Is(err, manifest.ErrPreconditionFailed)
}

func withGCMarkCASRetries(op string, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < gcCASMaxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if isGCMarkCASConflict(err) {
			lastErr = err
			continue
		}
		return err
	}

	if lastErr != nil {
		return fmt.Errorf("%s after retries: %w", op, lastErr)
	}
	return fmt.Errorf("%s exceeded retries", op)
}
