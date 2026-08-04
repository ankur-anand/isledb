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

const (
	gcCursorObjectKey   = "manifest/gc/CURRENT.json"
	gcMarkSchemaVersion = 1
	gcCASMaxRetries     = 8
)

type gcCursor struct {
	Version         int       `json:"version"`
	NextManifestSeq uint64    `json:"next_manifest_seq"`
	NextObjectIndex uint32    `json:"next_object_index,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type gcCursorStorageAdapter struct {
	store *blobstore.Store
}

func newGCCursorStorage(store *blobstore.Store) manifest.GCCursorStorage {
	return gcCursorStorageAdapter{store: store}
}

func (s gcCursorStorageAdapter) LoadGCCursor(ctx context.Context) ([]byte, string, bool, error) {
	return readObjectWithCAS(ctx, s.store, gcCursorPath(s.store))
}

func (s gcCursorStorageAdapter) StoreGCCursor(ctx context.Context, data []byte, matchToken string, exists bool) error {
	return writeObjectCAS(ctx, s.store, gcCursorPath(s.store), data, matchToken, exists)
}

func loadGCCursorWithCAS(ctx context.Context, storage manifest.GCCursorStorage) (*gcCursor, string, bool, error) {
	if storage == nil {
		return nil, "", false, errors.New("nil gc cursor storage")
	}
	data, matchToken, exists, err := storage.LoadGCCursor(ctx)
	if err != nil {
		return nil, "", false, err
	}
	if !exists {
		return &gcCursor{Version: gcMarkSchemaVersion}, "", false, nil
	}

	var cursor gcCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, "", false, fmt.Errorf("decode gc cursor: %w", err)
	}
	if cursor.Version != gcMarkSchemaVersion {
		return nil, "", false, fmt.Errorf("unsupported gc cursor version=%d", cursor.Version)
	}
	return &cursor, matchToken, true, nil
}

func storeGCCursorCAS(ctx context.Context, storage manifest.GCCursorStorage, cursor *gcCursor, matchToken string, exists bool) error {
	if storage == nil {
		return errors.New("nil gc cursor storage")
	}
	if cursor == nil {
		return errors.New("nil gc cursor")
	}
	next := *cursor
	next.Version = gcMarkSchemaVersion
	next.UpdatedAt = time.Now().UTC()
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	return storage.StoreGCCursor(ctx, payload, matchToken, exists)
}

func gcCursorPath(store *blobstore.Store) string {
	return storeKey(store, gcCursorObjectKey)
}

func readObjectWithCAS(ctx context.Context, store *blobstore.Store, key string) ([]byte, string, bool, error) {
	data, attrs, err := store.Read(ctx, key)
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil, "", false, nil
		}
		return nil, "", false, err
	}
	matchToken := matchTokenFromAttrs(attrs)
	if matchToken == "" {
		attrs, err = store.Attributes(ctx, key)
		if err != nil {
			return nil, "", false, err
		}
		matchToken = matchTokenFromAttrs(attrs)
	}
	return data, matchToken, true, nil
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
		return fmt.Errorf("%w: missing object version token for %s", blobstore.ErrPreconditionFailed, key)
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
