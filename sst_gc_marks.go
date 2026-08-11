package isledb

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

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
