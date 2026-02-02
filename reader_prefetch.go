package isledb

import (
	"context"
	"fmt"
	"path/filepath"
)

// RefreshAndPrefetchSSTs refreshes the manifest and prefetches any new SSTs
// that were added since the last refresh. This is useful for keeping the
// cache warm with new data. Call this periodically if you want to proactively
// cache new SSTs as they are written.
//
// WARNING: If you use this method, avoid calling Refresh() separately.
// Mixing RefreshAndPrefetchSSTs with manual Refresh calls may cause some SSTs
// to be missed during prefetch, as the function only prefetches SSTs that
// are new relative to the manifest state before the refresh. Choose one
// approach and use it consistently.
func (r *Reader) RefreshAndPrefetchSSTs(ctx context.Context) error {
	r.mu.RLock()
	oldManifest := r.manifest
	var oldIDs map[string]struct{}
	if oldManifest != nil {
		oldIDs = make(map[string]struct{})
		for _, id := range oldManifest.AllSSTIDs() {
			oldIDs[id] = struct{}{}
		}
	}
	r.mu.RUnlock()

	if err := r.Refresh(ctx); err != nil {
		return err
	}

	r.mu.RLock()
	newManifest := r.manifest
	r.mu.RUnlock()

	if newManifest == nil {
		return nil
	}

	for _, id := range newManifest.AllSSTIDs() {
		if oldIDs != nil {
			if _, existed := oldIDs[id]; existed {
				continue
			}
		}
		path := r.store.SSTPath(id)
		if err := r.prefetchSST(ctx, path); err != nil {
			return err
		}
	}

	return nil
}

func (r *Reader) prefetchSST(ctx context.Context, path string) error {
	if _, ok := r.sstCache.Get(path); ok {
		return nil
	}

	data, _, err := r.store.Read(ctx, path)
	if err != nil {
		return fmt.Errorf("read sst %s: %w", path, err)
	}

	if r.verifySST || r.verifier != nil {
		r.mu.RLock()
		m := r.manifest
		r.mu.RUnlock()
		if m != nil {
			id := filepath.Base(path)
			meta := m.LookupSST(id)
			if meta != nil {
				if err := r.validateSSTData(*meta, data); err != nil {
					return fmt.Errorf("validate sst %s: %w", path, err)
				}
			}
		}
	}

	if err := r.sstCache.Set(path, data); err != nil {
		return fmt.Errorf("cache sst %s: %w", path, err)
	}
	return nil
}
