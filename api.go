package isledb

import (
	"context"
	"errors"
	"fmt"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/cachestore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

type manifestState = manifest.Manifest
type sstMetadata = manifest.SSTMeta
type bloomMetadata = manifest.BloomMeta

func resolveManifestStorage(store *blobstore.Store, storage manifest.Storage) manifest.Storage {
	if storage != nil {
		return storage
	}
	return manifest.NewBlobStoreBackend(store)
}

func resolveManifestStorageWithCache(store *blobstore.Store, storage manifest.Storage, opts *readerOptions) manifest.Storage {
	base := resolveManifestStorage(store, storage)
	if opts != nil && opts.DisableManifestPageCache {
		return base
	}

	cacheOpts := cachestore.CachingStorageOptions{}
	if opts != nil {
		cacheOpts.PageCache = opts.ManifestPageCache
		cacheOpts.CacheSize = opts.ManifestPageCacheSize
	}
	return cachestore.NewCachingStorage(base, cacheOpts)
}

func newManifestStore(store *blobstore.Store, storage manifest.Storage) *manifest.Store {
	return manifest.NewStoreWithStorage(resolveManifestStorage(store, storage))
}

func newManifestStoreWithCache(store *blobstore.Store, opts *readerOptions) *manifest.Store {
	var storage manifest.Storage
	if opts != nil {
		storage = opts.ManifestStorage
	}
	return manifest.NewStoreWithStorage(resolveManifestStorageWithCache(store, storage, opts))
}

func replayManifestForOpen(
	ctx context.Context,
	store *blobstore.Store,
	manifestStore *manifest.Store,
	validateArtifacts bool,
) (*manifestState, error) {
	replay := manifestStore.Replay
	if validateArtifacts {
		replay = manifestStore.ReplayWithArtifactValidation
	}

	state, err := replay(ctx)
	if err != nil || manifestStore.CurrentData() != nil {
		return state, err
	}
	occupied, err := store.HasImmutableDatabaseObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("check database prefix after missing CURRENT: %w", err)
	}
	if !occupied {
		return state, nil
	}

	// A concurrent first writer may have created CURRENT between the replay and
	// listing. Re-read once before declaring the non-empty prefix headless.
	state, err = replay(ctx)
	if err != nil {
		return nil, err
	}
	if manifestStore.CurrentData() == nil {
		return nil, fmt.Errorf("%w: database prefix contains immutable state", ErrManifestUnavailable)
	}
	return state, nil
}

func isFenceError(err error) bool {
	return errors.Is(err, manifest.ErrFenced)
}
