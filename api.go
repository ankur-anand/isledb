package isledb

import (
	"errors"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/cachestore"
	"github.com/ankur-anand/isledb/manifest"
)

type Manifest = manifest.Manifest
type CompactionConfig = manifest.CompactionConfig
type SSTMeta = manifest.SSTMeta
type BloomMeta = manifest.BloomMeta
type SSTSignature = manifest.SSTSignature
type SortedRun = manifest.SortedRun
type CompactionLogPayload = manifest.CompactionLogPayload

func resolveManifestStorage(store *blobstore.Store, storage manifest.Storage) manifest.Storage {
	if storage != nil {
		return storage
	}
	return manifest.NewBlobStoreBackend(store)
}

func resolveManifestStorageWithCache(store *blobstore.Store, storage manifest.Storage, opts *ReaderOptions) manifest.Storage {
	base := resolveManifestStorage(store, storage)
	if opts != nil && opts.DisableManifestCache {
		return base
	}

	cacheOpts := cachestore.CachingStorageOptions{}
	if opts != nil {
		cacheOpts.ManifestCache = opts.ManifestLogCache
		cacheOpts.CacheSize = opts.ManifestLogCacheSize
	}
	return cachestore.NewCachingStorage(base, cacheOpts)
}

func newManifestStore(store *blobstore.Store, storage manifest.Storage) *manifest.Store {
	return manifest.NewStoreWithStorage(resolveManifestStorage(store, storage))
}

func newManifestStoreWithCache(store *blobstore.Store, opts *ReaderOptions) *manifest.Store {
	var storage manifest.Storage
	if opts != nil {
		storage = opts.ManifestStorage
	}
	return manifest.NewStoreWithStorage(resolveManifestStorageWithCache(store, storage, opts))
}

func isFenceError(err error) bool {
	return errors.Is(err, manifest.ErrFenced) || errors.Is(err, manifest.ErrFenceConflict)
}
