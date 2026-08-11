package isledb

import (
	"errors"

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

func isFenceError(err error) bool {
	return errors.Is(err, manifest.ErrFenced)
}
