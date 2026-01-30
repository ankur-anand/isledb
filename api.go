package isledb

import (
	"github.com/ankur-anand/isledb/blobstore"
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

func newManifestStore(store *blobstore.Store, storage manifest.Storage) *manifest.Store {
	return manifest.NewStoreWithStorage(resolveManifestStorage(store, storage))
}
