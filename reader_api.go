package isledb

import (
	"context"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/manifest"
)

// ReaderOpenOptions configures a read-only handle.
type ReaderOpenOptions struct {
	SSTCacheSize       int64
	SSTReaderCacheSize int
	BlobCacheSize      int64
	BlobCacheItemSize  int64

	BlobReadOptions config.BlobReadOptions
	ManifestStorage manifest.Storage
}

// DefaultReaderOpenOptions returns sane defaults for ReaderOpenOptions.
func DefaultReaderOpenOptions() ReaderOpenOptions {
	defaults := DefaultReaderOptions()
	return ReaderOpenOptions{
		SSTCacheSize:       defaults.SSTCacheSize,
		SSTReaderCacheSize: defaults.SSTReaderCacheSize,
		BlobReadOptions:    config.DefaultBlobReadOptions(),
	}
}

// OpenReader opens a read-only handle.
func OpenReader(ctx context.Context, store *blobstore.Store, opts ReaderOpenOptions) (*Reader, error) {
	blobReadOpts := opts.BlobReadOptions
	if blobReadOpts == (config.BlobReadOptions{}) {
		blobReadOpts = config.DefaultBlobReadOptions()
	}

	ropts := ReaderOptions{
		SSTCacheSize:       opts.SSTCacheSize,
		SSTReaderCacheSize: opts.SSTReaderCacheSize,
		BlobCacheSize:      opts.BlobCacheSize,
		BlobCacheItemSize:  opts.BlobCacheItemSize,
		ValueStorageConfig: config.ValueStorageConfig{
			ValueOptions:    config.DefaultValueOptions(),
			BlobReadOptions: blobReadOpts,
			BlobGCOptions:   config.DefaultBlobGCOptions(),
		},
		ManifestStorage: opts.ManifestStorage,
	}
	return newReader(ctx, store, ropts)
}
