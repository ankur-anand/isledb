package isledb

import (
	"context"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
)

// TailingReaderOpenOptions configures a tailing reader.
type TailingReaderOpenOptions struct {
	RefreshInterval time.Duration
	OnRefresh       func()
	OnRefreshError  func(error)

	ReaderOptions ReaderOpenOptions
}

// DefaultTailingReaderOpenOptions returns sane defaults for TailingReaderOpenOptions.
func DefaultTailingReaderOpenOptions() TailingReaderOpenOptions {
	return TailingReaderOpenOptions{
		RefreshInterval: 1 * time.Second,
		ReaderOptions:   DefaultReaderOpenOptions(),
	}
}

// OpenTailingReader opens a tailing reader handle.
func OpenTailingReader(ctx context.Context, store *blobstore.Store, opts TailingReaderOpenOptions) (*TailingReader, error) {
	blobReadOpts := opts.ReaderOptions.BlobReadOptions
	if blobReadOpts == (config.BlobReadOptions{}) {
		blobReadOpts = config.DefaultBlobReadOptions()
	}

	ropts := ReaderOptions{
		SSTCacheSize:       opts.ReaderOptions.SSTCacheSize,
		SSTReaderCacheSize: opts.ReaderOptions.SSTReaderCacheSize,
		BlobCacheSize:      opts.ReaderOptions.BlobCacheSize,
		BlobCacheItemSize:  opts.ReaderOptions.BlobCacheItemSize,
		ValueStorageConfig: config.ValueStorageConfig{
			ValueOptions:    config.DefaultValueOptions(),
			BlobReadOptions: blobReadOpts,
			BlobGCOptions:   config.DefaultBlobGCOptions(),
		},
		ManifestStorage: opts.ReaderOptions.ManifestStorage,
	}

	topts := TailingReaderOptions{
		RefreshInterval: opts.RefreshInterval,
		OnRefresh:       opts.OnRefresh,
		OnRefreshError:  opts.OnRefreshError,
		ReaderOptions:   ropts,
	}
	return newTailingReader(ctx, store, topts)
}
