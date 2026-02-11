package isledb

import (
	"context"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/manifest"
)

// ReaderOpenOptions configures a read-only handle.
type ReaderOpenOptions struct {
	// CacheDir is the directory for disk caches (required).
	CacheDir string

	// SSTCacheSize is the maximum bytes for SST cache (default 1GB).
	SSTCacheSize int64

	// BlobCacheSize is the maximum bytes for blob cache (default 1GB).
	BlobCacheSize int64

	// BlobCacheMaxItemSize is the maximum size per item in the blob cache.
	BlobCacheMaxItemSize int64

	// BlockCacheSize is the maximum bytes for the in-memory block cache used
	// when range-reading SSTs. Default 0 disables the block cache.
	BlockCacheSize int64

	// AllowUnverifiedRangeRead permits range-reading SSTs without verifying
	// full-file checksums or signatures.
	AllowUnverifiedRangeRead bool

	// RangeReadMinSSTSize is the minimum SST size (bytes) required to use
	// range-read + block cache. Default 0 means no size threshold.
	RangeReadMinSSTSize int64

	// ValidateSSTChecksum verifies SST checksums on first download.
	// If enabled and checksum is missing or mismatched, reads fail.
	ValidateSSTChecksum bool

	// SSTHashVerifier verifies SST signatures when present.
	// If provided and the SST has a signature, verification is enforced.
	SSTHashVerifier SSTHashVerifier

	Metrics *ReaderMetrics

	BlobReadOptions config.BlobReadOptions
	ManifestStorage manifest.Storage
}

// DefaultReaderOpenOptions returns sane defaults for ReaderOpenOptions.
func DefaultReaderOpenOptions() ReaderOpenOptions {
	defaults := DefaultReaderOptions()
	return ReaderOpenOptions{
		SSTCacheSize:    defaults.SSTCacheSize,
		BlobCacheSize:   defaults.BlobCacheSize,
		BlobReadOptions: config.DefaultBlobReadOptions(),
	}
}

// OpenReader opens a read-only handle.
func OpenReader(ctx context.Context, store *blobstore.Store, opts ReaderOpenOptions) (*Reader, error) {
	blobReadOpts := opts.BlobReadOptions
	if blobReadOpts == (config.BlobReadOptions{}) {
		blobReadOpts = config.DefaultBlobReadOptions()
	}

	ropts := ReaderOptions{
		CacheDir:                 opts.CacheDir,
		SSTCacheSize:             opts.SSTCacheSize,
		BlobCacheSize:            opts.BlobCacheSize,
		BlobCacheMaxItemSize:     opts.BlobCacheMaxItemSize,
		BlockCacheSize:           opts.BlockCacheSize,
		AllowUnverifiedRangeRead: opts.AllowUnverifiedRangeRead,
		RangeReadMinSSTSize:      opts.RangeReadMinSSTSize,
		ValidateSSTChecksum:      opts.ValidateSSTChecksum,
		SSTHashVerifier:          opts.SSTHashVerifier,
		Metrics:                  opts.Metrics,
		ValueStorageConfig: config.ValueStorageConfig{
			ValueOptions:    config.DefaultValueOptions(),
			BlobReadOptions: blobReadOpts,
			BlobGCOptions:   config.DefaultBlobGCOptions(),
		},
		ManifestStorage: opts.ManifestStorage,
	}
	return newReader(ctx, store, ropts)
}
