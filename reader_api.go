package isledb

import (
	"errors"
	"fmt"
	"time"

	"github.com/ankur-anand/isledb/internal/config"
)

var ErrInvalidReaderOptions = errors.New("invalid reader options")

const (
	defaultReaderRefreshAfter   = time.Minute
	defaultReaderSnapshotMaxAge = 5 * time.Minute
	defaultReaderIteratorMaxAge = 2 * time.Minute
)

// ReaderViewPolicy bounds how long a Reader may retain one manifest view.
type ReaderViewPolicy struct {
	// RefreshAfter is the maximum age of the Reader's loaded manifest before a
	// read refreshes it. Concurrent refreshes are coalesced. Zero selects the
	// default.
	RefreshAfter time.Duration

	// SnapshotMaxAge is the maximum lifetime of an explicit Snapshot. Zero
	// selects the default.
	SnapshotMaxAge time.Duration

	// IteratorMaxAge is the maximum lifetime of an Iterator. Zero selects the
	// default. Snapshot iterators also cannot outlive their Snapshot.
	IteratorMaxAge time.Duration
}

// CacheStats reports one reader cache's occupancy and lookup activity. Byte
// limits are zero for entry-count-bounded caches; MaxEntries is zero for
// byte-bounded caches.
type CacheStats struct {
	Hits       int64
	Misses     int64
	Bytes      int64
	MaxBytes   int64
	EntryCount int
	MaxEntries int
}

// ReaderOpenOptions configures a read-only handle.
type ReaderOpenOptions struct {
	// CacheDir is the directory for disk caches. It must be non-empty.
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

	// Views controls manifest refresh and the lifetime of retained read views.
	Views ReaderViewPolicy

	Metrics *ReaderMetrics

	// VerifyBlobsOnRead re-hashes external value objects before returning them.
	VerifyBlobsOnRead bool
}

// DefaultReaderOpenOptions returns default reader options using cacheDir for
// disk caches.
func DefaultReaderOpenOptions(cacheDir string) ReaderOpenOptions {
	defaults := defaultReaderOptions()
	return ReaderOpenOptions{
		CacheDir:      cacheDir,
		SSTCacheSize:  defaults.SSTCacheSize,
		BlobCacheSize: defaults.BlobCacheSize,
		Views:         defaults.ViewPolicy,
	}
}

func readerOptionsFromPublic(opts ReaderOpenOptions) (readerOptions, error) {
	views, err := normalizeReaderViewPolicy(opts.Views)
	if err != nil {
		return readerOptions{}, err
	}

	return readerOptions{
		CacheDir:                 opts.CacheDir,
		SSTCacheSize:             opts.SSTCacheSize,
		BlobCacheSize:            opts.BlobCacheSize,
		BlobCacheMaxItemSize:     opts.BlobCacheMaxItemSize,
		BlockCacheSize:           opts.BlockCacheSize,
		AllowUnverifiedRangeRead: opts.AllowUnverifiedRangeRead,
		RangeReadMinSSTSize:      opts.RangeReadMinSSTSize,
		ValidateSSTChecksum:      opts.ValidateSSTChecksum,
		SSTHashVerifier:          opts.SSTHashVerifier,
		ViewPolicy:               views,
		Metrics:                  opts.Metrics,
		ValueStorageConfig: config.ValueStorageConfig{
			ValueOptions: config.DefaultValueOptions(),
			BlobReadOptions: config.BlobReadOptions{
				VerifyBlobsOnRead: opts.VerifyBlobsOnRead,
			},
			BlobGCOptions: config.DefaultBlobGCOptions(),
		},
	}, nil
}

func normalizeReaderViewPolicy(policy ReaderViewPolicy) (ReaderViewPolicy, error) {
	if policy.RefreshAfter < 0 {
		return ReaderViewPolicy{}, fmt.Errorf("%w: refresh_after=%s", ErrInvalidReaderOptions, policy.RefreshAfter)
	}
	if policy.SnapshotMaxAge < 0 {
		return ReaderViewPolicy{}, fmt.Errorf("%w: snapshot_max_age=%s", ErrInvalidReaderOptions, policy.SnapshotMaxAge)
	}
	if policy.IteratorMaxAge < 0 {
		return ReaderViewPolicy{}, fmt.Errorf("%w: iterator_max_age=%s", ErrInvalidReaderOptions, policy.IteratorMaxAge)
	}

	if policy.RefreshAfter == 0 {
		policy.RefreshAfter = defaultReaderRefreshAfter
	}
	if policy.SnapshotMaxAge == 0 {
		policy.SnapshotMaxAge = defaultReaderSnapshotMaxAge
	}
	if policy.IteratorMaxAge == 0 {
		policy.IteratorMaxAge = defaultReaderIteratorMaxAge
	}
	return policy, nil
}
