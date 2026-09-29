package isledb

import (
	"errors"
	"fmt"
	"time"
)

var ErrInvalidReaderOptions = errors.New("invalid reader options")

const (
	defaultReaderRefreshAfter = time.Minute
)

// ReaderViewPolicy controls when a Reader refreshes its manifest view.
type ReaderViewPolicy struct {
	// RefreshAfter is the maximum age of the Reader's loaded manifest before a
	// read refreshes it. Concurrent refreshes are coalesced. Zero selects the
	// default.
	RefreshAfter time.Duration
}

// CacheStats reports one reader cache's occupancy and lookup activity. Byte
// limits are zero for entry-count-bounded caches; MaxEntries is zero for
// byte-bounded caches.
type CacheStats struct {
	Hits        int64
	Misses      int64
	Bytes       int64
	MaxBytes    int64
	EntryCount  int
	MaxEntries  int
	Evictions   int64
	Corruptions int64
	// Bypasses counts downloaded files too large for the cache's whole
	// budget; they are served without being cached.
	Bypasses int64
	// Failures counts downloaded files that could not be synced or renamed
	// into the cache; they are served without being cached.
	Failures int64
}

// ReaderOpenOptions configures a read-only handle.
type ReaderOpenOptions struct {
	// CacheDir is the directory for disk caches. It must be non-empty, may be
	// owned by only one live Reader process at a time, and must remain writable
	// for the Reader's lifetime. Beyond the cache budgets, it needs room for
	// SST downloads in progress: a read that needs a full SST fails if the
	// download cannot be written locally, since IsleDB never buffers a whole
	// SST in memory. A verified download that cannot be kept in the cache is
	// still served. Opening logs a warning when the directory's filesystem
	// cannot hold the budgets.
	CacheDir string

	// SSTCacheSize is the maximum bytes of SSTs cached on disk. Zero selects
	// the default (8 GiB).
	SSTCacheSize int64

	// BloomDiskCacheSize is the maximum bytes of Bloom filters cached on disk.
	// Zero selects the default (512 MiB).
	BloomDiskCacheSize int64

	// BlockCacheSize is the maximum bytes for the in-memory block cache used
	// when range-reading SSTs. Default 0 disables the block cache.
	BlockCacheSize int64

	// BloomCacheSize is the maximum accounted bytes for decoded SST bloom
	// filters. Zero selects the default (64 MiB).
	BloomCacheSize int64

	// AllowUnverifiedRangeRead permits range-reading SSTs without verifying
	// full-file checksums.
	AllowUnverifiedRangeRead bool

	// RangeReadMinSSTSize is the minimum SST size (bytes) required to use
	// range-read + block cache. Default 0 means no size threshold.
	RangeReadMinSSTSize int64

	// RangeReadChunkSize, when positive, makes range reads fetch and cache
	// aligned chunks of this many bytes of an SST's data instead of each block
	// Pebble requests: neighbouring blocks, which a scan reads next, then come
	// from the same request. Zero reads exactly the requested blocks.
	RangeReadChunkSize int64

	// ValidateSSTChecksum verifies SST checksums on read paths that can
	// otherwise skip it. Persistent disk-cache admissions always verify.
	ValidateSSTChecksum bool

	// Views controls manifest freshness. Read-view lifetime is a store policy
	// loaded from the manifest and cannot be extended by a reader.
	Views ReaderViewPolicy

	Metrics *ReaderMetrics
}

// DefaultReaderOpenOptions returns default reader options using cacheDir for
// disk caches.
func DefaultReaderOpenOptions(cacheDir string) ReaderOpenOptions {
	defaults := defaultReaderOptions()
	return ReaderOpenOptions{
		CacheDir:           cacheDir,
		SSTCacheSize:       defaults.SSTCacheSize,
		BloomDiskCacheSize: defaults.BloomDiskCacheSize,
		BloomCacheSize:     defaults.BloomCacheSize,
		Views:              defaults.ViewPolicy,
	}
}

func readerOptionsFromPublic(opts ReaderOpenOptions) (readerOptions, error) {
	if opts.CacheDir == "" {
		return readerOptions{}, fmt.Errorf("%w: cache_dir is required", ErrInvalidReaderOptions)
	}
	if opts.SSTCacheSize < 0 {
		return readerOptions{}, fmt.Errorf(
			"%w: sst_cache_size=%d", ErrInvalidReaderOptions, opts.SSTCacheSize)
	}
	if opts.BlockCacheSize < 0 {
		return readerOptions{}, fmt.Errorf(
			"%w: block_cache_size=%d", ErrInvalidReaderOptions, opts.BlockCacheSize)
	}
	if opts.BloomCacheSize < 0 {
		return readerOptions{}, fmt.Errorf(
			"%w: bloom_cache_size=%d", ErrInvalidReaderOptions, opts.BloomCacheSize)
	}
	if opts.BloomDiskCacheSize < 0 {
		return readerOptions{}, fmt.Errorf(
			"%w: bloom_disk_cache_size=%d", ErrInvalidReaderOptions, opts.BloomDiskCacheSize)
	}
	if opts.RangeReadMinSSTSize < 0 {
		return readerOptions{}, fmt.Errorf(
			"%w: range_read_min_sst_size=%d", ErrInvalidReaderOptions, opts.RangeReadMinSSTSize)
	}
	if opts.RangeReadChunkSize < 0 {
		return readerOptions{}, fmt.Errorf(
			"%w: range_read_chunk_size=%d", ErrInvalidReaderOptions, opts.RangeReadChunkSize)
	}
	views, err := normalizeReaderViewPolicy(opts.Views)
	if err != nil {
		return readerOptions{}, err
	}

	return readerOptions{
		CacheDir:                 opts.CacheDir,
		SSTCacheSize:             opts.SSTCacheSize,
		BloomDiskCacheSize:       opts.BloomDiskCacheSize,
		BlockCacheSize:           opts.BlockCacheSize,
		BloomCacheSize:           opts.BloomCacheSize,
		AllowUnverifiedRangeRead: opts.AllowUnverifiedRangeRead,
		RangeReadMinSSTSize:      opts.RangeReadMinSSTSize,
		RangeReadChunkSize:       opts.RangeReadChunkSize,
		ValidateSSTChecksum:      opts.ValidateSSTChecksum,
		ViewPolicy:               views,
		Metrics:                  opts.Metrics,
	}, nil
}

func normalizeReaderViewPolicy(policy ReaderViewPolicy) (ReaderViewPolicy, error) {
	if policy.RefreshAfter < 0 {
		return ReaderViewPolicy{}, fmt.Errorf("%w: refresh_after=%s", ErrInvalidReaderOptions, policy.RefreshAfter)
	}
	if policy.RefreshAfter == 0 {
		policy.RefreshAfter = defaultReaderRefreshAfter
	}
	return policy, nil
}
