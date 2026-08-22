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
	Hits              int64
	Misses            int64
	Bytes             int64
	MaxBytes          int64
	EntryCount        int
	MaxEntries        int
	PinnedBytes       int64
	PinnedEntries     int
	Evictions         int64
	Corruptions       int64
	AdmissionBypasses int64
	// SyncFailures counts verified fills that could not be made durable and
	// were therefore served transiently instead of entering the cache.
	SyncFailures int64
	// PublicationFailures counts failures while cleaning capacity victims or
	// publishing an artifact at its final cache path.
	PublicationFailures int64
}

// ReaderOpenOptions configures a read-only handle.
type ReaderOpenOptions struct {
	// CacheDir is the directory for disk caches. It must be non-empty, may be
	// owned by only one live Reader process at a time, and must remain writable
	// with enough free space to stage SST downloads for the Reader's lifetime.
	// A read that needs a full SST fails if local staging cannot be created;
	// IsleDB does not fall back to buffering an unbounded SST in memory. Once a
	// download has completed and passed checksum verification, later cache
	// publication failures are served from the staged file transiently.
	CacheDir string

	// SSTCacheSize is the maximum bytes for SST cache (default 1GB).
	SSTCacheSize int64

	// BloomDiskCacheSize is the maximum bytes for persistent, verified raw
	// Bloom sidecars. Zero selects the default (64 MiB).
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
