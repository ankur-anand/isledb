package isledb

import (
	"cmp"
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

	// BlockCacheSize is the maximum bytes of SST blocks kept in memory,
	// decoded (checksummed and decompressed), for every SST read, local or by
	// range. A lookup adds the blocks it reads; a scan, and each seek, adds
	// only the blocks holding the first 64 KiB it reads in each SST and reads
	// the rest into buffers of its own. Zero selects the default (256 MiB).
	// With cgo, it is allocated outside the Go heap.
	BlockCacheSize int64

	// RangeRead reads SSTs of at least RangeReadMinSSTSize by byte range
	// instead of downloading them whole: a lookup fetches only the blocks it
	// needs and a scan reads ahead in aligned chunks. DefaultReaderOpenOptions
	// enables it. The range-read sizes below may be set only when it is
	// enabled; zero selects each one's default.
	RangeRead bool

	// BloomCacheSize is the maximum accounted bytes for decoded SST bloom
	// filters. Zero selects the default (64 MiB).
	BloomCacheSize int64

	// MetaCacheSize is the maximum bytes of SST metadata (index, properties
	// and footer) that range reads keep in memory, separately from data so
	// data reads cannot evict it. Zero selects the default (128 MiB).
	MetaCacheSize int64

	// RangeReadMinSSTSize is the smallest SST read by byte range; smaller SSTs
	// are downloaded whole, which costs little more than one ranged request
	// and leaves the whole SST cached on disk. Zero selects the default
	// (4 MiB).
	RangeReadMinSSTSize int64

	// RangeReadAheadMin is how many bytes a scan first reads ahead once it
	// reads blocks in sequence, so the blocks it reads next come from the same
	// request. Every read-ahead starts and ends on a multiple of it. Zero
	// selects the default (128 KiB).
	RangeReadAheadMin int64

	// RangeReadAheadMax caps a scan's read-ahead, which doubles with each
	// further fetch while the scan continues, so a short scan fetches little
	// it does not read and a long scan needs few requests. Each open scan
	// holds up to this much per SST it reads. Zero selects the default
	// (4 MiB).
	//
	// Both read-ahead sizes must be between 16 KiB and 16 MiB, and the
	// maximum at least the minimum; the maximum is rounded down to a multiple
	// of the minimum.
	RangeReadAheadMax int64

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
		MetaCacheSize:      defaults.MetaCacheSize,
		RangeRead:          defaults.RangeRead,
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
	if opts.MetaCacheSize < 0 {
		return readerOptions{}, fmt.Errorf(
			"%w: meta_cache_size=%d", ErrInvalidReaderOptions, opts.MetaCacheSize)
	}
	if opts.BloomDiskCacheSize < 0 {
		return readerOptions{}, fmt.Errorf(
			"%w: bloom_disk_cache_size=%d", ErrInvalidReaderOptions, opts.BloomDiskCacheSize)
	}
	if opts.RangeReadMinSSTSize < 0 {
		return readerOptions{}, fmt.Errorf(
			"%w: range_read_min_sst_size=%d", ErrInvalidReaderOptions, opts.RangeReadMinSSTSize)
	}
	if !opts.RangeRead && (opts.RangeReadMinSSTSize != 0 ||
		opts.RangeReadAheadMin != 0 || opts.RangeReadAheadMax != 0) {
		return readerOptions{}, fmt.Errorf(
			"%w: range_read_min_sst_size and range_read_ahead_min/max need range_read",
			ErrInvalidReaderOptions)
	}
	aheadMin := cmp.Or(opts.RangeReadAheadMin, defaultRangeReadAheadMin)
	aheadMax := cmp.Or(opts.RangeReadAheadMax, defaultRangeReadAheadMax)
	if aheadMin < minRangeReadAhead || aheadMax > maxRangeReadAhead || aheadMax < aheadMin {
		return readerOptions{}, fmt.Errorf(
			"%w: range_read_ahead_min=%d range_read_ahead_max=%d, want 16 KiB <= min <= max <= 16 MiB",
			ErrInvalidReaderOptions, aheadMin, aheadMax)
	}
	views, err := normalizeReaderViewPolicy(opts.Views)
	if err != nil {
		return readerOptions{}, err
	}

	return readerOptions{
		CacheDir:            opts.CacheDir,
		SSTCacheSize:        opts.SSTCacheSize,
		BloomDiskCacheSize:  opts.BloomDiskCacheSize,
		BlockCacheSize:      opts.BlockCacheSize,
		BloomCacheSize:      opts.BloomCacheSize,
		MetaCacheSize:       opts.MetaCacheSize,
		RangeRead:           opts.RangeRead,
		RangeReadMinSSTSize: opts.RangeReadMinSSTSize,
		RangeReadAheadMin:   opts.RangeReadAheadMin,
		RangeReadAheadMax:   opts.RangeReadAheadMax,
		ViewPolicy:          views,
		Metrics:             opts.Metrics,
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
