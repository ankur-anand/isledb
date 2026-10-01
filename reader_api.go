package isledb

import (
	"errors"
	"fmt"
	"time"
)

var ErrInvalidReaderOptions = errors.New("invalid reader options")

const (
	defaultReaderRefreshAfter = time.Minute
	// minReaderRefreshAfter is the shortest RefreshAfter accepted: each
	// refresh reads CURRENT from object storage.
	minReaderRefreshAfter = time.Second
)

// ReaderViewPolicy controls when a Reader refreshes its manifest view.
type ReaderViewPolicy struct {
	// RefreshAfter is how often the Reader refreshes its manifest view in the
	// background. Reads never wait for a refresh; they use the last view
	// published. A failed refresh is retried after 30 seconds, or RefreshAfter
	// if shorter. Zero selects one minute; values under one second are
	// rejected.
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
	// Bypasses counts entries too large for the cache's whole budget; they
	// are served without being cached.
	Bypasses int64
	// Failures counts entries that could not be written into the cache; they
	// are served without being cached.
	Failures int64
}

// ReaderOpenOptions configures a read-only handle.
type ReaderOpenOptions struct {
	// CacheDir is the directory for the disk cache. It must be non-empty, may
	// be owned by only one live Reader process at a time, and must remain
	// writable for the Reader's lifetime. Opening logs a warning when the
	// directory's filesystem cannot hold DiskCacheSize.
	CacheDir string

	// DiskCacheSize bounds everything the reader keeps on disk: SST metadata,
	// Bloom filters, small SSTs and chunks of larger SSTs' data. An eighth of
	// it is kept for metadata and Bloom filters, so bulk data cannot evict
	// them. Zero selects the default (8 GiB).
	DiskCacheSize int64

	// BlockCacheSize is the maximum bytes of SST blocks kept in memory,
	// decoded (checksummed and decompressed). A lookup adds the blocks it
	// reads; a scan, and each seek, adds only the blocks holding the first
	// 64 KiB it reads in each SST and reads the rest into buffers of its own.
	// Zero selects the default (256 MiB). With cgo, it is allocated outside
	// the Go heap.
	BlockCacheSize int64

	// BloomCacheSize is the maximum accounted bytes of parsed Bloom filters
	// kept in memory. Zero selects the default (64 MiB).
	BloomCacheSize int64

	// Views controls manifest freshness. Read-view lifetime is a store policy
	// loaded from the manifest and cannot be extended by a reader.
	Views ReaderViewPolicy

	Metrics *ReaderMetrics
}

// DefaultReaderOpenOptions returns default reader options using cacheDir for
// the disk cache.
func DefaultReaderOpenOptions(cacheDir string) ReaderOpenOptions {
	defaults := defaultReaderOptions()
	return ReaderOpenOptions{
		CacheDir:       cacheDir,
		DiskCacheSize:  defaults.DiskCacheSize,
		BlockCacheSize: defaults.BlockCacheSize,
		BloomCacheSize: defaults.BloomCacheSize,
		Views:          defaults.ViewPolicy,
	}
}

func readerOptionsFromPublic(opts ReaderOpenOptions) (readerOptions, error) {
	if opts.CacheDir == "" {
		return readerOptions{}, fmt.Errorf("%w: cache_dir is required", ErrInvalidReaderOptions)
	}
	for _, size := range []struct {
		name  string
		value int64
	}{
		{"disk_cache_size", opts.DiskCacheSize},
		{"block_cache_size", opts.BlockCacheSize},
		{"bloom_cache_size", opts.BloomCacheSize},
	} {
		if size.value < 0 {
			return readerOptions{}, fmt.Errorf("%w: %s=%d", ErrInvalidReaderOptions, size.name, size.value)
		}
	}
	views, err := normalizeReaderViewPolicy(opts.Views)
	if err != nil {
		return readerOptions{}, err
	}
	return readerOptions{
		CacheDir:       opts.CacheDir,
		DiskCacheSize:  opts.DiskCacheSize,
		BlockCacheSize: opts.BlockCacheSize,
		BloomCacheSize: opts.BloomCacheSize,
		ViewPolicy:     views,
		Metrics:        opts.Metrics,
	}, nil
}

func normalizeReaderViewPolicy(policy ReaderViewPolicy) (ReaderViewPolicy, error) {
	if policy.RefreshAfter < 0 || (policy.RefreshAfter > 0 && policy.RefreshAfter < minReaderRefreshAfter) {
		return ReaderViewPolicy{}, fmt.Errorf("%w: refresh_after=%s, want zero or at least %s",
			ErrInvalidReaderOptions, policy.RefreshAfter, minReaderRefreshAfter)
	}
	if policy.RefreshAfter == 0 {
		policy.RefreshAfter = defaultReaderRefreshAfter
	}
	return policy, nil
}
