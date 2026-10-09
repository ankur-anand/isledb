package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

const defaultPrefetchConcurrency = 4

// PrefetchOptions controls explicit reader cache warming.
type PrefetchOptions struct {
	// Range limits prefetch to visible SSTs whose key span overlaps the range.
	Range KeyRange

	// All must be true to prefetch the full visible keyspace.
	All bool

	// MaxSSTs limits the number of uncached SSTs to download. Zero means no
	// limit.
	MaxSSTs int

	// MaxBytes limits the total bytes to download, each SST's Bloom filter
	// included. Zero means no limit.
	MaxBytes int64

	// Concurrency limits parallel SST downloads. Zero uses a small default.
	Concurrency int
}

// PrefetchStats reports what Reader.Prefetch matched and cached. CachedSSTs
// counts the selected SSTs wholly on disk when Prefetch returns.
type PrefetchStats struct {
	MatchedSSTs int
	CachedSSTs  int
	SkippedSSTs int
	BytesRead   int64
}

// Prefetch caches SSTs on disk for a fresh manifest view: their metadata,
// Bloom filters and data, fetching only what is missing. It selects only what
// fits in the disk cache's free space, counting the rest as skipped, and runs
// after any other prefetch of this Reader.
func (r *Reader) Prefetch(ctx context.Context, opts PrefetchOptions) (PrefetchStats, error) {
	if err := validatePrefetchOptions(opts); err != nil {
		return PrefetchStats{}, err
	}

	done, err := r.beginRead()
	if err != nil {
		return PrefetchStats{}, err
	}
	defer done()
	if err := r.checkManifestView(ctx); err != nil {
		return PrefetchStats{}, err
	}

	m, _, expiresAt := r.currentManifestState()
	if m != nil {
		m = m.Clone()
	}

	if m == nil {
		return PrefetchStats{}, nil
	}

	return r.prefetchSSTs(ctx, m, opts, nil, expiresAt, ErrReadViewExpired)
}

// prefetchSSTs selects SSTs of m to cache and downloads them, one prefetch at
// a time per Reader: each selects by free space once the last has stored, so
// two never fill the same space and evict what reads use.
func (r *Reader) prefetchSSTs(
	ctx context.Context,
	m *manifestState,
	opts PrefetchOptions,
	only map[string]struct{},
	expiresAt time.Time,
	expiredErr error,
) (PrefetchStats, error) {
	select {
	case r.prefetching <- struct{}{}:
	case <-ctx.Done():
		return PrefetchStats{}, ctx.Err()
	}
	defer func() { <-r.prefetching }()
	selected, stats := r.selectSSTsToPrefetch(m, opts, only)
	if r.prefetchSelected != nil {
		r.prefetchSelected()
	}
	return r.fetchPrefetchSSTs(ctx, selected, stats, opts.Concurrency, expiresAt, expiredErr)
}

// fetchPrefetchSSTs caches the selected SSTs on disk, a few at a time, before
// expiresAt, after which it fails with expiredErr.
func (r *Reader) fetchPrefetchSSTs(
	ctx context.Context,
	selected []sstMetadata,
	stats PrefetchStats,
	concurrency int,
	expiresAt time.Time,
	expiredErr error,
) (PrefetchStats, error) {
	if len(selected) == 0 {
		return stats, nil
	}
	if concurrency <= 0 {
		concurrency = defaultPrefetchConcurrency
	}

	readCtx := withReadDeadline(ctx, expiresAt, expiredErr)
	defer readCtx.release()

	var bytesRead atomic.Int64
	g, gctx := errgroup.WithContext(readCtx)
	g.SetLimit(concurrency)
	for _, sst := range selected {
		g.Go(func() error {
			fetched, err := r.prefetchSST(gctx, sst)
			bytesRead.Add(fetched)
			return err
		})
	}
	err := g.Wait()
	for _, sst := range selected {
		if r.fetcher.resident(r.fetcher.object(sst)) {
			stats.CachedSSTs++
		}
	}
	stats.BytesRead = bytesRead.Load()
	if err != nil {
		return stats, readCtx.err(err)
	}
	return stats, nil
}

func validatePrefetchOptions(opts PrefetchOptions) error {
	if opts.MaxSSTs < 0 {
		return fmt.Errorf("max ssts must be >= 0")
	}
	if opts.MaxBytes < 0 {
		return fmt.Errorf("max bytes must be >= 0")
	}
	if opts.Concurrency < 0 {
		return fmt.Errorf("concurrency must be >= 0")
	}
	hasRange := !opts.Range.isZero()
	if opts.All && hasRange {
		return errors.New("prefetch all cannot be combined with a key range")
	}
	if !opts.All && !hasRange {
		return errors.New("prefetch requires a key range or All=true")
	}
	return nil
}

// selectSSTsToPrefetch selects SSTs of m that are not on disk, and with only
// set only those in only, to fit in the disk cache's free space. Using only
// free space, a prefetch never evicts anything on disk, whichever view reads
// are using it for.
func (r *Reader) selectSSTsToPrefetch(
	m *manifestState,
	opts PrefetchOptions,
	only map[string]struct{},
) ([]sstMetadata, PrefetchStats) {
	var selected []sstMetadata
	var stats PrefetchStats
	seen := make(map[string]struct{})
	var free, downloadBytes int64
	if r.diskCache != nil {
		free = r.diskCache.Free()
	}

	visit := func(sst sstMetadata) {
		if _, ok := seen[sst.ID]; ok {
			return
		}
		seen[sst.ID] = struct{}{}

		if only != nil {
			if _, ok := only[sst.ID]; !ok {
				return
			}
		}
		if !opts.All && !sstOverlapsHalfOpenRange(sst, opts.Range) {
			return
		}
		stats.MatchedSSTs++

		o := r.fetcher.object(sst)
		if r.fetcher.resident(o) {
			stats.SkippedSSTs++
			return
		}
		if opts.MaxSSTs > 0 && len(selected) >= opts.MaxSSTs {
			stats.SkippedSSTs++
			return
		}
		// An SST takes its Size and its Bloom filter, stored after it.
		download := sst.Size + o.bloomLength
		if sst.Size <= 0 || downloadBytes+download > free ||
			(opts.MaxBytes > 0 && downloadBytes+download > opts.MaxBytes) {
			stats.SkippedSSTs++
			return
		}

		selected = append(selected, sst)
		downloadBytes += download
	}

	for _, sst := range m.L0SSTs {
		visit(sst)
	}
	for _, level := range m.Levels {
		for _, sst := range level.SSTs {
			visit(sst)
		}
	}

	return selected, stats
}

// prefetchSST caches one SST's parts on disk and reports the bytes fetched.
func (r *Reader) prefetchSST(ctx context.Context, sst sstMetadata) (int64, error) {
	return r.fetcher.prefetch(ctx, r.fetcher.object(sst))
}
