package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ankur-anand/isledb/internal/diskcache"
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

	// MaxBytes limits the total manifest-declared SST bytes to download. Zero
	// means no limit.
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
// Bloom filters and data, fetching only what is missing. It stops selecting
// SSTs once they would exceed the disk cache's data budget, counting the rest
// as skipped.
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

	selected, stats := r.selectPrefetchSSTs(m, opts)
	if len(selected) == 0 {
		return stats, nil
	}

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = defaultPrefetchConcurrency
	}

	readCtx := withReadDeadline(ctx, expiresAt, ErrReadViewExpired)
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
	err = g.Wait()
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

func (r *Reader) selectPrefetchSSTs(m *manifestState, opts PrefetchOptions) ([]sstMetadata, PrefetchStats) {
	var selected []sstMetadata
	var stats PrefetchStats
	seen := make(map[string]struct{})
	// The disk cache's data tier bounds the SSTs on disk and selected, so a
	// prefetch neither evicts what it fetches nor, repeated over more than
	// fits, what the last one cached. MaxBytes bounds only what this one
	// downloads.
	var tierMax, tierBytes, downloadBytes int64
	if r.diskCache != nil {
		tierMax = r.diskCache.Stats(diskcache.TierData).MaxBytes
	}

	visit := func(sst sstMetadata) {
		if _, ok := seen[sst.ID]; ok {
			return
		}
		seen[sst.ID] = struct{}{}

		if !opts.All && !sstOverlapsHalfOpenRange(sst, opts.Range) {
			return
		}
		stats.MatchedSSTs++

		if r.fetcher.resident(r.fetcher.object(sst)) {
			if sst.Size > 0 && tierBytes+sst.Size <= tierMax {
				tierBytes += sst.Size
			}
			stats.SkippedSSTs++
			return
		}
		if opts.MaxSSTs > 0 && len(selected) >= opts.MaxSSTs {
			stats.SkippedSSTs++
			return
		}
		if sst.Size <= 0 || tierBytes+sst.Size > tierMax ||
			(opts.MaxBytes > 0 && downloadBytes+sst.Size > opts.MaxBytes) {
			stats.SkippedSSTs++
			return
		}

		selected = append(selected, sst)
		tierBytes += sst.Size
		downloadBytes += sst.Size
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
