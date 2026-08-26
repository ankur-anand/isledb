package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

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

// PrefetchStats reports what Reader.Prefetch matched and cached.
type PrefetchStats struct {
	MatchedSSTs int
	CachedSSTs  int
	SkippedSSTs int
	BytesRead   int64
}

// Prefetch warms the reader's SST cache for a fresh manifest view.
func (r *Reader) Prefetch(ctx context.Context, opts PrefetchOptions) (PrefetchStats, error) {
	if err := validatePrefetchOptions(opts); err != nil {
		return PrefetchStats{}, err
	}

	done, err := r.beginRead()
	if err != nil {
		return PrefetchStats{}, err
	}
	defer done()
	if err := r.ensureFreshManifest(ctx); err != nil {
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

	readCtx, cancel := context.WithDeadlineCause(ctx, expiresAt, ErrReadViewExpired)
	defer cancel()

	var cached atomic.Int64
	var bytesRead atomic.Int64
	g, gctx := errgroup.WithContext(readCtx)
	g.SetLimit(concurrency)
	for _, sst := range selected {
		sst := sst
		g.Go(func() error {
			resident, downloaded, err := r.prefetchSST(gctx, sst)
			if err != nil {
				return err
			}
			if resident {
				cached.Add(1)
			}
			if downloaded > 0 {
				bytesRead.Add(downloaded)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		stats.CachedSSTs = int(cached.Load())
		stats.BytesRead = bytesRead.Load()
		return stats, readViewError(readCtx, err)
	}

	stats.CachedSSTs = int(cached.Load())
	stats.BytesRead = bytesRead.Load()
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
	var selectedBytes int64

	visit := func(sst sstMetadata) {
		if _, ok := seen[sst.ID]; ok {
			return
		}
		seen[sst.ID] = struct{}{}

		if !opts.All && !sstOverlapsHalfOpenRange(sst, opts.Range) {
			return
		}
		stats.MatchedSSTs++

		resident, err := r.sstResident(sst)
		if err != nil {
			// Descriptor errors will be returned by prefetchSST for selected
			// entries; presence probing itself must not inflate hit/miss metrics.
			resident = false
		}
		if resident {
			stats.SkippedSSTs++
			return
		}
		if opts.MaxSSTs > 0 && len(selected) >= opts.MaxSSTs {
			stats.SkippedSSTs++
			return
		}
		if opts.MaxBytes > 0 {
			if sst.Size <= 0 || sst.Size > opts.MaxBytes-selectedBytes {
				stats.SkippedSSTs++
				return
			}
		}

		selected = append(selected, sst)
		selectedBytes += sst.Size
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

func (r *Reader) prefetchSST(ctx context.Context, sst sstMetadata) (bool, int64, error) {
	path := r.store.SSTPath(sst.ID)
	if resident, err := r.sstResident(sst); err != nil {
		return false, 0, fmt.Errorf("probe sst %s: %w", sst.ID, err)
	} else if resident {
		return true, 0, nil
	}

	if err := r.cacheSST(ctx, &sst, path); err != nil {
		return false, 0, err
	}
	resident, err := r.sstResident(sst)
	if err != nil {
		return false, sst.Size, fmt.Errorf("probe prefetched sst %s: %w", sst.ID, err)
	}
	return resident, sst.Size, nil
}
