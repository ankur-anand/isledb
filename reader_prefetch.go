package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const defaultPrefetchConcurrency = 4

// KeyRange describes a half-open key range: [Min, Max).
//
// A nil Min means the beginning of the keyspace. A nil Max means the end of
// the keyspace.
type KeyRange struct {
	Min []byte
	Max []byte
}

// PrefixRange returns the half-open key range containing keys with prefix.
func PrefixRange(prefix []byte) KeyRange {
	min := append([]byte(nil), prefix...)
	return KeyRange{
		Min: min,
		Max: prefixUpperBound(prefix),
	}
}

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

// Prefetch warms the reader's SST cache for the currently loaded manifest.
//
// Prefetch does not refresh the manifest. Call Refresh first when the caller
// wants to discover newly committed SSTs before warming the cache.
func (r *Reader) Prefetch(ctx context.Context, opts PrefetchOptions) (PrefetchStats, error) {
	if err := validatePrefetchOptions(opts); err != nil {
		return PrefetchStats{}, err
	}

	done, err := r.beginRead()
	if err != nil {
		return PrefetchStats{}, err
	}
	defer done()

	m := r.currentManifest()
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

	var cached atomic.Int64
	var bytesRead atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, sst := range selected {
		sst := sst
		g.Go(func() error {
			if err := r.prefetchSST(gctx, sst); err != nil {
				return err
			}
			cached.Add(1)
			if sst.Size > 0 {
				bytesRead.Add(sst.Size)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		stats.CachedSSTs = int(cached.Load())
		stats.BytesRead = bytesRead.Load()
		return stats, err
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

func (r KeyRange) isZero() bool {
	return len(r.Min) == 0 && len(r.Max) == 0
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

func (r *Reader) selectPrefetchSSTs(m *Manifest, opts PrefetchOptions) ([]SSTMeta, PrefetchStats) {
	var selected []SSTMeta
	var stats PrefetchStats
	seen := make(map[string]struct{})
	var selectedBytes int64

	visit := func(sst SSTMeta) {
		if _, ok := seen[sst.ID]; ok {
			return
		}
		seen[sst.ID] = struct{}{}

		if !opts.All && !sstOverlapsPrefetchRange(sst, opts.Range) {
			return
		}
		stats.MatchedSSTs++

		path := r.store.SSTPath(sst.ID)
		if _, ok := r.sstCache.Get(path); ok {
			stats.SkippedSSTs++
			return
		}
		if opts.MaxSSTs > 0 && len(selected) >= opts.MaxSSTs {
			stats.SkippedSSTs++
			return
		}
		if opts.MaxBytes > 0 && sst.Size > 0 && selectedBytes+sst.Size > opts.MaxBytes {
			stats.SkippedSSTs++
			return
		}

		selected = append(selected, sst)
		if sst.Size > 0 {
			selectedBytes += sst.Size
		}
	}

	for _, sst := range m.L0SSTs {
		visit(sst)
	}
	for _, sr := range m.SortedRuns {
		for _, sst := range sr.SSTs {
			visit(sst)
		}
	}

	return selected, stats
}

func sstOverlapsPrefetchRange(sst SSTMeta, r KeyRange) bool {
	if len(sst.MinKey) == 0 || len(sst.MaxKey) == 0 {
		return false
	}
	if len(r.Min) > 0 && bytes.Compare(sst.MaxKey, r.Min) < 0 {
		return false
	}
	if len(r.Max) > 0 && bytes.Compare(sst.MinKey, r.Max) >= 0 {
		return false
	}
	return true
}

func (r *Reader) prefetchSST(ctx context.Context, sst SSTMeta) error {
	path := r.store.SSTPath(sst.ID)
	if _, ok := r.sstCache.Get(path); ok {
		return nil
	}

	if err := r.cacheSST(ctx, &sst, path); err != nil {
		return err
	}
	return nil
}
