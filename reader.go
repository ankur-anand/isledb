package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/cachestore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/dgraph-io/ristretto/v2/z"
)

type Reader struct {
	store         *blobstore.Store
	manifestStore *manifest.Store
	artifactCache *diskcache.ArtifactCache
	blockCache    *ristretto.Cache[string, []byte]
	bloomCache    *bloomFilterCache
	bloomLoads    coalescedLoadGroup
	sstLoads      coalescedLoadGroup
	sstRangeLoads coalescedLoadGroup
	manifestLoads coalescedLoadGroup

	verifySST                bool
	allowUnverifiedRangeRead bool
	rangeReadMinSSTSize      int64

	ownsArtifactCache bool
	ownsBlockCache    bool
	cacheDir          string

	lifecycleMu                        sync.RWMutex
	iteratorsMu                        sync.Mutex
	iterators                          map[*Iterator]struct{}
	mu                                 sync.RWMutex
	manifest                           *manifestState
	version                            Version
	changeFeed                         bool
	changeHead                         ChangeCursor
	viewPolicy                         ReaderViewPolicy
	viewRefreshAt                      time.Time
	viewExpiresAt                      time.Time
	viewExpired                        atomic.Bool
	viewTimerMu                        sync.Mutex
	viewTimer                          *time.Timer
	viewTimerID                        atomic.Uint64
	metrics                            *ReaderMetrics
	artifactInvariantDiagnosticLimiter readerDiagnosticLimiter
	bloomDiagnosticLimiter             readerDiagnosticLimiter
	closed                             atomic.Bool
	releaseOnce                        sync.Once
	release                            func()
}

type KV struct {
	Key   []byte
	Value []byte
}

func newReader(ctx context.Context, store *blobstore.Store, opts readerOptions) (*Reader, error) {
	viewPolicy, err := normalizeReaderViewPolicy(opts.ViewPolicy)
	if err != nil {
		return nil, err
	}

	ms := newManifestStoreWithCache(store, &opts)
	viewLoadedAt := time.Now()
	m, err := replayManifestForOpen(ctx, store, ms, true)
	if err != nil {
		return nil, err
	}

	artifactCache, ownsArtifactCache, err := initReaderDiskCache(opts)
	if err != nil {
		return nil, err
	}
	cleanupDiskCache := true
	defer func() {
		if cleanupDiskCache && ownsArtifactCache {
			_ = artifactCache.Close()
		}
	}()

	blockCache, ownsBlockCache, err := initBlockCache(opts)
	if err != nil {
		return nil, err
	}
	cleanupBlockCache := ownsBlockCache
	defer func() {
		if cleanupBlockCache {
			blockCache.Close()
		}
	}()

	viewRefreshAt := viewLoadedAt.Add(viewPolicy.RefreshAfter)
	current := ms.CurrentData()
	changeFeed, changeHead := readerChangeFeedState(current)
	viewExpiresAt := viewLoadedAt.Add(current.PinnedViewAge())
	reader := &Reader{
		store:                    store,
		manifestStore:            ms,
		manifest:                 m,
		version:                  versionFromCurrent(current),
		changeFeed:               changeFeed,
		changeHead:               changeHead,
		viewPolicy:               viewPolicy,
		viewRefreshAt:            viewRefreshAt,
		viewExpiresAt:            viewExpiresAt,
		artifactCache:            artifactCache,
		blockCache:               blockCache,
		bloomCache:               newBloomFilterCache(opts.BloomCacheSize),
		verifySST:                opts.ValidateSSTChecksum,
		allowUnverifiedRangeRead: opts.AllowUnverifiedRangeRead,
		rangeReadMinSSTSize:      opts.RangeReadMinSSTSize,
		ownsArtifactCache:        ownsArtifactCache,
		ownsBlockCache:           ownsBlockCache,
		cacheDir:                 opts.CacheDir,
		metrics:                  opts.Metrics,
	}
	reader.armManifestExpiry(viewRefreshAt, viewExpiresAt)
	cleanupDiskCache = false
	cleanupBlockCache = false
	return reader, nil
}

func initReaderDiskCache(opts readerOptions) (
	*diskcache.ArtifactCache,
	bool,
	error,
) {
	if opts.ArtifactCache != nil {
		return opts.ArtifactCache, false, nil
	}

	if opts.CacheDir == "" {
		return nil, false, errors.New("cache dir is required")
	}

	maxSize := opts.SSTCacheSize
	if maxSize == 0 {
		maxSize = defaultSSTCacheSize
	}

	cache, err := diskcache.OpenArtifactCache(diskcache.ArtifactCacheOptions{
		Dir:           filepath.Join(opts.CacheDir, "artifacts"),
		SSTMaxBytes:   maxSize,
		BloomMaxBytes: opts.BloomDiskCacheSize,
	})
	if err != nil {
		return nil, false, fmt.Errorf("create artifact cache: %w", err)
	}
	// The previous process-lifetime cache used CacheDir/sst. It cannot be
	// adopted by the digest-addressed artifact cache and otherwise survives an
	// upgrade forever outside the new cache's accounting.
	if err := os.RemoveAll(filepath.Join(opts.CacheDir, "sst")); err != nil {
		_ = cache.Close()
		return nil, false, fmt.Errorf("remove legacy SST cache: %w", err)
	}

	return cache, true, nil
}

// Refresh reloads and publishes the current manifest view.
func (r *Reader) Refresh(ctx context.Context) (err error) {
	done, err := r.beginRead()
	if err != nil {
		return err
	}
	defer done()
	return r.refreshManifest(ctx, true)
}

func (r *Reader) ensureFreshManifest(ctx context.Context) error {
	if !r.manifestViewExpired() {
		return nil
	}
	return r.refreshManifest(ctx, false)
}

func (r *Reader) refreshManifest(ctx context.Context, force bool) error {
	_, err := r.manifestLoads.Do(ctx, "manifest", func(loadCtx context.Context) (any, error) {
		if !force && !r.manifestViewExpired() {
			return nil, nil
		}
		return nil, r.reloadManifest(loadCtx)
	})
	return err
}

func (r *Reader) reloadManifest(ctx context.Context) (err error) {
	viewLoadedAt := time.Now()
	start := viewLoadedAt
	defer func() {
		r.metrics.ObserveRefresh(time.Since(start), err)
	}()

	var m *manifestState
	m, err = r.manifestStore.ReplayWithArtifactValidation(ctx)
	if err != nil {
		return err
	}
	current := r.manifestStore.CurrentData()
	r.publishManifestView(m, current, viewLoadedAt)
	return nil
}

func (r *Reader) publishManifestView(
	m *manifestState,
	current *manifest.Current,
	viewLoadedAt time.Time,
) {
	changeFeed, changeHead := readerChangeFeedState(current)
	refreshAt := viewLoadedAt.Add(r.viewPolicy.RefreshAfter)
	expiresAt := viewLoadedAt.Add(current.PinnedViewAge())

	// Manifest states and SST IDs are immutable after publication. Swap the view
	// and its metadata under one short critical section. Artifact and decoded
	// Bloom caches remain byte-bounded and age retired entries through their LRUs.
	r.mu.Lock()
	r.manifest = m
	r.version = versionFromCurrent(current)
	r.changeFeed = changeFeed
	r.changeHead = changeHead
	r.viewRefreshAt = refreshAt
	r.viewExpiresAt = expiresAt
	r.mu.Unlock()

	r.armManifestExpiry(refreshAt, expiresAt)
}

func readerChangeFeedState(current *manifest.Current) (bool, ChangeCursor) {
	if current == nil {
		return false, changeCursorAt(0, 0)
	}
	return current.ChangeFeedEnabled, changeCursorAt(current.NextSeq, 0)
}

func (r *Reader) armManifestExpiry(refreshAt, expiresAt time.Time) {
	timerID := r.viewTimerID.Add(1)
	r.viewExpired.Store(false)
	wakeAt := minTime(refreshAt, expiresAt)
	delay := time.Until(wakeAt)
	if delay < 0 {
		delay = 0
	}
	timer := time.AfterFunc(delay, func() {
		if r.viewTimerID.Load() == timerID && !r.closed.Load() {
			r.viewExpired.Store(true)
		}
	})

	r.viewTimerMu.Lock()
	previous := r.viewTimer
	r.viewTimer = timer
	r.viewTimerMu.Unlock()
	if previous != nil {
		previous.Stop()
	}
}

func (r *Reader) manifestViewExpired() bool {
	if r.viewExpired.Load() {
		return true
	}
	r.mu.RLock()
	wakeAt := minTime(r.viewRefreshAt, r.viewExpiresAt)
	r.mu.RUnlock()
	if !wakeAt.IsZero() && !time.Now().Before(wakeAt) {
		r.viewExpired.Store(true)
		return true
	}
	return false
}

func (r *Reader) stopManifestExpiry() {
	r.viewTimerID.Add(1)
	r.viewTimerMu.Lock()
	timer := r.viewTimer
	r.viewTimer = nil
	r.viewTimerMu.Unlock()
	if timer != nil {
		timer.Stop()
	}
}

func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	defer r.releaseReader()
	r.stopManifestExpiry()
	r.closeOpenIterators()
	r.manifestLoads.Close(ErrReaderClosed)
	r.bloomLoads.Close(ErrReaderClosed)
	r.sstLoads.Close(ErrReaderClosed)
	r.sstRangeLoads.Close(ErrReaderClosed)

	var firstErr error

	if r.artifactCache != nil && r.ownsArtifactCache {
		if err := r.artifactCache.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if r.blockCache != nil && r.ownsBlockCache {
		r.blockCache.Close()
	}
	r.bloomCache.clear()

	return firstErr
}

func (r *Reader) closeDB() error {
	return r.Close()
}

func (r *Reader) releaseReader() {
	if r == nil || r.release == nil {
		return
	}
	r.releaseOnce.Do(r.release)
}

func (r *Reader) registerIterator(it *Iterator) {
	r.iteratorsMu.Lock()
	defer r.iteratorsMu.Unlock()
	if r.iterators == nil {
		r.iterators = make(map[*Iterator]struct{})
	}
	r.iterators[it] = struct{}{}
}

func (r *Reader) unregisterIterator(it *Iterator) {
	r.iteratorsMu.Lock()
	delete(r.iterators, it)
	r.iteratorsMu.Unlock()
}

func (r *Reader) closeOpenIterators() {
	r.iteratorsMu.Lock()
	iters := make([]*Iterator, 0, len(r.iterators))
	for it := range r.iterators {
		iters = append(iters, it)
	}
	clear(r.iterators)
	r.iteratorsMu.Unlock()

	for _, it := range iters {
		_ = it.close(ErrReaderClosed)
	}
}

func (r *Reader) beginRead() (func(), error) {
	if r == nil {
		return nil, ErrReaderClosed
	}
	r.lifecycleMu.RLock()
	if r.closed.Load() {
		r.lifecycleMu.RUnlock()
		return nil, ErrReaderClosed
	}
	return r.lifecycleMu.RUnlock, nil
}

// currentManifest returns the currently published manifest pointer.
// Callers must treat it as read-only.
//
// It is safe for snapshots to retain this pointer because Refresh swaps
// r.manifest to a new manifest; it does not mutate the previous manifest in
// place.
func (r *Reader) currentManifest() *manifestState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manifest
}

func (r *Reader) currentManifestState() (*manifestState, Version, time.Time) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manifest, r.version, r.viewExpiresAt
}

func (r *Reader) currentBootstrapState() (*manifestState, Version, ChangeCursor, bool, time.Time) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manifest, r.version, r.changeHead, r.changeFeed, r.viewExpiresAt
}

// Snapshot returns an immutable read handle over a fresh manifest state. The
// returned snapshot does not refresh and inherits that view's store deadline.
func (r *Reader) Snapshot(ctx context.Context) (*Snapshot, error) {
	done, err := r.beginRead()
	if err != nil {
		return nil, err
	}
	defer done()

	if err := r.ensureFreshManifest(ctx); err != nil {
		return nil, err
	}
	m, version, expiresAt := r.currentManifestState()
	if m == nil {
		return nil, errors.New("manifest not loaded")
	}
	if !time.Now().Before(expiresAt) {
		return nil, ErrReadViewExpired
	}
	return newSnapshot(r, m, version, expiresAt), nil
}

// BootstrapView returns an immutable KV snapshot and the first change-feed
// cursor not represented by it. Snapshot, Cursor, and Version all come from
// one loaded CURRENT, so an application can materialize the snapshot and then
// resume the feed from Cursor without a gap.
//
// The returned snapshot inherits the loaded view's store deadline. The caller
// must close view.Snapshot when it is no longer needed. Like Snapshot, this
// method follows the reader's freshness policy; call Refresh first when the
// application requires the latest published CURRENT immediately.
func (r *Reader) BootstrapView(ctx context.Context) (*BootstrapView, error) {
	done, err := r.beginRead()
	if err != nil {
		return nil, err
	}
	defer done()

	if err := r.ensureFreshManifest(ctx); err != nil {
		return nil, err
	}
	m, version, cursor, changeFeed, expiresAt := r.currentBootstrapState()
	if m == nil {
		return nil, errors.New("manifest not loaded")
	}
	if !changeFeed {
		return nil, ErrChangeFeedDisabled
	}
	if !time.Now().Before(expiresAt) {
		return nil, ErrReadViewExpired
	}
	snapshot := newSnapshot(r, m, version, expiresAt)
	return &BootstrapView{
		Snapshot: snapshot,
		Cursor:   cursor,
		Version:  version,
	}, nil
}

// Get returns the value for key if present and not deleted/expired.
func (r *Reader) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	start := time.Now()
	defer func() {
		r.metrics.ObserveGet(time.Since(start), found, err)
	}()

	done, err := r.beginRead()
	if err != nil {
		return nil, false, err
	}
	defer done()

	if len(key) == 0 {
		return nil, false, errors.New("empty key")
	}
	if err := r.ensureFreshManifest(ctx); err != nil {
		return nil, false, err
	}

	m, _, expiresAt := r.currentManifestState()
	readCtx, cancel := context.WithDeadlineCause(ctx, expiresAt, ErrReadViewExpired)
	defer cancel()
	value, found, err = r.getWithManifest(readCtx, m, key)
	return value, found, readViewError(readCtx, err)
}

func (r *Reader) getWithManifest(ctx context.Context, m *manifestState, key []byte) ([]byte, bool, error) {
	if m == nil {
		return nil, false, errors.New("manifest not loaded")
	}

	for _, sst := range m.L0SSTs {
		if !keyInSSTRange(key, sst.MinKey, sst.MaxKey) {
			continue
		}
		val, got, deleted, err := r.getFromSST(ctx, sst, key)
		if err != nil {
			return nil, false, err
		}
		if got {
			if deleted {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	for i := range m.Levels {
		sst := m.Levels[i].FindSST(key)
		if sst == nil {
			continue
		}

		val, got, deleted, err := r.getFromSST(ctx, *sst, key)
		if err != nil {
			return nil, false, err
		}
		if got {
			if deleted {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	return nil, false, nil
}

// Scan returns all key-value pairs in the half-open range [minKey, maxKey).
// A nil or empty bound leaves that side unbounded.
func (r *Reader) Scan(ctx context.Context, minKey, maxKey []byte) (out []KV, err error) {
	start := time.Now()
	defer func() {
		r.metrics.ObserveScan(time.Since(start), len(out), err)
	}()

	done, err := r.beginRead()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := r.ensureFreshManifest(ctx); err != nil {
		return nil, err
	}

	m, _, expiresAt := r.currentManifestState()
	readCtx, cancel := context.WithDeadlineCause(ctx, expiresAt, ErrReadViewExpired)
	defer cancel()
	out, err = r.scanInternalWithManifest(readCtx, m, minKey, maxKey, 0)
	return out, readViewError(readCtx, err)
}

// ScanLimit returns at most limit key-value pairs in the half-open range
// [minKey, maxKey). A nil or empty bound leaves that side unbounded. A
// non-positive limit means no limit.
func (r *Reader) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) (out []KV, err error) {
	start := time.Now()
	defer func() {
		r.metrics.ObserveScanLimit(time.Since(start), len(out), err)
	}()

	done, err := r.beginRead()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := r.ensureFreshManifest(ctx); err != nil {
		return nil, err
	}

	m, _, expiresAt := r.currentManifestState()
	readCtx, cancel := context.WithDeadlineCause(ctx, expiresAt, ErrReadViewExpired)
	defer cancel()
	out, err = r.scanInternalWithManifest(readCtx, m, minKey, maxKey, limit)
	return out, readViewError(readCtx, err)
}

func (r *Reader) scanInternalWithManifest(ctx context.Context, m *manifestState, minKey, maxKey []byte, limit int) (out []KV, err error) {
	if m == nil {
		return nil, errors.New("manifest not loaded")
	}

	sources := r.openRangeSources(ctx, m, minKey, maxKey, false)
	defer func() {
		err = errors.Join(err, closeMergeSources(sources))
	}()

	if len(sources) == 0 {
		return nil, nil
	}

	mergeIter := newMergeIteratorSources(sources)

	nowMs := time.Now().UnixMilli()
	for (limit <= 0 || len(out) < limit) && mergeIter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		entry, err := mergeIter.entry()
		if err != nil {
			return nil, err
		}

		if len(minKey) > 0 && bytes.Compare(entry.Key, minKey) < 0 {
			continue
		}
		if len(maxKey) > 0 && bytes.Compare(entry.Key, maxKey) >= 0 {
			break
		}

		if entry.IsExpired(nowMs) {
			continue
		}

		if entry.Kind == internal.OpDelete {
			continue
		}

		value, err := r.entryValue(ctx, entry)
		if err != nil {
			return nil, err
		}

		out = append(out, KV{
			Key:   append([]byte(nil), entry.Key...),
			Value: value,
		})
	}

	if err := mergeIter.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func closeMergeSources(sources []mergeIteratorSource) error {
	var closeErr error
	for _, source := range sources {
		closeErr = errors.Join(closeErr, source.close())
	}
	return closeErr
}

func (r *Reader) openRangeSources(
	ctx context.Context,
	m *manifestState,
	minKey, maxKey []byte,
	detachNarrowLevels bool,
) []mergeIteratorSource {
	if len(minKey) > 0 && len(maxKey) > 0 && bytes.Compare(minKey, maxKey) >= 0 {
		return nil
	}

	var sources []mergeIteratorSource

	for _, sst := range m.L0SSTs {
		if !sstOverlapsHalfOpenRange(sst, KeyRange{Min: minKey, Max: maxKey}) {
			continue
		}
		sources = append(sources, newLevelMergeIteratorSourceWithMetadata(
			r, ctx, []sstMetadata{sst}, minKey, maxKey))
	}

	for i := range m.Levels {
		overlapping := m.Levels[i].OverlappingSSTsHalfOpen(minKey, maxKey)
		if len(overlapping) > 0 {
			// Reader manifests are immutable after publication. Borrowing their
			// metadata is therefore safe. A narrow long-lived iterator copies its
			// selection only to avoid retaining a much larger backing level; a full
			// level already needs all of that metadata, so copying saves no memory.
			if detachNarrowLevels && len(overlapping) < len(m.Levels[i].SSTs) {
				sources = append(sources,
					newLevelMergeIteratorSource(r, ctx, overlapping, minKey, maxKey))
			} else {
				sources = append(sources,
					newBorrowedLevelMergeIteratorSource(r, ctx, overlapping, minKey, maxKey))
			}
		}
	}

	return sources
}

// keyInSSTRange checks an SST's closed manifest span. It is not a
// caller-visible query range; reader query ranges are half-open.
func keyInSSTRange(key, minKey, maxKey []byte) bool {
	if len(minKey) > 0 && bytes.Compare(key, minKey) < 0 {
		return false
	}
	if len(maxKey) > 0 && bytes.Compare(key, maxKey) > 0 {
		return false
	}
	return true
}

func (r *Reader) getFromSST(
	ctx context.Context,
	sstMeta sstMetadata,
	key []byte,
) (value []byte, found bool, tombstone bool, err error) {
	if sstMeta.Bloom.Length > 0 {
		if filter, ok := r.bloomCache.get(sstMeta.ID); ok {
			if !filter.Has(bloomHashKey(key)) {
				return nil, false, false, nil
			}
		} else if resident, _ := r.sstResident(sstMeta); !resident {
			if !r.bloomMayContain(ctx, sstMeta, key) {
				return nil, false, false, nil
			}
		}
	}

	_, iter, err := r.openSSTIterBounded(ctx, sstMeta, key, nil)
	if err != nil {
		return nil, false, false, err
	}
	defer func() {
		if closeErr := iter.Close(); closeErr != nil {
			value = nil
			found = false
			tombstone = false
			err = errors.Join(err, closeErr)
		}
	}()

	kv := iter.First()
	if kv == nil {
		if err := iter.Error(); err != nil {
			return nil, false, false, err
		}
		return nil, false, false, nil
	}

	if !bytes.Equal(kv.K.UserKey, key) {
		return nil, false, false, nil
	}

	raw, _, err := kv.V.Value(nil)
	if err != nil {
		return nil, false, false, err
	}
	decoded, err := internal.DecodeKeyEntry(kv.K.UserKey, raw)
	if err != nil {
		return nil, false, false, err
	}

	nowMs := time.Now().UnixMilli()
	if decoded.IsExpired(nowMs) {

		return nil, true, true, nil
	}

	if decoded.Kind == internal.OpDelete {
		return nil, true, true, nil
	}
	return append([]byte(nil), decoded.Value...), true, false, nil
}

// bloomMayContain returns false only when a verified, decoded Bloom filter
// proves the key absent. Every loading, integrity, decoding, or cleanup
// failure returns true so Bloom availability can never suppress an SST read.
func (r *Reader) bloomMayContain(ctx context.Context, sstMeta sstMetadata, key []byte) bool {
	if filter, ok := r.bloomCache.get(sstMeta.ID); ok {
		return filter.Has(bloomHashKey(key))
	}

	value, err := r.bloomLoads.Do(ctx, sstMeta.ID, func(loadCtx context.Context) (any, error) {
		if filter, ok := r.bloomCache.peek(sstMeta.ID); ok {
			return filter, nil
		}
		if handle, ok, _ := r.acquireRawBloom(sstMeta); ok {
			filter, parseErr := parseBloomFilter(handle.Bytes())
			closeErr := handle.Close()
			if parseErr != nil {
				removeErr := r.artifactCache.Remove(
					bloomArtifactDescriptor(sstMeta), diskcache.ArtifactRemovalCorrupt)
				r.observeBloomFilterError(
					sstMeta.ID, errors.Join(parseErr, closeErr, removeErr))
			} else {
				// Cleanup errors are cache diagnostics. The decoded heap copy is
				// independent of the persistent handle and remains safe to use.
				r.observeArtifactCacheDiagnostic(
					"release", diskcache.ArtifactBloom, sstMeta.ID, closeErr)
				r.bloomCache.put(sstMeta.ID, filter)
				return filter, nil
			}
		}

		path := r.store.SSTPath(sstMeta.ID)
		data, err := r.store.ReadRange(loadCtx, path, sstMeta.Bloom.Offset, sstMeta.Bloom.Length)
		if err != nil {
			return nil, fmt.Errorf("read bloom %s: %w", sstMeta.ID, err)
		}
		// Reader manifest activation requires a checksum for every present Bloom.
		// A cache-less internal Reader must still verify the origin response before
		// a corrupted false negative can enter the decoded in-memory cache.
		if r.artifactCache == nil {
			if err := validateBloomChecksum(sstMeta.Bloom.Checksum, data); err != nil {
				return nil, fmt.Errorf("validate bloom %s: %w", sstMeta.ID, err)
			}
		}

		bloomData := data
		handle, err := r.admitRawBloom(sstMeta, data)
		if err != nil {
			return nil, fmt.Errorf("cache bloom %s: %w", sstMeta.ID, err)
		}
		if handle != nil {
			bloomData = handle.Bytes()
		}
		filter, err := parseBloomFilter(bloomData)
		if err != nil {
			var closeErr error
			var removeErr error
			if handle != nil {
				closeErr = handle.Close()
				removeErr = r.artifactCache.Remove(
					bloomArtifactDescriptor(sstMeta), diskcache.ArtifactRemovalCorrupt)
			}
			return nil, errors.Join(
				fmt.Errorf("decode bloom %s: %w", sstMeta.ID, err), closeErr, removeErr)
		}
		if handle != nil {
			if err := handle.Close(); err != nil {
				r.observeArtifactCacheDiagnostic(
					"release", diskcache.ArtifactBloom, sstMeta.ID, err)
			}
		}
		r.bloomCache.put(sstMeta.ID, filter)
		return filter, nil
	})
	if err != nil {
		r.observeBloomFilterError(sstMeta.ID, err)
		return true
	}
	filter, ok := value.(*z.Bloom)
	if !ok || filter == nil {
		err := fmt.Errorf("bloom load %s returned %T", sstMeta.ID, value)
		r.observeBloomFilterError(sstMeta.ID, err)
		return true
	}
	return filter.Has(bloomHashKey(key))
}

func (r *Reader) entryValue(_ context.Context, entry internal.CompactionEntry) ([]byte, error) {
	return append([]byte(nil), entry.Value...), nil
}

func (r *Reader) sstPayloadSize(meta sstMetadata) (int64, error) {
	if meta.Size > 0 {
		return meta.Size, nil
	}
	return 0, fmt.Errorf("sst %s: missing size in manifest", meta.ID)
}

func (r *Reader) openSSTIterBounded(ctx context.Context, sstMeta sstMetadata, lower, upper []byte) (*sstable.Reader, sstable.Iterator, error) {
	path := r.store.SSTPath(sstMeta.ID)
	var cachedOpenErr error

	if cached, releaseCache, ok, _ := r.acquireSST(sstMeta); ok {
		r.metrics.ObserveSSTCacheLookup(true)
		reader, iter, err := r.openSSTIterFromData(ctx, sstMeta, cached, lower, upper, releaseCache)
		if err == nil {
			return reader, iter, nil
		}
		cachedOpenErr = err
		// Cached bytes are not authoritative. If they cannot be opened, release
		// and evict them, then make one attempt against object storage below.
		cachedOpenErr = errors.Join(cachedOpenErr,
			r.removeSST(sstMeta, diskcache.ArtifactRemovalCorrupt))
	} else {
		r.metrics.ObserveSSTCacheLookup(false)
	}

	if ok, size, err := r.shouldRangeRead(sstMeta); err != nil {
		return nil, nil, errors.Join(cachedOpenErr, err)
	} else if ok {
		reader, iter, err := r.openSSTIterRange(ctx, sstMeta, path, lower, upper, size)
		if err != nil {
			err = errors.Join(cachedOpenErr, err)
		}
		return reader, iter, err
	}

	loaded, err := r.loadSSTArtifact(ctx, &sstMeta, path)
	if err != nil {
		return nil, nil, errors.Join(cachedOpenErr, err)
	}
	releaseLoaded := func() { _ = loaded.Close() }
	reader, iter, err := r.openSSTIterFromData(
		ctx, sstMeta, loaded.Bytes(), lower, upper, releaseLoaded)
	if err != nil {
		// A resident artifact can be removed for the next read. Transient bypass
		// bytes disappear when releaseLoaded closes their lease.
		if removeErr := r.removeSST(sstMeta, diskcache.ArtifactRemovalCorrupt); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
		err = errors.Join(cachedOpenErr, err)
	}
	return reader, iter, err
}

func (r *Reader) shouldRangeRead(sstMeta sstMetadata) (bool, int64, error) {
	if r.blockCache == nil {
		return false, 0, nil
	}
	if !r.allowUnverifiedRangeRead && r.verifySST {
		return false, 0, nil
	}

	size := sstMeta.Size
	if r.rangeReadMinSSTSize > 0 {
		if size <= 0 {
			s, err := r.sstPayloadSize(sstMeta)
			if err != nil {
				return false, 0, err
			}
			size = s
		}
		if size < r.rangeReadMinSSTSize {
			return false, 0, nil
		}
	}

	if size <= 0 {
		size = sstMeta.Size
		if size <= 0 {
			size = 0
		}
	}

	return true, size, nil
}

func (r *Reader) openSSTIterRange(ctx context.Context, sstMeta sstMetadata, path string, lower, upper []byte, size int64) (*sstable.Reader, sstable.Iterator, error) {
	if size <= 0 {
		var err error
		size, err = r.sstPayloadSize(sstMeta)
		if err != nil {
			return nil, nil, err
		}
	}
	readable := newSSTRangeReadable(
		r.store, path, sstMeta.ID, size, r.blockCache, &r.sstRangeLoads, r.metrics)
	return r.openSSTIterWithReadable(ctx, readable, lower, upper, nil)
}

func (r *Reader) openSSTIterFromData(ctx context.Context, sstMeta sstMetadata, data []byte, lower, upper []byte, release func()) (*sstable.Reader, sstable.Iterator, error) {
	trimmed, err := trimSSTData(sstMeta, data)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, nil, err
	}
	return r.openSSTIterWithReadable(ctx, newSSTReadable(trimmed), lower, upper, release)
}

func (r *Reader) openSSTIterWithReadable(ctx context.Context, readable objstorage.Readable, lower, upper []byte, release func()) (*sstable.Reader, sstable.Iterator, error) {
	readerOpts := sstable.ReaderOptions{}
	reader, err := sstable.NewReader(ctx, readable, readerOpts)
	if err != nil {
		_ = readable.Close()
		if release != nil {
			release()
		}
		return nil, nil, err
	}

	iter, err := reader.NewIter(sstable.NoTransforms, lower, upper, sstable.AssertNoBlobHandles)
	if err != nil {
		_ = reader.Close()
		if release != nil {
			release()
		}
		return nil, nil, err
	}

	wrapped := &sstIterWithClose{
		Iterator: iter,
		reader:   reader,
		release:  release,
	}

	return reader, wrapped, nil
}

type sstIterWithClose struct {
	sstable.Iterator
	reader  *sstable.Reader
	release func()
	closed  bool
}

func (it *sstIterWithClose) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true

	err := it.Iterator.Close()
	if it.reader != nil {
		if rerr := it.reader.Close(); err == nil {
			err = rerr
		}
	}
	if it.release != nil {
		it.release()
	}
	return err
}

func trimSSTData(meta sstMetadata, data []byte) ([]byte, error) {
	if meta.Size <= 0 {
		return nil, fmt.Errorf("sst %s: missing size in manifest", meta.ID)
	}
	if int64(len(data)) < meta.Size {
		return nil, fmt.Errorf("sst %s: short read: %d < %d", meta.ID, len(data), meta.Size)
	}
	return data[:meta.Size], nil
}

func (r *Reader) cacheSST(ctx context.Context, meta *sstMetadata, path string) (err error) {
	handle, _, err := r.downloadSSTArtifact(ctx, meta, path)
	if err != nil {
		return err
	}
	if err := handle.Close(); err != nil {
		return fmt.Errorf("release downloaded sst %s: %w", meta.ID, err)
	}
	return nil
}

func (r *Reader) loadSSTArtifact(
	ctx context.Context,
	meta *sstMetadata,
	path string,
) (*sstArtifactLease, error) {
	if meta == nil {
		return nil, errors.New("cache sst: missing metadata")
	}

	value, err := r.sstLoads.Do(ctx, path, func(loadCtx context.Context) (any, error) {
		if resident, _ := r.sstResident(*meta); resident {
			handle, ok, err := r.artifactCache.Acquire(sstArtifactDescriptor(*meta))
			if err == nil && ok {
				return newSharedSSTArtifact(handle), nil
			}
		}

		handle, _, err := r.downloadSSTArtifact(loadCtx, meta, path)
		if err != nil {
			return nil, err
		}
		return newSharedSSTArtifact(handle), nil
	})
	if err != nil {
		return nil, err
	}
	lease, ok := value.(*sstArtifactLease)
	if !ok || lease == nil {
		return nil, fmt.Errorf("load sst %s: missing artifact lease", meta.ID)
	}
	return lease, nil
}

func (r *Reader) downloadSSTArtifact(
	ctx context.Context,
	meta *sstMetadata,
	path string,
) (handle *diskcache.ArtifactHandle, admission diskcache.ArtifactAdmission, err error) {
	start := time.Now()
	var downloadedBytes int64
	defer func() {
		r.metrics.ObserveSSTDownload(time.Since(start), downloadedBytes, err)
	}()

	if meta == nil {
		return nil, 0, fmt.Errorf("cache sst %s: missing metadata", path)
	}
	// Local staging is a Reader availability requirement. Fail before paying
	// object-store egress when it cannot be created; buffering an arbitrarily
	// large SST in memory is deliberately not a fallback. Failures after the
	// verified download are handled by Commit's transient-file path.
	fill, err := r.artifactCache.BeginFill(sstArtifactDescriptor(*meta))
	if err != nil {
		r.observeArtifactCacheDiagnostic("begin-fill", diskcache.ArtifactSST, meta.ID, err)
		return nil, 0, fmt.Errorf("begin cache fill for sst %s: %w", meta.ID, err)
	}
	defer func() {
		err = errors.Join(err, fill.Abort())
	}()

	// The enclosing object may append Bloom and trailer bytes after the Pebble
	// payload. ArtifactFill intentionally rejects overflow, so keep this stream
	// bounded to exactly the manifest's [0, Size) SST extent.
	stream, err := r.store.ReadRangeStream(ctx, path, 0, meta.Size)
	if err != nil {
		return nil, 0, fmt.Errorf("read sst %s: %w", path, err)
	}
	downloadedBytes, err = io.Copy(fill, stream)
	closeErr := stream.Close()
	if err != nil {
		return nil, 0, errors.Join(fmt.Errorf("download sst %s: %w", path, err), closeErr)
	}
	if closeErr != nil {
		return nil, 0, fmt.Errorf("close downloaded sst %s: %w", path, closeErr)
	}
	handle, admission, err = fill.Commit()
	if err != nil {
		switch {
		case errors.Is(err, diskcache.ErrArtifactChecksumMismatch):
			return nil, 0, fmt.Errorf("validate sst %s: checksum mismatch: %w", meta.ID, err)
		case errors.Is(err, diskcache.ErrArtifactSizeMismatch):
			return nil, 0, fmt.Errorf("validate sst %s: size mismatch: %w", meta.ID, err)
		default:
			return nil, 0, fmt.Errorf("cache sst %s: %w", meta.ID, err)
		}
	}
	return handle, admission, nil
}

func (r *Reader) SSTCacheStats() CacheStats {
	return cacheStatsFromArtifact(r.artifactCache.Stats(diskcache.ArtifactSST))
}

// BloomCacheStats reports decoded Bloom-filter L1 occupancy.
func (r *Reader) BloomCacheStats() CacheStats {
	return r.bloomCache.stats()
}

// BloomDiskCacheStats reports verified raw Bloom sidecar occupancy.
func (r *Reader) BloomDiskCacheStats() CacheStats {
	if r.artifactCache == nil {
		return CacheStats{}
	}
	return cacheStatsFromArtifact(r.artifactCache.Stats(diskcache.ArtifactBloom))
}

func (r *Reader) ManifestPageCacheStats() CacheStats {
	if cs, ok := r.manifestStore.Storage().(*cachestore.CachingStorage); ok {
		stats := cs.CacheStats()
		return CacheStats{
			Hits:       stats.Hits,
			Misses:     stats.Misses,
			EntryCount: stats.EntryCount,
			MaxEntries: stats.MaxEntries,
		}
	}
	return CacheStats{}
}

type sstReadable struct {
	data []byte
	r    *bytes.Reader
	rh   objstorage.NoopReadHandle
}

func newSSTReadable(data []byte) *sstReadable {
	m := &sstReadable{
		data: data,
		r:    bytes.NewReader(data),
	}
	m.rh = objstorage.MakeNoopReadHandle(m)
	return m
}

func (m *sstReadable) NewReadHandle(_ objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &m.rh
}

func (m *sstReadable) ReadAt(_ context.Context, p []byte, off int64) error {
	n, err := m.r.ReadAt(p, off)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (*sstReadable) Close() error {
	return nil
}

func (m *sstReadable) Size() int64 {
	return int64(len(m.data))
}

type Iterator struct {
	mu        sync.Mutex
	reader    *Reader
	ctx       context.Context
	cancel    context.CancelFunc
	expiresAt time.Time
	minKey    []byte
	maxKey    []byte
	nowMs     int64
	mergeIter *kMergeIterator
	sources   []mergeIteratorSource
	current   *iterEntry
	started   bool
	closed    bool
	err       error
}

type iterEntry struct {
	key   []byte
	value []byte
}

func (r *Reader) NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error) {
	done, err := r.beginRead()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := r.ensureFreshManifest(ctx); err != nil {
		return nil, err
	}

	m, _, expiresAt := r.currentManifestState()
	if !time.Now().Before(expiresAt) {
		return nil, ErrReadViewExpired
	}
	it, err := r.newIteratorWithManifest(ctx, m, opts, expiresAt)
	if err != nil {
		return nil, err
	}
	return it, nil
}

func (r *Reader) newIteratorWithManifest(ctx context.Context, m *manifestState, opts IteratorOptions, expiresAt time.Time) (*Iterator, error) {
	if m == nil {
		return nil, errors.New("manifest not loaded")
	}
	iterCtx, cancel := context.WithDeadlineCause(ctx, expiresAt, ErrIteratorExpired)

	sources := r.openRangeSources(iterCtx, m, opts.MinKey, opts.MaxKey, true)

	if len(sources) == 0 {

		it := &Iterator{
			reader:    r,
			ctx:       iterCtx,
			cancel:    cancel,
			expiresAt: expiresAt,
			minKey:    opts.MinKey,
			maxKey:    opts.MaxKey,
			nowMs:     time.Now().UnixMilli(),
			closed:    false,
		}
		r.registerIterator(it)
		return it, nil
	}

	it := &Iterator{
		reader:    r,
		ctx:       iterCtx,
		cancel:    cancel,
		expiresAt: expiresAt,
		minKey:    opts.MinKey,
		maxKey:    opts.MaxKey,
		nowMs:     time.Now().UnixMilli(),
		mergeIter: newMergeIteratorSources(sources),
		sources:   sources,
		closed:    false,
	}
	r.registerIterator(it)
	return it, nil
}

func (it *Iterator) Next() bool {
	done, err := it.beginReaderOperation()
	if err != nil {
		it.mu.Lock()
		it.current = nil
		it.err = err
		it.mu.Unlock()
		return false
	}
	defer done()
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.next()
}

func (it *Iterator) next() bool {
	// A failed move leaves the iterator unpositioned. Clear the previous entry
	// before checking exhaustion, bounds, cancellation, or read errors; a
	// successful move installs the new current entry below.
	it.current = nil
	if it.closed || it.err != nil {
		return false
	}
	if err := it.contextErr(); err != nil {
		it.err = err
		return false
	}
	if it.mergeIter == nil {
		return false
	}

	for {

		if err := it.contextErr(); err != nil {
			it.err = err
			return false
		}

		if !it.mergeIter.Next() {
			if err := it.mergeIter.Err(); err != nil {
				it.err = err
			}
			return false
		}

		entry, err := it.mergeIter.entry()
		if err != nil {
			it.err = err
			return false
		}

		if len(it.minKey) > 0 && bytes.Compare(entry.Key, it.minKey) < 0 {
			continue
		}
		if len(it.maxKey) > 0 && bytes.Compare(entry.Key, it.maxKey) >= 0 {
			return false
		}

		if entry.IsExpired(it.nowMs) {
			continue
		}

		if entry.Kind == internal.OpDelete {
			continue
		}

		value, err := it.reader.entryValue(it.ctx, entry)
		if err != nil {
			it.err = err
			return false
		}

		it.current = &iterEntry{
			key:   append([]byte(nil), entry.Key...),
			value: value,
		}
		return true
	}
}

func (it *Iterator) Key() []byte {
	done := it.lockReaderLifecycle()
	defer done()
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.current == nil {
		return nil
	}
	return it.current.key
}

func (it *Iterator) Value() []byte {
	done := it.lockReaderLifecycle()
	defer done()
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.current == nil {
		return nil
	}
	return it.current.value
}

func (it *Iterator) Valid() bool {
	done := it.lockReaderLifecycle()
	defer done()
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.current != nil && !it.closed && it.err == nil
}

func (it *Iterator) Err() error {
	done := it.lockReaderLifecycle()
	defer done()
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.err != nil {
		return it.err
	}
	if it.closed {
		return nil
	}
	if err := it.contextErr(); err != nil {
		return err
	}
	if it.mergeIter != nil {
		return it.mergeIter.Err()
	}
	return nil
}

func (it *Iterator) Close() error {
	done := it.lockReaderLifecycle()
	defer done()
	return it.close(nil)
}

func (it *Iterator) close(cause error) error {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed {
		return nil
	}
	if cause != nil && it.err == nil {
		it.err = cause
	}
	it.closed = true
	it.current = nil

	closeErr := closeMergeSources(it.sources)
	it.sources = nil
	it.mergeIter = nil
	if it.cancel != nil {
		it.cancel()
		it.cancel = nil
	}
	if it.reader != nil {
		it.reader.unregisterIterator(it)
	}

	return closeErr
}

func (it *Iterator) SeekGE(target []byte) bool {
	done, err := it.beginReaderOperation()
	if err != nil {
		it.mu.Lock()
		it.current = nil
		it.err = err
		it.mu.Unlock()
		return false
	}
	defer done()
	it.mu.Lock()
	defer it.mu.Unlock()

	it.current = nil
	if it.closed || it.err != nil {
		return false
	}
	if err := it.contextErr(); err != nil {
		it.err = err
		return false
	}
	if it.mergeIter == nil {
		return false
	}

	it.mergeIter.seekGE(target)
	if err := it.mergeIter.Err(); err != nil {
		it.err = err
		return false
	}
	return it.next()
}

func (it *Iterator) beginReaderOperation() (func(), error) {
	if it == nil || it.reader == nil {
		return func() {}, nil
	}
	return it.reader.beginRead()
}

func (it *Iterator) lockReaderLifecycle() func() {
	if it == nil || it.reader == nil {
		return func() {}
	}
	it.reader.lifecycleMu.RLock()
	return it.reader.lifecycleMu.RUnlock
}

func (it *Iterator) contextErr() error {
	if !it.expiresAt.IsZero() && !time.Now().Before(it.expiresAt) {
		return ErrIteratorExpired
	}
	if err := it.ctx.Err(); err != nil {
		if cause := context.Cause(it.ctx); cause != nil {
			return cause
		}
		return err
	}
	return nil
}
