package isledb

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/cachestore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

type Reader struct {
	store         *blobstore.Store
	manifestStore *manifest.Store
	diskCache     *diskcache.Cache
	fetcher       *sstFetcher
	blockCache    *blockCache
	bloomCache    *bloomFilterCache
	openSSTs      *openSSTCache
	bloomLoads    coalescedLoadGroup
	// sstDrops counts dropSST calls.
	sstDrops      atomic.Int64
	manifestLoads coalescedLoadGroup

	ownsDiskCache bool
	cacheDir      string

	lifecycleMu            sync.RWMutex
	iteratorsMu            sync.Mutex
	iterators              map[*Iterator]struct{}
	mu                     sync.RWMutex
	manifest               *manifestState
	version                Version
	changeFeed             bool
	changeHead             ChangeCursor
	viewPolicy             ReaderViewPolicy
	viewRefreshAt          time.Time
	viewExpiresAt          time.Time
	viewExpired            atomic.Bool
	viewTimerMu            sync.Mutex
	viewTimer              *time.Timer
	viewTimerID            atomic.Uint64
	metrics                *ReaderMetrics
	bloomDiagnosticLimiter readerDiagnosticLimiter
	closed                 atomic.Bool
	releaseOnce            sync.Once
	release                func()
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

	disk, ownsDiskCache, err := initReaderDiskCache(opts)
	if err != nil {
		return nil, err
	}
	cleanupDiskCache := true
	defer func() {
		if cleanupDiskCache && ownsDiskCache {
			_ = disk.Close()
		}
	}()

	viewRefreshAt := viewLoadedAt.Add(viewPolicy.RefreshAfter)
	current := ms.CurrentData()
	changeFeed, changeHead := readerChangeFeedState(current)
	viewExpiresAt := viewLoadedAt.Add(current.PinnedViewAge())
	reader := &Reader{
		store:         store,
		manifestStore: ms,
		manifest:      m,
		version:       versionFromCurrent(current),
		changeFeed:    changeFeed,
		changeHead:    changeHead,
		viewPolicy:    viewPolicy,
		viewRefreshAt: viewRefreshAt,
		viewExpiresAt: viewExpiresAt,
		diskCache:     disk,
		fetcher:       newSSTFetcher(store, disk, opts.Metrics),
		blockCache:    newBlockCache(cmp.Or(opts.BlockCacheSize, defaultBlockCacheSize)),
		bloomCache:    newBloomFilterCache(opts.BloomCacheSize),
		openSSTs:      newOpenSSTCache(openSSTCacheSize(opts.OpenSSTCacheSize)),
		ownsDiskCache: ownsDiskCache,
		cacheDir:      opts.CacheDir,
		metrics:       opts.Metrics,
	}
	reader.armManifestExpiry(viewRefreshAt, viewExpiresAt)
	cleanupDiskCache = false
	return reader, nil
}

// openSSTCacheSize resolves the open-SST cache size: zero selects the default
// and a negative size, used by tests, disables the cache.
func openSSTCacheSize(size int) int {
	if size == 0 {
		return defaultOpenSSTCacheSize
	}
	return size
}

// initReaderDiskCache opens the disk cache under CacheDir, unless the caller
// supplied one. The budget is split between its tiers: an eighth for SST
// metadata and Bloom filters, the rest for data.
func initReaderDiskCache(opts readerOptions) (*diskcache.Cache, bool, error) {
	if opts.DiskCache != nil {
		return opts.DiskCache, false, nil
	}
	if opts.CacheDir == "" {
		return nil, false, errors.New("cache dir is required")
	}
	budget := cmp.Or(opts.DiskCacheSize, defaultDiskCacheSize)
	metaBudget := max(budget/8, 1)
	dir := filepath.Join(opts.CacheDir, "artifacts")
	cache, err := diskcache.Open(diskcache.Options{
		Dir:          dir,
		MetaMaxBytes: metaBudget,
		DataMaxBytes: max(budget-metaBudget, 1),
	})
	if err != nil {
		return nil, false, fmt.Errorf("open disk cache: %w", err)
	}
	warnIfCacheDirTooSmall(dir, cache, budget)
	return cache, true, nil
}

// warnIfCacheDirTooSmall logs when the cache's filesystem cannot hold its
// budget: the space still free plus what the cache already holds is less than
// the budget. The cache keeps working; it evicts sooner, and entries it cannot
// write are served without being kept.
func warnIfCacheDirTooSmall(dir string, cache *diskcache.Cache, budget int64) {
	free, err := diskcache.FreeBytes(dir)
	if err != nil {
		return
	}
	held := cache.Stats(diskcache.TierMeta).Bytes + cache.Stats(diskcache.TierData).Bytes
	if available := free + uint64(held); available < uint64(budget) {
		slog.Warn("isledb: cache directory has less space than the disk cache budget",
			"dir", dir, "available_bytes", available, "budget_bytes", budget)
	}
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
	// and its metadata under one short critical section. Caches age retired
	// SSTs out through their own LRUs; the block cache only forgets the file
	// numbers of retired SSTs no longer open, keeping its map bounded.
	r.mu.Lock()
	r.manifest = m
	r.version = versionFromCurrent(current)
	r.changeFeed = changeFeed
	r.changeHead = changeHead
	r.viewRefreshAt = refreshAt
	r.viewExpiresAt = expiresAt
	r.mu.Unlock()

	r.blockCache.prune(m, r.openSSTs.isOpen)
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
	r.fetcher.close()

	var firstErr error
	r.openSSTs.clear()
	r.blockCache.close()
	r.bloomCache.clear()
	if r.diskCache != nil && r.ownsDiskCache {
		if err := r.diskCache.Close(); err != nil {
			firstErr = err
		}
	}
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
	if hasUsableBloom(sstMeta) {
		if !r.bloomMayContain(ctx, sstMeta, key) {
			return nil, false, false, nil
		}
	}
	value, found, tombstone, err = r.lookupSST(ctx, sstMeta, key)
	if damaged(err) {
		// Damage found while reading dropped the SST, so the retry reads it
		// afresh: damaged cached bytes cost one more fetch, never a failed
		// lookup. Any other error, as damage at the origin or a value that
		// does not decode, fails the same way again.
		value, found, tombstone, err = r.lookupSST(ctx, sstMeta, key)
	}
	return value, found, tombstone, err
}

func (r *Reader) lookupSST(
	ctx context.Context,
	sstMeta sstMetadata,
	key []byte,
) (value []byte, found bool, tombstone bool, err error) {
	_, iter, err := r.openSSTIterBounded(ctx, sstMeta, key, nil, false)
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
		return filter.mayContain(bloomHashKey(key))
	}

	value, err := r.bloomLoads.Do(ctx, sstMeta.ID, func(loadCtx context.Context) (any, error) {
		if filter, ok := r.bloomCache.peek(sstMeta.ID); ok {
			return filter, nil
		}
		filter, err := r.loadBloomFilter(loadCtx, sstMeta)
		if err != nil {
			return nil, err
		}
		r.bloomCache.put(sstMeta.ID, filter)
		return filter, nil
	})
	if err != nil {
		r.observeBloomFilterError(sstMeta.ID, err)
		return true
	}
	filter, ok := value.(sstBloomFilter)
	if !ok {
		err := fmt.Errorf("bloom load %s returned %T", sstMeta.ID, value)
		r.observeBloomFilterError(sstMeta.ID, err)
		return true
	}
	return filter.mayContain(bloomHashKey(key))
}

// loadBloomFilter reads an SST's filter from the disk cache, or else from
// object storage, verified against the manifest checksum before use; a
// corrupted filter could otherwise report a present key as absent.
func (r *Reader) loadBloomFilter(ctx context.Context, sstMeta sstMetadata) (sstBloomFilter, error) {
	data, err := r.fetcher.bloom(ctx, r.fetcher.object(sstMeta), storeAsync)
	if err != nil {
		return sstBloomFilter{}, err
	}
	filter, err := parseSSTBloomFilter(data)
	if err != nil {
		// The bytes match the manifest checksum, so the SST's filter itself is
		// unusable; fetching it again would fail the same way.
		return sstBloomFilter{}, fmt.Errorf("decode bloom %s: %w", sstMeta.ID, err)
	}
	return filter, nil
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

// openSSTIterBounded opens an iterator over one SST's keys in [lower, upper),
// reusing the SST if it is already open. private makes the iterator fill no
// cache: it reads blocks into buffers of its own and stores no fetched bytes
// on disk; the long tail of a scan reads this way (see scanSSTSource).
//
// An SST whose open or iterator fails on damage is dropped, as is one whose
// iterator later fails on damage (see sstIterWithClose.Close).
func (r *Reader) openSSTIterBounded(ctx context.Context, sstMeta sstMetadata, lower, upper []byte, private bool) (*sstable.Reader, sstable.Iterator, error) {
	if _, err := r.sstPayloadSize(sstMeta); err != nil {
		return nil, nil, err
	}
	sst := r.openSSTs.acquire(sstMeta.ID)
	if sst == nil {
		var err error
		if sst, err = r.openSST(ctx, sstMeta); err != nil {
			if damaged(err) {
				r.dropSST(sstMeta)
			}
			return nil, nil, err
		}
	}
	reader, iter, err := r.newSSTIter(ctx, sst, lower, upper, private)
	if damaged(err) {
		r.dropSST(sstMeta)
	}
	return reader, iter, err
}

// openSST opens an SST and caches it open, returning it with a reference for
// the caller. Its blocks are cached under the SST's block cache file number,
// so a reopen finds them.
func (r *Reader) openSST(ctx context.Context, sstMeta sstMetadata) (*openSST, error) {
	readable := r.fetcher.readable(r.fetcher.object(sstMeta))
	readable.holdMetadata()
	reader, err := sstable.NewReader(ctx, readable, r.blockCache.readerOptions(r.blockCache.fileNum(sstMeta.ID)))
	readable.releaseMetadata()
	if err != nil {
		return nil, err
	}
	r.blockCache.noteOpen()
	return r.openSSTs.add(&openSST{
		id: sstMeta.ID, reader: reader,
		onDamage: func() { r.dropSST(sstMeta) },
	}), nil
}

// newSSTIter opens an iterator over an open SST, taking over the caller's
// reference, which the iterator drops when it closes.
func (r *Reader) newSSTIter(ctx context.Context, sst *openSST, lower, upper []byte, private bool) (*sstable.Reader, sstable.Iterator, error) {
	iterOpts := sstable.IterOptions{
		Lower:                lower,
		Upper:                upper,
		Transforms:           sstable.NoTransforms,
		FilterBlockSizeLimit: sstable.AlwaysUseFilterBlock,
		ReaderProvider:       sstable.MakeTrivialReaderProvider(sst.reader),
		BlobContext:          sstable.AssertNoBlobHandles,
	}
	var pool *block.BufferPool
	if private {
		pool = new(block.BufferPool)
		pool.Init(5)
		iterOpts.Env.Block.BufferPool = pool
	}
	if private {
		ctx = withPrivateReads(ctx)
	}
	iter, err := sst.reader.NewPointIter(ctx, iterOpts)
	if err != nil {
		if pool != nil {
			pool.Release()
		}
		sst.unref()
		return nil, nil, err
	}
	return sst.reader, &sstIterWithClose{Iterator: iter, sst: sst, pool: pool}, nil
}

type sstIterWithClose struct {
	sstable.Iterator
	// sst is the open SST this iterator holds a reference to.
	sst *openSST
	// pool, when set, holds a private iterator's blocks; it is released once
	// the iterator has returned them.
	pool   *block.BufferPool
	closed bool
}

func (it *sstIterWithClose) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true

	iterErr := it.Error()
	err := it.Iterator.Close()
	if it.pool != nil {
		it.pool.Release()
	}
	if it.sst.onDamage != nil && (damaged(iterErr) || damaged(err)) {
		it.sst.onDamage()
	}
	it.sst.unref()
	return err
}

// DiskCacheStats reports the disk cache's two tiers.
type DiskCacheStats struct {
	// Meta holds SST metadata regions and Bloom filters.
	Meta CacheStats
	// Data holds small SSTs whole and chunks of larger SSTs' data.
	Data CacheStats
	// SSTDrops counts reads that failed on what looked like damaged bytes,
	// each dropping the SST from every layer so it is fetched again. It
	// counts drops, not distinct SSTs: concurrent readers of one damaged SST
	// each count, and an SST bad at the origin counts on every read.
	SSTDrops int64
}

// DiskCacheStats reports the persistent disk cache under CacheDir.
func (r *Reader) DiskCacheStats() DiskCacheStats {
	if r.diskCache == nil {
		return DiskCacheStats{}
	}
	return DiskCacheStats{
		Meta:     diskTierStats(r.diskCache.Stats(diskcache.TierMeta)),
		Data:     diskTierStats(r.diskCache.Stats(diskcache.TierData)),
		SSTDrops: r.sstDrops.Load(),
	}
}

func diskTierStats(stats diskcache.Stats) CacheStats {
	return CacheStats{
		Hits:        stats.Hits,
		Misses:      stats.Misses,
		Bytes:       stats.Bytes,
		MaxBytes:    stats.MaxBytes,
		EntryCount:  stats.Entries,
		Evictions:   stats.Evictions,
		Corruptions: stats.Corruptions,
		Bypasses:    stats.Bypasses,
		Failures:    stats.Failures,
		Dropped:     stats.Dropped,
	}
}

// OpenSSTCacheStats reports the SSTs kept open across reads: hits are reads
// of an SST already open, misses reads that had to open it.
func (r *Reader) OpenSSTCacheStats() CacheStats {
	return r.openSSTs.stats()
}

// BlockCacheStats reports the in-memory cache of decoded SST blocks, shared
// by every SST read. Hits and misses count lookups of index and data blocks;
// the metadata blocks each SST open reads, which are never cached, are not
// counted.
func (r *Reader) BlockCacheStats() CacheStats {
	return r.blockCache.stats()
}

// BloomCacheStats reports the parsed Bloom filters kept in memory.
func (r *Reader) BloomCacheStats() CacheStats {
	return r.bloomCache.stats()
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
