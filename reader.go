package isledb

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
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
	// bloomLoading holds the IDs of SSTs whose filter is loading in the
	// background, or whose load failed within bloomRetryDelay.
	bloomLoading      sync.Map
	bloomLoadsRunning atomic.Int32
	bloomBackground   sync.WaitGroup
	// sstDrops counts dropSST calls.
	sstDrops      atomic.Int64
	manifestLoads coalescedLoadGroup
	// reloads counts manifest reloads started, so a forced refresh can tell
	// whether a reload began after it was called.
	reloads atomic.Uint64
	// refreshing is set while a background refresh runs; refreshRequested
	// makes it run again for a timer that fired meanwhile.
	refreshing       atomic.Bool
	refreshRequested atomic.Bool
	background       sync.WaitGroup
	refreshTimeout   time.Duration

	ownsDiskCache bool
	cacheDir      string

	lifecycleMu sync.RWMutex
	// endRead is lifecycleMu.RUnlock, made once: beginRead returns it, and a
	// method value made on every read would allocate.
	endRead     func()
	iteratorsMu sync.Mutex
	iterators   map[*Iterator]struct{}
	mu          sync.RWMutex
	manifest    *manifestState
	// viewSeq is the published CURRENT's NextSeq: the manifest log position
	// the view reflects. A view is only ever replaced by one at least as new.
	viewSeq uint64
	// viewLoadedAt is when the published view was loaded.
	viewLoadedAt time.Time
	// refreshFailures counts refreshes failed since the view was loaded, and
	// staleSince is when the first of them failed; see refreshFailed.
	refreshFailures int
	staleSince      time.Time
	version         Version
	changeFeed      bool
	changeHead      ChangeCursor
	viewPolicy      ReaderViewPolicy
	// refreshGrid is the origin of this reader's refresh schedule (see nextOnGrid).
	refreshGrid            time.Time
	viewRefreshAt          time.Time
	viewExpiresAt          time.Time
	viewDue                atomic.Bool
	viewTimerMu            sync.Mutex
	viewTimer              *time.Timer
	viewTimerID            atomic.Uint64
	metrics                *ReaderMetrics
	bloomDiagnosticLimiter readerDiagnosticLimiter
	// stale reports that a refresh failed and reads are answered from an
	// older, still valid view; it is cleared when a refresh succeeds.
	stale                    atomic.Bool
	refreshDiagnosticLimiter readerDiagnosticLimiter
	closed                   atomic.Bool
	releaseOnce              sync.Once
	release                  func()
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

	// A random phase in (0, RefreshAfter] spreads readers opened together,
	// by a deploy, evenly across the interval for good.
	refreshGrid := viewLoadedAt.Add(time.Duration(rand.Int64N(int64(viewPolicy.RefreshAfter))) + 1)
	viewRefreshAt := refreshGrid
	current := ms.CurrentData()
	changeFeed, changeHead := readerChangeFeedState(current)
	viewExpiresAt := viewLoadedAt.Add(current.PinnedViewAge())
	reader := &Reader{
		store:          store,
		manifestStore:  ms,
		manifest:       m,
		version:        versionFromCurrent(current),
		changeFeed:     changeFeed,
		changeHead:     changeHead,
		viewSeq:        currentNextSeq(current),
		viewLoadedAt:   viewLoadedAt,
		refreshTimeout: backgroundRefreshTimeout,
		viewPolicy:     viewPolicy,
		refreshGrid:    refreshGrid,
		viewRefreshAt:  viewRefreshAt,
		viewExpiresAt:  viewExpiresAt,
		diskCache:      disk,
		fetcher:        newSSTFetcher(store, disk, opts.Metrics),
		blockCache:     newBlockCache(cmp.Or(opts.BlockCacheSize, defaultBlockCacheSize)),
		bloomCache:     newBloomFilterCache(opts.BloomCacheSize),
		openSSTs:       newOpenSSTCache(openSSTCacheSize(opts.OpenSSTCacheSize)),
		ownsDiskCache:  ownsDiskCache,
		cacheDir:       opts.CacheDir,
		metrics:        opts.Metrics,
	}
	reader.endRead = reader.lifecycleMu.RUnlock
	reader.armViewTimer(viewRefreshAt, viewExpiresAt)
	opts.Metrics.ObserveViewLoaded(viewLoadedAt)
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

// checkManifestView runs before every read. A view that has not expired is
// used as is, since armViewTimer refreshes it; an expired one is refreshed
// first, and the read fails if that refresh does.
func (r *Reader) checkManifestView(ctx context.Context) error {
	r.mu.RLock()
	expired := !time.Now().Before(r.viewExpiresAt)
	r.mu.RUnlock()
	if expired {
		return r.refreshManifest(ctx, false)
	}
	if r.stale.Load() {
		r.metrics.ObserveStaleRead()
	}
	return nil
}

// backgroundRefreshTimeout bounds a background refresh, so a store that
// hangs counts as failing.
const backgroundRefreshTimeout = 30 * time.Second

// refreshInBackground refreshes the view unless the reader is closed. A
// request made while a refresh runs makes it run again, so the timer chain
// that each refresh re-arms never stops.
func (r *Reader) refreshInBackground() {
	// Holding the read lifecycle orders the start before Close's wait.
	done, err := r.beginRead()
	if err != nil {
		return
	}
	defer done()
	r.refreshRequested.Store(true)
	if !r.refreshing.CompareAndSwap(false, true) {
		return
	}
	r.background.Add(1)
	go func() {
		defer r.background.Done()
		for {
			r.refreshRequested.Store(false)
			r.refreshOnce()
			r.refreshing.Store(false)
			// A request made meanwhile runs here, unless a new start took it.
			if r.closed.Load() || !r.refreshRequested.Load() || !r.refreshing.CompareAndSwap(false, true) {
				return
			}
		}
	}()
}

// refreshOnce runs one background refresh, bounded by refreshTimeout; a
// timeout is recorded as a failure, as an error is by reloadManifest.
func (r *Reader) refreshOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), r.refreshTimeout)
	defer cancel()
	if err := r.refreshManifest(ctx, false); err != nil && ctx.Err() != nil {
		r.refreshFailed(fmt.Errorf("manifest refresh timed out after %s: %w", r.refreshTimeout, err))
	}
}

// refreshManifest reloads the view, sharing a reload in progress. A forced
// refresh must see every commit made before the call, so it waits for a
// reload that started after it.
func (r *Reader) refreshManifest(ctx context.Context, force bool) error {
	called := r.reloads.Load()
	for {
		value, err := r.manifestLoads.Do(ctx, "manifest", func(loadCtx context.Context) (any, error) {
			if !force && !r.manifestViewDue() {
				return uint64(0), nil
			}
			started := r.reloads.Add(1)
			return started, r.reloadManifest(loadCtx)
		})
		if err != nil || !force {
			return err
		}
		if started, _ := value.(uint64); started > called {
			return nil
		}
	}
}

func (r *Reader) reloadManifest(ctx context.Context) (err error) {
	viewLoadedAt := time.Now()
	start := viewLoadedAt
	defer func() {
		r.metrics.ObserveRefresh(time.Since(start), err)
	}()

	// The manifest and the CURRENT it was built from are published together:
	// reading CURRENT again could observe an overlapping reload's generation.
	m, current, err := r.manifestStore.ReplayWithCurrentValidated(ctx)
	// Every caller waiting for this reload gave up: publish nothing.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		r.refreshFailed(err)
		return err
	}
	if published, viewSeq := r.publishManifestView(m, current, viewLoadedAt); !published {
		// CURRENT is older than the view: either an overlapping reload
		// published a newer one, which leaves the view not due and so is
		// ignored, or the store went back, which is retried like a failure.
		r.refreshFailed(fmt.Errorf("manifest CURRENT at log position %d is older than the loaded view at %d",
			currentNextSeq(current), viewSeq))
		return nil
	}
	r.refreshSucceeded()
	return nil
}

// nextOnGrid returns the first time after after on this reader's grid,
// refreshGrid + k*period. The grid's random phase keeps readers spread out,
// and nothing that happens to one reader moves it into step with others. A
// reader without a grid schedules one period after after.
func (r *Reader) nextOnGrid(after time.Time, period time.Duration) time.Time {
	if r.refreshGrid.IsZero() {
		return after.Add(period)
	}
	offset := after.Sub(r.refreshGrid) % period
	if offset < 0 {
		offset += period // after is before the grid's origin
	}
	return after.Add(period - offset)
}

// refreshRetryAfter is the delay before retrying a failed refresh, capped at
// RefreshAfter.
const refreshRetryAfter = 30 * time.Second

// refreshFailed marks reads stale, schedules a retry about refreshRetryAfter
// later on the reader's grid, and logs at most once a minute. A failed forced
// refresh of a view not yet due changes nothing.
func (r *Reader) refreshFailed(err error) {
	if r.closed.Load() {
		return
	}
	now := time.Now()
	r.mu.Lock()
	expiresAt := r.viewExpiresAt
	expired := !now.Before(expiresAt)
	if !expired && now.Before(r.viewRefreshAt) {
		r.mu.Unlock()
		return
	}
	r.stale.Store(true)
	// Retries keep the reader's phase, so readers that failed together, in
	// an outage, retry and recover at their own offsets rather than in step.
	retryPeriod := min(refreshRetryAfter, r.viewPolicy.RefreshAfter)
	retryAt := r.nextOnGrid(now.Add(retryPeriod/2), retryPeriod)
	// The timer wakes at the earlier of the two times; once expired, only
	// the retry is ahead.
	wakeExpiry := expiresAt
	if expired {
		wakeExpiry = retryAt
	} else {
		retryAt = minTime(retryAt, expiresAt)
	}
	if r.refreshFailures == 0 {
		r.staleSince = now
	}
	r.refreshFailures++
	r.viewRefreshAt = retryAt
	age := now.Sub(r.viewLoadedAt)
	r.mu.Unlock()

	r.armViewTimer(retryAt, wakeExpiry)
	if allowed, suppressed := r.refreshDiagnosticLimiter.allow(now); allowed {
		message := "isledb: manifest refresh failed; reads use the loaded view until it expires"
		if expired {
			message = "isledb: manifest refresh failed and the loaded view has expired; reads fail until a refresh succeeds"
		}
		slog.Warn(message,
			"error", err,
			"view_age", age.Round(time.Second),
			"expires_in", max(expiresAt.Sub(now), 0).Round(time.Second),
			"retry_in", retryAt.Sub(now).Round(time.Second),
			"suppressed_since_last_log", suppressed,
		)
	}
}

// refreshSucceeded logs the end of a stale period, if one was under way.
func (r *Reader) refreshSucceeded() {
	r.mu.Lock()
	failures, since := r.refreshFailures, r.staleSince
	r.refreshFailures, r.staleSince = 0, time.Time{}
	r.stale.Store(false)
	r.mu.Unlock()
	if failures > 0 {
		slog.Info("isledb: manifest refresh recovered",
			"failed_refreshes", failures,
			"stale_for", time.Since(since).Round(time.Second),
		)
	}
}

// publishManifestView publishes a view unless it is older than the one
// published, and reports whether it did, with the published log position.
func (r *Reader) publishManifestView(
	m *manifestState,
	current *manifest.Current,
	viewLoadedAt time.Time,
) (bool, uint64) {
	changeFeed, changeHead := readerChangeFeedState(current)
	refreshAt := r.nextOnGrid(viewLoadedAt, r.viewPolicy.RefreshAfter)
	expiresAt := viewLoadedAt.Add(current.PinnedViewAge())

	// Reloads can finish in any order; an older view is dropped, so reads
	// never go back in time.
	seq := currentNextSeq(current)
	r.mu.Lock()
	if seq < r.viewSeq {
		published := r.viewSeq
		r.mu.Unlock()
		return false, published
	}
	r.manifest = m
	r.viewSeq = seq
	r.viewLoadedAt = viewLoadedAt
	r.version = versionFromCurrent(current)
	r.changeFeed = changeFeed
	r.changeHead = changeHead
	r.viewRefreshAt = refreshAt
	r.viewExpiresAt = expiresAt
	r.mu.Unlock()

	r.blockCache.prune(m, r.openSSTs.isOpen)
	r.armViewTimer(refreshAt, expiresAt)
	r.metrics.ObserveViewLoaded(viewLoadedAt)
	return true, seq
}

// currentNextSeq is the manifest log position a CURRENT reflects, zero for
// none.
func currentNextSeq(current *manifest.Current) uint64 {
	if current == nil {
		return 0
	}
	return current.NextSeq
}

func readerChangeFeedState(current *manifest.Current) (bool, ChangeCursor) {
	if current == nil {
		return false, changeCursorAt(0, 0)
	}
	return current.ChangeFeedEnabled, changeCursorAt(current.NextSeq, 0)
}

// armViewTimer schedules the next background refresh at refreshAt, or at
// expiresAt if sooner, so even an idle reader's view stays fresh.
func (r *Reader) armViewTimer(refreshAt, expiresAt time.Time) {
	timerID := r.viewTimerID.Add(1)
	r.viewDue.Store(false)
	wakeAt := minTime(refreshAt, expiresAt)
	delay := time.Until(wakeAt)
	if delay < 0 {
		delay = 0
	}
	timer := time.AfterFunc(delay, func() {
		if r.viewTimerID.Load() == timerID && !r.closed.Load() {
			r.viewDue.Store(true)
			r.refreshInBackground()
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

func (r *Reader) manifestViewDue() bool {
	if r.viewDue.Load() {
		return true
	}
	r.mu.RLock()
	wakeAt := minTime(r.viewRefreshAt, r.viewExpiresAt)
	r.mu.RUnlock()
	if !wakeAt.IsZero() && !time.Now().Before(wakeAt) {
		r.viewDue.Store(true)
		return true
	}
	return false
}

func (r *Reader) stopViewTimer() {
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
	r.stopViewTimer()
	r.closeOpenIterators()
	r.manifestLoads.Close(ErrReaderClosed)
	r.background.Wait()
	r.bloomLoads.Close(ErrReaderClosed)
	r.bloomBackground.Wait()
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
	if r.endRead != nil {
		return r.endRead, nil
	}
	return r.lifecycleMu.RUnlock, nil
}

// currentManifest returns the published manifest. A refresh replaces it
// rather than modifying it, so callers may keep it but must not modify it.
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

	if err := r.checkManifestView(ctx); err != nil {
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

	if err := r.checkManifestView(ctx); err != nil {
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
	if err := r.checkManifestView(ctx); err != nil {
		return nil, false, err
	}

	m, _, expiresAt := r.currentManifestState()
	readCtx := withReadDeadline(ctx, expiresAt, ErrReadViewExpired)
	defer readCtx.release()
	value, found, err = r.getWithManifest(readCtx, m, key)
	return value, found, readCtx.err(err)
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
	if err := r.checkManifestView(ctx); err != nil {
		return nil, err
	}

	m, _, expiresAt := r.currentManifestState()
	readCtx := withReadDeadline(ctx, expiresAt, ErrReadViewExpired)
	defer readCtx.release()
	out, err = r.scanInternalWithManifest(readCtx, m, minKey, maxKey, 0)
	return out, readCtx.err(err)
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
	if err := r.checkManifestView(ctx); err != nil {
		return nil, err
	}

	m, _, expiresAt := r.currentManifestState()
	readCtx := withReadDeadline(ctx, expiresAt, ErrReadViewExpired)
	defer readCtx.release()
	out, err = r.scanInternalWithManifest(readCtx, m, minKey, maxKey, limit)
	return out, readCtx.err(err)
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
			// Published manifests are immutable, so borrowing a level is safe.
			// A narrow iterator copies its selection so it does not keep a
			// large level alive.
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
		if !r.bloomMayContain(sstMeta, key) {
			return nil, false, false, nil
		}
	}
	value, found, tombstone, err = r.lookupSST(ctx, sstMeta, key)
	if damaged(err) {
		// The damaged SST was dropped, so the retry fetches it again. Other
		// errors would fail the same way again.
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

// bloomMayContain returns false only when the SST's filter rules key out. A
// lookup waits for a filter only to load it from the disk cache; one not on
// disk loads in the background while the lookup reads the SST.
func (r *Reader) bloomMayContain(sstMeta sstMetadata, key []byte) bool {
	if filter, ok := r.bloomCache.get(sstMeta.ID); ok {
		return filter.mayContain(bloomHashKey(key))
	}
	o := r.fetcher.object(sstMeta)
	if r.fetcher.diskHas(o.entry(diskcache.KindBloom, 0), o.bloomLength) {
		if filter, ok := r.bloomFromDisk(o); ok {
			return filter.mayContain(bloomHashKey(key))
		}
	}
	if r.bloomLoadToStart(sstMeta.ID) {
		r.loadBloomInBackground(sstMeta)
	}
	r.metrics.ObserveBloomFilterSkip()
	return true
}

// bloomFromDisk loads an SST's filter from the disk cache, shared by
// concurrent lookups. It never fetches from the store.
func (r *Reader) bloomFromDisk(o sstObject) (sstBloomFilter, bool) {
	value, err := r.bloomLoads.Do(context.Background(), "disk/"+o.id, func(context.Context) (any, error) {
		if filter, ok := r.bloomCache.peek(o.id); ok {
			return filter, nil
		}
		data, ok := r.fetcher.diskBloom(o)
		if !ok {
			return nil, errBloomNotOnDisk
		}
		filter, err := parseSSTBloomFilter(data)
		if err != nil {
			return nil, err
		}
		r.bloomCache.put(o.id, filter)
		return filter, nil
	})
	if err != nil {
		return sstBloomFilter{}, false
	}
	return value.(sstBloomFilter), true
}

var errBloomNotOnDisk = errors.New("bloom filter not on disk")

const (
	bloomLoadTimeout = time.Minute
	bloomRetryDelay  = 30 * time.Second
	maxBloomLoads    = 4
)

// bloomLoadToStart claims a background load of the SST's filter. It refuses
// when the filter is in bloomLoading or maxBloomLoads loads are running.
func (r *Reader) bloomLoadToStart(id string) bool {
	if _, busy := r.bloomLoading.LoadOrStore(id, struct{}{}); busy {
		return false
	}
	if r.bloomLoadsRunning.Add(1) > maxBloomLoads {
		r.bloomLoadsRunning.Add(-1)
		r.bloomLoading.Delete(id)
		return false
	}
	return true
}

// loadBloomInBackground is apart from bloomMayContain so that sstMeta, which
// the goroutine captures, moves to the heap only when a load starts.
func (r *Reader) loadBloomInBackground(sstMeta sstMetadata) {
	r.bloomBackground.Add(1)
	go func() {
		defer r.bloomBackground.Done()
		defer r.bloomLoadsRunning.Add(-1)
		ctx, cancel := context.WithTimeout(context.Background(), bloomLoadTimeout)
		defer cancel()
		_, err := r.bloomLoads.Do(ctx, sstMeta.ID, func(ctx context.Context) (any, error) {
			if filter, ok := r.bloomCache.peek(sstMeta.ID); ok {
				return filter, nil
			}
			filter, err := r.loadBloomFilter(ctx, sstMeta)
			if err != nil {
				return nil, err
			}
			r.bloomCache.put(sstMeta.ID, filter)
			return filter, nil
		})
		if err == nil || errors.Is(err, ErrReaderClosed) {
			r.bloomLoading.Delete(sstMeta.ID)
			return
		}
		r.observeBloomFilterError(sstMeta.ID, err)
		time.AfterFunc(bloomRetryDelay, func() { r.bloomLoading.Delete(sstMeta.ID) })
	}()
}

// loadBloomFilter reads an SST's filter from the disk cache, or else from
// object storage, verified against the manifest checksum before use; a
// corrupted filter could otherwise report a present key as absent.
func (r *Reader) loadBloomFilter(ctx context.Context, sstMeta sstMetadata) (sstBloomFilter, error) {
	data, err := r.fetcher.bloom(ctx, r.fetcher.object(sstMeta))
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

// openSSTIterBounded opens an iterator over the SST's keys in [lower, upper),
// reusing an open SST. A private iterator fills no cache (see scanSSTSource).
// An SST that fails on damage is dropped.
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

// DiskCacheStats describes the disk cache's two tiers.
type DiskCacheStats struct {
	// Meta holds SST metadata regions and Bloom filters.
	Meta CacheStats
	// Data holds small SSTs whole and chunks of larger SSTs' data.
	Data CacheStats
	// SSTDrops counts reads that dropped an SST from every cache after
	// finding damaged bytes. Each such read counts, not each SST.
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
	if err := r.checkManifestView(ctx); err != nil {
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
