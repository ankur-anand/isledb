package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	"golang.org/x/sync/singleflight"
)

type Reader struct {
	store         *blobstore.Store
	manifestStore *manifest.Store
	sstCache      diskcache.RefCountedCache
	blockCache    *ristretto.Cache[string, []byte]
	bloomCache    *bloomFilterCache
	bloomLoads    singleflight.Group
	sstLoads      singleflight.Group
	manifestLoads singleflight.Group

	verifySST                bool
	allowUnverifiedRangeRead bool
	rangeReadMinSSTSize      int64

	ownsSSTCache   bool
	ownsBlockCache bool
	cacheDir       string

	lifecycleMu   sync.RWMutex
	iteratorsMu   sync.Mutex
	iterators     map[*Iterator]struct{}
	mu            sync.RWMutex
	manifest      *manifestState
	version       Version
	viewPolicy    ReaderViewPolicy
	viewRefreshAt time.Time
	viewExpiresAt time.Time
	viewExpired   atomic.Bool
	viewTimerMu   sync.Mutex
	viewTimer     *time.Timer
	viewTimerID   atomic.Uint64
	metrics       *ReaderMetrics
	closed        atomic.Bool
	releaseOnce   sync.Once
	release       func()
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
	m, err := ms.Replay(ctx)
	if err != nil {
		return nil, err
	}

	sstCache, ownsSSTCache, err := initSSTCache(opts)
	if err != nil {
		return nil, err
	}
	cleanupSSTCache := ownsSSTCache
	defer func() {
		if cleanupSSTCache {
			_ = sstCache.Close()
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
	viewExpiresAt := viewLoadedAt.Add(ms.CurrentData().PinnedViewAge())
	reader := &Reader{
		store:                    store,
		manifestStore:            ms,
		manifest:                 m,
		version:                  versionFromCurrent(ms.CurrentData()),
		viewPolicy:               viewPolicy,
		viewRefreshAt:            viewRefreshAt,
		viewExpiresAt:            viewExpiresAt,
		sstCache:                 sstCache,
		blockCache:               blockCache,
		bloomCache:               newBloomFilterCache(opts.BloomCacheSize),
		verifySST:                opts.ValidateSSTChecksum,
		allowUnverifiedRangeRead: opts.AllowUnverifiedRangeRead,
		rangeReadMinSSTSize:      opts.RangeReadMinSSTSize,
		ownsSSTCache:             ownsSSTCache,
		ownsBlockCache:           ownsBlockCache,
		cacheDir:                 opts.CacheDir,
		metrics:                  opts.Metrics,
	}
	reader.armManifestExpiry(viewRefreshAt, viewExpiresAt)
	cleanupSSTCache = false
	cleanupBlockCache = false
	return reader, nil
}

func initSSTCache(opts readerOptions) (diskcache.RefCountedCache, bool, error) {
	if opts.SSTCache != nil {
		return opts.SSTCache, false, nil
	}

	if opts.CacheDir == "" {
		return nil, false, errors.New("cache dir is required")
	}

	maxSize := opts.SSTCacheSize
	if maxSize == 0 {
		maxSize = defaultSSTCacheSize
	}

	cache, err := diskcache.NewSSTCache(diskcache.SSTCacheOptions{
		Dir:     filepath.Join(opts.CacheDir, "sst"),
		MaxSize: maxSize,
	})
	if err != nil {
		return nil, false, fmt.Errorf("create sst cache: %w", err)
	}

	return cache, true, nil
}

// Refresh reloads the manifest and invalidates caches for removed SSTs.
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
	_, err, _ := r.manifestLoads.Do("manifest", func() (any, error) {
		if !force && !r.manifestViewExpired() {
			return nil, nil
		}
		return nil, r.reloadManifest(ctx)
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
	m, err = r.manifestStore.Replay(ctx)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.manifest != nil {
		r.invalidateRemovedSSTs(r.manifest, m)
	}

	// Publish a new manifest pointer. Existing readers/views may still retain
	// the previous manifest as an immutable snapshot.
	r.manifest = m
	r.version = versionFromCurrent(r.manifestStore.CurrentData())
	r.viewRefreshAt = viewLoadedAt.Add(r.viewPolicy.RefreshAfter)
	r.viewExpiresAt = viewLoadedAt.Add(r.manifestStore.CurrentData().PinnedViewAge())
	r.armManifestExpiry(r.viewRefreshAt, r.viewExpiresAt)
	return nil
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

func (r *Reader) invalidateRemovedSSTs(oldManifest, newManifest *manifestState) {
	oldIDs := make(map[string]struct{})
	for _, id := range oldManifest.AllSSTIDs() {
		oldIDs[id] = struct{}{}
	}

	newIDs := make(map[string]struct{})
	for _, id := range newManifest.AllSSTIDs() {
		newIDs[id] = struct{}{}
	}

	for id := range oldIDs {
		if _, exists := newIDs[id]; !exists {
			path := r.store.SSTPath(id)
			r.sstCache.Remove(path)
			r.bloomCache.delete(id)
		}
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

	var firstErr error

	if r.sstCache != nil && r.ownsSSTCache {
		if err := r.sstCache.Close(); err != nil {
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
		if !keyInRange(key, sst.MinKey, sst.MaxKey) {
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

// Scan returns all key-value pairs in the given key range.
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

	allIters, err := r.openRangeIters(ctx, m, minKey, maxKey)
	if err != nil {
		return nil, err
	}
	defer closeSSTIters(allIters)

	if len(allIters) == 0 {
		return nil, nil
	}

	mergeIter := newMergeIterator(allIters)

	nowMs := time.Now().UnixMilli()
	for mergeIter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if limit > 0 && len(out) >= limit {
			break
		}

		entry, err := mergeIter.entry()
		if err != nil {
			return nil, err
		}

		if len(minKey) > 0 && bytes.Compare(entry.Key, minKey) < 0 {
			continue
		}
		if len(maxKey) > 0 && bytes.Compare(entry.Key, maxKey) > 0 {
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

func closeSSTIters(iters []sstable.Iterator) {
	for _, it := range iters {
		_ = it.Close()
	}
}

func (r *Reader) openRangeIters(ctx context.Context, m *manifestState, minKey, maxKey []byte) ([]sstable.Iterator, error) {
	var allIters []sstable.Iterator
	upper := maxKey
	if len(maxKey) > 0 {
		upper = incrementKey(maxKey)
	}

	for _, sst := range m.L0SSTs {
		if !internal.OverlapsRange(sst.MinKey, sst.MaxKey, minKey, maxKey) {
			continue
		}
		_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, upper)
		if err != nil {
			closeSSTIters(allIters)
			return nil, err
		}
		allIters = append(allIters, iter)
	}

	for i := range m.Levels {
		overlapping := m.Levels[i].OverlappingSSTs(minKey, maxKey)
		for _, sst := range overlapping {
			_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, upper)
			if err != nil {
				closeSSTIters(allIters)
				return nil, err
			}
			allIters = append(allIters, iter)
		}
	}

	return allIters, nil
}

func keyInRange(key, minKey, maxKey []byte) bool {
	if len(minKey) > 0 && bytes.Compare(key, minKey) < 0 {
		return false
	}
	if len(maxKey) > 0 && bytes.Compare(key, maxKey) > 0 {
		return false
	}
	return true
}

func (r *Reader) getFromSST(ctx context.Context, sstMeta sstMetadata, key []byte) ([]byte, bool, bool, error) {
	if sstMeta.Bloom.Length > 0 {
		if filter, ok := r.bloomCache.get(sstMeta.ID); ok {
			if !filter.Has(bloomHashKey(key)) {
				return nil, false, false, nil
			}
		} else if !r.sstCached(sstMeta.ID) {
			mayContain, err := r.bloomMayContain(ctx, sstMeta, key)
			if err != nil {
				return nil, false, false, err
			}
			if !mayContain {
				return nil, false, false, nil
			}
		}
	}

	_, iter, err := r.openSSTIterBounded(ctx, sstMeta, key, nil)
	if err != nil {
		return nil, false, false, err
	}
	defer iter.Close()

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

func (r *Reader) bloomMayContain(ctx context.Context, sstMeta sstMetadata, key []byte) (bool, error) {
	if filter, ok := r.bloomCache.get(sstMeta.ID); ok {
		return filter.Has(bloomHashKey(key)), nil
	}

	value, err, _ := r.bloomLoads.Do(sstMeta.ID, func() (interface{}, error) {
		if filter, ok := r.bloomCache.peek(sstMeta.ID); ok {
			return filter, nil
		}

		path := r.store.SSTPath(sstMeta.ID)
		data, err := r.store.ReadRange(ctx, path, sstMeta.Bloom.Offset, sstMeta.Bloom.Length)
		if err != nil {
			return nil, fmt.Errorf("read bloom %s: %w", sstMeta.ID, err)
		}
		filter, err := parseBloomFilter(data)
		if err != nil {
			return nil, fmt.Errorf("decode bloom %s: %w", sstMeta.ID, err)
		}
		r.bloomCache.put(sstMeta.ID, filter)
		return filter, nil
	})
	if err != nil {
		return false, err
	}
	return value.(*z.Bloom).Has(bloomHashKey(key)), nil
}

func (r *Reader) sstCached(id string) bool {
	path := r.store.SSTPath(id)
	if _, ok := r.sstCache.Acquire(path); ok {
		r.sstCache.Release(path)
		return true
	}
	return false
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

	if cached, ok := r.sstCache.Acquire(path); ok {
		r.metrics.ObserveSSTCacheLookup(true)
		release := func() {
			r.sstCache.Release(path)
		}
		reader, iter, err := r.openSSTIterFromData(ctx, sstMeta, cached, lower, upper, release)
		if err == nil {
			return reader, iter, nil
		}
		// Cached bytes are not authoritative. If they cannot be opened, release
		// and evict them, then make one attempt against object storage below.
		r.sstCache.Remove(path)
	} else {
		r.metrics.ObserveSSTCacheLookup(false)
	}

	if ok, size, err := r.shouldRangeRead(sstMeta); err != nil {
		return nil, nil, err
	} else if ok {
		return r.openSSTIterRange(ctx, sstMeta, path, lower, upper, size)
	}

	if err := r.ensureSSTCached(ctx, &sstMeta, path); err != nil {
		return nil, nil, err
	}
	if cached, ok := r.sstCache.Acquire(path); ok {
		release := func() {
			r.sstCache.Release(path)
		}
		reader, iter, err := r.openSSTIterFromData(ctx, sstMeta, cached, lower, upper, release)
		if err != nil {
			// Do not let a failed download poison later reads. This is the only
			// object-store attempt in this call, so return the error after eviction.
			r.sstCache.Remove(path)
		}
		return reader, iter, err
	}
	return nil, nil, fmt.Errorf("cache sst %s: missing after download", sstMeta.ID)
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
	readable := newSSTRangeReadable(r.store, path, sstMeta.ID, size, r.blockCache, r.metrics)
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

func (r *Reader) validateSSTData(meta sstMetadata, data []byte) error {
	var err error
	data, err = trimSSTData(meta, data)
	if err != nil {
		return err
	}
	if !r.verifySST {
		return nil
	}

	sum := sha256.Sum256(data)
	return r.validateSSTChecksum(meta, sum)
}

func (r *Reader) validateSSTChecksum(meta sstMetadata, sum [32]byte) error {
	if !r.verifySST {
		return nil
	}

	hashHex := hex.EncodeToString(sum[:])
	if meta.Checksum == "" {
		return fmt.Errorf("sst %s: missing checksum", meta.ID)
	}
	algo, expected, ok := strings.Cut(meta.Checksum, ":")
	if !ok || algo != "sha256" {
		return fmt.Errorf("sst %s: unsupported checksum %q", meta.ID, meta.Checksum)
	}
	if expected != hashHex {
		return fmt.Errorf("sst %s: checksum mismatch", meta.ID)
	}

	return nil
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
	if cache, ok := r.sstCache.(diskcache.FileBackedCache); ok {
		return r.cacheSSTStream(ctx, cache, meta, path)
	}

	start := time.Now()
	var downloadedBytes int64
	defer func() {
		r.metrics.ObserveSSTDownload(time.Since(start), downloadedBytes, err)
	}()

	var data []byte
	if meta != nil && meta.Size > 0 {
		data, err = r.store.ReadRange(ctx, path, 0, meta.Size)
	} else {
		data, _, err = r.store.Read(ctx, path)
	}
	if err != nil {
		return fmt.Errorf("read sst %s: %w", path, err)
	}
	downloadedBytes = int64(len(data))

	if meta != nil {
		if err := r.validateSSTData(*meta, data); err != nil {
			return fmt.Errorf("validate sst %s: %w", path, err)
		}
	}

	if err := r.sstCache.Set(path, data); err != nil {
		return fmt.Errorf("cache sst %s: %w", path, err)
	}
	return nil
}

func (r *Reader) ensureSSTCached(ctx context.Context, meta *sstMetadata, path string) error {
	if _, ok := r.sstCache.Acquire(path); ok {
		r.sstCache.Release(path)
		return nil
	}

	_, err, _ := r.sstLoads.Do(path, func() (interface{}, error) {
		if _, ok := r.sstCache.Acquire(path); ok {
			r.sstCache.Release(path)
			return nil, nil
		}
		return nil, r.cacheSST(ctx, meta, path)
	})
	return err
}

func (r *Reader) cacheSSTStream(ctx context.Context, cache diskcache.FileBackedCache, meta *sstMetadata, path string) (err error) {
	start := time.Now()
	var downloadedBytes int64
	defer func() {
		r.metrics.ObserveSSTDownload(time.Since(start), downloadedBytes, err)
	}()

	tmpFile, err := os.CreateTemp(cache.CacheDir(), "sst-*")
	if err != nil {
		return fmt.Errorf("create temp sst %s: %w", path, err)
	}
	tmpPath := tmpFile.Name()
	cleanup := func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}

	var stream io.ReadCloser
	if meta != nil && meta.Size > 0 {
		stream, err = r.store.ReadRangeStream(ctx, path, 0, meta.Size)
	} else {
		stream, err = r.store.ReadStream(ctx, path)
	}
	if err != nil {
		cleanup()
		return fmt.Errorf("read sst %s: %w", path, err)
	}
	defer stream.Close()

	needHash := meta != nil && r.verifySST
	var hasher hash.Hash
	writer := io.Writer(tmpFile)
	if needHash {
		hasher = sha256.New()
		writer = io.MultiWriter(tmpFile, hasher)
	}

	written, err := io.Copy(writer, stream)
	if err != nil {
		cleanup()
		return fmt.Errorf("download sst %s: %w", path, err)
	}
	downloadedBytes = written
	if meta != nil && meta.Size > 0 && written < meta.Size {
		cleanup()
		return fmt.Errorf("validate sst %s: short read: %d < %d", path, written, meta.Size)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close sst %s: %w", path, err)
	}

	if needHash {
		var sum [32]byte
		copy(sum[:], hasher.Sum(nil))
		if err := r.validateSSTChecksum(*meta, sum); err != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("validate sst %s: %w", path, err)
		}
	}

	if err := cache.SetFromFile(path, tmpPath, written); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("cache sst %s: %w", path, err)
	}

	return nil
}

func (r *Reader) SSTCacheStats() CacheStats {
	return cacheStatsFromDisk(r.sstCache.Stats())
}

// BloomCacheStats reports the decoded bloom-filter cache occupancy.
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

func cacheStatsFromDisk(stats diskcache.Stats) CacheStats {
	return CacheStats{
		Hits:       stats.Hits,
		Misses:     stats.Misses,
		Bytes:      stats.Size,
		MaxBytes:   stats.MaxSize,
		EntryCount: stats.EntryCount,
	}
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
	sstIters  []sstable.Iterator
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

	allIters, err := r.openRangeIters(iterCtx, m, opts.MinKey, opts.MaxKey)
	if err != nil {
		cancel()
		return nil, err
	}

	if len(allIters) == 0 {

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
		mergeIter: newMergeIterator(allIters),
		sstIters:  allIters,
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
		if len(it.maxKey) > 0 && bytes.Compare(entry.Key, it.maxKey) > 0 {
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

	for _, iter := range it.sstIters {
		_ = iter.Close()
	}
	it.sstIters = nil
	it.mergeIter = nil
	if it.cancel != nil {
		it.cancel()
		it.cancel = nil
	}
	if it.reader != nil {
		it.reader.unregisterIterator(it)
	}

	return nil
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
	if it.mergeIter.err != nil {
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
