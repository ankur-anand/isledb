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
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/cachestore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/diskcache"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/dgraph-io/ristretto/v2/z"
	"golang.org/x/sync/singleflight"
)

type Reader struct {
	store         *blobstore.Store
	manifestStore *manifest.Store
	sstCache      SSTCache
	blockCache    *ristretto.Cache[string, []byte]
	bloomCache    sync.Map
	bloomLoads    singleflight.Group
	sstLoads      singleflight.Group
	blobLoads     singleflight.Group

	blobStorage              *internal.BlobStorage
	blobCache                internal.BlobCache
	valueConfig              config.ValueStorageConfig
	verifySST                bool
	verifier                 SSTHashVerifier
	allowUnverifiedRangeRead bool
	rangeReadMinSSTSize      int64

	ownsSSTCache   bool
	ownsBlockCache bool
	ownsBlobCache  bool

	mu       sync.RWMutex
	manifest *Manifest
	metrics  *ReaderMetrics
}

type KV struct {
	Key   []byte
	Value []byte
}

func newReader(ctx context.Context, store *blobstore.Store, opts ReaderOptions) (*Reader, error) {
	ms := newManifestStoreWithCache(store, &opts)
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

	blobCache, ownsBlobCache, err := initBlobCache(opts)
	if err != nil {
		return nil, err
	}
	cleanupBlobCache := ownsBlobCache
	defer func() {
		if cleanupBlobCache {
			_ = blobCache.Close()
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

	valueConfig := opts.ValueStorageConfig
	if valueConfig.BlobThreshold == 0 {
		valueConfig = config.DefaultValueStorageConfig()
	}

	reader := &Reader{
		store:                    store,
		manifestStore:            ms,
		manifest:                 m,
		sstCache:                 sstCache,
		blockCache:               blockCache,
		blobStorage:              internal.NewBlobStorage(store, valueConfig),
		blobCache:                blobCache,
		valueConfig:              valueConfig,
		verifySST:                opts.ValidateSSTChecksum,
		verifier:                 opts.SSTHashVerifier,
		allowUnverifiedRangeRead: opts.AllowUnverifiedRangeRead,
		rangeReadMinSSTSize:      opts.RangeReadMinSSTSize,
		ownsSSTCache:             ownsSSTCache,
		ownsBlockCache:           ownsBlockCache,
		ownsBlobCache:            ownsBlobCache,
		metrics:                  opts.Metrics,
	}
	cleanupSSTCache = false
	cleanupBlockCache = false
	cleanupBlobCache = false
	return reader, nil
}

func initSSTCache(opts ReaderOptions) (diskcache.RefCountedCache, bool, error) {
	if opts.SSTCache != nil {
		return opts.SSTCache, false, nil
	}

	if opts.CacheDir == "" {
		return nil, false, errors.New("CacheDir is required")
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

func initBlobCache(opts ReaderOptions) (diskcache.Cache, bool, error) {
	if opts.BlobCache != nil {
		return opts.BlobCache, false, nil
	}

	maxSize := opts.BlobCacheSize
	if maxSize == 0 {
		maxSize = defaultBlobCacheSize
	}

	cache, err := diskcache.NewBlobCache(diskcache.BlobCacheOptions{
		Dir:         filepath.Join(opts.CacheDir, "blob"),
		MaxSize:     maxSize,
		MaxItemSize: opts.BlobCacheMaxItemSize,
	})
	if err != nil {
		return nil, false, fmt.Errorf("create blob cache: %w", err)
	}

	return cache, true, nil
}

// Refresh reloads the manifest and invalidates caches for removed SSTs.
func (r *Reader) Refresh(ctx context.Context) (err error) {
	start := time.Now()
	defer func() {
		r.metrics.ObserveRefresh(time.Since(start), err)
	}()

	var m *Manifest
	m, err = r.manifestStore.Replay(ctx)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.manifest != nil {
		r.invalidateRemovedSSTs(r.manifest, m)
	}

	r.manifest = m
	return nil
}

func (r *Reader) invalidateRemovedSSTs(oldManifest, newManifest *Manifest) {
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
			r.bloomCache.Delete(id)
		}
	}
}

func (r *Reader) Close() error {
	var firstErr error

	if r.sstCache != nil && r.ownsSSTCache {
		if err := r.sstCache.Close(); err != nil {
			firstErr = err
		}
	}

	if r.blockCache != nil && r.ownsBlockCache {
		r.blockCache.Close()
	}

	if r.blobCache != nil && r.ownsBlobCache {
		if err := r.blobCache.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (r *Reader) Manifest() *Manifest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.manifest == nil {
		return nil
	}
	return r.manifest.Clone()
}

func (r *Reader) ManifestUnsafe() *Manifest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manifest
}

// Get returns the value for key if present and not deleted/expired.
func (r *Reader) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	start := time.Now()
	defer func() {
		r.metrics.ObserveGet(time.Since(start), found, err)
	}()

	if len(key) == 0 {
		return nil, false, errors.New("empty key")
	}

	r.mu.RLock()
	m := r.manifest
	r.mu.RUnlock()

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

	for _, sr := range m.SortedRuns {

		sst := sr.FindSST(key)
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

	r.mu.RLock()
	m := r.manifest
	r.mu.RUnlock()

	if m == nil {
		return nil, errors.New("manifest not loaded")
	}

	var allIters []sstable.Iterator
	upper := maxKey
	if len(maxKey) > 0 {
		upper = incrementKey(maxKey)
	}

	cleanup := func() {
		for _, it := range allIters {
			_ = it.Close()
		}

	}

	for _, sst := range m.L0SSTs {
		if !internal.OverlapsRange(sst.MinKey, sst.MaxKey, minKey, maxKey) {
			continue
		}
		_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, upper)
		if err != nil {
			cleanup()
			return nil, err
		}
		allIters = append(allIters, iter)
	}

	for _, sr := range m.SortedRuns {
		overlapping := sr.OverlappingSSTs(minKey, maxKey)
		for _, sst := range overlapping {
			_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, upper)
			if err != nil {
				cleanup()
				return nil, err
			}
			allIters = append(allIters, iter)
		}
	}

	defer cleanup()

	if len(allIters) == 0 {
		return nil, nil
	}

	mergeIter := newMergeIterator(allIters)

	nowMs := time.Now().UnixMilli()
	for mergeIter.Next() {
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

func (r *Reader) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) (out []KV, err error) {
	start := time.Now()
	defer func() {
		r.metrics.ObserveScanLimit(time.Since(start), len(out), err)
	}()

	r.mu.RLock()
	m := r.manifest
	r.mu.RUnlock()

	if m == nil {
		return nil, errors.New("manifest not loaded")
	}

	var allIters []sstable.Iterator
	upper := maxKey
	if len(maxKey) > 0 {
		upper = incrementKey(maxKey)
	}

	cleanup := func() {
		for _, it := range allIters {
			_ = it.Close()
		}
	}

	for _, sst := range m.L0SSTs {
		if !internal.OverlapsRange(sst.MinKey, sst.MaxKey, minKey, maxKey) {
			continue
		}
		_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, upper)
		if err != nil {
			cleanup()
			return nil, err
		}
		allIters = append(allIters, iter)
	}

	for _, sr := range m.SortedRuns {
		overlapping := sr.OverlappingSSTs(minKey, maxKey)
		for _, sst := range overlapping {
			_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, upper)
			if err != nil {
				cleanup()
				return nil, err
			}
			allIters = append(allIters, iter)
		}
	}

	defer cleanup()

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

func keyInRange(key, minKey, maxKey []byte) bool {
	if len(minKey) > 0 && bytes.Compare(key, minKey) < 0 {
		return false
	}
	if len(maxKey) > 0 && bytes.Compare(key, maxKey) > 0 {
		return false
	}
	return true
}

func (r *Reader) getFromSST(ctx context.Context, sstMeta SSTMeta, key []byte) ([]byte, bool, bool, error) {
	if sstMeta.Bloom.Length > 0 {
		if filter, ok := r.bloomCache.Load(sstMeta.ID); ok {
			if !filter.(*z.Bloom).Has(bloomHashKey(key)) {
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
	if decoded.Inline {
		return append([]byte(nil), decoded.Value...), true, false, nil
	}

	if decoded.HasBlobID() {
		value, err := r.fetchBlob(ctx, decoded.BlobID)
		if err != nil {
			return nil, false, false, err
		}
		return value, true, false, nil
	}

	return nil, false, false, errors.New("corrupt entry: non-inline, non-blob")
}

func (r *Reader) bloomMayContain(ctx context.Context, sstMeta SSTMeta, key []byte) (bool, error) {
	if filter, ok := r.bloomCache.Load(sstMeta.ID); ok {
		return filter.(*z.Bloom).Has(bloomHashKey(key)), nil
	}

	value, err, _ := r.bloomLoads.Do(sstMeta.ID, func() (interface{}, error) {
		if filter, ok := r.bloomCache.Load(sstMeta.ID); ok {
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
		r.bloomCache.Store(sstMeta.ID, filter)
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

func (r *Reader) entryValue(ctx context.Context, entry internal.CompactionEntry) ([]byte, error) {
	if entry.Inline {
		return append([]byte(nil), entry.Value...), nil
	}

	if entry.HasBlobID() {
		return r.fetchBlob(ctx, entry.BlobID)
	}

	return nil, errors.New("corrupt entry: non-inline, non-blob")
}

type blobFetchResult struct {
	data     []byte
	cacheHit bool
}

func (r *Reader) fetchBlob(ctx context.Context, blobID [32]byte) (value []byte, err error) {
	start := time.Now()
	cacheHit := false
	defer func() {
		r.metrics.ObserveBlobFetch(time.Since(start), len(value), cacheHit, err)
	}()

	blobIDHex := internal.BlobIDToHex(blobID)

	if r.blobCache != nil {
		if data, ok := r.blobCache.Get(blobIDHex); ok {
			cacheHit = true
			return data, nil
		}
	}

	var sfValue interface{}
	sfValue, err, _ = r.blobLoads.Do(blobIDHex, func() (interface{}, error) {
		if r.blobCache != nil {
			if data, ok := r.blobCache.Get(blobIDHex); ok {
				return blobFetchResult{data: data, cacheHit: true}, nil
			}
		}

		data, err := r.blobStorage.Read(ctx, blobID)
		if err != nil {
			return nil, err
		}
		if r.blobCache != nil {
			r.blobCache.Set(blobIDHex, data)
		}
		return blobFetchResult{data: data, cacheHit: false}, nil
	})
	if err != nil {
		return nil, err
	}
	result, ok := sfValue.(blobFetchResult)
	if !ok {
		return nil, errors.New("internal error: unexpected blob fetch result")
	}
	cacheHit = result.cacheHit
	return result.data, nil
}

func (r *Reader) sstPayloadSize(meta SSTMeta) (int64, error) {
	if meta.Size > 0 {
		return meta.Size, nil
	}
	return 0, fmt.Errorf("sst %s: missing size in manifest", meta.ID)
}

func (r *Reader) BlobCacheStats() internal.BlobCacheStats {
	if r.blobCache != nil {
		return r.blobCache.Stats()
	}
	return internal.BlobCacheStats{}
}

func (r *Reader) openSSTIterBounded(ctx context.Context, sstMeta SSTMeta, lower, upper []byte) (*sstable.Reader, sstable.Iterator, error) {
	path := r.store.SSTPath(sstMeta.ID)

	if cached, ok := r.sstCache.Acquire(path); ok {
		r.metrics.ObserveSSTCacheLookup(true)
		release := func() {
			r.sstCache.Release(path)
		}
		return r.openSSTIterFromData(ctx, sstMeta, cached, lower, upper, release)
	}
	r.metrics.ObserveSSTCacheLookup(false)

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
		return r.openSSTIterFromData(ctx, sstMeta, cached, lower, upper, release)
	}
	return nil, nil, fmt.Errorf("cache sst %s: missing after download", sstMeta.ID)
}

func (r *Reader) shouldRangeRead(sstMeta SSTMeta) (bool, int64, error) {
	if r.blockCache == nil {
		return false, 0, nil
	}
	if !r.allowUnverifiedRangeRead && (r.verifySST || r.verifier != nil) {
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

func (r *Reader) openSSTIterRange(ctx context.Context, sstMeta SSTMeta, path string, lower, upper []byte, size int64) (*sstable.Reader, sstable.Iterator, error) {
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

func (r *Reader) openSSTIterFromData(ctx context.Context, sstMeta SSTMeta, data []byte, lower, upper []byte, release func()) (*sstable.Reader, sstable.Iterator, error) {
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

func (r *Reader) validateSSTData(meta SSTMeta, data []byte) error {
	needHash := r.verifySST || r.verifier != nil
	if !needHash {
		return nil
	}

	var err error
	data, err = trimSSTData(meta, data)
	if err != nil {
		return err
	}

	sum := sha256.Sum256(data)
	return r.validateSSTHash(meta, sum)
}

func (r *Reader) validateSSTHash(meta SSTMeta, sum [32]byte) error {
	if r.verifier != nil && meta.Signature == nil {
		return fmt.Errorf("sst %s: missing signature", meta.ID)
	}

	needHash := r.verifySST || r.verifier != nil
	if !needHash {
		return nil
	}

	hashHex := hex.EncodeToString(sum[:])

	if r.verifySST {
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
	}

	if r.verifier != nil {
		if meta.Signature.Hash != "" && meta.Signature.Hash != hashHex {
			return fmt.Errorf("sst %s: signature hash mismatch", meta.ID)
		}
		if err := r.verifier.VerifyHash(sum[:], *meta.Signature); err != nil {
			return fmt.Errorf("sst %s: signature verify: %w", meta.ID, err)
		}
	}

	return nil
}

func trimSSTData(meta SSTMeta, data []byte) ([]byte, error) {
	if meta.Size <= 0 {
		return nil, fmt.Errorf("sst %s: missing size in manifest", meta.ID)
	}
	if int64(len(data)) < meta.Size {
		return nil, fmt.Errorf("sst %s: short read: %d < %d", meta.ID, len(data), meta.Size)
	}
	return data[:meta.Size], nil
}

func (r *Reader) cacheSST(ctx context.Context, meta *SSTMeta, path string) (err error) {
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

func (r *Reader) ensureSSTCached(ctx context.Context, meta *SSTMeta, path string) error {
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

func (r *Reader) cacheSSTStream(ctx context.Context, cache diskcache.FileBackedCache, meta *SSTMeta, path string) (err error) {
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

	needHash := meta != nil && (r.verifySST || r.verifier != nil)
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

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close sst %s: %w", path, err)
	}

	if needHash {
		var sum [32]byte
		copy(sum[:], hasher.Sum(nil))
		if err := r.validateSSTHash(*meta, sum); err != nil {
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

func (r *Reader) SSTCacheStats() SSTCacheStats {
	return r.sstCache.Stats()
}

func (r *Reader) ManifestLogCacheStats() cachestore.ManifestLogCacheStats {
	if cs, ok := r.manifestStore.Storage().(*cachestore.CachingStorage); ok {
		return cs.CacheStats()
	}
	return cachestore.ManifestLogCacheStats{}
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
	reader    *Reader
	ctx       context.Context
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
	r.mu.RLock()
	m := r.manifest
	r.mu.RUnlock()

	if m == nil {
		return nil, errors.New("manifest not loaded")
	}

	var allIters []sstable.Iterator
	upper := opts.MaxKey
	if len(opts.MaxKey) > 0 {
		upper = incrementKey(opts.MaxKey)
	}

	cleanup := func() {
		for _, it := range allIters {
			_ = it.Close()
		}
	}

	for _, sst := range m.L0SSTs {
		if !internal.OverlapsRange(sst.MinKey, sst.MaxKey, opts.MinKey, opts.MaxKey) {
			continue
		}
		_, iter, err := r.openSSTIterBounded(ctx, sst, opts.MinKey, upper)
		if err != nil {
			cleanup()
			return nil, err
		}
		allIters = append(allIters, iter)
	}

	for _, sr := range m.SortedRuns {
		overlapping := sr.OverlappingSSTs(opts.MinKey, opts.MaxKey)
		for _, sst := range overlapping {
			_, iter, err := r.openSSTIterBounded(ctx, sst, opts.MinKey, upper)
			if err != nil {
				cleanup()
				return nil, err
			}
			allIters = append(allIters, iter)
		}
	}

	if len(allIters) == 0 {

		return &Iterator{
			reader: r,
			ctx:    ctx,
			minKey: opts.MinKey,
			maxKey: opts.MaxKey,
			nowMs:  time.Now().UnixMilli(),
			closed: false,
		}, nil
	}

	return &Iterator{
		reader:    r,
		ctx:       ctx,
		minKey:    opts.MinKey,
		maxKey:    opts.MaxKey,
		nowMs:     time.Now().UnixMilli(),
		mergeIter: newMergeIterator(allIters),
		sstIters:  allIters,
		closed:    false,
	}, nil
}

func (it *Iterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	if it.mergeIter == nil {
		return false
	}

	for {

		if err := it.ctx.Err(); err != nil {
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
	if it.current == nil {
		return nil
	}
	return it.current.key
}

func (it *Iterator) Value() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.value
}

func (it *Iterator) Valid() bool {
	return it.current != nil && !it.closed && it.err == nil
}

func (it *Iterator) Err() error {
	if it.err != nil {
		return it.err
	}
	if it.mergeIter != nil {
		return it.mergeIter.Err()
	}
	return nil
}

func (it *Iterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.current = nil

	for _, iter := range it.sstIters {
		_ = iter.Close()
	}
	it.sstIters = nil
	it.mergeIter = nil

	return nil
}

func (it *Iterator) SeekGE(target []byte) bool {
	if it.closed || it.err != nil {
		return false
	}
	it.current = nil
	if it.mergeIter == nil {
		return false
	}

	it.mergeIter.seekGE(target)
	if it.mergeIter.err != nil {
		return false
	}
	return it.Next()
}
