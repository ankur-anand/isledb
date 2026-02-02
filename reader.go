package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
)

type Reader struct {
	store         *blobstore.Store
	manifestStore *manifest.Store
	sstCache      SSTCache

	blobStorage *internal.BlobStorage
	blobCache   internal.BlobCache
	valueConfig config.ValueStorageConfig
	verifySST   bool
	verifier    SSTHashVerifier

	ownsSSTCache  bool
	ownsBlobCache bool

	mu       sync.RWMutex
	manifest *Manifest
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

	valueConfig := opts.ValueStorageConfig
	if valueConfig.BlobThreshold == 0 {
		valueConfig = config.DefaultValueStorageConfig()
	}

	reader := &Reader{
		store:         store,
		manifestStore: ms,
		manifest:      m,
		sstCache:      sstCache,
		blobStorage:   internal.NewBlobStorage(store, valueConfig),
		blobCache:     blobCache,
		valueConfig:   valueConfig,
		verifySST:     opts.ValidateSSTChecksum,
		verifier:      opts.SSTHashVerifier,
		ownsSSTCache:  ownsSSTCache,
		ownsBlobCache: ownsBlobCache,
	}
	cleanupSSTCache = false
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
func (r *Reader) Refresh(ctx context.Context) error {
	m, err := r.manifestStore.Replay(ctx)
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
func (r *Reader) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
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
		value, found, deleted, err := r.getFromSST(ctx, sst, key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if deleted {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	for _, sr := range m.SortedRuns {

		sst := sr.FindSST(key)
		if sst == nil {
			continue
		}

		value, found, deleted, err := r.getFromSST(ctx, *sst, key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if deleted {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	return nil, false, nil
}

// Scan returns all key-value pairs in the given key range.
func (r *Reader) Scan(ctx context.Context, minKey, maxKey []byte) ([]KV, error) {
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
		if !overlapsRange(sst.MinKey, sst.MaxKey, minKey, maxKey) {
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
	var out []KV
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

func (r *Reader) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error) {
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
		if !overlapsRange(sst.MinKey, sst.MaxKey, minKey, maxKey) {
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
	var out []KV
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

func overlapsRange(aMin, aMax, bMin, bMax []byte) bool {
	if len(aMin) == 0 || len(aMax) == 0 {
		return false
	}
	if len(bMin) == 0 && len(bMax) == 0 {
		return true
	}
	if len(bMin) == 0 {
		return bytes.Compare(aMin, bMax) <= 0
	}
	if len(bMax) == 0 {
		return bytes.Compare(bMin, aMax) <= 0
	}
	return bytes.Compare(aMin, bMax) <= 0 && bytes.Compare(bMin, aMax) <= 0
}

func (r *Reader) getFromSST(ctx context.Context, sstMeta SSTMeta, key []byte) ([]byte, bool, bool, error) {

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

func (r *Reader) entryValue(ctx context.Context, entry internal.CompactionEntry) ([]byte, error) {
	if entry.Inline {
		return append([]byte(nil), entry.Value...), nil
	}

	if entry.HasBlobID() {
		return r.fetchBlob(ctx, entry.BlobID)
	}

	return nil, errors.New("corrupt entry: non-inline, non-blob")
}

func (r *Reader) fetchBlob(ctx context.Context, blobID [32]byte) ([]byte, error) {
	blobIDHex := internal.BlobIDToHex(blobID)

	if r.blobCache != nil {
		if data, ok := r.blobCache.Get(blobIDHex); ok {
			return data, nil
		}
	}

	data, err := r.blobStorage.Read(ctx, blobID)
	if err != nil {
		return nil, err
	}

	if r.blobCache != nil {
		r.blobCache.Set(blobIDHex, data)
	}

	return data, nil
}

func (r *Reader) BlobCacheStats() internal.BlobCacheStats {
	if r.blobCache != nil {
		return r.blobCache.Stats()
	}
	return internal.BlobCacheStats{}
}

func (r *Reader) openSSTIterBounded(ctx context.Context, sstMeta SSTMeta, lower, upper []byte) (*sstable.Reader, sstable.Iterator, error) {
	path := r.store.SSTPath(sstMeta.ID)

	var data []byte
	var err error
	var release func()
	if cached, ok := r.sstCache.Acquire(path); ok {
		data = cached
		release = func() {
			r.sstCache.Release(path)
		}
	} else {
		data, _, err = r.store.Read(ctx, path)
		if err != nil {
			return nil, nil, fmt.Errorf("read sst %s: %w", sstMeta.ID, err)
		}
		if err := r.validateSSTData(sstMeta, data); err != nil {
			return nil, nil, err
		}
		_ = r.sstCache.Set(path, data)
		if cached, ok := r.sstCache.Acquire(path); ok {
			data = cached
			release = func() {
				r.sstCache.Release(path)
			}
		}
	}

	reader, err := sstable.NewReader(ctx, newSSTReadable(data), sstable.ReaderOptions{})
	if err != nil {
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
	if r.verifier != nil && meta.Signature == nil {
		return fmt.Errorf("sst %s: missing signature", meta.ID)
	}

	needHash := r.verifySST || r.verifier != nil
	if !needHash {
		return nil
	}

	sum := sha256.Sum256(data)
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
		if !overlapsRange(sst.MinKey, sst.MaxKey, opts.MinKey, opts.MaxKey) {
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

	for it.Next() {
		if bytes.Compare(it.Key(), target) >= 0 {
			return true
		}
	}
	return false
}
