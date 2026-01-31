package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/cachestore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
)

type Reader struct {
	store          *blobstore.Store
	manifestStore  *manifest.Store
	sstCache       SSTCache
	sstReaderCache SSTReaderCache

	blobStorage *internal.BlobStorage
	blobCache   internal.BlobCache
	valueConfig config.ValueStorageConfig

	mu       sync.RWMutex
	manifest *Manifest
}

type KV struct {
	Key   []byte
	Value []byte
}

func NewReader(ctx context.Context, store *blobstore.Store, opts ReaderOptions) (*Reader, error) {
	ms := newManifestStoreWithCache(store, &opts)
	m, err := ms.Replay(ctx)
	if err != nil {
		return nil, err
	}

	var sstCache SSTCache
	if opts.SSTCache != nil {
		sstCache = opts.SSTCache
	} else if opts.SSTCacheSize > 0 {
		sstCache = NewLRUSSTCache(opts.SSTCacheSize)
	} else {
		sstCache = NewLRUSSTCache(DefaultSSTCacheSize)
	}

	var sstReaderCache SSTReaderCache
	if opts.SSTReaderCache != nil {
		sstReaderCache = opts.SSTReaderCache
	} else if opts.SSTReaderCacheSize > 0 {
		sstReaderCache = NewLRUSSTReaderCache(opts.SSTReaderCacheSize)
	} else {
		sstReaderCache = NewLRUSSTReaderCache(DefaultSSTReaderCacheSize)
	}

	var blobCache internal.BlobCache
	if opts.BlobCache != nil {
		blobCache = opts.BlobCache
	} else {
		cacheSize := opts.BlobCacheSize
		if cacheSize == 0 {
			cacheSize = internal.DefaultBlobCacheSize
		}
		itemSize := opts.BlobCacheItemSize
		if itemSize == 0 {
			itemSize = internal.DefaultBlobCacheMaxItemSize
		}
		blobCache = internal.NewLRUBlobCache(cacheSize, itemSize)
	}

	valueConfig := opts.ValueStorageConfig
	if valueConfig.BlobThreshold == 0 {
		valueConfig = config.DefaultValueStorageConfig()
	}

	return &Reader{
		store:          store,
		manifestStore:  ms,
		manifest:       m,
		sstCache:       sstCache,
		sstReaderCache: sstReaderCache,
		blobStorage:    internal.NewBlobStorage(store, valueConfig),
		blobCache:      blobCache,
		valueConfig:    valueConfig,
	}, nil
}

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

			r.sstReaderCache.Remove(path)

			r.sstCache.Remove(path)
		}
	}
}

func (r *Reader) Close() error {

	if r.sstReaderCache != nil {
		r.sstReaderCache.Clear()
	}

	if r.sstCache != nil {
		r.sstCache.Clear()
	}
	return nil
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

func (r *Reader) Scan(ctx context.Context, minKey, maxKey []byte) ([]KV, error) {
	r.mu.RLock()
	m := r.manifest
	r.mu.RUnlock()

	if m == nil {
		return nil, errors.New("manifest not loaded")
	}

	var allIters []sstable.Iterator

	cleanup := func() {
		for _, it := range allIters {
			_ = it.Close()
		}

	}

	for _, sst := range m.L0SSTs {
		if !overlapsRange(sst.MinKey, sst.MaxKey, minKey, maxKey) {
			continue
		}
		_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, nil)
		if err != nil {
			cleanup()
			return nil, err
		}
		allIters = append(allIters, iter)
	}

	for _, sr := range m.SortedRuns {
		overlapping := sr.OverlappingSSTs(minKey, maxKey)
		for _, sst := range overlapping {
			_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, nil)
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

	mergeIter := NewMergeIterator(allIters)

	nowMs := time.Now().UnixMilli()
	var out []KV
	for mergeIter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		entry, err := mergeIter.Entry()
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

	cleanup := func() {
		for _, it := range allIters {
			_ = it.Close()
		}
	}

	for _, sst := range m.L0SSTs {
		if !overlapsRange(sst.MinKey, sst.MaxKey, minKey, maxKey) {
			continue
		}
		_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, nil)
		if err != nil {
			cleanup()
			return nil, err
		}
		allIters = append(allIters, iter)
	}

	for _, sr := range m.SortedRuns {
		overlapping := sr.OverlappingSSTs(minKey, maxKey)
		for _, sst := range overlapping {
			_, iter, err := r.openSSTIterBounded(ctx, sst, minKey, nil)
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

	mergeIter := NewMergeIterator(allIters)

	nowMs := time.Now().UnixMilli()
	var out []KV
	for mergeIter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if limit > 0 && len(out) >= limit {
			break
		}

		entry, err := mergeIter.Entry()
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

	if reader, ok := r.sstReaderCache.Get(path); ok {
		iter, err := reader.NewIter(sstable.NoTransforms, lower, upper, sstable.AssertNoBlobHandles)
		if err != nil {
			return nil, nil, err
		}
		return reader, iter, nil
	}

	var data []byte
	var err error
	if cached, ok := r.sstCache.Get(path); ok {
		data = cached
	} else {

		data, _, err = r.store.Read(ctx, path)
		if err != nil {
			return nil, nil, fmt.Errorf("read sst %s: %w", sstMeta.ID, err)
		}

		r.sstCache.Set(path, data)
	}

	reader, err := sstable.NewReader(ctx, newSSTReadable(data), sstable.ReaderOptions{})
	if err != nil {
		return nil, nil, err
	}

	iter, err := reader.NewIter(sstable.NoTransforms, lower, upper, sstable.AssertNoBlobHandles)
	if err != nil {
		_ = reader.Close()
		return nil, nil, err
	}

	r.sstReaderCache.Set(path, reader, data)

	return reader, iter, nil
}

func (r *Reader) SSTCacheStats() SSTCacheStats {
	return r.sstCache.Stats()
}

func (r *Reader) SSTReaderCacheStats() SSTReaderCacheStats {
	return r.sstReaderCache.Stats()
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
	mergeIter *KMergeIterator
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

	cleanup := func() {
		for _, it := range allIters {
			_ = it.Close()
		}
	}

	for _, sst := range m.L0SSTs {
		if !overlapsRange(sst.MinKey, sst.MaxKey, opts.MinKey, opts.MaxKey) {
			continue
		}
		_, iter, err := r.openSSTIterBounded(ctx, sst, opts.MinKey, nil)
		if err != nil {
			cleanup()
			return nil, err
		}
		allIters = append(allIters, iter)
	}

	for _, sr := range m.SortedRuns {
		overlapping := sr.OverlappingSSTs(opts.MinKey, opts.MaxKey)
		for _, sst := range overlapping {
			_, iter, err := r.openSSTIterBounded(ctx, sst, opts.MinKey, nil)
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
		mergeIter: NewMergeIterator(allIters),
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

		entry, err := it.mergeIter.Entry()
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
