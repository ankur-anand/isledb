package isledb

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/dgraph-io/ristretto/v2"
)

type sstRangeReadable struct {
	store *blobstore.Store
	path  string
	sstID string
	size  int64
	cache *ristretto.Cache[string, []byte]
	loads *coalescedLoadGroup
	m     *ReaderMetrics

	// metaOffset, when positive, marks where the SST's trailing metadata
	// begins. Reads inside [metaOffset, size) are served from that whole
	// region, fetched once and cached under one key.
	metaOffset int64

	// metaMu guards metaBytes, the region retained for this open SST. The
	// block cache applies sets asynchronously, so relying on it alone lets
	// Pebble's next metadata read miss and fetch the region again.
	metaMu    sync.Mutex
	metaBytes []byte

	// metaCache, when set, holds metadata regions across opens under their
	// own budget, so data reads cannot evict them.
	metaCache *sstMetaCache

	// chunkSize, when positive, is the read-ahead unit for scans: once an
	// iterator reads data blocks back to back, its read handle fetches the
	// aligned chunks of this many bytes that hold them, into a buffer of its
	// own; see sstRangeReadHandle.
	chunkSize int64
}

// maxSSTMetaRegionBytes bounds the metadata region fetched in one request. A
// larger region falls back to Pebble's read-before hints.
const maxSSTMetaRegionBytes = 4 << 20

func newSSTRangeReadable(
	store *blobstore.Store,
	path, sstID string,
	size int64,
	cache *ristretto.Cache[string, []byte],
	loads *coalescedLoadGroup,
	metrics *ReaderMetrics,
) *sstRangeReadable {
	r := &sstRangeReadable{
		store: store,
		path:  path,
		sstID: sstID,
		size:  size,
		cache: cache,
		loads: loads,
		m:     metrics,
	}
	return r
}

// useMetaRegion enables whole-region metadata reads for an SST whose writer
// recorded where its metadata begins. Offsets outside the SST, or regions too
// large to fetch at once, leave the readable unchanged.
func (r *sstRangeReadable) useMetaRegion(offset int64) {
	if offset <= 0 || offset >= r.size || r.size-offset > maxSSTMetaRegionBytes {
		return
	}
	r.metaOffset = offset
}

// useMetaCache makes metadata regions come from, and go to, cache instead of
// the block cache.
func (r *sstRangeReadable) useMetaCache(cache *sstMetaCache) {
	r.metaCache = cache
}

// useChunks enables read-ahead of aligned chunks of size bytes for iterators
// that read data blocks sequentially. A size of zero or less keeps exact
// block reads for every iterator.
func (r *sstRangeReadable) useChunks(size int64) {
	if size > 0 {
		r.chunkSize = size
	}
}

func (r *sstRangeReadable) ReadAt(ctx context.Context, p []byte, off int64) error {
	if r.inMetaRegion(off, len(p)) {
		region, err := r.metaRegion(ctx)
		if err != nil {
			return err
		}
		copy(p, region[off-r.metaOffset:])
		return nil
	}
	data, err := r.read(ctx, off, len(p))
	if err != nil {
		return err
	}
	copy(p, data)
	return nil
}

// read returns immutable bytes for one exact range. Returning the cached slice
// lets a ReadHandle retain an expanded read-before window without allocating
// and copying it again on every warm lookup.
func (r *sstRangeReadable) read(ctx context.Context, off int64, length int) ([]byte, error) {
	if off < 0 || off > r.size || int64(length) > r.size-off {
		return nil, io.ErrUnexpectedEOF
	}

	var key string
	if r.cache != nil {
		key = blockCacheKey(r.sstID, off, length)
		if cached, ok := r.cache.Get(key); ok {
			r.m.ObserveSSTRangeBlockCacheLookup(true)
			return cached, nil
		}
		r.m.ObserveSSTRangeBlockCacheLookup(false)
	}

	if r.cache != nil && r.loads != nil {
		value, err := r.loads.Do(ctx, key, func(loadCtx context.Context) (any, error) {
			// A preceding load may have filled the cache after this caller's
			// first lookup but before it joined the coalesced load.
			if cached, ok := r.cache.Get(key); ok {
				return cached, nil
			}
			data, err := r.readRange(loadCtx, off, length)
			if err != nil {
				return nil, err
			}
			r.cache.Set(key, data, int64(len(data)))
			return data, nil
		})
		if err != nil {
			return nil, err
		}
		return value.([]byte), nil
	}

	data, err := r.readRange(ctx, off, length)
	if err != nil {
		return nil, err
	}
	if r.cache != nil {
		r.cache.Set(key, data, int64(len(data)))
	}
	return data, nil
}

// dataEnd is where the chunked data region ends: the start of the metadata
// region when known, otherwise the end of the SST.
func (r *sstRangeReadable) dataEnd() int64 {
	if r.metaOffset > 0 {
		return r.metaOffset
	}
	return r.size
}

// inDataRegion reports whether [off, off+length) lies within the data region.
func (r *sstRangeReadable) inDataRegion(off int64, length int) bool {
	return length > 0 && off >= 0 && off+int64(length) <= r.dataEnd()
}

// chunkBounds returns the byte range of chunk k, cut at the data region's end.
func (r *sstRangeReadable) chunkBounds(k int64) (int64, int64) {
	return k * r.chunkSize, min((k+1)*r.chunkSize, r.dataEnd())
}

// cachedBlock returns an exact block from the shared block cache without
// fetching or caching anything.
func (r *sstRangeReadable) cachedBlock(off int64, length int) ([]byte, bool) {
	if r.cache == nil {
		return nil, false
	}
	data, ok := r.cache.Get(blockCacheKey(r.sstID, off, length))
	r.m.ObserveSSTRangeBlockCacheLookup(ok)
	return data, ok
}

func (r *sstRangeReadable) readRange(ctx context.Context, off int64, length int) ([]byte, error) {
	start := time.Now()
	reader, err := r.store.ReadRangeStream(ctx, r.path, off, int64(length))
	if err != nil {
		r.m.ObserveSSTRangeRead(time.Since(start), 0, err)
		return nil, err
	}

	data := make([]byte, length)
	n, readErr := io.ReadFull(reader, data)
	err = errors.Join(readErr, reader.Close())
	r.m.ObserveSSTRangeRead(time.Since(start), int64(n), err)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Close releases the retained metadata region; the readable holds no other
// resources.
func (r *sstRangeReadable) Close() error {
	r.metaMu.Lock()
	r.metaBytes = nil
	r.metaMu.Unlock()
	return nil
}

func (r *sstRangeReadable) Size() int64 {
	return r.size
}

func (r *sstRangeReadable) inMetaRegion(off int64, length int) bool {
	return r.metaOffset > 0 && off >= r.metaOffset && off+int64(length) <= r.size
}

func (r *sstRangeReadable) metaRegion(ctx context.Context) ([]byte, error) {
	r.metaMu.Lock()
	defer r.metaMu.Unlock()
	if r.metaBytes != nil {
		return r.metaBytes, nil
	}
	var data []byte
	var err error
	if r.metaCache != nil {
		data, err = r.loadMetaRegion(ctx)
	} else {
		data, err = r.read(ctx, r.metaOffset, int(r.size-r.metaOffset))
	}
	if err != nil {
		return nil, err
	}
	r.metaBytes = data
	return data, nil
}

// loadMetaRegion returns the metadata region from the metadata cache, or
// fetches it with one request shared by concurrent opens and caches it.
func (r *sstRangeReadable) loadMetaRegion(ctx context.Context) ([]byte, error) {
	length := r.size - r.metaOffset
	if region, ok := r.metaCache.get(r.sstID, length); ok {
		return region, nil
	}
	fetch := func(ctx context.Context) (any, error) {
		if region, ok := r.metaCache.peek(r.sstID, length); ok {
			return region, nil
		}
		data, err := r.readRange(ctx, r.metaOffset, int(length))
		if err != nil {
			return nil, err
		}
		r.metaCache.put(r.sstID, data)
		return data, nil
	}
	var value any
	var err error
	if r.loads != nil {
		value, err = r.loads.Do(ctx, r.sstID+":meta", fetch)
	} else {
		value, err = fetch(ctx)
	}
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
}

func (r *sstRangeReadable) NewReadHandle(requested objstorage.ReadBeforeSize) objstorage.ReadHandle {
	readBeforeSize := rangeReadBeforeSize(r.size, requested)
	if r.metaOffset > 0 {
		// Pebble asks for read-before only on its metadata reads, and the
		// metadata region already covers those.
		readBeforeSize = 0
	}
	return &sstRangeReadHandle{
		readable:       r,
		readBeforeSize: readBeforeSize,
		nextOff:        -1,
	}
}

// rangeReadBeforeSize bounds Pebble's read-before hint according to the
// logical SST size. The buckets are deliberately conservative relative to the
// metadata spans measured by BenchmarkFakeS3_KVReaderGet_RangeReadRequestShape
// and capped at Pebble's 512 KiB index/filter hint.
func rangeReadBeforeSize(sstSize int64, requested objstorage.ReadBeforeSize) int64 {
	if sstSize <= 0 || requested <= 0 {
		return 0
	}

	var window int64
	switch {
	case sstSize <= 4<<20:
		window = 32 << 10
	case sstSize <= 8<<20:
		window = 64 << 10
	case sstSize <= 16<<20:
		window = 128 << 10
	case sstSize <= 32<<20:
		window = 256 << 10
	default:
		window = 512 << 10
	}
	window = min(window, sstSize)
	return min(window, int64(requested))
}

// sstRangeReadHandle serves one iterator's reads. Pebble gives each iterator
// its own handle and does not call a handle concurrently.
//
// It retains the extra bytes fetched before its first read, so related
// metadata reads of SSTs without a MetaOffset need no further request.
//
// It also adapts data reads to the iterator's access pattern. Data blocks are
// stored back to back, so a read that starts where the previous one ended
// means the iterator is scanning. The first read, and any read elsewhere,
// fetches the exact block through the shared block cache, as a point lookup
// needs. Sequential reads instead fetch the aligned chunks holding the block
// into ahead, a buffer only this iterator uses: a scan takes one request per
// chunk rather than per block, and its chunks, which it reads once, never
// displace the blocks point lookups reuse. A scan still uses a block already
// in the shared cache.
type sstRangeReadHandle struct {
	readable       *sstRangeReadable
	readBeforeSize int64
	buffer         []byte
	bufferOffset   int64

	// nextOff is where the previous data read ended, or -1.
	nextOff     int64
	ahead       []byte
	aheadOffset int64
}

func (h *sstRangeReadHandle) ReadAt(ctx context.Context, p []byte, off int64) error {
	if h.readable == nil {
		return io.ErrClosedPipe
	}
	if h.bufferContains(off, len(p)) {
		copy(p, h.buffer[off-h.bufferOffset:])
		return nil
	}
	r := h.readable
	if r.chunkSize > 0 && r.inDataRegion(off, len(p)) {
		sequential := off == h.nextOff
		h.nextOff = off + int64(len(p))
		if h.aheadContains(off, len(p)) {
			copy(p, h.ahead[off-h.aheadOffset:])
			return nil
		}
		if sequential {
			if data, ok := r.cachedBlock(off, len(p)); ok {
				copy(p, data)
				return nil
			}
			return h.readAhead(ctx, p, off)
		}
	}

	readBeforeSize := h.readBeforeSize
	h.readBeforeSize = 0
	if readBeforeSize > int64(len(p)) {
		extra := min(readBeforeSize-int64(len(p)), off)
		if extra > 0 {
			h.bufferOffset = off - extra
			var err error
			h.buffer, err = r.read(ctx, h.bufferOffset, int(int64(len(p))+extra))
			if err != nil {
				h.buffer = nil
				return err
			}
			copy(p, h.buffer[extra:])
			return nil
		}
	}
	return r.ReadAt(ctx, p, off)
}

// readAhead fetches the aligned chunks holding [off, off+len(p)) into this
// handle's buffer with one request. When the block starts inside the buffer
// and runs past it, only the chunks after the buffer are fetched.
func (h *sstRangeReadHandle) readAhead(ctx context.Context, p []byte, off int64) error {
	r := h.readable
	end := off + int64(len(p))
	_, fetchEnd := r.chunkBounds((end - 1) / r.chunkSize)
	fetchStart, _ := r.chunkBounds(off / r.chunkSize)
	keep := []byte(nil)
	if aheadEnd := h.aheadOffset + int64(len(h.ahead)); len(h.ahead) > 0 &&
		off >= h.aheadOffset && off < aheadEnd {
		keep = h.ahead[off-h.aheadOffset:]
		fetchStart = aheadEnd
	}
	data, err := r.readRange(ctx, fetchStart, int(fetchEnd-fetchStart))
	if err != nil {
		return err
	}
	if keep != nil {
		data = append(append(make([]byte, 0, len(keep)+len(data)), keep...), data...)
		fetchStart = off
	}
	h.ahead, h.aheadOffset = data, fetchStart
	copy(p, h.ahead[off-h.aheadOffset:])
	return nil
}

func (h *sstRangeReadHandle) aheadContains(off int64, length int) bool {
	return len(h.ahead) > 0 && off >= h.aheadOffset &&
		off+int64(length) <= h.aheadOffset+int64(len(h.ahead))
}

func (h *sstRangeReadHandle) bufferContains(off int64, length int) bool {
	if len(h.buffer) == 0 || off < h.bufferOffset {
		return false
	}
	end := off + int64(length)
	return end >= off && end <= h.bufferOffset+int64(len(h.buffer))
}

func (h *sstRangeReadHandle) Close() error {
	h.readable = nil
	h.buffer = nil
	h.ahead = nil
	return nil
}

func (*sstRangeReadHandle) SetupForCompaction() {}

func (h *sstRangeReadHandle) RecordCacheHit(_ context.Context, off, length int64) {
	// Match Pebble's remote readable: if the first block was already cached,
	// do not over-read on a later miss from the same handle.
	h.readBeforeSize = 0
	// A block Pebble served from its own cache still advances a scan, so the
	// next miss is recognised as sequential.
	if h.readable != nil && h.readable.inDataRegion(off, int(length)) {
		h.nextOff = off + length
	}
}
