package isledb

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/cockroachdb/pebble/v2/objstorage"
)

// sstRangeReadable serves Pebble's reads of one SST from object storage by
// byte range. Decoded blocks are cached above it, by Pebble's block cache, so
// a read here is always a request: exact for a lookup's block, or ahead of a
// scan; see sstRangeReadHandle.
type sstRangeReadable struct {
	store *blobstore.Store
	path  string
	sstID string
	size  int64
	// metaLoads coalesces concurrent fetches of the metadata region.
	metaLoads *coalescedLoadGroup
	m         *ReaderMetrics

	// metaOffset, when positive, marks where the SST's trailing metadata
	// begins. Reads inside [metaOffset, size) are served from that whole
	// region, fetched once.
	metaOffset int64

	// metaMu guards metaBytes, the region retained for this open SST.
	metaMu    sync.Mutex
	metaBytes []byte

	// metaCache, when set, holds metadata regions across opens under their
	// own budget.
	metaCache *sstMetaCache

	// aheadMin and aheadMax, when positive, bound scan read-ahead: once an
	// iterator reads data blocks back to back, its read handle fetches the
	// data ahead into a buffer of its own, starting with aheadMin bytes and
	// doubling on each further fetch up to aheadMax. Every fetch starts and
	// ends on a multiple of aheadMin; see sstRangeReadHandle.
	aheadMin int64
	aheadMax int64
}

func newSSTRangeReadable(
	store *blobstore.Store,
	path, sstID string,
	size int64,
	metaLoads *coalescedLoadGroup,
	metrics *ReaderMetrics,
) *sstRangeReadable {
	return &sstRangeReadable{
		store:     store,
		path:      path,
		sstID:     sstID,
		size:      size,
		metaLoads: metaLoads,
		m:         metrics,
	}
}

// useMetaRegion enables whole-region metadata reads for an SST whose writer
// recorded where its metadata begins. Offsets outside the SST leave the
// readable unchanged. The region is fetched in one request whatever its size;
// a region larger than the whole metadata cache is fetched on every open.
func (r *sstRangeReadable) useMetaRegion(offset int64) {
	if offset <= 0 || offset >= r.size {
		return
	}
	r.metaOffset = offset
}

// useMetaCache makes metadata regions come from, and go to, cache.
func (r *sstRangeReadable) useMetaCache(cache *sstMetaCache) {
	r.metaCache = cache
}

// useReadAhead enables read-ahead for iterators that read data blocks
// sequentially, growing from minSize to maxSize bytes. maxSize is rounded down
// to a multiple of minSize, and at least minSize, so every read-ahead stays
// aligned. A minSize of zero or less keeps exact block reads for every
// iterator.
func (r *sstRangeReadable) useReadAhead(minSize, maxSize int64) {
	if minSize > 0 {
		r.aheadMin = minSize
		r.aheadMax = max(maxSize/minSize*minSize, minSize)
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

// read fetches one exact range. It returns a fresh slice the caller may keep.
func (r *sstRangeReadable) read(ctx context.Context, off int64, length int) ([]byte, error) {
	if off < 0 || off > r.size || int64(length) > r.size-off {
		return nil, io.ErrUnexpectedEOF
	}
	return r.readRange(ctx, off, length)
}

// dataEnd is where the data region read-ahead covers ends: the start of the metadata
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

// alignDown and alignUp round off to a multiple of aheadMin.
func (r *sstRangeReadable) alignDown(off int64) int64 {
	return off / r.aheadMin * r.aheadMin
}

func (r *sstRangeReadable) alignUp(off int64) int64 {
	return r.alignDown(off + r.aheadMin - 1)
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
		data, err = r.readRange(ctx, r.metaOffset, int(r.size-r.metaOffset))
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
	if r.metaLoads != nil {
		value, err = r.metaLoads.Do(ctx, r.sstID, fetch)
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
// fetches the exact block, as a point lookup needs. Sequential reads instead
// fetch the block and the data after it into ahead, a buffer only this
// iterator uses, so a scan takes one request per read-ahead rather than per
// block. Pebble reaches the handle only for blocks missing from its block
// cache, and reports the cached ones through RecordCacheHit, which keeps the
// scan's position.
//
// The read-ahead starts small, so a short scan fetches little beyond what it
// reads, and doubles with each fetch of a continuing scan, so a long scan
// needs few requests. A read elsewhere starts it small again.
type sstRangeReadHandle struct {
	readable       *sstRangeReadable
	readBeforeSize int64
	buffer         []byte
	bufferOffset   int64

	// nextOff is where the previous data read ended, or -1.
	nextOff     int64
	ahead       []byte
	aheadOffset int64
	// aheadSize is the size of the last read-ahead of the current scan, or
	// zero before its first.
	aheadSize int64
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
	if r.aheadMin > 0 && r.inDataRegion(off, len(p)) {
		sequential := off == h.nextOff
		h.nextOff = off + int64(len(p))
		if !sequential {
			h.aheadSize = 0
		}
		if h.aheadContains(off, len(p)) {
			copy(p, h.ahead[off-h.aheadOffset:])
			return nil
		}
		if sequential {
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

// readAhead fetches [off, off+len(p)) and the data after it into this
// handle's buffer with one request: the next read-ahead size from the aligned
// start, extended to cover the whole block, rounded up to the alignment and
// cut at the data region's end. When the block starts inside the buffer and
// runs past it, the fetch starts where the buffer ends.
func (h *sstRangeReadHandle) readAhead(ctx context.Context, p []byte, off int64) error {
	r := h.readable
	h.aheadSize = min(max(2*h.aheadSize, r.aheadMin), r.aheadMax)
	end := off + int64(len(p))
	fetchStart := r.alignDown(off)
	keep := []byte(nil)
	if aheadEnd := h.aheadOffset + int64(len(h.ahead)); len(h.ahead) > 0 &&
		off >= h.aheadOffset && off < aheadEnd {
		keep = h.ahead[off-h.aheadOffset:]
		fetchStart = aheadEnd
	}
	fetchEnd := min(r.alignUp(max(end, fetchStart+h.aheadSize)), r.dataEnd())
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
