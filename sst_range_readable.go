package isledb

import (
	"context"
	"errors"
	"io"
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
}

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

func (r *sstRangeReadable) ReadAt(ctx context.Context, p []byte, off int64) error {
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

// Close is a no-op because sstRangeReadable does not hold open resources.
func (r *sstRangeReadable) Close() error {
	return nil
}

func (r *sstRangeReadable) Size() int64 {
	return r.size
}

func (r *sstRangeReadable) NewReadHandle(requested objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &sstRangeReadHandle{
		readable:       r,
		readBeforeSize: rangeReadBeforeSize(r.size, requested),
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

// sstRangeReadHandle retains the extra bytes fetched before its first read so
// later related Pebble metadata reads can be served without another range GET.
// Pebble does not call a ReadHandle concurrently.
type sstRangeReadHandle struct {
	readable       *sstRangeReadable
	readBeforeSize int64
	buffer         []byte
	bufferOffset   int64
}

func (h *sstRangeReadHandle) ReadAt(ctx context.Context, p []byte, off int64) error {
	if h.readable == nil {
		return io.ErrClosedPipe
	}
	if h.bufferContains(off, len(p)) {
		copy(p, h.buffer[off-h.bufferOffset:])
		return nil
	}

	readBeforeSize := h.readBeforeSize
	h.readBeforeSize = 0
	if readBeforeSize > int64(len(p)) {
		extra := min(readBeforeSize-int64(len(p)), off)
		if extra > 0 {
			h.bufferOffset = off - extra
			var err error
			h.buffer, err = h.readable.read(ctx, h.bufferOffset, int(int64(len(p))+extra))
			if err != nil {
				h.buffer = nil
				return err
			}
			copy(p, h.buffer[extra:])
			return nil
		}
	}
	return h.readable.ReadAt(ctx, p, off)
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
	return nil
}

func (*sstRangeReadHandle) SetupForCompaction() {}

func (h *sstRangeReadHandle) RecordCacheHit(_ context.Context, _, _ int64) {
	// Match Pebble's remote readable: if the first block was already cached,
	// do not over-read on a later miss from the same handle.
	h.readBeforeSize = 0
}
