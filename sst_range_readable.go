package isledb

import (
	"context"
	"io"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/dgraph-io/ristretto/v2"
	"golang.org/x/sync/singleflight"
)

type sstRangeReadable struct {
	store *blobstore.Store
	path  string
	sstID string
	size  int64
	cache *ristretto.Cache[string, []byte]
	loads *singleflight.Group
	m     *ReaderMetrics
	rh    objstorage.NoopReadHandle
}

func newSSTRangeReadable(
	store *blobstore.Store,
	path, sstID string,
	size int64,
	cache *ristretto.Cache[string, []byte],
	loads *singleflight.Group,
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
	r.rh = objstorage.MakeNoopReadHandle(r)
	return r
}

func (r *sstRangeReadable) ReadAt(ctx context.Context, p []byte, off int64) error {
	if off < 0 || off+int64(len(p)) > r.size {
		return io.ErrUnexpectedEOF
	}

	var key string
	if r.cache != nil {
		key = blockCacheKey(r.sstID, off, len(p))
		if cached, ok := r.cache.Get(key); ok {
			r.m.ObserveSSTRangeBlockCacheLookup(true)
			copy(p, cached)
			return nil
		}
		r.m.ObserveSSTRangeBlockCacheLookup(false)
	}

	if r.cache != nil && r.loads != nil {
		result := r.loads.DoChan(key, func() (any, error) {
			// A preceding load may have filled the cache after this caller's
			// first lookup but before it joined the singleflight group.
			if cached, ok := r.cache.Get(key); ok {
				return cached, nil
			}
			data, err := r.readRange(ctx, off, len(p))
			if err != nil {
				return nil, err
			}
			r.cache.Set(key, data, int64(len(data)))
			return data, nil
		})

		select {
		case <-ctx.Done():
			return ctx.Err()
		case loaded := <-result:
			if loaded.Err != nil {
				return loaded.Err
			}
			copy(p, loaded.Val.([]byte))
			return nil
		}
	}

	data, err := r.readRange(ctx, off, len(p))
	if err != nil {
		return err
	}
	if r.cache != nil {
		r.cache.Set(key, data, int64(len(data)))
	}
	copy(p, data)
	return nil
}

func (r *sstRangeReadable) readRange(ctx context.Context, off int64, length int) ([]byte, error) {
	start := time.Now()
	reader, err := r.store.ReadRangeStream(ctx, r.path, off, int64(length))
	if err != nil {
		r.m.ObserveSSTRangeRead(time.Since(start), 0, err)
		return nil, err
	}
	defer reader.Close()

	data := make([]byte, length)
	n, err := io.ReadFull(reader, data)
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

func (r *sstRangeReadable) NewReadHandle(_ objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &r.rh
}
