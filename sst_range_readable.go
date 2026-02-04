package isledb

import (
	"context"
	"io"

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
	rh    objstorage.NoopReadHandle
}

func newSSTRangeReadable(store *blobstore.Store, path, sstID string, size int64, cache *ristretto.Cache[string, []byte]) *sstRangeReadable {
	r := &sstRangeReadable{
		store: store,
		path:  path,
		sstID: sstID,
		size:  size,
		cache: cache,
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
			copy(p, cached)
			return nil
		}
	}

	reader, err := r.store.ReadRangeStream(ctx, r.path, off, int64(len(p)))
	if err != nil {
		return err
	}
	data, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if len(data) != len(p) {
		return io.ErrUnexpectedEOF
	}

	if r.cache != nil {
		r.cache.Set(key, data, int64(len(data)))
	}
	copy(p, data)
	return nil
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
