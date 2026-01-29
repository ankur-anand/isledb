package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
)

type ReaderOptions struct {
	VLogCache VLogCache
}

type Reader struct {
	store         *blobstore.Store
	manifestStore *manifest.Store
	manifest      *Manifest
	vlogFetcher   *VLogFetcher
}

type KV struct {
	Key   []byte
	Value []byte
}

func NewReader(ctx context.Context, store *blobstore.Store, opts ReaderOptions) (*Reader, error) {
	ms := manifest.NewStore(store)
	m, err := ms.Replay(ctx)
	if err != nil {
		return nil, err
	}
	return &Reader{
		store:         store,
		manifestStore: ms,
		manifest:      m,
		vlogFetcher:   NewVLogFetcherWithCache(store, opts.VLogCache),
	}, nil
}

func (r *Reader) Refresh(ctx context.Context) error {
	m, err := r.manifestStore.Replay(ctx)
	if err != nil {
		return err
	}
	r.manifest = m
	return nil
}

func (r *Reader) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, errors.New("empty key")
	}
	if r.manifest == nil {
		return nil, false, errors.New("manifest not loaded")
	}

	// L0: newest first
	if value, found, deleted, err := r.getFromLevel(ctx, 0, key); err != nil {
		return nil, false, err
	} else if found {
		if deleted {
			return nil, false, nil
		}
		return value, true, nil
	}

	// L1+
	maxLevel := r.manifest.MaxLevel()
	for level := 1; level <= maxLevel; level++ {
		value, found, deleted, err := r.getFromLevel(ctx, level, key)
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
	if r.manifest == nil {
		return nil, errors.New("manifest not loaded")
	}

	ssts := r.collectOverlapping(minKey, maxKey)
	if len(ssts) == 0 {
		return nil, nil
	}

	readers := make([]*sstable.Reader, 0, len(ssts))
	iters := make([]sstable.Iterator, 0, len(ssts))
	for _, sst := range ssts {
		reader, iter, err := r.openSSTIter(ctx, sst)
		if err != nil {
			for _, it := range iters {
				_ = it.Close()
			}
			for _, rd := range readers {
				_ = rd.Close()
			}
			return nil, err
		}
		readers = append(readers, reader)
		iters = append(iters, iter)
	}
	// imp: merge iterator closes the iterator
	// pebble doc: iterator should be closed only once.
	defer func() {
		for _, rd := range readers {
			_ = rd.Close()
		}
	}()

	if len(iters) == 0 {
		return nil, nil
	}

	mergeIter := NewMergeIterator(iters)
	defer mergeIter.Close()

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

		if entry.Kind == OpDelete {
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

func (r *Reader) collectOverlapping(minKey, maxKey []byte) []SSTMeta {
	var out []SSTMeta
	maxLevel := r.manifest.MaxLevel()
	for level := 0; level <= maxLevel; level++ {
		levelSSTs := r.manifest.OverlappingSSTs(level, minKey, maxKey)
		out = append(out, levelSSTs...)
	}
	return out
}

func (r *Reader) getFromLevel(ctx context.Context, level int, key []byte) ([]byte, bool, bool, error) {
	ssts := r.manifest.Level(level)
	if len(ssts) == 0 {
		return nil, false, false, nil
	}

	for _, sst := range ssts {
		if len(sst.MinKey) > 0 && bytes.Compare(key, sst.MinKey) < 0 {
			if level != 0 {
				break
			}
			continue
		}
		if len(sst.MaxKey) > 0 && bytes.Compare(key, sst.MaxKey) > 0 {
			continue
		}

		value, found, deleted, err := r.getFromSST(ctx, sst, key)
		if err != nil {
			return nil, false, false, err
		}
		if found {
			return value, true, deleted, nil
		}

		if level != 0 {
			break
		}
	}

	return nil, false, false, nil
}

func (r *Reader) getFromSST(ctx context.Context, sstMeta SSTMeta, key []byte) ([]byte, bool, bool, error) {
	reader, iter, err := r.openSSTIter(ctx, sstMeta)
	if err != nil {
		return nil, false, false, err
	}
	defer iter.Close()
	defer reader.Close()

	for kv := iter.Next(); kv != nil; kv = iter.Next() {
		cmp := bytes.Compare(kv.K.UserKey, key)
		if cmp < 0 {
			continue
		}
		if cmp > 0 {
			break
		}

		raw, _, err := kv.V.Value(nil)
		if err != nil {
			return nil, false, false, err
		}
		decoded, err := DecodeKeyEntry(kv.K.UserKey, raw)
		if err != nil {
			return nil, false, false, err
		}

		if decoded.Kind == OpDelete {
			return nil, true, true, nil
		}
		if decoded.Inline {
			return append([]byte(nil), decoded.Value...), true, false, nil
		}

		ptr := VLogPointer{
			VLogID:   decoded.VLogID,
			Offset:   decoded.VOffset,
			Length:   decoded.VLength,
			Checksum: decoded.VChecksum,
		}
		value, err := r.vlogFetcher.GetValue(ctx, ptr)
		if err != nil {
			return nil, false, false, err
		}
		return value, true, false, nil
	}
	if err := iter.Error(); err != nil {
		return nil, false, false, err
	}
	return nil, false, false, nil
}

func (r *Reader) entryValue(ctx context.Context, entry CompactionEntry) ([]byte, error) {
	if entry.Inline {
		return append([]byte(nil), entry.Value...), nil
	}
	ptr := VLogPointer{
		VLogID:   entry.VLogID,
		Offset:   entry.VOffset,
		Length:   entry.VLength,
		Checksum: entry.VChecksum,
	}
	return r.vlogFetcher.GetValue(ctx, ptr)
}

func (r *Reader) openSSTIter(ctx context.Context, sstMeta SSTMeta) (*sstable.Reader, sstable.Iterator, error) {
	path := r.store.SSTPath(sstMeta.ID)
	data, _, err := r.store.Read(ctx, path)
	if err != nil {
		return nil, nil, fmt.Errorf("read sst %s: %w", sstMeta.ID, err)
	}

	reader, err := sstable.NewReader(ctx, newSSTReadable(data), sstable.ReaderOptions{})
	if err != nil {
		return nil, nil, err
	}
	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		_ = reader.Close()
		return nil, nil, err
	}
	return reader, iter, nil
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

func (m *sstReadable) NewReadHandle(_ objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &m.rh
}
