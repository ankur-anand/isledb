package isledb

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
)

type writer struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        WriterOptions
	valueConfig config.ValueStorageConfig

	mu                      sync.Mutex
	memtable                *internal.Memtable
	memtableInlineThreshold int
	seq                     uint64
	epoch                   uint64
	blobStorage             *internal.BlobStorage

	flushMu     sync.Mutex
	flushTicker *time.Ticker
	stopCh      chan struct{}
	wg          sync.WaitGroup

	fenced     atomic.Bool
	fenceToken *manifest.FenceToken

	closed atomic.Bool
}

func newWriter(ctx context.Context, store *blobstore.Store, opts WriterOptions) (*writer, error) {

	d := DefaultWriterOptions()
	opts.MemtableSize = cmp.Or(opts.MemtableSize, d.MemtableSize)
	opts.FlushInterval = cmp.Or(opts.FlushInterval, d.FlushInterval)
	opts.BloomBitsPerKey = cmp.Or(opts.BloomBitsPerKey, d.BloomBitsPerKey)
	opts.BlockSize = cmp.Or(opts.BlockSize, d.BlockSize)
	opts.Compression = cmp.Or(opts.Compression, d.Compression)

	vd := config.DefaultValueStorageConfig()
	valueConfig := opts.ValueStorage
	valueConfig.BlobThreshold = cmp.Or(valueConfig.BlobThreshold, vd.BlobThreshold)
	valueConfig.MaxKeySize = cmp.Or(valueConfig.MaxKeySize, 64*1024)
	valueConfig.MaxValueSize = cmp.Or(valueConfig.MaxValueSize, 256*1024*1024)

	manifestLog := newManifestStore(store, opts.ManifestStorage)
	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	memtableInlineThreshold := valueConfig.BlobThreshold

	w := &writer{
		store:                   store,
		manifestLog:             manifestLog,
		opts:                    opts,
		valueConfig:             valueConfig,
		memtable:                internal.NewMemtable(opts.MemtableSize*2, memtableInlineThreshold),
		memtableInlineThreshold: memtableInlineThreshold,
		seq:                     m.MaxSeqNum(),
		epoch:                   m.NextEpoch,
		blobStorage:             internal.NewBlobStorage(store, valueConfig),
		stopCh:                  make(chan struct{}),
	}

	if opts.EnableFencing {
		ownerID := opts.OwnerID
		if ownerID == "" {
			ownerID = fmt.Sprintf("writer-%d-%d", time.Now().UnixNano(), m.NextEpoch)
		}
		token, err := manifestLog.ClaimWriter(ctx, ownerID)
		if err != nil {
			return nil, fmt.Errorf("claim writer fence: %w", err)
		}
		w.fenceToken = token
	}

	if opts.FlushInterval > 0 {
		w.flushTicker = time.NewTicker(opts.FlushInterval)
		w.wg.Add(1)
		go w.flushLoop()
	}

	return w, nil
}

func (w *writer) put(key, value []byte) error {
	return w.putWithTTL(key, value, 0)
}

func (w *writer) putWithTTL(key, value []byte, ttl time.Duration) error {
	if w.closed.Load() {
		return errors.New("writer closed")
	}
	if w.fenced.Load() {
		return manifest.ErrFenced
	}

	if len(key) == 0 {
		return errors.New("empty key")
	}
	if len(key) > w.valueConfig.MaxKeySize {
		return fmt.Errorf("key size %d exceeds max %d", len(key), w.valueConfig.MaxKeySize)
	}
	if int64(len(value)) > w.valueConfig.MaxValueSize {
		return fmt.Errorf("value size %d exceeds max %d", len(value), w.valueConfig.MaxValueSize)
	}

	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixMilli()
	}

	if len(value) >= w.valueConfig.BlobThreshold {
		return w.putBlob(key, value, expireAt)
	}
	return w.putInline(key, value, expireAt)
}

func (w *writer) putInline(key, value []byte, expireAt int64) error {
	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.memtable.PutWithTTL(key, value, seq, expireAt)
	size := w.memtable.TotalSize()
	w.mu.Unlock()

	if size >= w.opts.MemtableSize {
		return w.flush(context.Background())
	}
	return nil
}

func (w *writer) putBlob(key, value []byte, expireAt int64) error {
	ctx := context.Background()

	blobID, err := w.blobStorage.Write(ctx, value)
	if err != nil {
		return fmt.Errorf("write blob: %w", err)
	}

	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.memtable.PutBlobRefWithTTL(key, blobID, seq, expireAt)
	size := w.memtable.TotalSize()
	w.mu.Unlock()

	if size >= w.opts.MemtableSize {
		return w.flush(ctx)
	}
	return nil
}

func (w *writer) delete(key []byte) error {
	return w.deleteWithTTL(key, 0)
}

func (w *writer) deleteWithTTL(key []byte, ttl time.Duration) error {
	if w.closed.Load() {
		return errors.New("writer closed")
	}
	if w.fenced.Load() {
		return manifest.ErrFenced
	}

	if len(key) == 0 {
		return errors.New("empty key")
	}
	if len(key) > w.valueConfig.MaxKeySize {
		return fmt.Errorf("key size %d exceeds max %d", len(key), w.valueConfig.MaxKeySize)
	}

	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixMilli()
	}

	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.memtable.DeleteWithTTL(key, seq, expireAt)
	w.mu.Unlock()

	return nil
}

func (w *writer) flush(ctx context.Context) error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if w.fenced.Load() {
		return manifest.ErrFenced
	}

	w.mu.Lock()
	if w.memtable.ApproxSize() == 0 {
		w.mu.Unlock()
		return nil
	}

	oldMemtable := w.memtable
	w.memtable = internal.NewMemtable(w.opts.MemtableSize*2, w.memtableInlineThreshold)
	epoch := w.epoch
	w.epoch++
	w.mu.Unlock()

	sstOpts := SSTWriterOptions{
		BloomBitsPerKey: w.opts.BloomBitsPerKey,
		BlockSize:       w.opts.BlockSize,
		Compression:     w.opts.Compression,
	}

	result, err := writeSST(ctx, oldMemtable.Iterator(), sstOpts, epoch)
	if err != nil {
		if errors.Is(err, ErrEmptyIterator) {
			return nil
		}
		return fmt.Errorf("build sst: %w", err)
	}

	sstPath := w.store.SSTPath(result.Meta.ID)
	if _, err := w.store.Write(ctx, sstPath, result.SSTData); err != nil {
		return fmt.Errorf("upload sst: %w", err)
	}

	var appendErr error
	if w.opts.EnableFencing {
		_, appendErr = w.manifestLog.AppendAddSSTableWithFence(ctx, result.Meta)
	} else {
		_, appendErr = w.manifestLog.AppendAddSSTable(ctx, result.Meta)
	}

	if appendErr != nil {
		if errors.Is(appendErr, manifest.ErrFenced) {
			w.fenced.Store(true)
		}
		return fmt.Errorf("update manifest: %w", appendErr)
	}

	return nil
}

func (w *writer) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.flushTicker.C:
			if err := w.flush(context.Background()); err != nil {

				if errors.Is(err, manifest.ErrFenced) {
					slog.Error("isledb: writer fenced, stopping background flush")
					return
				}
				if w.opts.OnFlushError != nil {
					w.opts.OnFlushError(err)
				} else {
					slog.Error("isledb: background flush failed", "error", err)
				}
			}
		case <-w.stopCh:
			return
		}
	}
}

func (w *writer) close() error {

	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(w.stopCh)
	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}
	w.wg.Wait()

	return w.flush(context.Background())
}
