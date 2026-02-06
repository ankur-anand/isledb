package isledb

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
)

var ErrBackpressure = errors.New("writer backpressure")

type writer struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        WriterOptions
	valueConfig config.ValueStorageConfig
	ctx         context.Context

	mu                      sync.Mutex
	memtable                *internal.Memtable
	immQueue                []*internal.Memtable
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

func newWriter(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts WriterOptions) (*writer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
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
		ctx:                     ctx,
		memtable:                internal.NewMemtable(opts.MemtableSize*2, memtableInlineThreshold),
		memtableInlineThreshold: memtableInlineThreshold,
		seq:                     m.MaxSeqNum(),
		epoch:                   m.NextEpoch,
		blobStorage:             internal.NewBlobStorage(store, valueConfig),
		stopCh:                  make(chan struct{}),
	}

	ownerID := opts.OwnerID
	if ownerID == "" {
		ownerID = fmt.Sprintf("writer-%d-%d", time.Now().UnixNano(), m.NextEpoch)
	}
	token, err := manifestLog.ClaimWriter(ctx, ownerID)
	if err != nil {
		return nil, fmt.Errorf("claim writer fence: %w", err)
	}
	w.fenceToken = token

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
	if err := w.ensureCapacityLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.seq++
	seq := w.seq
	w.memtable.PutWithTTL(key, value, seq, expireAt)
	w.mu.Unlock()
	return nil
}

func (w *writer) putBlob(key, value []byte, expireAt int64) error {
	ctx := w.ctx

	w.mu.Lock()
	if err := w.ensureCapacityLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.mu.Unlock()

	blobID, err := w.blobStorage.Write(ctx, value)
	if err != nil {
		return fmt.Errorf("write blob: %w", err)
	}

	w.mu.Lock()
	if err := w.ensureCapacityLocked(); err != nil {
		w.mu.Unlock()
		_ = w.blobStorage.Delete(ctx, blobID)
		return err
	}
	w.seq++
	seq := w.seq
	w.memtable.PutBlobRefWithTTL(key, blobID, seq, expireAt)
	w.mu.Unlock()
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

	w.seq++
	seq := w.seq
	w.mu.Lock()
	if err := w.ensureCapacityLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.memtable.DeleteWithTTL(key, seq, expireAt)
	w.mu.Unlock()

	return nil
}

func (w *writer) ensureCapacityLocked() error {
	if w.memtable.ApproxSize() < w.opts.MemtableSize {
		return nil
	}
	if w.opts.MaxImmutableMemtables > 0 && len(w.immQueue) >= w.opts.MaxImmutableMemtables {
		return ErrBackpressure
	}
	if w.memtable.ApproxSize() > 0 {
		w.immQueue = append(w.immQueue, w.memtable)
		w.memtable = internal.NewMemtable(w.opts.MemtableSize*2, w.memtableInlineThreshold)
	}
	return nil
}

func (w *writer) flush(ctx context.Context) error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if w.fenced.Load() {
		return manifest.ErrFenced
	}

	w.mu.Lock()
	toFlush := append([]*internal.Memtable(nil), w.immQueue...)
	w.immQueue = nil
	if w.memtable.ApproxSize() > 0 {
		toFlush = append(toFlush, w.memtable)
		w.memtable = internal.NewMemtable(w.opts.MemtableSize*2, w.memtableInlineThreshold)
	}
	w.mu.Unlock()

	for i, mt := range toFlush {
		if err := w.flushMemtable(ctx, mt); err != nil {
			if errors.Is(err, ErrEmptyIterator) {
				continue
			}
			w.mu.Lock()
			remaining := toFlush[i:]
			if len(remaining) > 0 {
				w.immQueue = append(remaining, w.immQueue...)
			}
			w.mu.Unlock()
			return err
		}
	}

	return nil
}

func (w *writer) flushMemtable(ctx context.Context, mt *internal.Memtable) error {
	w.mu.Lock()
	epoch := w.epoch
	w.epoch++
	w.mu.Unlock()

	sstOpts := SSTWriterOptions{
		BloomBitsPerKey: w.opts.BloomBitsPerKey,
		BlockSize:       w.opts.BlockSize,
		Compression:     w.opts.Compression,
	}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		sstPath := w.store.SSTPath(sstID)
		_, err := w.store.WriteReader(ctx, sstPath, r, nil)
		return err
	}

	seqLo, seqHi := mt.SeqLo(), mt.SeqHi()
	result, err := writeSSTStreaming(ctx, mt.Iterator(), sstOpts, epoch, seqLo, seqHi, uploadFn)
	if err != nil {
		return fmt.Errorf("stream sst: %w", err)
	}

	var appendErr error
	_, appendErr = w.manifestLog.AppendAddSSTableWithFence(ctx, result.Meta)

	if appendErr != nil {
		if isFenceError(appendErr) {
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

				if isFenceError(err) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return w.flush(ctx)
}
