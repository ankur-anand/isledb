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

var (
	ErrBackpressure = errors.New("writer backpressure")
	ErrNilContext   = errors.New("nil context")
)

type writer struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        WriterOptions
	valueConfig config.ValueStorageConfig
	ctx         context.Context
	cancel      context.CancelFunc

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

	closed  atomic.Bool
	metrics *WriterMetrics
}

func newWriter(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts WriterOptions) (*writer, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	opts, valueConfig := normalizeWriterOptions(opts)

	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	memtableInlineThreshold := valueConfig.BlobThreshold
	writerCtx, cancel := context.WithCancel(context.Background())

	w := &writer{
		store:                   store,
		manifestLog:             manifestLog,
		opts:                    opts,
		valueConfig:             valueConfig,
		ctx:                     writerCtx,
		cancel:                  cancel,
		memtable:                internal.NewMemtable(opts.Memtable.TargetBytes*2, memtableInlineThreshold),
		memtableInlineThreshold: memtableInlineThreshold,
		seq:                     m.MaxSeqNum(),
		epoch:                   m.NextEpoch,
		blobStorage:             internal.NewBlobStorage(store, valueConfig),
		stopCh:                  make(chan struct{}),
		metrics:                 opts.Metrics,
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

	if opts.Flush.Interval > 0 {
		w.flushTicker = time.NewTicker(opts.Flush.Interval)
		w.wg.Add(1)
		go w.flushLoop()
	}

	return w, nil
}

func normalizeWriterOptions(opts WriterOptions) (WriterOptions, config.ValueStorageConfig) {
	d := DefaultWriterOptions()
	if opts.Memtable.TargetBytes <= 0 {
		opts.Memtable.TargetBytes = d.Memtable.TargetBytes
	}
	if opts.SST.BloomBitsPerKey == 0 {
		opts.SST.BloomBitsPerKey = d.SST.BloomBitsPerKey
	}
	if opts.SST.BlockBytes == 0 {
		opts.SST.BlockBytes = d.SST.BlockBytes
	}
	opts.SST.Compression = cmp.Or(opts.SST.Compression, d.SST.Compression)

	vd := config.DefaultValueStorageConfig()
	valueConfig := opts.Values
	valueConfig.BlobThreshold = cmp.Or(valueConfig.BlobThreshold, vd.BlobThreshold)
	valueConfig.MaxKeySize = cmp.Or(valueConfig.MaxKeySize, 64*1024)
	valueConfig.MaxValueSize = cmp.Or(valueConfig.MaxValueSize, 256*1024*1024)
	return opts, valueConfig
}

func (w *writer) ensureWritable() error {
	if w.closed.Load() {
		return errors.New("writer closed")
	}
	if w.fenced.Load() {
		return manifest.ErrFenced
	}
	return nil
}

func (w *writer) put(ctx context.Context, key, value []byte) error {
	return w.putWithTTL(ctx, key, value, 0)
}

func (w *writer) putWithTTL(ctx context.Context, key, value []byte, ttl time.Duration) (err error) {
	defer func() {
		w.metrics.ObservePut(err)
	}()

	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := w.ensureWritable(); err != nil {
		return err
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
		return w.putBlob(ctx, key, value, expireAt)
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

func (w *writer) putBlob(ctx context.Context, key, value []byte, expireAt int64) (err error) {
	start := time.Now()
	defer func() {
		w.metrics.ObservePutBlob(len(value), time.Since(start), err)
	}()

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
		return err
	}
	w.seq++
	seq := w.seq
	w.memtable.PutBlobRefWithTTL(key, blobID, seq, expireAt)
	w.mu.Unlock()
	return nil
}

func (w *writer) delete(ctx context.Context, key []byte) error {
	return w.deleteWithTTL(ctx, key, 0)
}

func (w *writer) deleteWithTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	w.metrics.ObserveDelete()

	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := w.ensureWritable(); err != nil {
		return err
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
	if err := w.ensureCapacityLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.seq++
	seq := w.seq
	w.memtable.DeleteWithTTL(key, seq, expireAt)
	w.mu.Unlock()

	return nil
}

func (w *writer) ensureCapacityLocked() error {
	if w.memtable.ApproxSize() < w.opts.Memtable.TargetBytes {
		return nil
	}
	if w.opts.Memtable.MaxFrozen > 0 && len(w.immQueue) >= w.opts.Memtable.MaxFrozen {
		w.metrics.ObserveBackpressure()
		return ErrBackpressure
	}
	if w.memtable.ApproxSize() > 0 {
		w.immQueue = append(w.immQueue, w.memtable)
		w.memtable = internal.NewMemtable(w.opts.Memtable.TargetBytes*2, w.memtableInlineThreshold)
	}
	return nil
}

func (w *writer) flush(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

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
		w.memtable = internal.NewMemtable(w.opts.Memtable.TargetBytes*2, w.memtableInlineThreshold)
	}
	w.mu.Unlock()

	for i, mt := range toFlush {
		start := time.Now()
		err := w.flushMemtable(ctx, mt)
		w.metrics.ObserveFlush(time.Since(start), err)
		if err != nil {
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
		BloomBitsPerKey: w.opts.SST.BloomBitsPerKey,
		BlockSize:       w.opts.SST.BlockBytes,
		Compression:     w.opts.SST.Compression,
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

	var changeBatchMeta *manifest.ChangeBatchMeta
	if w.opts.ChangeFeed.Enabled {
		changeBatch, err := buildChangeBatch(ctx, mt.Iterator(), epoch, seqLo, seqHi, result.Meta.CreatedAt)
		if err != nil {
			return fmt.Errorf("build change batch: %w", err)
		}
		changeBatch.Meta.Path = w.store.ChangeBatchPath(changeBatch.Meta.ID)
		if _, err := w.store.Write(ctx, changeBatch.Meta.Path, changeBatch.Data); err != nil {
			return fmt.Errorf("write change batch: %w", err)
		}
		changeBatchMeta = &changeBatch.Meta
	}

	var appendErr error
	_, appendErr = w.manifestLog.AppendAddSSTableWithChangeBatchWithFence(ctx, result.Meta, changeBatchMeta)

	if appendErr != nil {
		if isFenceError(appendErr) {
			w.fenced.Store(true)
		}
		return fmt.Errorf("update manifest: %w", appendErr)
	}
	w.metrics.ObserveFlushBytes(result.Meta.Size)

	slog.Debug("isledb: memtable flushed", "component", "writer", "sst_id", result.Meta.ID,
		"size", result.Meta.Size, "epoch", epoch)
	return nil
}

func (w *writer) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.flushTicker.C:
			if err := w.flush(w.ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if isFenceError(err) {
					slog.Error("isledb: writer fenced, stopping background flush",
						"component", "writer", "epoch", w.epoch)
					return
				}
				if w.opts.OnFlushError != nil {
					w.opts.OnFlushError(err)
				} else {
					slog.Error("isledb: background flush failed",
						"component", "writer", "error", err)
				}
			}
		case <-w.stopCh:
			return
		}
	}
}

func (w *writer) close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	if w.closed.CompareAndSwap(false, true) {
		w.cancel()
		close(w.stopCh)
		if w.flushTicker != nil {
			w.flushTicker.Stop()
		}
	}
	if err := waitGroupContext(ctx, &w.wg); err != nil {
		return err
	}

	return w.flush(ctx)
}

func (w *writer) closeWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return w.close(ctx)
}

func waitGroupContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
