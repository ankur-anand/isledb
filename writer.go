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
	"github.com/segmentio/ksuid"
)

var (
	ErrBackpressure         = errors.New("writer backpressure")
	ErrNilContext           = errors.New("nil context")
	ErrInvalidWriterOptions = errors.New("invalid writer options")
	ErrWriterFailed         = errors.New("writer failed")
)

const minMemtableArenaHeadroom = 1 << 20

type writer struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        WriterOptions
	valueConfig config.ValueStorageConfig
	ctx         context.Context
	cancel      context.CancelFunc

	mu                      sync.Mutex
	memtable                *internal.Memtable
	immQueue                []*pendingFlush
	pendingMemtables        int
	memtableInlineThreshold int
	seq                     uint64
	epoch                   uint64
	blobStorage             *internal.BlobStorage

	flushMu     sync.Mutex
	flushTicker *time.Ticker
	stopCh      chan struct{}
	workerDone  chan struct{}

	fenced     atomic.Bool
	fenceToken *manifest.FenceToken

	closed            atomic.Bool
	backgroundFailure atomic.Pointer[writerFailure]
	metrics           *WriterMetrics
}

type writerFailure struct {
	err error
}

// pendingFlush owns one logical memtable publication. Uploaded objects and the
// commit ID survive manifest retries, so publication never creates a second
// visible commit for the same sequence range.
type pendingFlush struct {
	commitID    string
	epoch       uint64
	memtable    *internal.Memtable
	sstable     *manifest.SSTMeta
	changeBatch *manifest.ChangeBatchMeta
}

func (p *pendingFlush) SeqLo() uint64 {
	return p.memtable.SeqLo()
}

func newWriter(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts WriterOptions) (*writer, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	opts, valueConfig, err := normalizeWriterOptions(opts)
	if err != nil {
		return nil, err
	}

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
		memtable:                internal.NewMemtable(defaultMemtableArenaBytes(opts.Memtable.TargetBytes, valueConfig), memtableInlineThreshold),
		memtableInlineThreshold: memtableInlineThreshold,
		seq:                     m.MaxSeqNum(),
		epoch:                   m.NextEpoch,
		blobStorage:             internal.NewBlobStorage(store, valueConfig),
		stopCh:                  make(chan struct{}),
		workerDone:              make(chan struct{}),
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
		go w.flushLoop()
	} else {
		close(w.workerDone)
	}

	return w, nil
}

func normalizeWriterOptions(opts WriterOptions) (WriterOptions, config.ValueStorageConfig, error) {
	d := DefaultWriterOptions()
	if opts.Memtable.TargetBytes <= 0 {
		opts.Memtable.TargetBytes = d.Memtable.TargetBytes
	}
	if opts.Memtable.MaxPendingMemtables < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: max_pending_memtables=%d", ErrInvalidWriterOptions, opts.Memtable.MaxPendingMemtables)
	}
	if opts.Memtable.MaxPendingMemtables == 0 {
		opts.Memtable.MaxPendingMemtables = d.Memtable.MaxPendingMemtables
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
	return opts, valueConfig, nil
}

func defaultMemtableArenaBytes(targetBytes int64, valueConfig config.ValueStorageConfig) int64 {
	headroom := targetBytes / 4
	if headroom < minMemtableArenaHeadroom {
		headroom = minMemtableArenaHeadroom
	}

	maxInlineValue := int64(valueConfig.BlobThreshold - 1)
	if maxInlineValue < 0 || maxInlineValue > valueConfig.MaxValueSize {
		maxInlineValue = valueConfig.MaxValueSize
	}
	maxInlineEntry := int64(valueConfig.MaxKeySize) + maxInlineValue + 1024
	if headroom < maxInlineEntry {
		headroom = maxInlineEntry
	}

	if targetBytes > 0 && headroom > (1<<63-1)-targetBytes {
		return 1<<63 - 1
	}
	return targetBytes + headroom
}

func (w *writer) newMemtable() *internal.Memtable {
	arenaBytes := defaultMemtableArenaBytes(w.opts.Memtable.TargetBytes, w.valueConfig)
	return internal.NewMemtable(arenaBytes, w.memtableInlineThreshold)
}

// newPendingFlushLocked assigns identity and epoch exactly once. The caller
// holds w.mu.
func (w *writer) newPendingFlushLocked(memtable *internal.Memtable) *pendingFlush {
	pending := &pendingFlush{
		commitID: ksuid.New().String(),
		epoch:    w.epoch,
		memtable: memtable,
	}
	w.epoch++
	return pending
}

func (w *writer) ensureWritable() error {
	if err := w.backgroundError(); err != nil {
		return err
	}
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
	if err := w.ensureWritable(); err != nil {
		return err
	}
	if w.memtable.ApproxSize() < w.opts.Memtable.TargetBytes {
		return nil
	}
	if w.pendingMemtables >= w.opts.Memtable.MaxPendingMemtables {
		w.metrics.ObserveBackpressure()
		return ErrBackpressure
	}
	if !w.memtable.Empty() {
		w.immQueue = append(w.immQueue, w.newPendingFlushLocked(w.memtable))
		w.pendingMemtables++
		w.memtable = w.newMemtable()
	}
	return nil
}

func (w *writer) flush(ctx context.Context) error {
	return w.flushInternal(ctx, false)
}

func (w *writer) flushBackground(ctx context.Context) error {
	return w.flushInternal(ctx, true)
}

func (w *writer) flushInternal(ctx context.Context, terminalOnError bool) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if err := w.backgroundError(); err != nil {
		return err
	}
	if w.fenced.Load() {
		return manifest.ErrFenced
	}

	w.mu.Lock()
	throughSeq := w.seq
	w.mu.Unlock()

	for {
		w.mu.Lock()
		toFlush := w.takeFlushBatchLocked(throughSeq)
		w.mu.Unlock()
		if len(toFlush) == 0 {
			return nil
		}

		for i, pending := range toFlush {
			start := time.Now()
			err := w.flushPending(ctx, pending)
			w.metrics.ObserveFlush(time.Since(start), err)
			if err != nil && !errors.Is(err, ErrEmptyIterator) {
				w.mu.Lock()
				w.immQueue = append(toFlush[i:], w.immQueue...)
				if terminalOnError && !errors.Is(err, context.Canceled) && !isFenceError(err) {
					err = w.recordBackgroundFailureLocked(err)
				}
				w.mu.Unlock()
				return err
			}
			w.mu.Lock()
			w.pendingMemtables--
			w.mu.Unlock()
		}
	}
}

func (w *writer) backgroundError() error {
	failure := w.backgroundFailure.Load()
	if failure == nil {
		return nil
	}
	return failure.err
}

// recordBackgroundFailureLocked stores the first unobserved flush failure.
// The caller holds w.mu so mutation acceptance and terminal failure recording
// have one ordering point.
func (w *writer) recordBackgroundFailureLocked(cause error) error {
	if err := w.backgroundError(); err != nil {
		return err
	}
	failure := &writerFailure{
		err: fmt.Errorf("%w: background flush: %w", ErrWriterFailed, cause),
	}
	if w.backgroundFailure.CompareAndSwap(nil, failure) {
		return failure.err
	}
	return w.backgroundError()
}

// takeFlushBatchLocked returns pending work that may contain mutations at or
// below throughSeq. Work already in immQueue remains counted while it is in
// flight. The active memtable is rotated only when a pending slot is available.
func (w *writer) takeFlushBatchLocked(throughSeq uint64) []*pendingFlush {
	cut := 0
	for cut < len(w.immQueue) && w.immQueue[cut].SeqLo() <= throughSeq {
		cut++
	}
	toFlush := append([]*pendingFlush(nil), w.immQueue[:cut]...)
	clear(w.immQueue[:cut])
	w.immQueue = w.immQueue[cut:]

	if !w.memtable.Empty() && w.memtable.SeqLo() <= throughSeq &&
		w.pendingMemtables < w.opts.Memtable.MaxPendingMemtables {
		toFlush = append(toFlush, w.newPendingFlushLocked(w.memtable))
		w.pendingMemtables++
		w.memtable = w.newMemtable()
	}
	return toFlush
}

func (w *writer) flushPending(ctx context.Context, pending *pendingFlush) error {
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

	seqLo, seqHi := pending.memtable.SeqLo(), pending.memtable.SeqHi()
	if pending.sstable == nil {
		result, err := writeSSTStreaming(ctx, pending.memtable.Iterator(), sstOpts,
			pending.epoch, seqLo, seqHi, uploadFn)
		if err != nil {
			return fmt.Errorf("stream sst: %w", err)
		}
		pending.sstable = &result.Meta
	}

	if w.opts.ChangeFeed.Enabled && pending.changeBatch == nil {
		changeBatch, err := buildChangeBatch(ctx, pending.memtable.Iterator(), pending.epoch,
			seqLo, seqHi, pending.sstable.CreatedAt)
		if err != nil {
			return fmt.Errorf("build change batch: %w", err)
		}
		changeBatch.Meta.Path = w.store.ChangeBatchPath(changeBatch.Meta.ID)
		if _, err := w.store.Write(ctx, changeBatch.Meta.Path, changeBatch.Data); err != nil {
			return fmt.Errorf("write change batch: %w", err)
		}
		pending.changeBatch = &changeBatch.Meta
	}

	_, appendErr := w.manifestLog.AppendWriterCommit(ctx, manifest.WriterCommit{
		ID:          pending.commitID,
		SSTable:     *pending.sstable,
		ChangeBatch: pending.changeBatch,
	})
	if appendErr != nil {
		if isFenceError(appendErr) {
			w.fenced.Store(true)
		}
		return fmt.Errorf("update manifest: %w", appendErr)
	}
	w.metrics.ObserveFlushBytes(pending.sstable.Size)

	slog.Debug("isledb: memtable flushed", "component", "writer", "sst_id", pending.sstable.ID,
		"commit_id", pending.commitID, "size", pending.sstable.Size, "epoch", pending.epoch)
	return nil
}

func (w *writer) flushLoop() {
	var notifyErr error
	defer func() {
		w.flushTicker.Stop()
		// The flush worker is considered finished before user code runs.
		// OnFlushError may therefore call Close without waiting on itself.
		close(w.workerDone)
		if notifyErr == nil {
			return
		}
		if w.opts.OnFlushError != nil {
			w.opts.OnFlushError(notifyErr)
			return
		}
		slog.Error("isledb: background flush failed",
			"component", "writer", "error", notifyErr)
	}()

	for {
		select {
		case <-w.flushTicker.C:
			if err := w.flushBackground(w.ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if isFenceError(err) {
					slog.Error("isledb: writer fenced, stopping background flush",
						"component", "writer", "epoch", w.epoch)
					return
				}
				notifyErr = err
				return
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
	select {
	case <-w.workerDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	return w.flush(ctx)
}

func (w *writer) closeWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return w.close(ctx)
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
