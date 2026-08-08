package isledb

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/config"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/segmentio/ksuid"
	"golang.org/x/sync/errgroup"
)

var (
	ErrBackpressure         = errors.New("writer backpressure")
	ErrNilContext           = errors.New("nil context")
	ErrInvalidWriterOptions = errors.New("invalid writer options")
	ErrWriterFailed         = errors.New("writer failed")
)

const (
	minMemtableArenaHeadroom = 1 << 20
	maxMemtableArenaBytes    = 1<<32 - 1
	maxWriterOwnerIDBytes    = 256
)

type writer struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        WriterOptions
	valueConfig config.ValueStorageConfig

	changeFeedPayload ChangeFeedPayload
	ctx               context.Context
	cancel            context.CancelFunc

	mu                      sync.Mutex
	memtable                *internal.Memtable
	changeBuffer            *changeBatchBuffer
	immQueue                []*pendingFlush
	pendingMemtables        int
	memtableInlineThreshold int
	seq                     uint64
	epoch                   uint64
	blobStorage             *internal.BlobStorage

	flushMu             sync.Mutex
	flushTicker         *time.Ticker
	maintenanceWake     <-chan struct{}
	nextMaintenancePoll time.Time
	stopCh              chan struct{}
	workerDone          chan struct{}

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
	changes     *changeBatchBuffer
}

func (p *pendingFlush) SeqLo() uint64 {
	return p.memtable.SeqLo()
}

func newWriter(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts WriterOptions) (*writer, error) {
	return newWriterWithMaintenanceWake(ctx, store, manifestLog, opts, nil, StorePolicy{MaxPinnedViewAge: DefaultMaxPinnedViewAge})
}

func newWriterWithMaintenanceWake(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts WriterOptions, maintenanceWake <-chan struct{}, storePolicy StorePolicy) (*writer, error) {
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
		maintenanceWake:         maintenanceWake,
		stopCh:                  make(chan struct{}),
		workerDone:              make(chan struct{}),
		metrics:                 opts.Metrics,
	}

	ownerID := opts.OwnerID
	if ownerID == "" {
		ownerID = fmt.Sprintf("writer-%d-%d", time.Now().UnixNano(), m.NextEpoch)
	}
	token, err := manifestLog.ClaimWriterWithPolicy(ctx, ownerID, storePolicy.MaxPinnedViewAge)
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
	if len(opts.OwnerID) > maxWriterOwnerIDBytes {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: owner_id bytes=%d max=%d", ErrInvalidWriterOptions, len(opts.OwnerID), maxWriterOwnerIDBytes)
	}
	if opts.Memtable.TargetBytes < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: target_bytes=%d", ErrInvalidWriterOptions, opts.Memtable.TargetBytes)
	}
	if opts.Memtable.TargetBytes == 0 {
		opts.Memtable.TargetBytes = d.Memtable.TargetBytes
	}
	if opts.Memtable.MaxPendingMemtables < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: max_pending_memtables=%d", ErrInvalidWriterOptions, opts.Memtable.MaxPendingMemtables)
	}
	if opts.Memtable.MaxPendingMemtables == 0 {
		opts.Memtable.MaxPendingMemtables = d.Memtable.MaxPendingMemtables
	}
	if opts.Flush.Interval < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: flush_interval=%s", ErrInvalidWriterOptions, opts.Flush.Interval)
	}
	if opts.Maintenance.PollInterval < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: maintenance_poll_interval=%s", ErrInvalidWriterOptions, opts.Maintenance.PollInterval)
	}
	if opts.Maintenance.PollInterval == 0 {
		opts.Maintenance.PollInterval = d.Maintenance.PollInterval
	}
	if opts.SST.BloomBitsPerKey < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: bloom_bits_per_key=%d", ErrInvalidWriterOptions, opts.SST.BloomBitsPerKey)
	}
	if opts.SST.BloomBitsPerKey == 0 {
		opts.SST.BloomBitsPerKey = d.SST.BloomBitsPerKey
	}
	if opts.SST.BlockBytes < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: block_bytes=%d", ErrInvalidWriterOptions, opts.SST.BlockBytes)
	}
	if opts.SST.BlockBytes == 0 {
		opts.SST.BlockBytes = d.SST.BlockBytes
	}
	compression := strings.ToLower(strings.TrimSpace(cmp.Or(opts.SST.Compression, d.SST.Compression)))
	switch compression {
	case "none", "snappy", "zstd":
		opts.SST.Compression = compression
	default:
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: unsupported compression=%q", ErrInvalidWriterOptions, opts.SST.Compression)
	}

	vd := defaultWriterValueOptions()
	values := opts.Values
	if values.InlineValueBytes < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: inline_value_bytes=%d", ErrInvalidWriterOptions, values.InlineValueBytes)
	}
	if values.MaxKeyBytes < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: max_key_bytes=%d", ErrInvalidWriterOptions, values.MaxKeyBytes)
	}
	if values.MaxValueBytes < 0 {
		return WriterOptions{}, config.ValueStorageConfig{}, fmt.Errorf(
			"%w: max_value_bytes=%d", ErrInvalidWriterOptions, values.MaxValueBytes)
	}
	values.InlineValueBytes = cmp.Or(values.InlineValueBytes, vd.InlineValueBytes)
	values.MaxKeyBytes = cmp.Or(values.MaxKeyBytes, vd.MaxKeyBytes)
	values.MaxValueBytes = cmp.Or(values.MaxValueBytes, vd.MaxValueBytes)
	opts.Values = values

	valueConfig := config.DefaultValueStorageConfig()
	valueConfig.BlobThreshold = values.InlineValueBytes
	valueConfig.MaxKeySize = values.MaxKeyBytes
	valueConfig.MaxValueSize = values.MaxValueBytes
	if err := validateMemtableArena(opts.Memtable.TargetBytes, valueConfig); err != nil {
		return WriterOptions{}, config.ValueStorageConfig{}, err
	}
	return opts, valueConfig, nil
}

func validateMemtableArena(targetBytes int64, valueConfig config.ValueStorageConfig) error {
	if targetBytes > maxMemtableArenaBytes {
		return fmt.Errorf("%w: target_bytes=%d exceeds arena max=%d",
			ErrInvalidWriterOptions, targetBytes, maxMemtableArenaBytes)
	}

	maxInlineValue := int64(valueConfig.BlobThreshold - 1)
	if maxInlineValue > valueConfig.MaxValueSize {
		maxInlineValue = valueConfig.MaxValueSize
	}
	maxKeySize := int64(valueConfig.MaxKeySize)
	if maxKeySize > maxMemtableArenaBytes || maxInlineValue > maxMemtableArenaBytes ||
		maxKeySize+maxInlineValue+1024 > maxMemtableArenaBytes {
		return fmt.Errorf("%w: maximum inline entry exceeds arena max=%d",
			ErrInvalidWriterOptions, maxMemtableArenaBytes)
	}
	if arenaBytes := defaultMemtableArenaBytes(targetBytes, valueConfig); arenaBytes > maxMemtableArenaBytes {
		return fmt.Errorf("%w: memtable arena bytes=%d max=%d",
			ErrInvalidWriterOptions, arenaBytes, maxMemtableArenaBytes)
	}
	return nil
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
		changes:  w.changeBuffer,
	}
	w.changeBuffer = nil
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
	seq := w.seq + 1
	if w.changeFeedPayload != 0 {
		if w.changeBuffer == nil {
			w.changeBuffer = &changeBatchBuffer{payload: w.changeFeedPayload}
		}
		if err := w.changeBuffer.appendPutForPayload(seq, key, value, expireAt, w.changeFeedPayload); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	w.seq = seq
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
	seq := w.seq + 1
	if w.changeFeedPayload != 0 {
		if w.changeBuffer == nil {
			w.changeBuffer = &changeBatchBuffer{payload: w.changeFeedPayload}
		}
		if err := w.changeBuffer.appendPutForPayload(seq, key, value, expireAt, w.changeFeedPayload); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	w.seq = seq
	w.memtable.PutBlobRefWithTTL(key, blobID, seq, expireAt)
	w.mu.Unlock()
	return nil
}

func (w *writer) delete(ctx context.Context, key []byte) error {
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

	w.mu.Lock()
	if err := w.ensureCapacityLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	seq := w.seq + 1
	if w.changeFeedPayload != 0 {
		if w.changeBuffer == nil {
			w.changeBuffer = &changeBatchBuffer{payload: w.changeFeedPayload}
		}
		if err := w.changeBuffer.appendDelete(seq, key); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	w.seq = seq
	w.memtable.Delete(key, seq)
	w.mu.Unlock()

	return nil
}

func (w *writer) ensureCapacityLocked() error {
	if err := w.ensureWritable(); err != nil {
		return err
	}
	memtableBytes := w.memtable.ApproxSize()
	changeBytes := int64(0)
	if w.changeBuffer != nil {
		changeBytes = w.changeBuffer.bodySize
	}
	if memtableBytes < w.opts.Memtable.TargetBytes && changeBytes < w.opts.Memtable.TargetBytes {
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
	return w.flushInternal(ctx, false, false)
}

func (w *writer) flushBackground(ctx context.Context) error {
	return w.flushInternal(ctx, true, false)
}

func (w *writer) flushMaintenanceBackground(ctx context.Context) error {
	return w.flushInternal(ctx, true, true)
}

func (w *writer) flushFinal(ctx context.Context) error {
	return w.flushInternal(ctx, false, true)
}

func (w *writer) flushInternal(ctx context.Context, terminalOnError, forceMaintenancePoll bool) error {
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
	if w.consumeMaintenanceWake() {
		forceMaintenancePoll = true
	}
	if err := w.pollPendingMaintenance(ctx, forceMaintenancePoll); err != nil {
		if isFenceError(err) {
			w.fenced.Store(true)
		}
		return fmt.Errorf("apply maintenance command: %w", err)
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
			if err != nil && !errors.Is(err, errEmptyIterator) {
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

func (w *writer) pollPendingMaintenance(ctx context.Context, force bool) error {
	now := time.Now()
	if !force && !w.nextMaintenancePoll.IsZero() && now.Before(w.nextMaintenancePoll) {
		return nil
	}
	_, err := w.manifestLog.ApplyPendingMaintenance(ctx)
	if err == nil {
		w.nextMaintenancePoll = now.Add(w.opts.Maintenance.PollInterval)
	}
	return err
}

func (w *writer) consumeMaintenanceWake() bool {
	select {
	case <-w.maintenanceWake:
		return true
	default:
		return false
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
	sstOpts := sstWriterOptions{
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
	buildSST := func(uploadCtx context.Context) (streamSSTResult, error) {
		result, err := writeSSTStreaming(uploadCtx, pending.memtable.Iterator(), sstOpts,
			pending.epoch, seqLo, seqHi, uploadFn)
		if err != nil {
			return streamSSTResult{}, fmt.Errorf("stream sst: %w", err)
		}
		return result, nil
	}
	buildChangeBatch := func(uploadCtx context.Context) (changeBatchStreamResult, error) {
		result, err := writeChangeBatchStreaming(uploadCtx, pending.changes, pending.epoch, time.Now().UTC(),
			func(ctx context.Context, id string, reader io.Reader) error {
				_, err := w.store.WriteReader(ctx, w.store.ChangeBatchPath(id), reader, nil)
				return err
			})
		if err != nil {
			return changeBatchStreamResult{}, fmt.Errorf("stream change batch: %w", err)
		}
		result.Meta.Path = w.store.ChangeBatchPath(result.Meta.ID)
		return result, nil
	}

	needSST := pending.sstable == nil
	needChangeBatch := pending.changes != nil && pending.changeBatch == nil
	if needSST && needChangeBatch {
		group, uploadCtx := errgroup.WithContext(ctx)
		var sstResult streamSSTResult
		var changeResult changeBatchStreamResult
		var sstErr, changeErr error
		group.Go(func() error {
			sstResult, sstErr = buildSST(uploadCtx)
			return sstErr
		})
		group.Go(func() error {
			changeResult, changeErr = buildChangeBatch(uploadCtx)
			return changeErr
		})
		groupErr := group.Wait()
		if sstErr == nil {
			pending.sstable = &sstResult.Meta
		}
		if changeErr == nil {
			pending.changeBatch = &changeResult.Meta
			pending.changes = nil
		}
		if groupErr != nil {
			return groupErr
		}
	} else {
		if needSST {
			result, err := buildSST(ctx)
			if err != nil {
				return err
			}
			pending.sstable = &result.Meta
		}
		if needChangeBatch {
			result, err := buildChangeBatch(ctx)
			if err != nil {
				return err
			}
			pending.changeBatch = &result.Meta
			pending.changes = nil
		}
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
		var err error
		select {
		case <-w.flushTicker.C:
			err = w.flushBackground(w.ctx)
		case <-w.maintenanceWake:
			err = w.flushMaintenanceBackground(w.ctx)
		case <-w.stopCh:
			return
		}
		if err == nil {
			continue
		}
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

	return w.flushFinal(ctx)
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
