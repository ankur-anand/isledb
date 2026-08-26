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
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/segmentio/ksuid"
	"golang.org/x/sync/errgroup"
)

var (
	ErrBackpressure         = errors.New("writer backpressure")
	ErrInvalidMutation      = errors.New("invalid mutation")
	ErrNilContext           = errors.New("nil context")
	ErrInvalidWriterOptions = errors.New("invalid writer options")
	ErrWriterClosed         = errors.New("writer closed")
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
	sstOutput   SSTEncodingOptions

	changeFeedPayload ChangeFeedPayload
	ctx               context.Context
	cancel            context.CancelFunc

	mu               sync.Mutex
	memtable         *internal.Memtable
	changeBuffer     *changeBatchBuffer
	immQueue         []*pendingFlush
	pendingMemtables int
	seq              uint64
	epoch            uint64

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
	commitID             string
	epoch                uint64
	sstIdentity          sstStreamIdentity
	memtable             *internal.Memtable
	sstable              *manifest.SSTMeta
	changeBatch          *manifest.ChangeBatchMeta
	changes              *changeBatchBuffer
	changeBatchCreatedAt time.Time
}

func (p *pendingFlush) SeqLo() uint64 {
	return p.memtable.SeqLo()
}

func newWriter(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts WriterOptions) (*writer, error) {
	return newWriterWithMaintenanceWake(
		ctx,
		store,
		manifestLog,
		opts,
		nil,
		StorePolicy{MaxPinnedViewAge: DefaultMaxPinnedViewAge},
		DefaultSSTOutputOptions().L0,
	)
}

func newWriterWithMaintenanceWake(
	ctx context.Context,
	store *blobstore.Store,
	manifestLog *manifest.Store,
	opts WriterOptions,
	maintenanceWake <-chan struct{},
	storePolicy StorePolicy,
	sstOutput SSTEncodingOptions,
) (*writer, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	opts, err := normalizeWriterOptions(opts)
	if err != nil {
		return nil, err
	}

	ownerID := opts.OwnerID
	if ownerID == "" {
		ownerID = fmt.Sprintf("writer-%d-%s", time.Now().UnixNano(), ksuid.New().String())
	}
	token, err := manifestLog.ClaimWriterWithPolicy(ctx, ownerID, storePolicy.MaxPinnedViewAge)
	if err != nil {
		return nil, fmt.Errorf("claim writer fence: %w", err)
	}

	// The replay must happen after the fence claim. Once the claim succeeds,
	// the previous writer can no longer publish, so these sequence and epoch
	// counters include every commit that won the race before takeover.
	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest after writer fence claim: %w", err)
	}

	writerCtx, cancel := context.WithCancel(context.Background())

	w := &writer{
		store:           store,
		manifestLog:     manifestLog,
		opts:            opts,
		sstOutput:       sstOutput,
		ctx:             writerCtx,
		cancel:          cancel,
		memtable:        internal.NewMemtable(defaultMemtableArenaBytes(opts.Memtable.TargetBytes, opts.Values)),
		seq:             m.MaxSeqNum(),
		epoch:           m.NextEpoch,
		maintenanceWake: maintenanceWake,
		stopCh:          make(chan struct{}),
		workerDone:      make(chan struct{}),
		fenceToken:      token,
		metrics:         opts.Metrics,
	}

	if opts.Flush.Interval > 0 {
		w.flushTicker = time.NewTicker(opts.Flush.Interval)
		go w.flushLoop()
	} else {
		close(w.workerDone)
	}

	return w, nil
}

func normalizeWriterOptions(opts WriterOptions) (WriterOptions, error) {
	d := DefaultWriterOptions()
	if len(opts.OwnerID) > maxWriterOwnerIDBytes {
		return WriterOptions{}, fmt.Errorf(
			"%w: owner_id bytes=%d max=%d", ErrInvalidWriterOptions, len(opts.OwnerID), maxWriterOwnerIDBytes)
	}
	if opts.Memtable.TargetBytes < 0 {
		return WriterOptions{}, fmt.Errorf(
			"%w: target_bytes=%d", ErrInvalidWriterOptions, opts.Memtable.TargetBytes)
	}
	if opts.Memtable.TargetBytes == 0 {
		opts.Memtable.TargetBytes = d.Memtable.TargetBytes
	}
	if opts.Memtable.MaxPendingMemtables < 0 {
		return WriterOptions{}, fmt.Errorf(
			"%w: max_pending_memtables=%d", ErrInvalidWriterOptions, opts.Memtable.MaxPendingMemtables)
	}
	if opts.Memtable.MaxPendingMemtables == 0 {
		opts.Memtable.MaxPendingMemtables = d.Memtable.MaxPendingMemtables
	}
	if opts.Flush.Interval < 0 {
		return WriterOptions{}, fmt.Errorf(
			"%w: flush_interval=%s", ErrInvalidWriterOptions, opts.Flush.Interval)
	}
	if opts.Maintenance.PollInterval < 0 {
		return WriterOptions{}, fmt.Errorf(
			"%w: maintenance_poll_interval=%s", ErrInvalidWriterOptions, opts.Maintenance.PollInterval)
	}
	if opts.Maintenance.PollInterval == 0 {
		opts.Maintenance.PollInterval = d.Maintenance.PollInterval
	}
	vd := defaultWriterValueOptions()
	values := opts.Values
	if values.MaxKeyBytes < 0 {
		return WriterOptions{}, fmt.Errorf(
			"%w: max_key_bytes=%d", ErrInvalidWriterOptions, values.MaxKeyBytes)
	}
	if values.MaxValueBytes < 0 {
		return WriterOptions{}, fmt.Errorf(
			"%w: max_value_bytes=%d", ErrInvalidWriterOptions, values.MaxValueBytes)
	}
	values.MaxKeyBytes = cmp.Or(values.MaxKeyBytes, vd.MaxKeyBytes)
	values.MaxValueBytes = cmp.Or(values.MaxValueBytes, vd.MaxValueBytes)
	if values.MaxKeyBytes > maxMemtableUserKeyBytes {
		return WriterOptions{}, fmt.Errorf(
			"%w: max_key_bytes=%d exceeds memtable key max=%d",
			ErrInvalidWriterOptions, values.MaxKeyBytes, maxMemtableUserKeyBytes)
	}
	opts.Values = values

	if err := validateMemtableArena(opts.Memtable.TargetBytes, values); err != nil {
		return WriterOptions{}, err
	}
	return opts, nil
}

func validateMemtableArena(targetBytes int64, values ValueOptions) error {
	if targetBytes > maxMemtableArenaBytes {
		return fmt.Errorf("%w: target_bytes=%d exceeds arena max=%d",
			ErrInvalidWriterOptions, targetBytes, maxMemtableArenaBytes)
	}

	maxKeySize := int64(values.MaxKeyBytes)
	if maxKeySize > maxMemtableArenaBytes || values.MaxValueBytes > maxMemtableArenaBytes ||
		maxKeySize+values.MaxValueBytes+1024 > maxMemtableArenaBytes {
		return fmt.Errorf("%w: maximum inline entry exceeds arena max=%d",
			ErrInvalidWriterOptions, maxMemtableArenaBytes)
	}
	if arenaBytes := defaultMemtableArenaBytes(targetBytes, values); arenaBytes > maxMemtableArenaBytes {
		return fmt.Errorf("%w: memtable arena bytes=%d max=%d",
			ErrInvalidWriterOptions, arenaBytes, maxMemtableArenaBytes)
	}
	return nil
}

func defaultMemtableArenaBytes(targetBytes int64, values ValueOptions) int64 {
	headroom := targetBytes / 4
	if headroom < minMemtableArenaHeadroom {
		headroom = minMemtableArenaHeadroom
	}

	maxInlineEntry := int64(values.MaxKeyBytes) + values.MaxValueBytes + 1024
	if headroom < maxInlineEntry {
		headroom = maxInlineEntry
	}

	if targetBytes > 0 && headroom > (1<<63-1)-targetBytes {
		return 1<<63 - 1
	}
	return targetBytes + headroom
}

func (w *writer) newMemtable() *internal.Memtable {
	arenaBytes := defaultMemtableArenaBytes(w.opts.Memtable.TargetBytes, w.opts.Values)
	return internal.NewMemtable(arenaBytes)
}

// newPendingFlushLocked assigns identity and epoch exactly once. The caller
// holds w.mu.
func (w *writer) newPendingFlushLocked(memtable *internal.Memtable) *pendingFlush {
	createdAt := time.Now().UTC()
	pending := &pendingFlush{
		commitID:    ksuid.New().String(),
		epoch:       w.epoch,
		sstIdentity: newSSTStreamIdentity(w.epoch, memtable.SeqLo(), memtable.SeqHi(), createdAt),
		memtable:    memtable,
		changes:     w.changeBuffer,
	}
	if pending.changes != nil {
		pending.changeBatchCreatedAt = createdAt
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
		return ErrWriterClosed
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
	if ttl < 0 {
		return fmt.Errorf("%w: negative TTL %s", ErrInvalidMutation, ttl)
	}

	if len(key) == 0 {
		return fmt.Errorf("%w: empty key", ErrInvalidMutation)
	}
	if len(key) > w.opts.Values.MaxKeyBytes {
		return fmt.Errorf("%w: key size %d exceeds max %d",
			ErrInvalidMutation, len(key), w.opts.Values.MaxKeyBytes)
	}
	if int64(len(value)) > w.opts.Values.MaxValueBytes {
		return fmt.Errorf("%w: value size %d exceeds max %d",
			ErrInvalidMutation, len(value), w.opts.Values.MaxValueBytes)
	}

	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixMilli()
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

func (w *writer) delete(ctx context.Context, key []byte) error {
	w.metrics.ObserveDelete()

	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := w.ensureWritable(); err != nil {
		return err
	}

	if len(key) == 0 {
		return fmt.Errorf("%w: empty key", ErrInvalidMutation)
	}
	if len(key) > w.opts.Values.MaxKeyBytes {
		return fmt.Errorf("%w: key size %d exceeds max %d",
			ErrInvalidMutation, len(key), w.opts.Values.MaxKeyBytes)
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
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := w.ensureWritable(); err != nil {
		return err
	}
	return w.flushInternal(ctx, false, false, false)
}

func (w *writer) flushBackground(ctx context.Context) error {
	return w.flushInternal(ctx, true, false, false)
}

func (w *writer) flushMaintenanceBackground(ctx context.Context) error {
	return w.flushInternal(ctx, true, true, true)
}

func (w *writer) flushFinal(ctx context.Context) error {
	return w.flushInternal(ctx, false, true, false)
}

func (w *writer) flushInternal(ctx context.Context, terminalOnError, forceMaintenancePoll, maintenanceOnly bool) error {
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
	if maintenanceOnly {
		// A process-local mailbox wake publishes only the maintenance command.
		// User mutations retain the same visibility boundary as when no
		// maintenance handle exists: explicit Flush, configured background
		// flush, or Close.
		return nil
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
			if err != nil {
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
		BloomBitsPerKey: w.sstOutput.BloomBitsPerKey,
		BlockSize:       w.sstOutput.BlockBytes,
		Compression:     w.sstOutput.Compression,
	}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		sstPath := w.store.SSTPath(sstID)
		_, err := w.store.WriteReader(ctx, sstPath, r, nil)
		return err
	}

	buildSST := func(uploadCtx context.Context) (streamSSTResult, error) {
		result, err := writeSSTStreaming(uploadCtx, pending.memtable.Iterator(), sstOpts,
			pending.sstIdentity, uploadFn)
		if err != nil {
			return streamSSTResult{}, fmt.Errorf("stream sst: %w", err)
		}
		return result, nil
	}
	buildChangeBatch := func(uploadCtx context.Context) (changeBatchStreamResult, error) {
		result, err := writePendingChangeBatch(uploadCtx, pending,
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
	if !w.manifestLog.WriterFenceObservedActive(w.fenceToken) {
		// Reconciliation can prove the pending commit succeeded after a
		// successor claimed the writer fence. Preserve success for that commit,
		// but make this writer terminal before it accepts or uploads more work.
		w.fenced.Store(true)
	}
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

func writePendingChangeBatch(
	ctx context.Context,
	pending *pendingFlush,
	uploadFn func(context.Context, string, io.Reader) error,
) (changeBatchStreamResult, error) {
	return writeChangeBatchStreaming(
		ctx,
		pending.changes,
		pending.epoch,
		pending.changeBatchCreatedAt,
		uploadFn,
	)
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
