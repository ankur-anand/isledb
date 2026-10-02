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

	fenceToken *manifest.FenceToken
	metrics    *WriterMetrics

	// state is the writer's lifecycle, guarded by mu and changed only by
	// transitionLocked. statusNow, failure and committed mirror it for
	// lock-free reads on the write path.
	state     writerState
	statusNow atomic.Uint32
	failure   atomic.Pointer[writerFailure]
	committed atomic.Uint64
	stopOnce  sync.Once
	// onTransition, set only by tests, sees every state the writer enters.
	onTransition func(writerState)
}

// writerStatus is where a writer is in its lifecycle. It only moves forward:
// Open, then Closing, then one of the final statuses, and the first final
// status wins.
type writerStatus uint32

const (
	// writerOpen accepts mutations. With a flush interval, the flush loop is
	// running; without one, committing is the caller's Flush or Close.
	writerOpen writerStatus = iota
	// writerClosing is set when Close starts. The flush loop has stopped, and
	// a failed Close can be retried, so pending mutations may still commit.
	writerClosing
	// writerClosed: Close committed everything.
	writerClosed
	// writerFenced: another writer holds the fence; nothing more commits.
	writerFenced
	// writerFailed: a background flush failed; nothing more commits.
	writerFailed
)

func (s writerStatus) final() bool { return s >= writerClosed }

// writerState is the writer's lifecycle record. changed is closed and
// replaced on every transition, waking WaitCommitted callers.
type writerState struct {
	status    writerStatus
	committed uint64
	failure   error
	changed   chan struct{}
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

	// What the manifest holds is committed: report it before the first
	// commit, so a writer taking over continues the series.
	w.state = writerState{status: writerOpen, committed: w.seq, changed: make(chan struct{})}
	w.committed.Store(w.seq)
	w.metrics.ObserveCommittedSequence(w.seq)

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

// ensureWritable reports why the writer accepts no more mutations, without a
// lock: Put calls it before taking mu, and again under mu.
func (w *writer) ensureWritable() error {
	return w.statusError(writerStatus(w.statusNow.Load()))
}

// statusError is the error for a writer in status s, or nil while open.
func (w *writer) statusError(s writerStatus) error {
	switch s {
	case writerOpen:
		return nil
	case writerFenced:
		return manifest.ErrFenced
	case writerFailed:
		return w.failure.Load().err
	default:
		return ErrWriterClosed
	}
}

// transitionLocked moves the writer's state forward, with mu held: the
// committed sequence to committed if higher, and the status to status if it
// is later and the current one is not final. Both change together, so a
// waiter never sees a commit's fencing without the commit. It wakes waiters
// once if anything changed. It never does I/O.
func (w *writer) transitionLocked(committed uint64, status writerStatus, failure error) {
	changed := false
	if committed > w.state.committed {
		w.state.committed = committed
		w.committed.Store(committed)
		w.metrics.ObserveCommittedSequence(committed)
		changed = true
	}
	if status > w.state.status && !w.state.status.final() {
		w.state.status = status
		if status == writerFailed {
			w.state.failure = failure
			w.failure.Store(&writerFailure{err: failure})
		}
		w.statusNow.Store(uint32(status))
		changed = true
	}
	if changed {
		close(w.state.changed)
		w.state.changed = make(chan struct{})
		if w.onTransition != nil {
			w.onTransition(w.state)
		}
	}
}

func (w *writer) transition(committed uint64, status writerStatus, failure error) {
	w.mu.Lock()
	w.transitionLocked(committed, status, failure)
	w.mu.Unlock()
}

func (w *writer) put(ctx context.Context, key, value []byte) (uint64, error) {
	return w.putWithTTL(ctx, key, value, 0)
}

// putWithTTL buffers a put and returns the sequence it was given.
func (w *writer) putWithTTL(ctx context.Context, key, value []byte, ttl time.Duration) (seq uint64, err error) {
	defer func() {
		w.metrics.ObservePut(err)
	}()

	if err := checkContext(ctx); err != nil {
		return 0, err
	}
	if err := w.ensureWritable(); err != nil {
		return 0, err
	}
	if ttl < 0 {
		return 0, fmt.Errorf("%w: negative TTL %s", ErrInvalidMutation, ttl)
	}

	if len(key) == 0 {
		return 0, fmt.Errorf("%w: empty key", ErrInvalidMutation)
	}
	if len(key) > w.opts.Values.MaxKeyBytes {
		return 0, fmt.Errorf("%w: key size %d exceeds max %d",
			ErrInvalidMutation, len(key), w.opts.Values.MaxKeyBytes)
	}
	if int64(len(value)) > w.opts.Values.MaxValueBytes {
		return 0, fmt.Errorf("%w: value size %d exceeds max %d",
			ErrInvalidMutation, len(value), w.opts.Values.MaxValueBytes)
	}

	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixMilli()
	}

	return w.putInline(key, value, expireAt)
}

func (w *writer) putInline(key, value []byte, expireAt int64) (uint64, error) {
	w.mu.Lock()
	if err := w.ensureCapacityLocked(); err != nil {
		w.mu.Unlock()
		return 0, err
	}
	seq := w.seq + 1
	if w.changeFeedPayload != 0 {
		if w.changeBuffer == nil {
			w.changeBuffer = &changeBatchBuffer{payload: w.changeFeedPayload}
		}
		if err := w.changeBuffer.appendPutForPayload(seq, key, value, expireAt, w.changeFeedPayload); err != nil {
			w.mu.Unlock()
			return 0, err
		}
	}
	w.seq = seq
	w.memtable.PutWithTTL(key, value, seq, expireAt)
	w.mu.Unlock()
	return seq, nil
}

// delete buffers a tombstone and returns the sequence it was given.
func (w *writer) delete(ctx context.Context, key []byte) (uint64, error) {
	w.metrics.ObserveDelete()

	if err := checkContext(ctx); err != nil {
		return 0, err
	}
	if err := w.ensureWritable(); err != nil {
		return 0, err
	}

	if len(key) == 0 {
		return 0, fmt.Errorf("%w: empty key", ErrInvalidMutation)
	}
	if len(key) > w.opts.Values.MaxKeyBytes {
		return 0, fmt.Errorf("%w: key size %d exceeds max %d",
			ErrInvalidMutation, len(key), w.opts.Values.MaxKeyBytes)
	}

	w.mu.Lock()
	if err := w.ensureCapacityLocked(); err != nil {
		w.mu.Unlock()
		return 0, err
	}
	seq := w.seq + 1
	if w.changeFeedPayload != 0 {
		if w.changeBuffer == nil {
			w.changeBuffer = &changeBatchBuffer{payload: w.changeFeedPayload}
		}
		if err := w.changeBuffer.appendDelete(seq, key); err != nil {
			w.mu.Unlock()
			return 0, err
		}
	}
	w.seq = seq
	w.memtable.Delete(key, seq)
	w.mu.Unlock()

	return seq, nil
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

	if s := writerStatus(w.statusNow.Load()); s == writerFenced || s == writerFailed {
		return w.statusError(s)
	}
	if w.consumeMaintenanceWake() {
		forceMaintenancePoll = true
	}
	if err := w.pollPendingMaintenance(ctx, forceMaintenancePoll); err != nil {
		err = fmt.Errorf("apply maintenance command: %w", err)
		if isFenceError(err) {
			w.transition(0, writerFenced, nil)
		} else if terminalOnError && !errors.Is(err, context.Canceled) {
			// Like any background flush failure, it is final: the flush
			// loop stops, so the writer must not look open.
			w.mu.Lock()
			err = w.recordBackgroundFailureLocked(err)
			w.mu.Unlock()
		}
		return err
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

// backgroundError is the failure that made the writer final, if any.
func (w *writer) backgroundError() error {
	if writerStatus(w.statusNow.Load()) != writerFailed {
		return nil
	}
	return w.failure.Load().err
}

// recordBackgroundFailureLocked stores the first unobserved flush failure.
// The caller holds w.mu so mutation acceptance and terminal failure recording
// have one ordering point.
func (w *writer) recordBackgroundFailureLocked(cause error) error {
	if s := w.state.status; s.final() {
		if err := w.statusError(s); err != ErrWriterClosed {
			return err
		}
	}
	w.transitionLocked(0, writerFailed, fmt.Errorf("%w: background flush: %w", ErrWriterFailed, cause))
	return w.statusError(w.state.status)
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
	fenced := !w.manifestLog.WriterFenceObservedActive(w.fenceToken)
	if appendErr != nil {
		if fenced || isFenceError(appendErr) {
			w.transition(0, writerFenced, nil)
		}
		return fmt.Errorf("update manifest: %w", appendErr)
	}
	w.metrics.ObserveFlushBytes(pending.sstable.Size)
	// One memtable committed: its last sequence, not the writer's counter,
	// which counts mutations still in memory. Reconciliation can prove the
	// commit succeeded after a successor claimed the writer fence: record the
	// commit and the fencing as one transition, so the commit stays a success.
	status := writerOpen
	if fenced {
		status = writerFenced
	}
	w.transition(pending.sstable.SeqHi, status, nil)

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
			w.transition(0, writerFenced, nil)
			return
		}
		// The loop stops, so the writer must be final: never open with
		// nothing flushing.
		w.mu.Lock()
		notifyErr = w.recordBackgroundFailureLocked(err)
		w.mu.Unlock()
		return
	}
}

func (w *writer) close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	w.transition(0, writerClosing, nil)
	w.stopOnce.Do(func() {
		w.cancel()
		close(w.stopCh)
		if w.flushTicker != nil {
			w.flushTicker.Stop()
		}
	})
	select {
	case <-w.workerDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Only success ends a Closing writer: a failed Close can be retried, and
	// may still commit what is pending.
	err := w.flushFinal(ctx)
	if err == nil {
		w.transition(0, writerClosed, nil)
	}
	return err
}

// waitCommitted waits until seq is committed, or until the writer reaches a
// final status without committing it. It reads one consistent snapshot of the
// writer's state at a time.
func (w *writer) waitCommitted(ctx context.Context, seq uint64) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	for {
		w.mu.Lock()
		state, assigned := w.state, w.seq
		w.mu.Unlock()
		if seq > assigned {
			return fmt.Errorf("isledb: sequence %d has not been assigned; the writer is at %d", seq, assigned)
		}
		if state.committed >= seq {
			return nil
		}
		if state.status.final() {
			return w.statusError(state.status)
		}
		select {
		case <-state.changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
