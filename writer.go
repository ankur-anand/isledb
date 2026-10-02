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
)

const (
	// maxFlushRetryDelay caps the background flush's backoff while commits
	// keep failing.
	maxFlushRetryDelay = 30 * time.Second

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
	// activeSince is when the active memtable took its first mutation, zero
	// while empty; pendingSince holds the same for each frozen memtable not
	// yet committed, oldest first. Together they date the oldest write not yet
	// in object storage.
	activeSince  time.Time
	pendingSince []time.Time

	flushMu             sync.Mutex
	flushTicker         *time.Ticker
	maintenanceWake     <-chan struct{}
	nextMaintenancePoll time.Time
	stopCh              chan struct{}
	workerDone          chan struct{}

	fenceToken *manifest.FenceToken
	metrics    *WriterMetrics

	// state is the writer's lifecycle, guarded by mu and changed only by
	// transitionLocked. statusNow and committed mirror it for lock-free reads
	// on the write path.
	state     writerState
	statusNow atomic.Uint32
	committed atomic.Uint64
	stopOnce  sync.Once

	// Failures to commit and to apply maintenance are retried, never final,
	// and reported as separate runs (see failureRun).
	commitFailures      failureRun
	maintenanceFailures failureRun
	// onTransition, set only by tests, sees every state the writer enters.
	onTransition func(writerState)
}

// writerStatus is where a writer is in its lifecycle. It only moves forward:
// Open, then Closing, then Closed or Fenced, and the first final status wins.
// No error is final except losing the fence: a commit that fails stays
// queued, and the next attempt reconciles and retries it.
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
)

func (s writerStatus) final() bool { return s >= writerClosed }

// writerState is the writer's lifecycle record. changed is closed and
// replaced on every transition, waking WaitCommitted callers.
type writerState struct {
	status    writerStatus
	committed uint64
	changed   chan struct{}
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
	w.commitFailures.kind = "commits"
	w.maintenanceFailures.kind = "maintenance"

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
	w.pendingSince = append(w.pendingSince, w.activeSince)
	w.activeSince = time.Time{}
	return pending
}

// noteAcceptedLocked dates the active memtable's first mutation, with mu held.
func (w *writer) noteAcceptedLocked() {
	if w.activeSince.IsZero() {
		w.activeSince = time.Now()
		if len(w.pendingSince) == 0 {
			w.metrics.ObserveOldestUncommitted(w.activeSince)
		}
	}
}

// noteMemtableCommittedLocked drops the oldest frozen memtable's date after
// it commits, with mu held, and reports the next oldest uncommitted write.
func (w *writer) noteMemtableCommittedLocked() {
	if len(w.pendingSince) > 0 {
		w.pendingSince = w.pendingSince[1:]
	}
	oldest := w.activeSince
	if len(w.pendingSince) > 0 {
		oldest = w.pendingSince[0]
	}
	w.metrics.ObserveOldestUncommitted(oldest)
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
	default:
		return ErrWriterClosed
	}
}

// transitionLocked moves the writer's state forward, with mu held: the
// committed sequence to committed if higher, and the status to status if it
// is later and the current one is not final. Both change together, so a
// waiter never sees a commit's fencing without the commit. It wakes waiters
// once if anything changed. It never does I/O.
func (w *writer) transitionLocked(committed uint64, status writerStatus) {
	changed := false
	if committed > w.state.committed {
		w.state.committed = committed
		w.committed.Store(committed)
		w.metrics.ObserveCommittedSequence(committed)
		changed = true
	}
	if status > w.state.status && !w.state.status.final() {
		w.state.status = status
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

func (w *writer) transition(committed uint64, status writerStatus) {
	w.mu.Lock()
	w.transitionLocked(committed, status)
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
	w.noteAcceptedLocked()
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
	w.noteAcceptedLocked()
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
	return w.flushInternal(ctx, false, false)
}

func (w *writer) flushBackground(ctx context.Context) error {
	return w.flushInternal(ctx, false, false)
}

func (w *writer) flushMaintenanceBackground(ctx context.Context) error {
	return w.flushInternal(ctx, true, true)
}

func (w *writer) flushFinal(ctx context.Context) error {
	return w.flushInternal(ctx, true, false)
}

// flushInternal applies any pending maintenance command, then commits the
// queued memtables oldest first. A memtable that fails to commit stays at the
// head of the queue; the next attempt reconciles it, so a commit applied
// before its response was lost is found, not repeated. Only losing the fence
// is final. A maintenance failure is reported and does not hold up commits.
func (w *writer) flushInternal(ctx context.Context, forceMaintenancePoll, maintenanceOnly bool) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if writerStatus(w.statusNow.Load()) == writerFenced {
		return manifest.ErrFenced
	}
	if w.consumeMaintenanceWake() {
		forceMaintenancePoll = true
	}
	polled, err := w.pollPendingMaintenance(ctx, forceMaintenancePoll)
	switch {
	case err == nil:
		if polled {
			w.reportRecovery(&w.maintenanceFailures)
		}
	case isFenceError(err):
		w.transition(0, writerFenced)
		return fmt.Errorf("apply maintenance command: %w", err)
	case ctx.Err() != nil:
		// The caller's own cancellation or deadline, not a storage failure.
		return fmt.Errorf("apply maintenance command: %w", err)
	default:
		// Maintenance is retried with the next poll; commits go on.
		w.reportFailure(&w.maintenanceFailures, fmt.Errorf("apply maintenance command: %w", err))
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
				w.mu.Unlock()
				return err
			}
			w.mu.Lock()
			w.pendingMemtables--
			w.noteMemtableCommittedLocked()
			w.mu.Unlock()
			w.reportRecovery(&w.commitFailures)
		}
	}
}

// pollPendingMaintenance applies a staged maintenance command once a poll is
// due or forced, and reports whether it polled. A failed poll is next due a
// poll interval later, like a successful one.
func (w *writer) pollPendingMaintenance(ctx context.Context, force bool) (bool, error) {
	now := time.Now()
	if !force && !w.nextMaintenancePoll.IsZero() && now.Before(w.nextMaintenancePoll) {
		return false, nil
	}
	w.nextMaintenancePoll = now.Add(w.opts.Maintenance.PollInterval)
	_, err := w.manifestLog.ApplyPendingMaintenance(ctx)
	return true, err
}

func (w *writer) consumeMaintenanceWake() bool {
	select {
	case <-w.maintenanceWake:
		return true
	default:
		return false
	}
}

// failureRun tracks one kind of retried failure, commits or maintenance, from
// its first failure to the next success: it dates the run and rate-limits
// its reports.
type failureRun struct {
	kind  string
	mu    sync.Mutex
	since time.Time
	limit readerDiagnosticLimiter
}

// fail records a failure and reports whether to report it.
func (r *failureRun) fail(now time.Time) (report bool, since time.Time, suppressed uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.since.IsZero() {
		r.since = now
	}
	report, suppressed = r.limit.allow(now)
	return report, r.since, suppressed
}

// ok ends the run, if any, and returns when it started.
func (r *failureRun) ok() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	since := r.since
	if !since.IsZero() {
		r.since = time.Time{}
		r.limit = readerDiagnosticLimiter{}
	}
	return since
}

// failing reports whether a run is in progress.
func (r *failureRun) failing() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.since.IsZero()
}

// reportFailure reports a failure that will be retried: through
// OnFlushError, or a warning log without one, on the first failure of a run
// and at most once a minute after.
func (w *writer) reportFailure(run *failureRun, err error) {
	now := time.Now()
	report, since, suppressed := run.fail(now)
	if !report {
		return
	}
	if w.opts.OnFlushError != nil {
		// Its own goroutine: the caller may hold flushMu or be the flush
		// loop, and the callback may call Close, which waits for both.
		go w.opts.OnFlushError(err)
		return
	}
	slog.Warn("isledb: writer failure; retrying",
		"component", "writer", "failing", run.kind, "error", err,
		"failing_for", now.Sub(since).Round(time.Second),
		"suppressed_since_last_log", suppressed)
}

// reportRecovery ends a run of failures, logging how long it lasted.
func (w *writer) reportRecovery(run *failureRun) {
	if since := run.ok(); !since.IsZero() {
		slog.Info("isledb: writer recovered", "component", "writer",
			"recovered", run.kind, "failed_for", time.Since(since).Round(time.Second))
	}
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
			w.transition(0, writerFenced)
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
	w.transition(pending.sstable.SeqHi, status)

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

// flushLoop commits on each tick until Close or until the writer loses its
// fence. A failed commit stays queued and is retried, after a delay that
// doubles while failures continue, up to maxFlushRetryDelay; nothing else
// stops the loop, so an open writer always has its commits retried.
func (w *writer) flushLoop() {
	defer func() {
		w.flushTicker.Stop()
		close(w.workerDone)
	}()

	interval := w.opts.Flush.Interval
	var delay time.Duration
	var retryAt time.Time
	for {
		var err error
		commit := false
		select {
		case <-w.flushTicker.C:
			if time.Now().Before(retryAt) {
				continue
			}
			commit = true
			err = w.flushBackground(w.ctx)
		case <-w.maintenanceWake:
			// Applies maintenance only, so its result says nothing about
			// commits: it leaves the backoff alone.
			err = w.flushMaintenanceBackground(w.ctx)
		case <-w.stopCh:
			return
		}
		switch {
		case err == nil:
			if commit {
				delay, retryAt = 0, time.Time{}
			}
		case errors.Is(err, context.Canceled):
			return
		case isFenceError(err):
			slog.Error("isledb: writer fenced, stopping background flush",
				"component", "writer", "epoch", w.epoch)
			w.transition(0, writerFenced)
			return
		case commit:
			w.reportFailure(&w.commitFailures, err)
			delay = min(max(2*delay, interval), maxFlushRetryDelay)
			retryAt = time.Now().Add(delay)
		}
	}
}

func (w *writer) close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	w.transition(0, writerClosing)
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
		w.transition(0, writerClosed)
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
