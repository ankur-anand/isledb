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
	// ErrCommitTimeout reports a commit attempt that ran out of its own
	// deadline, as when a storage request hangs. The writes stay queued and
	// are retried. It is not the caller's context deadline, and does not wrap
	// context.DeadlineExceeded.
	ErrCommitTimeout = errors.New("commit attempt timed out")
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

	maintenanceWake <-chan struct{}
	// stagedMaintenance is a maintenance command the poller has read from
	// maintenance/HEAD and the next pass applies; nil when there is none.
	stagedMaintenance atomic.Pointer[manifest.MaintenanceCommand]
	// applyWake asks the committer to apply a newly fetched command.
	applyWake  chan struct{}
	pollerDone chan struct{}
	stopCh     chan struct{} // stops the poller
	stopOnce   sync.Once

	// The committer is the one goroutine that commits. Flush and Close ask it
	// for a pass and wait for it (requestPass); kick wakes it. Passes are
	// numbered as they start; the fields below are guarded by mu.
	kick            chan struct{}
	committerCancel context.CancelFunc
	committerDone   chan struct{}
	passStarted     uint64
	passDone        uint64
	passErr         error         // the result of pass passDone
	passChanged     chan struct{} // closed and replaced when a pass ends
	finalRequested  bool          // the next pass reads the mailbox first (Close)
	closing         chan struct{} // serializes Close calls
	closeErr        error         // what Close returned; guarded by closing
	// attemptTimeoutBase is the fixed part of commitAttemptTimeout; tests
	// shorten it.
	attemptTimeoutBase atomic.Int64
	// attemptTimeouts counts attempts in a row that ran out of time; each
	// doubles the next attempt's deadline. Only the committer uses it.
	attemptTimeouts int

	fenceToken *manifest.FenceToken
	metrics    *WriterMetrics

	// state is the writer's lifecycle, guarded by mu and changed only by
	// transitionLocked. statusNow and committed mirror it for lock-free reads
	// on the write path.
	state     writerState
	statusNow atomic.Uint32
	committed atomic.Uint64

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
		applyWake:       make(chan struct{}, 1),
		pollerDone:      make(chan struct{}),
		kick:            make(chan struct{}, 1),
		passChanged:     make(chan struct{}),
		closing:         make(chan struct{}, 1),
		fenceToken:      token,
		metrics:         opts.Metrics,
	}
	w.commitFailures.kind = "commits"
	w.maintenanceFailures.kind = "maintenance"
	w.attemptTimeoutBase.Store(int64(commitAttemptBase))

	// What the manifest holds is committed: report it before the first
	// commit, so a writer taking over continues the series.
	w.state = writerState{status: writerOpen, committed: w.seq, changed: make(chan struct{})}
	w.committed.Store(w.seq)
	w.metrics.ObserveCommittedSequence(w.seq)

	go w.maintenancePollLoop()
	committerCtx, cancelCommitter := context.WithCancel(context.Background())
	w.committerCancel, w.committerDone = cancelCommitter, make(chan struct{})
	go w.commitLoop(committerCtx, w.committerDone)

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

// flush asks the committer for a pass and waits for it. ctx bounds the wait,
// not the pass: a Flush that gives up leaves the commit going, under the
// committer's own deadlines.
func (w *writer) flush(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := w.ensureWritable(); err != nil {
		return err
	}
	return w.requestPass(ctx, false)
}

// requestPass asks the committer for a pass that starts after this call, and
// waits, bounded by ctx, until one ends. It returns nil once that pass, or a
// later one, has committed everything accepted before the call; otherwise the
// pass's error, or why the writer can commit nothing more. final makes the
// pass read the maintenance mailbox first, as Close does.
func (w *writer) requestPass(ctx context.Context, final bool) error {
	w.mu.Lock()
	target, want := w.seq, w.passStarted+1
	if final {
		w.finalRequested = true
	}
	w.mu.Unlock()
	select {
	case w.kick <- struct{}{}:
	default:
	}
	for {
		w.mu.Lock()
		state, done, passErr, passChanged := w.state, w.passDone, w.passErr, w.passChanged
		w.mu.Unlock()
		if done >= want {
			if passErr != nil && state.committed < target {
				return passErr
			}
			return nil
		}
		if state.status.final() {
			return w.statusError(state.status)
		}
		select {
		case <-passChanged:
		case <-state.changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// commitPass applies the maintenance command the poller has fetched, if any,
// then commits the queued memtables oldest first, each attempt under its own
// deadline (commitAttemptTimeout). A memtable that fails to commit stays at
// the head of the queue; the next attempt reconciles it, so a commit applied
// before its response was lost, or after its attempt timed out, is found, not
// repeated. Only losing the fence is final. A maintenance failure is reported
// and does not hold up commits. A pass never waits to read the maintenance
// mailbox, except Close's (final), which reads it once, bounded, so a command
// staged just before shutdown is not left behind. ctx is the committer's
// lifetime.
func (w *writer) commitPass(ctx context.Context, final bool) error {
	if writerStatus(w.statusNow.Load()) == writerFenced {
		return manifest.ErrFenced
	}
	if final {
		w.pollMaintenance(ctx)
	}
	if err := w.applyStagedMaintenance(ctx); err != nil {
		return err
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
			timeout := w.commitAttemptTimeout(pending)
			attemptCtx, cancel := context.WithTimeout(ctx, timeout)
			status, err := w.flushPending(attemptCtx, pending)
			// Whether the attempt ran out of time is read from its context,
			// not its error, which each storage client wraps its own way.
			timedOut := ctx.Err() == nil && errors.Is(attemptCtx.Err(), context.DeadlineExceeded)
			cancel()
			w.metrics.ObserveFlush(time.Since(start), err)
			if err != nil {
				if timedOut {
					// The writer's deadline, not the caller's: report it without
					// wrapping context.DeadlineExceeded.
					w.attemptTimeouts++
					err = fmt.Errorf("%w after %s: %v", ErrCommitTimeout, timeout, err)
				}
				w.mu.Lock()
				w.immQueue = append(toFlush[i:], w.immQueue...)
				w.mu.Unlock()
				return err
			}
			w.attemptTimeouts = 0
			w.mu.Lock()
			w.pendingMemtables--
			w.noteMemtableCommittedLocked()
			w.mu.Unlock()
			w.reportRecovery(&w.commitFailures)
			// Record the commit last: a waiter it wakes finds the memtable's
			// slot free, the oldest-uncommitted gauge advanced and any failure
			// run over, so a Put right after WaitCommitted is not refused for
			// space the commit has already freed.
			w.transition(pending.sstable.SeqHi, status)
		}
	}
}

const (
	// commitAttemptBase is the fixed part of an attempt's deadline: time for
	// updating CURRENT and for a small upload.
	commitAttemptBase = 30 * time.Second
	// minCommitUploadBytesPerSecond is the slowest upload an attempt allows
	// for: each MiB to upload adds a second to its deadline.
	minCommitUploadBytesPerSecond = 1 << 20
	// maxCommitAttemptTimeout caps how far timeouts in a row grow the
	// deadline.
	maxCommitAttemptTimeout = 10 * time.Minute
)

// commitAttemptTimeout is the deadline of one attempt to commit pending:
// uploading its SST and change batch and updating CURRENT. A request that
// hangs costs one attempt, which then fails, is reported and is retried.
//
// A timed-out attempt starts its upload again from the beginning, so on a
// link slower than minCommitUploadBytesPerSecond every attempt would time out.
// Each attempt in a row that runs out of time doubles the next one's
// deadline, up to maxCommitAttemptTimeout, until one has time to finish; a
// commit resets it.
func (w *writer) commitAttemptTimeout(pending *pendingFlush) time.Duration {
	bytes := pending.memtable.ApproxSize()
	if pending.changes != nil {
		bytes += pending.changes.bodySize
	}
	timeout := time.Duration(w.attemptTimeoutBase.Load()) + time.Duration(bytes/minCommitUploadBytesPerSecond)*time.Second
	return grownAttemptTimeout(timeout, w.attemptTimeouts)
}

// grownAttemptTimeout doubles timeout once per earlier timeout in a row, up
// to maxCommitAttemptTimeout; a timeout already above the cap is kept.
func grownAttemptTimeout(timeout time.Duration, timeouts int) time.Duration {
	if timeout <= 0 {
		return timeout
	}
	for range timeouts {
		if timeout >= maxCommitAttemptTimeout/2 {
			return max(timeout, maxCommitAttemptTimeout)
		}
		timeout *= 2
	}
	return timeout
}

// minMaintenancePollInterval keeps a tiny PollInterval from turning the
// poller into a loop of back-to-back mailbox reads.
const minMaintenancePollInterval = 10 * time.Millisecond

// maintenancePollTimeout bounds one read of maintenance/HEAD: a mailbox that
// hangs costs the poller this long, never a commit.
const maintenancePollTimeout = 5 * time.Second

// maintenanceApplyTimeout bounds applying a fetched command, which writes
// CURRENT: a hung apply delays the flush's data commit by at most this long.
const maintenanceApplyTimeout = 30 * time.Second

// maintenancePollLoop reads maintenance/HEAD every PollInterval, and at once
// when maintenance in this process stages a command, until Close. It keeps
// the mailbox read off the commit path: a flush applies what the poller has
// fetched and never waits on the mailbox itself.
func (w *writer) maintenancePollLoop() {
	defer close(w.pollerDone)
	ticker := time.NewTicker(max(w.opts.Maintenance.PollInterval, minMaintenancePollInterval))
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
		case <-w.maintenanceWake:
		}
		if writerStatus(w.statusNow.Load()) == writerFenced {
			return
		}
		w.pollMaintenance(w.ctx)
	}
}

// pollMaintenance reads maintenance/HEAD, bounded by maintenancePollTimeout,
// and hands a pending command to the next flush, nudging the flush loop to
// apply it. A failed read is reported and tried again at the next poll.
func (w *writer) pollMaintenance(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, maintenancePollTimeout)
	defer cancel()
	head, _, err := w.manifestLog.ReadMaintenanceHead(ctx)
	if err != nil {
		if parent.Err() == nil { // not our own shutdown or caller's deadline
			w.reportFailure(&w.maintenanceFailures, fmt.Errorf("read maintenance command: %w", err))
		}
		return
	}
	if head == nil || head.Pending == nil {
		w.reportRecovery(&w.maintenanceFailures)
		return
	}
	command := *head.Pending
	w.stagedMaintenance.Store(&command)
	select {
	case w.applyWake <- struct{}{}:
	default:
	}
}

// applyStagedMaintenance publishes the command the poller fetched, bounded by
// maintenanceApplyTimeout. A failure other than losing the fence or the
// caller's own deadline is reported, and the command stays for the next
// flush; the flush's data commit goes on either way.
func (w *writer) applyStagedMaintenance(ctx context.Context) error {
	command := w.stagedMaintenance.Load()
	if command == nil {
		return nil
	}
	applyCtx, cancel := context.WithTimeout(ctx, maintenanceApplyTimeout)
	defer cancel()
	_, err := w.manifestLog.ApplyMaintenanceCommand(applyCtx, *command)
	switch {
	case err == nil:
		w.stagedMaintenance.CompareAndSwap(command, nil)
		w.reportRecovery(&w.maintenanceFailures)
		return nil
	case isFenceError(err):
		w.transition(0, writerFenced)
		return fmt.Errorf("apply maintenance command: %w", err)
	case ctx.Err() != nil:
		// The caller's own cancellation or deadline, not a storage failure.
		return fmt.Errorf("apply maintenance command: %w", err)
	default:
		w.reportFailure(&w.maintenanceFailures, fmt.Errorf("apply maintenance command: %w", err))
		return nil
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
		// Its own goroutine: the caller may be the committer, and the
		// callback may call Close, which waits for it.
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

// flushPending uploads and commits one memtable. On success it returns the
// status the commit leaves the writer in, open or, for a commit reconciled
// after a successor took the fence, fenced; the caller records it, with the
// commit, once its own bookkeeping is done.
func (w *writer) flushPending(ctx context.Context, pending *pendingFlush) (writerStatus, error) {
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
			return 0, groupErr
		}
	} else {
		if needSST {
			result, err := buildSST(ctx)
			if err != nil {
				return 0, err
			}
			pending.sstable = &result.Meta
		}
		if needChangeBatch {
			result, err := buildChangeBatch(ctx)
			if err != nil {
				return 0, err
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
		return 0, fmt.Errorf("update manifest: %w", appendErr)
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

	slog.Debug("isledb: memtable flushed", "component", "writer", "sst_id", pending.sstable.ID,
		"commit_id", pending.commitID, "size", pending.sstable.Size, "epoch", pending.epoch)
	return status, nil
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

// commitLoop is the committer, from the writer's start until Close stops it
// or the writer loses its fence. It runs passes: when Flush or Close asks
// (kick), and on each flush tick while the writer is open. With a flush interval, it also applies a
// newly fetched maintenance command when the poller asks (applyWake), without
// committing data. A failed commit stays queued; ticks retry it after a delay
// that doubles while failures continue, up to maxFlushRetryDelay, and a Flush
// retries it at once, without waiting for the delay.
func (w *writer) commitLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	// Without a flush interval, nothing commits on its own: neither data nor a
	// fetched maintenance command, until Flush or Close.
	var tick <-chan time.Time
	var applyWake <-chan struct{}
	if w.opts.Flush.Interval > 0 {
		ticker := time.NewTicker(w.opts.Flush.Interval)
		defer ticker.Stop()
		tick = ticker.C
		applyWake = w.applyWake
	}

	interval := w.opts.Flush.Interval
	var delay time.Duration
	var retryAt time.Time
	for {
		pass := true
		select {
		case <-ctx.Done():
			return
		case <-w.kick:
		case <-tick:
			if writerStatus(w.statusNow.Load()) != writerOpen || time.Now().Before(retryAt) {
				continue
			}
		case <-applyWake:
			// Applies maintenance only, so its result says nothing about
			// commits: it leaves the backoff alone.
			pass = false
		}
		var err error
		if pass {
			err = w.runPass(ctx)
		} else if writerStatus(w.statusNow.Load()) != writerFenced {
			err = w.applyStagedMaintenance(ctx)
		}
		switch {
		case err == nil:
			if pass {
				delay, retryAt = 0, time.Time{}
			}
		case ctx.Err() != nil:
			return
		case isFenceError(err):
			slog.Error("isledb: writer fenced, stopping commits",
				"component", "writer", "epoch", w.epoch)
			w.transition(0, writerFenced)
			return
		case pass:
			w.reportFailure(&w.commitFailures, err)
			delay = min(max(2*delay, interval), maxFlushRetryDelay)
			retryAt = time.Now().Add(delay)
		}
	}
}

// runPass numbers a pass, runs it, and publishes its result to requestPass.
func (w *writer) runPass(ctx context.Context) error {
	w.mu.Lock()
	w.passStarted++
	id, final := w.passStarted, w.finalRequested
	w.finalRequested = false
	w.mu.Unlock()

	err := w.commitPass(ctx, final)
	if err != nil && ctx.Err() != nil {
		// Close stopped the committer: a Flush waiting for this pass learns
		// the writer closed, not a cancellation it never asked for.
		err = ErrWriterClosed
	}

	w.mu.Lock()
	w.passDone, w.passErr = id, err
	close(w.passChanged)
	w.passChanged = make(chan struct{})
	w.mu.Unlock()
	return err
}

// stopPoller stops the maintenance poller; it does not wait for it.
func (w *writer) stopPoller() {
	w.stopOnce.Do(func() {
		w.cancel()
		close(w.stopCh)
	})
}

// close finishes the writer. It stops accepting writes and asks the
// committer for one last pass, which commits what is pending under the
// committer's own deadlines; ctx bounds the wait. Then it stops the committer
// and the poller, ending any attempt in flight, and waits for both: after
// Close returns, nothing of the writer runs. The writer is finished either
// way. If the pass failed or ctx ended first, Close returns an error naming
// the writes not known to be committed; the last attempt may still land, so
// they are unknown, not lost. A later Close returns the same result.
func (w *writer) close(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	select {
	case w.closing <- struct{}{}:
	default:
		select {
		case w.closing <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err() // another Close is finishing the writer
		}
	}
	defer func() { <-w.closing }()

	switch writerStatus(w.statusNow.Load()) {
	case writerClosed:
		return w.closeErr
	case writerFenced:
		w.stop()
		return manifest.ErrFenced
	}

	w.transition(0, writerClosing)
	commitErr := w.requestPass(ctx, true) // true: read the maintenance mailbox once more
	w.stop()

	w.mu.Lock()
	committed, accepted := w.state.committed, w.seq
	w.mu.Unlock()
	if writerStatus(w.statusNow.Load()) == writerFenced {
		w.closeErr = manifest.ErrFenced
		return w.closeErr
	}
	if committed < accepted {
		w.closeErr = fmt.Errorf("isledb: writer closed with writes %d to %d not known to be committed",
			committed+1, accepted)
		if commitErr != nil {
			w.closeErr = fmt.Errorf("%w: %w", w.closeErr, commitErr)
		}
	}
	w.transition(0, writerClosed)
	return w.closeErr
}

// stop ends the poller and the committer, cancelling any read or attempt in
// flight, and waits for both to exit. The wait has no bound: every storage
// call runs under the cancelled context and returns promptly, and a bound
// would let Close return while the writer still runs.
func (w *writer) stop() {
	w.stopPoller()
	w.committerCancel()
	<-w.pollerDone
	<-w.committerDone
}

// finished reports whether the writer has stopped for good: closed or fenced.
func (w *writer) finished() bool {
	return writerStatus(w.statusNow.Load()).final()
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
