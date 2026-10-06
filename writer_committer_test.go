package isledb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// hangingCurrentStorage makes the conditional write of CURRENT hang while
// hang is set: before storage applies it, or, with applyFirst, after, as when
// the request lands and its response never comes. A hung write returns when
// its context ends or hang is cleared.
type hangingCurrentStorage struct {
	manifest.Storage
	hang       atomic.Bool
	applyFirst atomic.Bool
	opaque     atomic.Bool  // a hung write fails with its own error, not ctx.Err()
	hung       atomic.Int64 // writes that hung
	applied    atomic.Int64 // writes that reached storage

	mu        sync.Mutex
	deadlines []time.Duration // time each hung write had left when it arrived
}

func (s *hangingCurrentStorage) hungDeadlines() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.deadlines...)
}

func (s *hangingCurrentStorage) WriteCurrentCAS(ctx context.Context, data []byte, etag string) (string, error) {
	if s.hang.Load() {
		if deadline, ok := ctx.Deadline(); ok {
			s.mu.Lock()
			s.deadlines = append(s.deadlines, time.Until(deadline))
			s.mu.Unlock()
		}
	}
	if !s.hang.Load() {
		s.applied.Add(1)
		return s.Storage.WriteCurrentCAS(ctx, data, etag)
	}
	var newETag string
	if s.applyFirst.Load() {
		var err error
		if newETag, err = s.Storage.WriteCurrentCAS(context.Background(), data, etag); err != nil {
			return "", err
		}
		s.applied.Add(1)
	}
	s.hung.Add(1)
	for s.hang.Load() {
		select {
		case <-ctx.Done():
			if s.opaque.Load() {
				return "", errors.New("storage client: request timed out")
			}
			return "", ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	if s.applyFirst.Load() {
		return newETag, nil
	}
	s.applied.Add(1)
	return s.Storage.WriteCurrentCAS(ctx, data, etag)
}

func newHangingCommitWriter(t *testing.T, opts WriterOptions, attemptTimeout time.Duration) (*writer, *hangingCurrentStorage, *manifest.Store, chan error) {
	t.Helper()
	ctx := context.Background()
	store := blobstore.NewMemory(t.Name())
	t.Cleanup(func() { store.Close() })
	storage := &hangingCurrentStorage{Storage: manifest.NewBlobStoreBackend(store)}
	manifestStore := manifest.NewStoreWithStorage(storage)
	reports := make(chan error, 64)
	opts.OnFlushError = func(err error) {
		select {
		case reports <- err:
		default:
		}
	}
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	w.attemptTimeoutBase.Store(int64(attemptTimeout))
	// Runs before the writer's Close, so a failure does not leave Close
	// waiting on a hung write.
	t.Cleanup(func() {
		storage.hang.Store(false)
		closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = w.close(closeCtx)
	})
	return w, storage, manifestStore, reports
}

func awaitCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestWriterHungCommitTimesOutAndRecovers hangs the background commit's write
// of CURRENT. The attempt gives up at its deadline, the failure is reported,
// and once storage answers again the next attempt commits.
func TestWriterHungCommitTimesOutAndRecovers(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 20 * time.Millisecond
	w, storage, _, reports := newHangingCommitWriter(t, opts, 200*time.Millisecond)

	storage.hang.Store(true)
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	select {
	case err := <-reports:
		if !errors.Is(err, ErrCommitTimeout) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("reported %v, want ErrCommitTimeout, not a context deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a hung commit was never reported")
	}
	if got := w.committed.Load(); got >= seq {
		t.Fatalf("committed %d while every write of CURRENT hangs", got)
	}

	storage.hang.Store(false)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.waitCommitted(waitCtx, seq); err != nil {
		t.Fatalf("the writer did not recover once storage answered: %v", err)
	}
}

// TestWriterFlushHonorsDeadlineWhileCommitHangs: a Flush whose deadline
// passes while a commit hangs returns at the deadline, not when the commit's
// own, much longer, deadline passes.
func TestWriterFlushHonorsDeadlineWhileCommitHangs(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 20 * time.Millisecond
	w, storage, _, _ := newHangingCommitWriter(t, opts, time.Minute)

	storage.hang.Store(true)
	if _, err := w.put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	awaitCondition(t, "the background commit hangs", func() bool { return storage.hung.Load() > 0 })

	flushCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	flushed := make(chan error, 1)
	go func() { flushed <- w.flush(flushCtx) }()
	select {
	case err := <-flushed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Flush = %v, want its deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Flush did not return within a second; its deadline was 100ms")
	}
}

// TestWriterCancelledFlushDoesNotAbortCommit: Flush's context bounds only its
// wait. A Flush that gives up leaves its commit going, and the commit lands
// with no further Flush, even without a flush interval.
func TestWriterCancelledFlushDoesNotAbortCommit(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, storage, _, reports := newHangingCommitWriter(t, opts, time.Minute)

	storage.hang.Store(true)
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	flushCtx, cancel := context.WithCancel(ctx)
	flushed := make(chan error, 1)
	go func() { flushed <- w.flush(flushCtx) }()
	awaitCondition(t, "the commit hangs", func() bool { return storage.hung.Load() > 0 })
	cancel()
	if err := <-flushed; !errors.Is(err, context.Canceled) {
		t.Fatalf("Flush = %v, want its cancellation", err)
	}

	storage.hang.Store(false)
	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	if err := w.waitCommitted(waitCtx, seq); err != nil {
		t.Fatalf("the commit did not go on after Flush gave up: %v", err)
	}
	assertNoReport(t, reports)
}

// TestWriterCloseReturnsAtDeadlineAndFinishes: a Close whose deadline passes
// while its commit hangs returns at the deadline, naming the write not known
// to be committed. The writer is finished: nothing of it runs, nothing
// commits after, and a second Close returns the same result.
func TestWriterCloseReturnsAtDeadlineAndFinishes(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, storage, _, _ := newHangingCommitWriter(t, opts, time.Minute)

	storage.hang.Store(true)
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	closeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- w.close(closeCtx) }()
	var closeErr error
	select {
	case closeErr = <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s; its deadline was 100ms")
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) || !strings.Contains(closeErr.Error(), "not known to be committed") {
		t.Fatalf("Close = %v, want its deadline, naming the uncommitted write", closeErr)
	}
	select {
	case <-w.committerDone:
	default:
		t.Fatal("the committer is still running after Close returned")
	}
	select {
	case <-w.pollerDone:
	default:
		t.Fatal("the poller is still running after Close returned")
	}
	if status := writerStatus(w.statusNow.Load()); status != writerClosed {
		t.Fatalf("status after Close = %d, want closed", status)
	}
	if _, err := w.put(ctx, []byte("b"), []byte("2")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("put after Close = %v, want ErrWriterClosed", err)
	}
	if err := w.waitCommitted(ctx, seq); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("WaitCommitted = %v, want ErrWriterClosed", err)
	}

	storage.hang.Store(false)
	time.Sleep(50 * time.Millisecond)
	if got := w.committed.Load(); got >= seq {
		t.Fatalf("committed %d after the writer finished", got)
	}
	if again := w.close(ctx); again != closeErr {
		t.Fatalf("second Close = %v, want %v", again, closeErr)
	}
}

// TestWriterFlushDuringFailedCloseLearnsWriterClosed: a Flush waiting when a
// Close gives up learns the writer closed, not a cancellation it never asked
// for, and does not wait any longer.
func TestWriterFlushDuringFailedCloseLearnsWriterClosed(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, storage, _, _ := newHangingCommitWriter(t, opts, time.Minute)

	storage.hang.Store(true)
	if _, err := w.put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- w.flush(ctx) }()
	awaitCondition(t, "the commit hangs", func() bool { return storage.hung.Load() > 0 })

	closeCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_ = w.close(closeCtx)
	select {
	case err := <-flushed:
		if !errors.Is(err, ErrWriterClosed) || errors.Is(err, context.Canceled) {
			t.Fatalf("Flush = %v, want ErrWriterClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Flush still waiting after Close finished the writer")
	}
}

// TestWriterTimedOutCommitThatLandedIsNotRepeated: an attempt's write of
// CURRENT lands but its response never comes, so the attempt times out. The
// retry finds the commit already in CURRENT and records it; the memtable is
// committed once.
func TestWriterTimedOutCommitThatLandedIsNotRepeated(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 20 * time.Millisecond
	w, storage, manifestStore, reports := newHangingCommitWriter(t, opts, 200*time.Millisecond)

	storage.applyFirst.Store(true)
	storage.hang.Store(true)
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	select {
	case err := <-reports:
		if !errors.Is(err, ErrCommitTimeout) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("reported %v, want ErrCommitTimeout, not a context deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the attempt never timed out")
	}
	if storage.applied.Load() == 0 {
		t.Fatal("the hung write never reached storage")
	}

	storage.hang.Store(false)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.waitCommitted(waitCtx, seq); err != nil {
		t.Fatalf("WaitCommitted: %v", err)
	}
	m, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if ids := m.AllSSTIDs(); len(ids) != 1 {
		t.Fatalf("the memtable was committed as %d SSTs %v, want 1", len(ids), ids)
	}
}

func TestGrownAttemptTimeout(t *testing.T) {
	for _, tc := range []struct {
		timeout  time.Duration
		timeouts int
		want     time.Duration
	}{
		{30 * time.Second, 0, 30 * time.Second},
		{30 * time.Second, 1, time.Minute},
		{30 * time.Second, 3, 4 * time.Minute},
		{30 * time.Second, 5, maxCommitAttemptTimeout},
		{30 * time.Second, 1000, maxCommitAttemptTimeout},
		{20 * time.Minute, 0, 20 * time.Minute},
		{20 * time.Minute, 3, 20 * time.Minute},
		{0, 1000, 0},
	} {
		if got := grownAttemptTimeout(tc.timeout, tc.timeouts); got != tc.want {
			t.Errorf("grownAttemptTimeout(%s, %d) = %s, want %s", tc.timeout, tc.timeouts, got, tc.want)
		}
	}
}

// TestWriterAttemptDeadlineGrowsWhileAttemptsTimeOut: each attempt that runs
// out of time doubles the next one's deadline, so a link too slow for the
// first deadline still gets a commit through; a commit resets it.
func TestWriterAttemptDeadlineGrowsWhileAttemptsTimeOut(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 10 * time.Millisecond
	// Large enough that the upload before the write of CURRENT, which the
	// measured deadlines do not include, is small beside it.
	base := 200 * time.Millisecond
	w, storage, _, _ := newHangingCommitWriter(t, opts, base)

	storage.hang.Store(true)
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	awaitCondition(t, "three attempts time out", func() bool { return len(storage.hungDeadlines()) >= 3 })
	deadlines := storage.hungDeadlines()
	for i := 1; i < 3; i++ {
		if deadlines[i] < deadlines[i-1]*3/2 {
			t.Fatalf("attempt deadlines %v: attempt %d did not get about twice the time of the one before", deadlines[:3], i+1)
		}
	}

	storage.hang.Store(false)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.waitCommitted(waitCtx, seq); err != nil {
		t.Fatalf("WaitCommitted: %v", err)
	}

	// The commit reset the growth: the next hung attempt gets the base again.
	before := len(storage.hungDeadlines())
	storage.hang.Store(true)
	if _, err := w.put(ctx, []byte("b"), []byte("2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	awaitCondition(t, "the next commit hangs", func() bool { return len(storage.hungDeadlines()) > before })
	if got := storage.hungDeadlines()[before]; got > base {
		t.Fatalf("deadline after a commit = %s, want at most the base %s", got, base)
	}
}

// TestWriterTimeoutSeenWhateverTheStorageError: a storage client that fails a
// timed-out request with its own error, not wrapping the context's, still has
// the attempt counted as timed out: reported as ErrCommitTimeout, with the
// next deadline doubled.
func TestWriterTimeoutSeenWhateverTheStorageError(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 10 * time.Millisecond
	w, storage, _, reports := newHangingCommitWriter(t, opts, 200*time.Millisecond)

	storage.opaque.Store(true)
	storage.hang.Store(true)
	if _, err := w.put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	select {
	case err := <-reports:
		if !errors.Is(err, ErrCommitTimeout) {
			t.Fatalf("reported %v, want ErrCommitTimeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the timed-out attempt was never reported")
	}
	awaitCondition(t, "a second attempt hangs", func() bool { return len(storage.hungDeadlines()) >= 2 })
	if d := storage.hungDeadlines(); d[1] < d[0]*3/2 {
		t.Fatalf("attempt deadlines %v: the second did not grow", d[:2])
	}
}

// TestWriterStopWritesThenDrain: after StopWrites, mutations are refused with
// ErrWritesStopped, but Flush still commits what was accepted, and Close then
// finds nothing pending.
func TestWriterStopWritesThenDrain(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, _, _, _ := newHangingCommitWriter(t, opts, time.Minute)

	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	w.stopWrites()
	if _, err := w.put(ctx, []byte("b"), []byte("2")); !errors.Is(err, ErrWritesStopped) {
		t.Fatalf("put after StopWrites = %v, want ErrWritesStopped", err)
	}
	if _, err := w.delete(ctx, []byte("a")); !errors.Is(err, ErrWritesStopped) {
		t.Fatalf("delete after StopWrites = %v, want ErrWritesStopped", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("Flush after StopWrites: %v", err)
	}
	if err := w.waitCommitted(ctx, seq); err != nil {
		t.Fatalf("WaitCommitted: %v", err)
	}
	if err := w.close(ctx); err != nil {
		t.Fatalf("Close after draining: %v", err)
	}
	if _, err := w.put(ctx, []byte("c"), []byte("3")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("put after Close = %v, want ErrWriterClosed", err)
	}
}

// TestWriterStopWritesDrainsInBackground: with a flush interval, a writer
// that stopped taking writes commits the ones it has by itself.
func TestWriterStopWritesDrainsInBackground(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 10 * time.Millisecond
	w, storage, _, _ := newHangingCommitWriter(t, opts, 200*time.Millisecond)

	storage.hang.Store(true) // nothing commits before StopWrites
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	w.stopWrites()
	storage.hang.Store(false)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.waitCommitted(waitCtx, seq); err != nil {
		t.Fatalf("the stopped writer did not commit in the background: %v", err)
	}
}

// TestWriterWaiterKeepsWaitingAcrossStopWrites: StopWrites is not the end of
// the writer, so a waiter is not told it closed; it learns its write
// committed once a Flush gets it through.
func TestWriterWaiterKeepsWaitingAcrossStopWrites(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, storage, _, _ := newHangingCommitWriter(t, opts, 100*time.Millisecond)

	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	done := waitAsync(ctx, w, seq)
	storage.hang.Store(true)
	w.stopWrites()
	if err := w.flush(ctx); !errors.Is(err, ErrCommitTimeout) {
		t.Fatalf("Flush with storage hanging = %v, want ErrCommitTimeout", err)
	}
	assertWaiting(t, done)

	storage.hang.Store(false)
	if err := w.flush(ctx); err != nil {
		t.Fatalf("Flush once storage answers: %v", err)
	}
	if err := awaitResult(t, done); err != nil {
		t.Fatalf("WaitCommitted across StopWrites: %v", err)
	}
}

// TestWriterDrainWithoutFlushInterval: Drain commits everything accepted
// although nothing commits on its own, and refuses later writes.
func TestWriterDrainWithoutFlushInterval(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, _, _, _ := newHangingCommitWriter(t, opts, time.Minute)

	for i := range 10 {
		if _, err := w.put(ctx, fmt.Appendf(nil, "k%02d", i), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	accepted := w.acceptedSequence()
	if err := w.drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got := w.committed.Load(); got != accepted {
		t.Fatalf("committed %d after Drain, want everything accepted, %d", got, accepted)
	}
	if _, err := w.put(ctx, []byte("late"), []byte("v")); !errors.Is(err, ErrWritesStopped) {
		t.Fatalf("put after Drain = %v, want ErrWritesStopped", err)
	}
}

// TestWriterDrainDuringOutage: with every write of CURRENT hanging, Drain
// returns at its deadline, with the last commit failure; Close names exactly
// the writes not known to be committed; and the next writer continues the
// sequence from what was committed.
func TestWriterDrainDuringOutage(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, storage, manifestStore, _ := newHangingCommitWriter(t, opts, 100*time.Millisecond)

	first, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	storage.hang.Store(true)
	for _, k := range []string{"b", "c", "d"} {
		if _, err := w.put(ctx, []byte(k), []byte("2")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	drainCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err = w.drain(drainCtx)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "commit attempt timed out") {
		t.Fatalf("Drain during the outage = %v, want its deadline with the last commit failure", err)
	}
	closeCtx, cancelClose := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelClose()
	closeErr := w.close(closeCtx)
	want := fmt.Sprintf("writes %d to %d not known to be committed", first+1, first+3)
	if closeErr == nil || !strings.Contains(closeErr.Error(), want) {
		t.Fatalf("Close = %v, want it to name %q", closeErr, want)
	}

	storage.hang.Store(false)
	next, err := newWriter(ctx, w.store, manifestStore, testWriterOptions(1<<20, 16))
	if err != nil {
		t.Fatalf("open the next writer: %v", err)
	}
	defer next.close(ctx)
	seq, err := next.put(ctx, []byte("b"), []byte("2"))
	if err != nil {
		t.Fatalf("put on the next writer: %v", err)
	}
	if seq != first+1 {
		t.Fatalf("the next writer's first sequence = %d, want %d, after the last committed", seq, first+1)
	}
}

// TestWriterPutRacingDrain: writes race a Drain. Every write that got a
// sequence is committed and readable; every write refused with
// ErrWritesStopped is in no SST.
func TestWriterPutRacingDrain(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory(t.Name())
	defer store.Close()
	opts := testWriterOptions(64<<10, 64)
	opts.Flush.Interval = 5 * time.Millisecond
	w, err := newWriter(ctx, store, newManifestStore(store, nil), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	var mu sync.Mutex
	accepted, refused := map[string]bool{}, map[string]bool{}
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				key := fmt.Sprintf("g%d-%05d", g, i)
				_, err := w.put(ctx, []byte(key), []byte(key))
				mu.Lock()
				switch {
				case err == nil:
					accepted[key] = true
				case errors.Is(err, ErrWritesStopped):
					refused[key] = true
				default:
					t.Errorf("put %s: %v", key, err)
				}
				mu.Unlock()
				if err != nil {
					return
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	if err := w.drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	wg.Wait()
	if got, want := w.committed.Load(), w.acceptedSequence(); got != want {
		t.Fatalf("committed %d after Drain, want everything accepted, %d", got, want)
	}
	if err := w.close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader := openReaderFromDBForTest(t, ctx, store, ReaderOpenOptions{CacheDir: t.TempDir()})
	defer reader.Close()
	rows, err := reader.Scan(ctx, nil, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	visible := map[string]bool{}
	for _, row := range rows {
		visible[string(row.Key)] = true
	}
	if len(refused) != 4 {
		t.Fatalf("%d writers were refused, want all 4", len(refused))
	}
	for key := range accepted {
		if !visible[key] {
			t.Errorf("accepted %s is not readable after Drain", key)
		}
	}
	for key := range refused {
		if visible[key] {
			t.Errorf("refused %s is readable", key)
		}
	}
	if len(visible) != len(accepted) {
		t.Fatalf("%d keys readable, want the %d accepted", len(visible), len(accepted))
	}
	t.Logf("accepted %d writes before Drain", len(accepted))
}

// TestWriterFencedDuringDrain: another writer takes the fence while Drain
// waits on a hung commit. Drain returns ErrFenced once the commit answers,
// and the writes it was draining are not in the database.
func TestWriterFencedDuringDrain(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, storage, _, _ := newHangingCommitWriter(t, opts, time.Minute)

	if _, err := w.put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	storage.hang.Store(true)
	drained := make(chan error, 1)
	go func() { drained <- w.drain(ctx) }()
	awaitCondition(t, "the drain's commit hangs", func() bool { return storage.hung.Load() > 0 })

	successor := manifest.NewStoreWithStorage(storage.Storage)
	if _, err := successor.ClaimWriter(ctx, "successor"); err != nil {
		t.Fatalf("successor claim: %v", err)
	}
	storage.hang.Store(false)
	select {
	case err := <-drained:
		if !errors.Is(err, manifest.ErrFenced) {
			t.Fatalf("Drain = %v, want ErrFenced", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not return after the fence")
	}
	m, err := successor.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if ids := m.AllSSTIDs(); len(ids) != 0 {
		t.Fatalf("the fenced writer's drain landed SSTs %v", ids)
	}
}
