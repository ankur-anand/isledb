package isledb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus"
)

// countingCurrentWriteStorage fails conditional writes of CURRENT, without
// applying them, while failing is set, and counts the attempts.
type countingCurrentWriteStorage struct {
	manifest.Storage
	failing  atomic.Bool
	attempts atomic.Int64
}

var errCurrentUnavailable = errors.New("CURRENT unavailable")

func (s *countingCurrentWriteStorage) WriteCurrentCAS(ctx context.Context, data []byte, etag string) (string, error) {
	if s.failing.Load() {
		s.attempts.Add(1)
		return "", errCurrentUnavailable
	}
	return s.Storage.WriteCurrentCAS(ctx, data, etag)
}

// TestWriterRetriesUntilCommitted fails every background commit for a while:
// the writer stays open, retries with a growing delay, reports the failure
// run once, bounds memory with backpressure, and commits everything once
// storage recovers.
func TestWriterRetriesUntilCommitted(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-retries-until-committed")
	defer store.Close()
	storage := &countingCurrentWriteStorage{Storage: manifest.NewBlobStoreBackend(store)}
	reports := make(chan error, 16)
	opts := testWriterOptions(1<<20, 1)
	opts.Flush.Interval = time.Millisecond
	opts.OnFlushError = func(err error) { reports <- err }
	w, err := newWriter(ctx, store, manifest.NewStoreWithStorage(storage), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)
	// One write fills a memtable; an empty one is not full.
	w.mu.Lock()
	w.opts.Memtable.TargetBytes = w.memtable.ApproxSize() + 1
	w.mu.Unlock()

	storage.failing.Store(true)
	first, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	done := waitAsync(ctx, w, first)
	select {
	case err := <-reports:
		if !errors.Is(err, errCurrentUnavailable) {
			t.Fatalf("reported %v, want %v", err, errCurrentUnavailable)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the failed commit was not reported")
	}

	// The failed memtable holds the only pending slot: one more write fills
	// the active memtable, and the next is refused rather than buffered.
	if _, err := w.put(ctx, []byte("b"), []byte("2")); err != nil {
		t.Fatalf("put into the active memtable: %v", err)
	}
	if _, err := w.put(ctx, []byte("c"), []byte("3")); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("put with commits failing err=%v, want %v", err, ErrBackpressure)
	}

	time.Sleep(300 * time.Millisecond)
	// At a fixed 1ms interval this would be about 300 attempts; doubling the
	// delay makes it about log2(300).
	if got := storage.attempts.Load(); got < 3 || got > 20 {
		t.Fatalf("commit attempts in 300ms=%d, want a few retries with backoff", got)
	}
	select {
	case err := <-reports:
		t.Fatalf("one failure run reported more than once: %v", err)
	default:
	}
	assertWaiting(t, done)
	if got := writerStatus(w.statusNow.Load()); got != writerOpen {
		t.Fatalf("status while commits fail=%v, want open", got)
	}

	storage.failing.Store(false)
	if err := awaitResult(t, done); err != nil {
		t.Fatalf("WaitCommitted after storage recovered: %v", err)
	}
	last, err := w.put(ctx, []byte("c"), []byte("3"))
	if err != nil {
		t.Fatalf("put after recovery: %v", err)
	}
	if err := awaitResult(t, waitAsync(ctx, w, last)); err != nil {
		t.Fatalf("WaitCommitted after recovery: %v", err)
	}
	if w.commitFailures.failing() {
		t.Fatal("the failure run did not end after commits recovered")
	}
}

// TestWriterOldestUncommittedGauge reports when the oldest uncommitted write
// was accepted: the first write of each memtable dates it, it advances as
// memtables commit oldest first, and it is 0 once everything is committed.
func TestWriterOldestUncommittedGauge(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-oldest-uncommitted")
	defer store.Close()
	gauge := &recordingGauge{Gauge: prometheus.NewGauge(prometheus.GaugeOpts{Name: "oldest_uncommitted"})}
	// Each memtable holds one write: the second write freezes the first.
	opts := testWriterOptions(1, 16)
	opts.Metrics = &WriterMetrics{OldestUncommitted: gauge}
	w, err := newWriter(ctx, store, newManifestStore(store, nil), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	before := float64(time.Now().UnixNano()) / 1e9
	if _, err := w.put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := w.put(ctx, []byte("b"), []byte("2")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	got := gauge.recorded()
	if len(got) != 1 || got[0] < before {
		t.Fatalf("gauge before commit=%v, want the first write's time (>= %v)", got, before)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	got = gauge.recorded()
	if len(got) != 3 || !(got[1] > got[0]) || got[2] != 0 {
		t.Fatalf("gauge through the commit=%v, want [first write, second write, 0]", got)
	}
}

// toggleMaintenanceStorage reads maintenance/HEAD normally, fails it, or
// blocks it until the context ends, and counts reads while not normal.
type toggleMaintenanceStorage struct {
	manifest.Storage
	mode  atomic.Int32 // maintenanceOK, maintenanceFail or maintenanceBlock
	reads atomic.Int64
}

const (
	maintenanceOK int32 = iota
	maintenanceFail
	maintenanceBlock
)

var errMaintenanceUnavailable = errors.New("maintenance HEAD unavailable")

func (s *toggleMaintenanceStorage) ReadMaintenanceHead(ctx context.Context) ([]byte, string, error) {
	switch s.mode.Load() {
	case maintenanceFail:
		s.reads.Add(1)
		return nil, "", errMaintenanceUnavailable
	case maintenanceBlock:
		s.reads.Add(1)
		<-ctx.Done()
		return nil, "", ctx.Err()
	}
	return s.Storage.ReadMaintenanceHead(ctx)
}

func newToggleMaintenanceWriter(t *testing.T, ctx context.Context, opts WriterOptions) (*writer, *toggleMaintenanceStorage, chan error) {
	t.Helper()
	store := blobstore.NewMemory(t.Name())
	t.Cleanup(func() { store.Close() })
	storage := &toggleMaintenanceStorage{Storage: manifest.NewBlobStoreBackend(store)}
	reports := make(chan error, 16)
	opts.OnFlushError = func(err error) { reports <- err }
	w, err := newWriter(ctx, store, manifest.NewStoreWithStorage(storage), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	t.Cleanup(func() { _ = w.close(ctx) })
	return w, storage, reports
}

func awaitReport(t *testing.T, reports <-chan error, want error) {
	t.Helper()
	select {
	case err := <-reports:
		if !errors.Is(err, want) {
			t.Fatalf("reported %v, want %v", err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%v was not reported", want)
	}
}

func assertNoReport(t *testing.T, reports <-chan error) {
	t.Helper()
	select {
	case err := <-reports:
		t.Fatalf("unexpected report: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestWriterMaintenanceWakeKeepsCommitBackoff wakes the writer for
// maintenance while commits fail: a wake applies maintenance only, so it
// neither resets the commit backoff nor ends the failure run.
func TestWriterMaintenanceWakeKeepsCommitBackoff(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-maintenance-wake-backoff")
	defer store.Close()
	storage := &countingCurrentWriteStorage{Storage: manifest.NewBlobStoreBackend(store)}
	reports := make(chan error, 16)
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = time.Millisecond
	opts.OnFlushError = func(err error) { reports <- err }
	wake := make(chan struct{}, 1)
	w, err := newWriterWithMaintenanceWake(ctx, store, manifest.NewStoreWithStorage(storage), opts, wake,
		StorePolicy{MaxPinnedViewAge: DefaultMaxPinnedViewAge}, DefaultSSTOutputOptions().L0)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	storage.failing.Store(true)
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	done := waitAsync(ctx, w, seq)
	awaitReport(t, reports, errCurrentUnavailable)

	for deadline := time.Now().Add(300 * time.Millisecond); time.Now().Before(deadline); {
		select {
		case wake <- struct{}{}:
		default:
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := storage.attempts.Load(); got > 20 {
		t.Fatalf("commit attempts in 300ms of wakes=%d: a wake reset the backoff", got)
	}
	if !w.commitFailures.failing() {
		t.Fatal("a maintenance wake ended the commit failure run")
	}
	assertNoReport(t, reports)

	storage.failing.Store(false)
	if err := awaitResult(t, done); err != nil {
		t.Fatalf("WaitCommitted after recovery: %v", err)
	}
	if w.commitFailures.failing() {
		t.Fatal("the commit failure run did not end after a commit")
	}
}

// TestWriterMaintenanceFailureRunEnds fails maintenance, recovers it, and
// fails it again: a successful poll ends the run, so the next failure is
// reported as a new one.
func TestWriterMaintenanceFailureRunEnds(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Maintenance.PollInterval = time.Nanosecond
	w, storage, reports := newToggleMaintenanceWriter(t, ctx, opts)

	storage.mode.Store(maintenanceFail)
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush with maintenance failing: %v", err)
	}
	awaitReport(t, reports, errMaintenanceUnavailable)
	if !w.maintenanceFailures.failing() || w.commitFailures.failing() {
		t.Fatal("a maintenance failure was not recorded as a maintenance run")
	}

	storage.mode.Store(maintenanceOK)
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if w.maintenanceFailures.failing() {
		t.Fatal("a successful poll did not end the maintenance failure run")
	}

	storage.mode.Store(maintenanceFail)
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush with maintenance failing again: %v", err)
	}
	awaitReport(t, reports, errMaintenanceUnavailable)
}

// TestWriterFailedMaintenancePollBacksOff fails every maintenance poll with
// a fast flush interval: the mailbox is read once per poll interval, not on
// every tick.
func TestWriterFailedMaintenancePollBacksOff(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = time.Millisecond
	opts.Maintenance.PollInterval = 100 * time.Millisecond
	_, storage, reports := newToggleMaintenanceWriter(t, ctx, opts)

	storage.mode.Store(maintenanceFail)
	time.Sleep(350 * time.Millisecond)
	if got := storage.reads.Load(); got < 2 || got > 6 {
		t.Fatalf("failing maintenance reads in 350ms=%d, want about one per 100ms", got)
	}
	awaitReport(t, reports, errMaintenanceUnavailable)
	assertNoReport(t, reports)
}

// TestWriterFlushDeadlineIsNotReported runs out a Flush's own deadline
// during the maintenance poll: Flush returns it, and it is not reported as
// a writer failure.
func TestWriterFlushDeadlineIsNotReported(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Maintenance.PollInterval = time.Nanosecond
	w, storage, reports := newToggleMaintenanceWriter(t, ctx, opts)

	storage.mode.Store(maintenanceBlock)
	flushCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := w.flush(flushCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("flush err=%v, want %v", err, context.DeadlineExceeded)
	}
	if storage.reads.Load() == 0 {
		t.Fatal("the flush did not reach the maintenance poll")
	}
	assertNoReport(t, reports)
	if w.maintenanceFailures.failing() {
		t.Fatal("the caller's deadline started a maintenance failure run")
	}
	storage.mode.Store(maintenanceOK)
}
