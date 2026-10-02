package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus"
)

// recordingGauge records every value set on it.
type recordingGauge struct {
	prometheus.Gauge
	mu     sync.Mutex
	values []float64
}

func (g *recordingGauge) Set(v float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.values = append(g.values, v)
}

func (g *recordingGauge) recorded() []float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]float64(nil), g.values...)
}

func (g *recordingGauge) last() float64 {
	values := g.recorded()
	if len(values) == 0 {
		return -1
	}
	return values[len(values)-1]
}

func newCommittedSequenceWriter(t *testing.T, ctx context.Context, store *blobstore.Store, memtableBytes int64) (*writer, *recordingGauge) {
	t.Helper()
	gauge := &recordingGauge{Gauge: prometheus.NewGauge(prometheus.GaugeOpts{Name: "committed_sequence"})}
	opts := testWriterOptions(memtableBytes, 16)
	opts.Metrics = &WriterMetrics{CommittedSequence: gauge}
	w, err := newWriter(ctx, store, newManifestStore(store, nil), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	return w, gauge
}

// TestWriterCommittedSequencePerMemtable flushes several memtables at once:
// the gauge advances one committed memtable at a time, to that memtable's last
// sequence, never to mutations still in memory, and ends at the last one.
func TestWriterCommittedSequencePerMemtable(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-committed-sequence")
	defer store.Close()
	w, gauge := newCommittedSequenceWriter(t, ctx, store, 1<<10)
	defer w.close(ctx)

	const puts = 60
	value := make([]byte, 100)
	for i := range puts {
		if _, err := w.put(ctx, kvLeveledBenchmarkKey(i), value); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	w.mu.Lock()
	frozen := len(w.immQueue)
	w.mu.Unlock()
	if frozen < 2 {
		t.Fatalf("%d frozen memtables before flush, want several", frozen)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	values := gauge.recorded()
	if len(values) < 3 || values[0] != 0 {
		t.Fatalf("gauge values %v, want 0 at open then one per committed memtable", values)
	}
	for i := 1; i < len(values); i++ {
		if values[i] <= values[i-1] {
			t.Fatalf("gauge values %v do not rise one memtable at a time", values)
		}
	}
	if values[1] >= puts {
		t.Fatalf("first commit reported %v, the writer's counter rather than its memtable's last sequence", values[1])
	}
	if got := values[len(values)-1]; got != puts {
		t.Fatalf("gauge after flush = %v, want %d", got, puts)
	}
}

// TestWriterCommittedSequenceAfterFailover opens a second writer after the
// first committed: it reports the committed position before committing
// anything, so the series continues across failover.
func TestWriterCommittedSequenceAfterFailover(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-committed-sequence-failover")
	defer store.Close()
	first, _ := newCommittedSequenceWriter(t, ctx, store, 1<<20)
	for i := range 5 {
		if _, err := first.put(ctx, kvLeveledBenchmarkKey(i), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := first.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	second, gauge := newCommittedSequenceWriter(t, ctx, store, 1<<20)
	defer second.close(ctx)
	if got := gauge.recorded(); len(got) != 1 || got[0] != 5 {
		t.Fatalf("new writer's gauge before any commit = %v, want [5]", got)
	}
	_ = first.close(ctx)
}

// TestWriterCommittedSequenceUnchangedByFailedCommit fences a writer holding
// buffered mutations: its flush fails, and the gauge stays at the position
// committed before.
func TestWriterCommittedSequenceUnchangedByFailedCommit(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-committed-sequence-fenced")
	defer store.Close()
	w, gauge := newCommittedSequenceWriter(t, ctx, store, 1<<20)
	if _, err := w.put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for _, key := range []string{"b", "c"} {
		if _, err := w.put(ctx, []byte(key), []byte("2")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	successor, _ := newCommittedSequenceWriter(t, ctx, store, 1<<20) // fences w
	defer successor.close(ctx)

	if err := w.flush(ctx); err == nil {
		t.Fatal("flush of a fenced writer succeeded")
	}
	if got := gauge.last(); got != 1 {
		t.Fatalf("gauge after a failed commit = %v, want 1", got)
	}
	_ = w.close(ctx)
}

// TestWriterMutationsReturnSequences checks Put, PutWithTTL and Delete return
// one increasing sequence per mutation.
func TestWriterMutationsReturnSequences(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-mutation-sequences")
	defer store.Close()
	w, _ := newCommittedSequenceWriter(t, ctx, store, 1<<20)
	defer w.close(ctx)

	var got []uint64
	for _, mutate := range []func() (uint64, error){
		func() (uint64, error) { return w.put(ctx, []byte("a"), []byte("1")) },
		func() (uint64, error) { return w.putWithTTL(ctx, []byte("b"), []byte("2"), time.Hour) },
		func() (uint64, error) { return w.delete(ctx, []byte("a")) },
	} {
		seq, err := mutate()
		if err != nil {
			t.Fatalf("mutation: %v", err)
		}
		got = append(got, seq)
	}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("sequences = %v, want [1 2 3]", got)
	}
	if seq, err := w.put(ctx, nil, []byte("x")); err == nil || seq != 0 {
		t.Fatalf("invalid put returned seq=%d err=%v, want 0 and an error", seq, err)
	}
}

// waitAsync runs waitCommitted in the background and returns its result.
func waitAsync(ctx context.Context, w *writer, seq uint64) <-chan error {
	done := make(chan error, 1)
	go func() { done <- w.waitCommitted(ctx, seq) }()
	return done
}

func assertWaiting(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("WaitCommitted returned %v before the sequence was committed", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func awaitResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("WaitCommitted did not return")
		return nil
	}
}

// TestWriterWaitCommitted waits for a buffered mutation: it blocks until the
// mutation is flushed, returns at once once committed, rejects a sequence
// never assigned, and honours its context.
func TestWriterWaitCommitted(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-wait-committed")
	defer store.Close()
	w, _ := newCommittedSequenceWriter(t, ctx, store, 1<<20)
	defer w.close(ctx)

	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	done := waitAsync(ctx, w, seq)
	assertWaiting(t, done)
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := awaitResult(t, done); err != nil {
		t.Fatalf("WaitCommitted after flush: %v", err)
	}
	if got := w.committed.Load(); got != seq {
		t.Fatalf("committed sequence = %d, want %d", got, seq)
	}
	if err := w.waitCommitted(ctx, seq); err != nil {
		t.Fatalf("WaitCommitted on a committed sequence: %v", err)
	}
	if err := w.waitCommitted(ctx, seq+5); err == nil {
		t.Fatal("WaitCommitted accepted a sequence never assigned")
	}

	next, err := w.put(ctx, []byte("b"), []byte("2"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := w.waitCommitted(waitCtx, next); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitCommitted without a flush err=%v, want %v", err, context.DeadlineExceeded)
	}
}

// TestWriterWaitCommittedBackgroundFlush commits through the background
// flush alone: the waiter returns without any explicit Flush.
func TestWriterWaitCommittedBackgroundFlush(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-wait-committed-background")
	defer store.Close()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 20 * time.Millisecond
	w, err := newWriter(ctx, store, newManifestStore(store, nil), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := awaitResult(t, waitAsync(ctx, w, seq)); err != nil {
		t.Fatalf("WaitCommitted with background flush: %v", err)
	}
}

// TestWriterWaitCommittedFenced fences a writer holding an uncommitted
// mutation: once its flush finds the fence lost, the waiter returns ErrFenced
// rather than waiting for a commit that cannot happen.
func TestWriterWaitCommittedFenced(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-wait-committed-fenced")
	defer store.Close()
	w, _ := newCommittedSequenceWriter(t, ctx, store, 1<<20)
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	done := waitAsync(ctx, w, seq)
	successor, _ := newCommittedSequenceWriter(t, ctx, store, 1<<20)
	defer successor.close(ctx)
	assertWaiting(t, done)

	if err := w.flush(ctx); err == nil {
		t.Fatal("flush of a fenced writer succeeded")
	}
	if err := awaitResult(t, done); !errors.Is(err, manifest.ErrFenced) {
		t.Fatalf("WaitCommitted on a fenced writer err=%v, want %v", err, manifest.ErrFenced)
	}
	_ = w.close(ctx)
}

// failingMaintenanceStorage fails every read of maintenance/HEAD.
type failingMaintenanceStorage struct {
	manifest.Storage
	err error
}

func (s *failingMaintenanceStorage) ReadMaintenanceHead(context.Context) ([]byte, string, error) {
	return nil, "", s.err
}

// TestWriterMaintenancePollFailureIsFinal fails the background flush's poll
// of maintenance/HEAD: the flush loop stops, so the writer becomes failed
// rather than looking open with nothing flushing, and a waiter returns the
// failure instead of waiting.
func TestWriterMaintenancePollFailureIsFinal(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-maintenance-poll-failure")
	defer store.Close()
	pollErr := errors.New("maintenance head unavailable")
	storage := &failingMaintenanceStorage{Storage: manifest.NewBlobStoreBackend(store), err: pollErr}
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 20 * time.Millisecond
	notified := make(chan error, 1)
	opts.OnFlushError = func(err error) { notified <- err }
	w, err := newWriter(ctx, store, manifest.NewStoreWithStorage(storage), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := awaitResult(t, waitAsync(ctx, w, seq)); !errors.Is(err, ErrWriterFailed) || !errors.Is(err, pollErr) {
		t.Fatalf("WaitCommitted after a failed maintenance poll err=%v, want %v wrapping %v", err, ErrWriterFailed, pollErr)
	}
	if _, err := w.put(ctx, []byte("b"), []byte("2")); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("Put after the failure err=%v, want %v", err, ErrWriterFailed)
	}
	select {
	case err := <-notified:
		if !errors.Is(err, pollErr) {
			t.Fatalf("OnFlushError got %v, want %v", err, pollErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnFlushError was not called")
	}
}

// failingCurrentWriteStorage fails conditional writes of CURRENT while fail
// is set, without applying them.
type failingCurrentWriteStorage struct {
	manifest.Storage
	mu   sync.Mutex
	fail error
}

func (s *failingCurrentWriteStorage) setFail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = err
}

func (s *failingCurrentWriteStorage) WriteCurrentCAS(ctx context.Context, data []byte, etag string) (string, error) {
	s.mu.Lock()
	fail := s.fail
	s.mu.Unlock()
	if fail != nil {
		return "", fail
	}
	return s.Storage.WriteCurrentCAS(ctx, data, etag)
}

// TestWriterRetriedCloseCommits fails Close's final flush once: a waiter is
// not told the writer closed, since a retried Close commits the mutation,
// after which the waiter succeeds.
func TestWriterRetriedCloseCommits(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-retried-close")
	defer store.Close()
	storage := &failingCurrentWriteStorage{Storage: manifest.NewBlobStoreBackend(store)}
	w, err := newWriter(ctx, store, manifest.NewStoreWithStorage(storage), testWriterOptions(1<<20, 16))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	done := waitAsync(ctx, w, seq)

	transient := errors.New("transient CURRENT write failure")
	storage.setFail(transient)
	if err := w.close(ctx); !errors.Is(err, transient) {
		t.Fatalf("first Close err=%v, want %v", err, transient)
	}
	assertWaiting(t, done)
	if s := writerStatus(w.statusNow.Load()); s != writerClosing {
		t.Fatalf("status after a failed Close = %d, want closing", s)
	}

	storage.setFail(nil)
	if err := w.close(ctx); err != nil {
		t.Fatalf("retried Close: %v", err)
	}
	if err := awaitResult(t, done); err != nil {
		t.Fatalf("WaitCommitted after the retried Close: %v", err)
	}
	if s := writerStatus(w.statusNow.Load()); s != writerClosed {
		t.Fatalf("status after Close = %d, want closed", s)
	}
}

// TestWriterConcurrentMutations runs Put and Delete from many goroutines:
// every mutation gets a distinct sequence, and they cover 1..n.
func TestWriterConcurrentMutations(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-concurrent-mutations")
	defer store.Close()
	w, _ := newCommittedSequenceWriter(t, ctx, store, 1<<20)
	defer w.close(ctx)

	const goroutines, each = 8, 50
	seqs := make(chan uint64, goroutines*each)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range each {
				key := []byte(fmt.Sprintf("g%d-%d", g, i))
				var seq uint64
				var err error
				if i%3 == 0 {
					seq, err = w.delete(ctx, key)
				} else {
					seq, err = w.put(ctx, key, []byte("v"))
				}
				if err != nil {
					t.Errorf("mutation: %v", err)
					return
				}
				seqs <- seq
			}
		})
	}
	wg.Wait()
	close(seqs)
	seen := make(map[uint64]bool)
	for seq := range seqs {
		if seen[seq] {
			t.Fatalf("sequence %d given twice", seq)
		}
		seen[seq] = true
	}
	for seq := uint64(1); seq <= goroutines*each; seq++ {
		if !seen[seq] {
			t.Fatalf("sequence %d never given", seq)
		}
	}
}
