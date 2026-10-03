package isledb

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// pausingManifestStorage pauses the next CURRENT read once armed: the read
// completes, then waits for release, so a test can commit while a reload
// holds an older CURRENT. With fail set, CURRENT reads fail; reads counts
// them.
type pausingManifestStorage struct {
	pagedManifestStorage

	mu      sync.Mutex
	armed   bool
	read    chan struct{}
	release chan struct{}
	fail    error
	hang    chan struct{}
	reads   int
}

// setHang makes CURRENT reads block until the returned function is called,
// or the read's context ends.
func (s *pausingManifestStorage) setHang() (release func()) {
	hang := make(chan struct{})
	s.mu.Lock()
	s.hang = hang
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		if s.hang == hang {
			s.hang = nil
		}
		s.mu.Unlock()
		close(hang)
	}
}

func (s *pausingManifestStorage) setFail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = err
}

func (s *pausingManifestStorage) currentReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

type pagedManifestStorage interface {
	manifest.Storage
	manifest.PageStorage
}

func (s *pausingManifestStorage) arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = true
	s.read = make(chan struct{})
	s.release = make(chan struct{})
}

func (s *pausingManifestStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	s.mu.Lock()
	s.reads++
	fail, hang := s.fail, s.hang
	s.mu.Unlock()
	if hang != nil {
		select {
		case <-hang:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	if fail != nil {
		return nil, "", fail
	}
	data, etag, err := s.pagedManifestStorage.ReadCurrent(ctx)
	s.mu.Lock()
	armed, read, release := s.armed, s.read, s.release
	s.armed = false
	s.mu.Unlock()
	if armed {
		close(read)
		<-release
	}
	return data, etag, err
}

// newRefreshTestReader opens a reader over one committed SST holding "a",
// reading its manifest through a pausing storage.
func newRefreshTestReader(t *testing.T) (context.Context, *Reader, *pausingManifestStorage, *blobstore.Store, *manifest.Store) {
	t.Helper()
	ctx := context.Background()
	store := blobstore.NewMemory("reader-refresh-order")
	t.Cleanup(func() { _ = store.Close() })
	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("1")},
	}, 0, 1)

	storage := &pausingManifestStorage{pagedManifestStorage: manifest.NewBlobStoreBackend(store)}
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(), ManifestStorage: storage, DisableManifestPageCache: true,
	})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return ctx, reader, storage, store, ms
}

func commitKeyB(t *testing.T, ctx context.Context, store *blobstore.Store, ms *manifest.Store) {
	t.Helper()
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay before commit: %v", err)
	}
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Value: []byte("2")},
	}, 0, 2)
}

func assertReaderHasB(t *testing.T, ctx context.Context, reader *Reader) {
	t.Helper()
	reader.mu.RLock()
	m := reader.manifest
	reader.mu.RUnlock()
	value, found, err := reader.getWithManifest(ctx, m, []byte("b"))
	if err != nil || !found || !bytes.Equal(value, []byte("2")) {
		t.Fatalf("view after refresh: b = (%q, %t, %v), want the commit", value, found, err)
	}
}

// waitForManifestWaiters waits until n callers share the reload in flight.
func waitForManifestWaiters(t *testing.T, reader *Reader, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		g := &reader.manifestLoads
		g.mu.Lock()
		call := g.calls["manifest"]
		waiters := 0
		if call != nil {
			waiters = call.waiters
		}
		g.mu.Unlock()
		if waiters == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("reload never had %d waiters", n)
}

// TestReaderRefreshSeesCommitBeforeCall calls Refresh after a commit while a
// reload that read the older CURRENT is still running: Refresh must not
// settle for that reload, so the commit is visible when it returns.
func TestReaderRefreshSeesCommitBeforeCall(t *testing.T) {
	ctx, reader, storage, store, ms := newRefreshTestReader(t)

	storage.arm()
	first := make(chan error, 1)
	go func() { first <- reader.Refresh(ctx) }()
	<-storage.read // the first reload holds the CURRENT before the commit

	commitKeyB(t, ctx, store, ms)
	second := make(chan error, 1)
	go func() { second <- reader.Refresh(ctx) }()
	waitForManifestWaiters(t, reader, 2) // the second Refresh joined the first reload
	close(storage.release)

	if err := <-first; err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second Refresh: %v", err)
	}
	assertReaderHasB(t, ctx, reader)
}

// TestReaderAbandonedReloadDoesNotPublish abandons a reload holding an older
// CURRENT, lets a newer reload publish, then lets the abandoned one finish:
// the view stays at the newer one.
func TestReaderAbandonedReloadDoesNotPublish(t *testing.T) {
	ctx, reader, storage, store, ms := newRefreshTestReader(t)

	storage.arm()
	abandonCtx, abandon := context.WithCancel(ctx)
	first := make(chan error, 1)
	go func() { first <- reader.Refresh(abandonCtx) }()
	<-storage.read
	abandon()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned Refresh error=%v, want context.Canceled", err)
	}

	commitKeyB(t, ctx, store, ms)
	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	reader.mu.RLock()
	newer := reader.viewSeq
	reader.mu.RUnlock()

	close(storage.release)
	reader.manifestLoads.active.Wait() // the abandoned reload has finished
	reader.mu.RLock()
	seq := reader.viewSeq
	reader.mu.RUnlock()
	if seq != newer {
		t.Fatalf("view went from log position %d to %d", newer, seq)
	}
	assertReaderHasB(t, ctx, reader)
}

// TestReaderViewNeverGoesBack publishes a view older than the current one:
// it is dropped, while one at the same position or newer is published.
func TestReaderViewNeverGoesBack(t *testing.T) {
	_, reader, _, _, _ := newRefreshTestReader(t)
	reader.mu.RLock()
	published, seq, version := reader.manifest, reader.viewSeq, reader.version
	reader.mu.RUnlock()
	if seq == 0 {
		t.Fatal("published view has no log position")
	}

	empty := &manifestState{}
	reader.publishManifestView(empty, &manifest.Current{NextSeq: seq - 1}, time.Now())
	reader.mu.RLock()
	if reader.manifest != published || reader.viewSeq != seq || reader.version != version {
		reader.mu.RUnlock()
		t.Fatal("an older view replaced the published one")
	}
	reader.mu.RUnlock()

	reader.publishManifestView(empty, &manifest.Current{NextSeq: seq + 1}, time.Now())
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	if reader.manifest != empty || reader.viewSeq != seq+1 {
		t.Fatal("a newer view was not published")
	}
}

// startViewTimer does what the view's timer does when its refresh time
// arrives: marks the view due and starts a background refresh.
func startViewTimer(reader *Reader) {
	reader.mu.Lock()
	reader.viewRefreshAt = time.Now().Add(-time.Second)
	reader.mu.Unlock()
	reader.viewDue.Store(true)
	reader.refreshInBackground()
}

// fireViewTimer starts the view's refresh and waits for it to finish.
func fireViewTimer(reader *Reader) {
	startViewTimer(reader)
	reader.background.Wait()
}

// TestReaderReadsNeverStartRefresh reads from a view past its refresh time
// whose timer has not fired: the read is answered from it and does not reach
// object storage.
func TestReaderReadsNeverStartRefresh(t *testing.T) {
	ctx, reader, storage, _, _ := newRefreshTestReader(t)
	reader.mu.Lock()
	reader.viewRefreshAt = time.Now().Add(-time.Second)
	reader.mu.Unlock()
	reader.viewDue.Store(true)
	readsBefore := storage.currentReads()
	if _, _, err := reader.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := storage.currentReads() - readsBefore; got != 0 || reader.refreshing.Load() {
		t.Fatalf("read started a refresh: %d CURRENT reads", got)
	}
}

// TestReaderServesValidViewWhenRefreshFails fails the refresh of a view past
// its refresh time but not expired: reads are answered from it, those after
// the failure counted as stale, the next refresh is put off by
// refreshRetryAfter, so later reads do not reach object storage, and a
// refresh that then succeeds ends the stale period.
func TestReaderServesValidViewWhenRefreshFails(t *testing.T) {
	ctx, reader, storage, store, ms := newRefreshTestReader(t)
	metrics := DefaultReaderMetrics(nil)
	reader.metrics = metrics
	commitKeyB(t, ctx, store, ms)

	storage.setFail(errors.New("object storage unavailable"))
	readsBefore := storage.currentReads()
	failedAt := time.Now()
	fireViewTimer(reader)
	for range 2 {
		value, found, err := reader.Get(ctx, []byte("a"))
		if err != nil || !found || !bytes.Equal(value, []byte("1")) {
			t.Fatalf("Get during outage = (%q, %t, %v), want the loaded view's value", value, found, err)
		}
	}
	if _, found, err := reader.Get(ctx, []byte("b")); err != nil || found {
		t.Fatalf("Get b during outage found=%t err=%v, want the loaded view without the commit", found, err)
	}
	if got := storage.currentReads() - readsBefore; got != 1 {
		t.Fatalf("CURRENT read %d times during the outage, want once", got)
	}
	if got := testutil.ToFloat64(metrics.StaleReads); got != 3 {
		t.Fatalf("stale reads = %v, want 3", got)
	}
	reader.mu.RLock()
	retryIn := reader.viewRefreshAt.Sub(failedAt)
	reader.mu.RUnlock()
	if retryIn < refreshRetryAfter/2-time.Second || retryIn > refreshRetryAfter*3/2+time.Second {
		t.Fatalf("next refresh in %v, want within [%v, %v]", retryIn, refreshRetryAfter/2, refreshRetryAfter*3/2)
	}

	storage.setFail(nil)
	loadedBefore := testutil.ToFloat64(metrics.ViewLoaded)
	fireViewTimer(reader)
	if _, found, err := reader.Get(ctx, []byte("b")); err != nil || !found {
		t.Fatalf("Get b after recovery found=%t err=%v, want the commit", found, err)
	}
	if reader.stale.Load() {
		t.Fatal("reader still stale after a successful refresh")
	}
	if got := testutil.ToFloat64(metrics.ViewLoaded); got <= loadedBefore {
		t.Fatalf("view loaded time %v not advanced past %v", got, loadedBefore)
	}
}

// TestReaderRefreshRetryFollowsShortRefreshAfter fails a refresh with a
// RefreshAfter shorter than refreshRetryAfter: the retry waits RefreshAfter.
func TestReaderRefreshRetryFollowsShortRefreshAfter(t *testing.T) {
	_, reader, storage, _, _ := newRefreshTestReader(t)
	reader.viewPolicy.RefreshAfter = 5 * time.Second
	storage.setFail(errors.New("object storage unavailable"))
	failedAt := time.Now()
	fireViewTimer(reader)
	reader.mu.RLock()
	retryIn := reader.viewRefreshAt.Sub(failedAt)
	reader.mu.RUnlock()
	if retryIn < 2*time.Second || retryIn > 8*time.Second {
		t.Fatalf("next refresh in %v, want within [2.5s, 7.5s]", retryIn)
	}
}

// TestReaderExpiredViewFailsWhenRefreshFails fails the refresh of an expired
// view: its SSTs may be gone, so the read fails.
func TestReaderExpiredViewFailsWhenRefreshFails(t *testing.T) {
	ctx, reader, storage, _, _ := newRefreshTestReader(t)
	outage := errors.New("object storage unavailable")
	storage.setFail(outage)
	reader.mu.Lock()
	reader.viewRefreshAt = time.Now().Add(-2 * time.Second)
	reader.viewExpiresAt = time.Now().Add(-time.Second)
	reader.mu.Unlock()
	reader.viewDue.Store(true)
	if _, _, err := reader.Get(ctx, []byte("a")); !errors.Is(err, outage) {
		t.Fatalf("Get on an expired view err=%v, want %v", err, outage)
	}
}

// TestReaderForcedRefreshFailureKeepsSchedule fails an explicit Refresh of a
// view that is not yet due: the caller gets the error, and the view keeps its
// schedule and is not marked stale.
func TestReaderForcedRefreshFailureKeepsSchedule(t *testing.T) {
	ctx, reader, storage, _, _ := newRefreshTestReader(t)
	outage := errors.New("object storage unavailable")
	storage.setFail(outage)
	reader.mu.RLock()
	refreshAt := reader.viewRefreshAt
	reader.mu.RUnlock()
	if err := reader.Refresh(ctx); !errors.Is(err, outage) {
		t.Fatalf("Refresh err=%v, want %v", err, outage)
	}
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	if reader.viewRefreshAt != refreshAt || reader.stale.Load() {
		t.Fatal("a failed Refresh of a fresh view changed its schedule or marked it stale")
	}
}

// TestReaderServesValidViewWhenRefreshHangs hangs the store while the view is
// due but valid: reads are answered at once from the loaded view while the
// background refresh hangs, which times out and is recorded like a failure;
// later reads do not reach the store, and once it answers again the next
// refresh publishes.
func TestReaderServesValidViewWhenRefreshHangs(t *testing.T) {
	ctx, reader, storage, store, ms := newRefreshTestReader(t)
	metrics := DefaultReaderMetrics(nil)
	reader.metrics = metrics
	reader.refreshTimeout = 100 * time.Millisecond
	commitKeyB(t, ctx, store, ms)

	release := storage.setHang()
	readsBefore := storage.currentReads()
	startViewTimer(reader)
	readCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	value, found, err := reader.Get(readCtx, []byte("a"))
	if err != nil || !found || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("Get while the store hangs = (%q, %t, %v), want the loaded view's value at once", value, found, err)
	}
	reader.background.Wait()
	if !reader.stale.Load() {
		t.Fatal("a timed-out refresh was not recorded")
	}
	for range 3 {
		if _, _, err := reader.Get(ctx, []byte("a")); err != nil {
			t.Fatalf("Get after the timeout: %v", err)
		}
	}
	if got := storage.currentReads() - readsBefore; got != 1 {
		t.Fatalf("CURRENT read %d times, want once until the retry", got)
	}
	if got := testutil.ToFloat64(metrics.StaleReads); got != 3 {
		t.Fatalf("stale reads = %v, want 3", got)
	}

	release()
	fireViewTimer(reader)
	assertReaderHasB(t, ctx, reader)
	if reader.stale.Load() {
		t.Fatal("reader still stale after the store recovered")
	}
}

// TestReaderExpiredViewWaitsForRefresh hangs the store once the view has
// expired: the read waits for the refresh, and fails when its own deadline
// passes.
func TestReaderExpiredViewWaitsForRefresh(t *testing.T) {
	ctx, reader, storage, _, _ := newRefreshTestReader(t)
	release := storage.setHang()
	defer release()
	reader.mu.Lock()
	reader.viewRefreshAt = time.Now().Add(-2 * time.Second)
	reader.viewExpiresAt = time.Now().Add(-time.Second)
	reader.mu.Unlock()
	reader.viewDue.Store(true)
	readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, _, err := reader.Get(readCtx, []byte("a")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get on an expired view while the store hangs err=%v, want %v", err, context.DeadlineExceeded)
	}
}

// TestReaderCurrentGoingBackIsRetried makes the store's CURRENT older than
// the loaded view, as a restored store would be: the refresh is retried like
// a failure, not repeated on every read.
func TestReaderCurrentGoingBackIsRetried(t *testing.T) {
	ctx, reader, storage, _, _ := newRefreshTestReader(t)
	reader.mu.Lock()
	reader.viewSeq += 1000 // the loaded view is ahead of the store
	reader.mu.Unlock()
	readsBefore := storage.currentReads()
	fireViewTimer(reader)
	for range 3 {
		if _, _, err := reader.Get(ctx, []byte("a")); err != nil {
			t.Fatalf("Get after the refresh: %v", err)
		}
	}
	if got := storage.currentReads() - readsBefore; got != 1 {
		t.Fatalf("CURRENT read %d times, want once until the retry", got)
	}
	if !reader.stale.Load() {
		t.Fatal("a store behind the loaded view was not recorded as stale")
	}
}

// TestReaderTimerChainSurvivesTinyRefreshAfter refreshes with a RefreshAfter
// so short that each next timer fires while the refresh that armed it is still
// finishing: the chain keeps refreshing instead of stopping.
func TestReaderTimerChainSurvivesTinyRefreshAfter(t *testing.T) {
	_, reader, storage, _, _ := newRefreshTestReader(t)
	reader.viewPolicy.RefreshAfter = time.Microsecond // below the public minimum
	reader.mu.RLock()
	expiresAt := reader.viewExpiresAt
	reader.mu.RUnlock()
	reader.armViewTimer(time.Now(), expiresAt)

	time.Sleep(100 * time.Millisecond)
	first := storage.currentReads()
	time.Sleep(100 * time.Millisecond)
	if second := storage.currentReads(); second <= first {
		t.Fatalf("refreshes stopped: CURRENT reads %d then %d", first, second)
	}
	// Close, in cleanup, stops the timer and waits for the refresh.
}

// TestReaderExpiredViewRecoversInBackground lets the view expire while the
// store is down: background refreshes keep being retried, so once the store
// answers the view is published again with no read involved.
func TestReaderExpiredViewRecoversInBackground(t *testing.T) {
	ctx, reader, storage, store, ms := newRefreshTestReader(t)
	reader.viewPolicy.RefreshAfter = 20 * time.Millisecond // fast retries; below the public minimum
	commitKeyB(t, ctx, store, ms)
	outage := errors.New("object storage unavailable")
	storage.setFail(outage)
	reader.mu.Lock()
	reader.viewRefreshAt = time.Now().Add(-2 * time.Second)
	reader.viewExpiresAt = time.Now().Add(-time.Second)
	reader.mu.Unlock()
	fireViewTimer(reader)
	if _, _, err := reader.Get(ctx, []byte("a")); !errors.Is(err, outage) {
		t.Fatalf("Get on an expired view during the outage err=%v, want %v", err, outage)
	}

	storage.setFail(nil)
	deadline := time.Now().Add(5 * time.Second)
	for {
		reader.mu.RLock()
		expired := !time.Now().Before(reader.viewExpiresAt)
		reader.mu.RUnlock()
		if !expired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired view never refreshed in the background")
		}
		time.Sleep(5 * time.Millisecond)
	}
	assertReaderHasB(t, ctx, reader)
}

// TestReaderRefreshGridSpreadsReaders opens several readers at the same
// moment, as a deploy would: each gets its own phase, so their first refreshes
// differ, and each first refresh comes within RefreshAfter of opening.
func TestReaderRefreshGridSpreadsReaders(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-refresh-grid")
	defer store.Close()
	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("1")},
	}, 0, 1)

	const refreshAfter = time.Minute
	offsets := map[time.Duration]bool{}
	for i := 0; i < 8; i++ {
		reader, err := newReader(ctx, store, readerOptions{
			CacheDir:   t.TempDir(),
			ViewPolicy: ReaderViewPolicy{RefreshAfter: refreshAfter},
		})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		reader.mu.RLock()
		wait := reader.viewRefreshAt.Sub(reader.viewLoadedAt)
		reader.mu.RUnlock()
		_ = reader.Close()
		if wait <= 0 || wait > refreshAfter {
			t.Fatalf("reader %d first refreshes %v after opening, want within (0, %v]", i, wait, refreshAfter)
		}
		offsets[wait] = true
	}
	if len(offsets) == 1 {
		t.Fatal("every reader got the same phase")
	}
}

// TestReaderRefreshGridKeepsPhase reloads at, between, and long after grid
// points, and fails a refresh: the next refresh is always the next point of
// the reader's grid, so a forced refresh or a failure never moves its phase.
func TestReaderRefreshGridKeepsPhase(t *testing.T) {
	const period = time.Minute
	origin := time.Now()
	r := &Reader{refreshGrid: origin}
	for _, tc := range []struct {
		after time.Duration
		want  time.Duration
	}{
		{after: -30 * time.Second, want: 0},
		{after: -period - time.Second, want: -period},
		{after: 0, want: period},
		{after: time.Millisecond, want: period},
		{after: 59 * time.Second, want: period},
		{after: period, want: 2 * period},
		{after: 10*period + 7*time.Second, want: 11 * period},
	} {
		got := r.nextOnGrid(origin.Add(tc.after), period).Sub(origin)
		if got != tc.want {
			t.Errorf("nextOnGrid(origin%+v) = origin%+v, want origin%+v", tc.after, got, tc.want)
		}
	}
}

// TestReaderRefreshRetryKeepsPhase fails refreshes of two readers at the same
// moment: each retries on its own phase, not in step, between half and one
// and a half retry periods later.
func TestReaderRefreshRetryKeepsPhase(t *testing.T) {
	_, a, storageA, _, _ := newRefreshTestReader(t)
	_, b, storageB, _, _ := newRefreshTestReader(t)
	now := time.Now()
	a.refreshGrid = now.Add(3 * time.Second)
	b.refreshGrid = now.Add(17 * time.Second)
	storageA.setFail(errors.New("object storage unavailable"))
	storageB.setFail(errors.New("object storage unavailable"))
	fireViewTimer(a)
	fireViewTimer(b)

	retryAt := func(r *Reader) time.Time {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.viewRefreshAt
	}
	for name, r := range map[string]*Reader{"a": a, "b": b} {
		at := retryAt(r)
		if wait := at.Sub(now); wait < refreshRetryAfter/2-time.Second || wait > refreshRetryAfter*3/2+time.Second {
			t.Errorf("reader %s retries after %v, want within [%v, %v]", name, wait, refreshRetryAfter/2, refreshRetryAfter*3/2)
		}
		if off := at.Sub(r.refreshGrid) % refreshRetryAfter; off != 0 {
			t.Errorf("reader %s retry is %v off its grid", name, off)
		}
	}
	if retryAt(a).Equal(retryAt(b)) {
		t.Error("readers that failed together retry at the same moment")
	}
}

// TestReaderForcedRefreshKeepsPhase forces a Refresh between grid points: the
// view's next background refresh stays on the reader's grid.
func TestReaderForcedRefreshKeepsPhase(t *testing.T) {
	ctx, reader, _, store, ms := newRefreshTestReader(t)
	reader.refreshGrid = time.Now().Add(7 * time.Second)
	commitKeyB(t, ctx, store, ms)
	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	reader.mu.RLock()
	next, loaded := reader.viewRefreshAt, reader.viewLoadedAt
	reader.mu.RUnlock()
	period := reader.viewPolicy.RefreshAfter
	if off := next.Sub(reader.refreshGrid) % period; off != 0 {
		t.Fatalf("next refresh is %v off the reader's grid", off)
	}
	if wait := next.Sub(loaded); wait <= 0 || wait > period {
		t.Fatalf("next refresh %v after the load, want within (0, %v]", wait, period)
	}
}
