package isledb

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeRelay serves a CURRENT, position and age the test sets, with the long
// poll; it answers nothing else, so object reads fall back to storage.
type fakeRelay struct {
	mu       sync.Mutex
	data     []byte
	position uint64
	age      time.Duration
	changed  chan struct{}
	// hold is how long a long poll waits for a position; zero is 2s, and
	// below zero answers at once.
	hold      time.Duration
	longPolls atomic.Int64
	// blocked, when set, holds each CURRENT read until it is closed, after
	// sending on entered.
	blocked chan struct{}
	entered chan struct{}
}

func (f *fakeRelay) set(t *testing.T, data []byte, age time.Duration) {
	t.Helper()
	current, err := manifest.DecodeCurrent(data)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data, f.position, f.age = data, current.NextSeq, age
	if f.changed != nil {
		close(f.changed)
	}
	f.changed = make(chan struct{})
}

func (f *fakeRelay) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/current" {
		http.NotFound(w, r)
		return
	}
	if after := r.URL.Query().Get("after"); after != "" {
		f.longPolls.Add(1)
		n, _ := strconv.ParseUint(after, 10, 64)
		hold := 2 * time.Second
		f.mu.Lock()
		if f.hold != 0 {
			hold = max(f.hold, 0)
		}
		f.mu.Unlock()
		timeout := time.After(hold)
		for {
			f.mu.Lock()
			position, changed := f.position, f.changed
			f.mu.Unlock()
			if position > n {
				break
			}
			select {
			case <-changed:
				continue
			case <-timeout:
			case <-r.Context().Done():
				return
			}
			break
		}
		f.mu.Lock()
		w.Header().Set(readcache.HeaderPosition, strconv.FormatUint(f.position, 10))
		w.Header().Set(readcache.HeaderAgeMillis, strconv.FormatInt(f.age.Milliseconds(), 10))
		f.mu.Unlock()
		return
	}
	f.mu.Lock()
	blocked, entered := f.blocked, f.entered
	f.mu.Unlock()
	if blocked != nil {
		entered <- struct{}{}
		select {
		case <-blocked:
		case <-r.Context().Done():
			return
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set(readcache.HeaderPosition, strconv.FormatUint(f.position, 10))
	w.Header().Set(readcache.HeaderAgeMillis, strconv.FormatInt(f.age.Milliseconds(), 10))
	_, _ = w.Write(f.data)
}

type relayFixture struct {
	ctx    context.Context
	store  *blobstore.Store
	db     *DB
	writer *Writer
	relay  *fakeRelay
	// ahead, when set, answers the long polls in relay's place, as a
	// replica ahead of the one reads go to.
	ahead   atomic.Pointer[fakeRelay]
	reader  *Reader
	metrics *ReaderMetrics
}

// newRelayFixture opens a database with one commit, a fake relay serving its
// CURRENT, and a Reader through the relay that refreshes only when asked.
func newRelayFixture(t *testing.T) *relayFixture {
	t.Helper()
	return newRelayFixtureRefreshing(t, time.Hour)
}

// newRelayFixtureRefreshing is newRelayFixture with a Reader that refreshes
// every refreshAfter.
func newRelayFixtureRefreshing(t *testing.T, refreshAfter time.Duration) *relayFixture {
	t.Helper()
	ctx := context.Background()
	f := &relayFixture{ctx: ctx, store: blobstore.NewMemory("db-relay"), relay: &fakeRelay{}}
	t.Cleanup(func() { _ = f.store.Close() })
	var err error
	if f.db, err = openDB(ctx, f.store, dbOpenOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	if f.writer, err = f.db.OpenWriter(ctx, WriterOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.writer.Close(ctx) })
	f.commit(t, "a")
	f.relay.set(t, f.storageCurrent(t), 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ahead := f.ahead.Load(); ahead != nil && r.URL.Query().Has("after") {
			ahead.ServeHTTP(w, r)
			return
		}
		f.relay.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	f.metrics = DefaultReaderMetrics(nil)
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.Views.RefreshAfter = refreshAfter
	opts.ReadCache = ReadCacheOptions{URL: server.URL, Timeout: time.Second}
	opts.Metrics = f.metrics
	if f.reader, err = f.db.OpenReader(ctx, opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.reader.Close() })
	return f
}

func (f *relayFixture) commit(t *testing.T, key string) {
	t.Helper()
	if _, err := f.writer.Put(f.ctx, []byte(key), []byte("value-"+key)); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func (f *relayFixture) storageCurrent(t *testing.T) []byte {
	t.Helper()
	data, _, err := f.store.Read(f.ctx, f.store.ManifestPath())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// refresh runs one unforced refresh, as the timer or RequestRefresh does.
func (f *relayFixture) refresh(t *testing.T) {
	t.Helper()
	f.reader.viewDue.Store(true)
	if err := f.reader.refreshManifest(f.ctx, false); err != nil {
		t.Fatal(err)
	}
}

func (f *relayFixture) loadedAt() time.Time {
	f.reader.mu.RLock()
	defer f.reader.mu.RUnlock()
	return f.reader.viewLoadedAt
}

func (f *relayFixture) outcome(name string) float64 {
	return testutil.ToFloat64(f.metrics.ReadCacheCurrent.WithLabelValues(name))
}

func TestRefreshTakesTheRelayedCurrentAndCountsItsAge(t *testing.T) {
	f := newRelayFixture(t)
	f.commit(t, "b")
	f.relay.set(t, f.storageCurrent(t), 3*time.Second)
	before := time.Now()
	f.refresh(t)
	after := time.Now()
	if _, found, err := f.reader.Get(f.ctx, []byte("b")); err != nil || !found {
		t.Fatalf("Get(b) after a relayed refresh: found %t, %v", found, err)
	}
	if f.outcome("relayed") != 1 {
		t.Fatalf("outcomes: relayed %v, want 1", f.outcome("relayed"))
	}
	// The refresh took its start between before and after; the view's load
	// time is that start less the copy's 3s age.
	if loaded := f.loadedAt(); loaded.Before(before.Add(-3*time.Second)) || loaded.After(after.Add(-3*time.Second)) {
		t.Fatalf("view loaded at %s, want 3s before the refresh, between %s and %s",
			loaded, before.Add(-3*time.Second), after.Add(-3*time.Second))
	}
}

func TestRefreshReadsStorageForACopyTooOldOrBehind(t *testing.T) {
	f := newRelayFixture(t)
	old := f.storageCurrent(t)
	f.commit(t, "b")

	f.relay.set(t, f.storageCurrent(t), 10*time.Second)
	before := time.Now()
	f.refresh(t)
	if f.outcome("too_old") != 1 || f.loadedAt().Before(before) {
		t.Fatalf("too old: outcome %v, loaded %s; want storage's view, loaded now", f.outcome("too_old"), f.loadedAt())
	}
	published := f.reader.ViewPosition()

	f.relay.set(t, old, 0)
	before = time.Now()
	f.refresh(t)
	if f.outcome("behind") != 1 {
		t.Fatalf("behind: outcome %v, want 1", f.outcome("behind"))
	}
	if f.reader.ViewPosition() != published || f.loadedAt().Before(before) {
		t.Fatal("a copy behind the view did not leave it at its position, renewed from storage")
	}
	if n := testutil.ToFloat64(f.metrics.RefreshErrors); n != 0 {
		t.Fatalf("%v refresh errors, want none for a copy behind the view", n)
	}
}

func TestForcedRefreshReadsStorage(t *testing.T) {
	f := newRelayFixture(t)
	f.commit(t, "b") // the relay still serves the CURRENT before it
	if err := f.reader.Refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, err := f.reader.Get(f.ctx, []byte("b")); err != nil || !found {
		t.Fatalf("Get(b) after Refresh: found %t, %v; want the commit made before it", found, err)
	}
	for _, outcome := range []string{"relayed", "too_old", "behind", "failed"} {
		if n := f.outcome(outcome); n != 0 {
			t.Fatalf("Refresh asked the relay (%s %v)", outcome, n)
		}
	}
}

func TestDoorbellRefreshesOnACommitAndStopsOnClose(t *testing.T) {
	f := newRelayFixture(t)
	start := f.reader.ViewPosition()
	f.commit(t, "b")
	f.relay.set(t, f.storageCurrent(t), 0)
	deadline := time.Now().Add(3 * time.Second)
	for f.reader.ViewPosition() == start {
		if time.Now().After(deadline) {
			t.Fatal("the doorbell did not bring the commit within 3s, with refreshes an hour apart")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := testutil.ToFloat64(f.metrics.ReadCacheDoorbells.WithLabelValues("rang")); n == 0 {
		t.Fatal("no doorbell counted")
	}

	closed := time.Now()
	if err := f.reader.Close(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(closed); took > time.Second {
		t.Fatalf("Close took %s with a long poll held", took)
	}
}

func TestDoorbellRingsAgainWhenTheRefreshReadsAReplicaBehind(t *testing.T) {
	f := newRelayFixture(t)
	start := f.reader.ViewPosition()
	f.commit(t, "b")
	ahead := &fakeRelay{}
	ahead.set(t, f.storageCurrent(t), 0)
	f.ahead.Store(ahead) // rings for the commit; reads still get the CURRENT before it
	rang := func() float64 { return testutil.ToFloat64(f.metrics.ReadCacheDoorbells.WithLabelValues("rang")) }
	// The long poll already held on the replica behind answers at its
	// timeout; the next one goes to the replica ahead.
	for deadline := time.Now().Add(3 * time.Second); rang() == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the replica ahead never rang")
		}
	}
	before := rang()
	time.Sleep(500 * time.Millisecond)
	if f.reader.ViewPosition() != start {
		t.Fatal("the view moved while every read came from the replica behind")
	}
	// Rung again and again, but paced by the refresh gap, not as fast as the
	// replica ahead answers.
	if n := rang() - before; n > 2 {
		t.Fatalf("%v rings in 500ms with the reads behind, want them a second apart", n)
	}
	f.relay.set(t, f.storageCurrent(t), 0) // the replica behind catches up
	deadline := time.Now().Add(3 * time.Second)
	for f.reader.ViewPosition() == start {
		if time.Now().After(deadline) {
			t.Fatal("the reader did not reach the commit once the replica behind caught up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCloseStopsTheDoorbellBeforeTakingTheLifecycleLock(t *testing.T) {
	f := newRelayFixture(t)
	r := f.reader
	r.lifecycleMu.RLock() // a read in progress
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	// Close is waiting for the lock once a new read cannot start.
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(time.Millisecond) {
		if !r.lifecycleMu.TryRLock() {
			break
		}
		r.lifecycleMu.RUnlock()
		if time.Now().After(deadline) {
			r.lifecycleMu.RUnlock()
			t.Fatal("Close never waited for the lifecycle lock")
		}
	}
	// A commit now rings any doorbell still running; its refresh would wait
	// for the lifecycle lock behind Close.
	f.commit(t, "b")
	f.relay.set(t, f.storageCurrent(t), 0)
	time.Sleep(200 * time.Millisecond)
	r.lifecycleMu.RUnlock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close deadlocked with the doorbell")
	}
}

// quickRelay makes the fake relay answer long polls at once, as a cache
// misconfigured with a tiny long-poll timeout does.
func quickRelay(f *fakeRelay) {
	f.mu.Lock()
	f.hold = -1
	f.mu.Unlock()
}

func TestDoorbellPacesQuietAnswers(t *testing.T) {
	f := newRelayFixture(t)
	quickRelay(f.relay)
	before := f.relay.longPolls.Load()
	time.Sleep(time.Second)
	if n := f.relay.longPolls.Load() - before; n > 12 {
		t.Fatalf("%d long polls in 1s against a cache answering at once, want at most about 10", n)
	}
}

func TestRefreshesContinueWhenTheCacheCopyIsNoNewer(t *testing.T) {
	f := newRelayFixtureRefreshing(t, time.Second)
	// The view came from storage at open; the relay's copy of the same
	// CURRENT is 3s old, so each refresh through it loads a view dated
	// before the published one.
	f.relay.set(t, f.storageCurrent(t), 3*time.Second)
	relayed := func() float64 { return testutil.ToFloat64(f.metrics.ReadCacheCurrent.WithLabelValues("relayed")) }
	time.Sleep(3500 * time.Millisecond)
	if n := relayed(); n < 2 {
		t.Fatalf("%v refreshes through the cache in 3.5s at one a second, want at least 2", n)
	}

	// A newer copy, also 3s old, is published dated 3s back: the next
	// refresh is still a period away, not at once and again.
	f.commit(t, "b")
	f.relay.set(t, f.storageCurrent(t), 3*time.Second)
	before := relayed()
	time.Sleep(3500 * time.Millisecond)
	if n := relayed() - before; n < 2 || n > 5 {
		t.Fatalf("%v refreshes through the cache in 3.5s at one a second, want 2 to 5", n)
	}
}

// TestExpiredViewReadsStorageWhenTheCacheCopyIsNoNewer: the view has
// expired, the cache's copy of CURRENT is the view's own, and storage has a
// newer one. The refresh through the cache renews nothing, so the read reads
// CURRENT from storage and succeeds with the newer data.
func TestExpiredViewReadsStorageWhenTheCacheCopyIsNoNewer(t *testing.T) {
	f := newRelayFixture(t)
	viewCurrent := f.storageCurrent(t)
	f.commit(t, "b") // only storage has it
	// The view's own CURRENT, dated before the view was loaded: a refresh
	// through the cache takes it and publishes nothing.
	f.relay.set(t, viewCurrent, 4*time.Second)
	f.reader.mu.Lock()
	f.reader.viewExpiresAt = time.Now().Add(-time.Second)
	f.reader.mu.Unlock()
	before := f.outcome("relayed")

	// Only storage's CURRENT holds b: finding it shows the read went there,
	// after the refresh through the cache renewed nothing. (That refresh also
	// re-arms the timer against the past expiry, so one more background
	// refresh may run through the cache alongside.)
	if _, found, err := f.reader.Get(f.ctx, []byte("b")); err != nil || !found {
		t.Fatalf("Get(b) on an expired view: found %t, %v; want storage's newer view", found, err)
	}
	if f.outcome("relayed") == before {
		t.Fatal("the refresh did not try the cache first")
	}
}

// TestForcedRefreshDoesNotJoinALoadThroughTheCache: while a refresh through
// the cache is held reading CURRENT, a forced Refresh reads storage on its
// own and publishes the newer view, rather than waiting on that load.
func TestForcedRefreshDoesNotJoinALoadThroughTheCache(t *testing.T) {
	f := newRelayFixture(t)
	f.commit(t, "b") // the relay still serves the CURRENT before it
	release := make(chan struct{})
	f.relay.mu.Lock()
	f.relay.blocked, f.relay.entered = release, make(chan struct{}, 1)
	entered := f.relay.entered
	f.relay.mu.Unlock()
	defer close(release)

	f.reader.viewDue.Store(true)
	go func() { _ = f.reader.refreshManifest(f.ctx, false) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the refresh through the cache never asked it")
	}
	// Below the fixture's 1s cache timeout, after which the held load would
	// read storage itself: Refresh must not be waiting on it.
	ctx, cancel := context.WithTimeout(f.ctx, 500*time.Millisecond)
	defer cancel()
	if err := f.reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh while a load through the cache is held: %v", err)
	}
	if _, found, err := f.reader.Get(f.ctx, []byte("b")); err != nil || !found {
		t.Fatalf("Get(b) after Refresh: found %t, %v", found, err)
	}
}
