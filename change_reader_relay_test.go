package isledb

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type changeRelayFixture struct {
	ctx    context.Context
	store  *blobstore.Store
	db     *DB
	writer *Writer
	relay  *fakeRelay
	// ahead, when set, answers the long polls in relay's place.
	ahead   atomic.Pointer[fakeRelay]
	url     string
	reader  *ChangeReader
	metrics *ReaderMetrics
	head    ChangeCursor
}

// newChangeRelayFixture opens a change feed with one commit, a fake relay
// serving its CURRENT, and a ChangeReader through the relay positioned at
// the head.
func newChangeRelayFixture(t *testing.T) *changeRelayFixture {
	t.Helper()
	f := &changeRelayFixture{ctx: context.Background(), relay: &fakeRelay{}}
	f.store, f.db, f.writer = openChangeReaderTestDB(t, "change-relay")
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
	f.url = server.URL
	f.metrics = DefaultReaderMetrics(nil)
	var err error
	f.reader, err = f.db.OpenChangeReader(f.ctx, ChangeReaderOptions{
		ReadCache: ReadCacheOptions{URL: f.url, Timeout: time.Second},
		Metrics:   f.metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.reader.Close() })
	bounds, err := f.reader.Bounds(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.head = bounds.Head
	return f
}

func (f *changeRelayFixture) commit(t *testing.T, key string) {
	t.Helper()
	if _, err := f.writer.Put(f.ctx, []byte(key), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func (f *changeRelayFixture) storageCurrent(t *testing.T) []byte {
	t.Helper()
	data, _, err := f.store.Read(f.ctx, f.store.ManifestPath())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (f *changeRelayFixture) outcome(name string) float64 {
	return testutil.ToFloat64(f.metrics.ReadCacheCurrent.WithLabelValues(name))
}

func TestChangeReaderViewThroughTheRelay(t *testing.T) {
	f := newChangeRelayFixture(t)
	f.commit(t, "b")
	f.relay.set(t, f.storageCurrent(t), 3*time.Second)
	before := time.Now()
	page, err := f.reader.Read(f.ctx, f.head, ChangeReadOptions{})
	after := time.Now()
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Changes) != 1 || string(page.Changes[0].Key) != "b" || f.outcome("relayed") == 0 {
		t.Fatalf("%d changes, relayed %v; want b through the relay", len(page.Changes), f.outcome("relayed"))
	}
	f.reader.viewMu.RLock()
	view := f.reader.view
	f.reader.viewMu.RUnlock()
	// Expiry is the load's start plus the pinned age, less the copy's age.
	if view.ExpiredAt(before.Add(DefaultMaxPinnedViewAge-3*time.Second-time.Millisecond)) ||
		!view.ExpiredAt(after.Add(DefaultMaxPinnedViewAge-3*time.Second)) {
		t.Fatal("the view's expiry was not moved back by the copy's 3s age")
	}
}

func TestChangeReaderViewReadsStorageForACopyTooOldOrBehind(t *testing.T) {
	f := newChangeRelayFixture(t)
	old := f.storageCurrent(t)
	f.commit(t, "b")
	f.relay.set(t, f.storageCurrent(t), 10*time.Second)
	page, err := f.reader.Read(f.ctx, f.head, ChangeReadOptions{})
	if err != nil || len(page.Changes) != 1 || f.outcome("too_old") == 0 {
		t.Fatalf("too old: %d changes, %v, too_old %v; want b from storage", len(page.Changes), err, f.outcome("too_old"))
	}

	f.relay.set(t, old, 0) // a replica behind the view the reader now holds
	if _, err := f.reader.Read(f.ctx, page.Next, ChangeReadOptions{}); err != nil {
		t.Fatalf("a copy behind the view: %v, want a read from storage", err)
	}
	if f.outcome("behind") == 0 {
		t.Fatal("a copy behind the view was not counted as behind")
	}
}

func TestWaitForChangesWithTheRelay(t *testing.T) {
	f := newChangeRelayFixture(t)
	waited := make(chan error, 1)
	go func() { waited <- f.reader.WaitForChanges(f.ctx, f.head) }()
	// Past the relay's 2s long-poll timeout: a quiet answer must not end it.
	time.Sleep(2300 * time.Millisecond)
	select {
	case err := <-waited:
		t.Fatalf("returned before any commit: %v", err)
	default:
	}
	if n := testutil.ToFloat64(f.metrics.ReadCacheDoorbells.WithLabelValues("quiet")); n == 0 {
		t.Fatal("no quiet answer came in 2.3s")
	}
	f.commit(t, "b")
	f.relay.set(t, f.storageCurrent(t), 0)
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not return within 2s of a commit")
	}
	if n := testutil.ToFloat64(f.metrics.ReadCacheDoorbells.WithLabelValues("rang")); n == 0 {
		t.Fatal("the long poll was not counted")
	}
}

func TestWaitForChangesWithoutACachePollsCurrent(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "change-wait-poll")
	if _, err := writer.Put(ctx, []byte("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- reader.WaitForChanges(ctx, bounds.Head) }()
	if _, err := writer.Put(ctx, []byte("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not return within 3s of a commit, polling every second")
	}
}

func TestWaitForChangesEndsOnCloseOrCancel(t *testing.T) {
	f := newChangeRelayFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	waited := make(chan error, 1)
	go func() { waited <- f.reader.WaitForChanges(ctx, f.head) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-waited:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled: %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not return when cancelled")
	}

	go func() { waited <- f.reader.WaitForChanges(f.ctx, f.head) }()
	time.Sleep(50 * time.Millisecond)
	if err := f.reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if !errors.Is(err, ErrChangeReaderClosed) {
			t.Fatalf("closed: %v, want ErrChangeReaderClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not return when the reader closed")
	}
}

func TestWaitForChangesWaitsForTheViewNotJustTheRing(t *testing.T) {
	f := newChangeRelayFixture(t)
	f.commit(t, "b")
	ahead := &fakeRelay{}
	ahead.set(t, f.storageCurrent(t), 0)
	f.ahead.Store(ahead) // rings for the commit; CURRENT still comes from the replica behind
	waited := make(chan error, 1)
	go func() { waited <- f.reader.WaitForChanges(f.ctx, f.head) }()
	time.Sleep(1500 * time.Millisecond)
	select {
	case err := <-waited:
		t.Fatalf("returned while the view a Read would use had not moved: %v", err)
	default:
	}
	if n := testutil.ToFloat64(f.metrics.ReadCacheDoorbells.WithLabelValues("rang")); n == 0 || n > 3 {
		t.Fatalf("%v rings in 1.5s, want them a second apart", n)
	}
	f.relay.set(t, f.storageCurrent(t), 0) // the replica behind catches up
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not return once the replica behind caught up")
	}
}

func TestWaitForChangesAnswersFromTheViewHeld(t *testing.T) {
	f := newChangeRelayFixture(t)
	f.commit(t, "b")
	f.relay.set(t, f.storageCurrent(t), 0)
	if _, err := f.reader.Bounds(f.ctx); err != nil { // the view held now has b
		t.Fatal(err)
	}
	polls := f.relay.longPolls.Load()
	start := time.Now()
	if err := f.reader.WaitForChanges(f.ctx, f.head); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 50*time.Millisecond || f.relay.longPolls.Load() != polls {
		t.Fatalf("took %s and %d long polls with changes already in the view held, want none",
			took, f.relay.longPolls.Load()-polls)
	}
}

func TestWaitForChangesPacesQuietAnswers(t *testing.T) {
	f := newChangeRelayFixture(t)
	quickRelay(f.relay)
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	before := f.relay.longPolls.Load()
	if err := f.reader.WaitForChanges(ctx, f.head); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("no commit: %v, want the deadline", err)
	}
	if n := f.relay.longPolls.Load() - before; n > 12 {
		t.Fatalf("%d long polls in 1s against a cache answering at once, want at most about 10", n)
	}
}

func TestWaitForChangesReadsStorageBehindAFrozenRelay(t *testing.T) {
	f := newChangeRelayFixture(t)
	f.relay.mu.Lock()
	f.relay.hold = 200 * time.Millisecond
	f.relay.mu.Unlock()
	// The relay cannot read storage: its copy stays at the head the reader
	// holds and ages past the limit, while a commit lands in storage.
	f.relay.set(t, f.storageCurrent(t), 10*time.Second)
	f.commit(t, "b")
	waited := make(chan error, 1)
	go func() { waited <- f.reader.WaitForChanges(f.ctx, f.head) }()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not return within 3s of a commit behind a frozen relay")
	}
	if f.outcome("too_old") == 0 {
		t.Fatal("the frozen copy was not refused as too old")
	}
}

func TestWaitForChangesTrustsAYoungQuietRelay(t *testing.T) {
	f := newChangeRelayFixture(t)
	f.relay.mu.Lock()
	f.relay.hold = 100 * time.Millisecond
	f.relay.mu.Unlock()
	reads := func() float64 { return f.outcome("relayed") + f.outcome("too_old") }
	before := reads()
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	if err := f.reader.WaitForChanges(ctx, f.head); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("no commit: %v, want the deadline", err)
	}
	if n := reads() - before; n != 0 {
		t.Fatalf("%v reads of CURRENT while a young relay answered quiet, want none", n)
	}
}
