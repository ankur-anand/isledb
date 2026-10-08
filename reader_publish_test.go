package isledb

import (
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/manifest"
)

func publishedSeq(r *Reader) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.viewSeq
}

// makeViewDue lets a failed refresh mark reads stale.
func makeViewDue(r *Reader) {
	r.mu.Lock()
	r.viewRefreshAt = time.Now().Add(-time.Second)
	r.mu.Unlock()
}

func TestPublishLoadedViewOncePerLoad(t *testing.T) {
	_, reader, _, _, _ := newRefreshTestReader(t)
	seq := publishedSeq(reader)
	view := loadedView{
		manifest: &manifestState{},
		current:  &manifest.Current{NextSeq: seq + 1},
		loadedAt: time.Now(),
		fromSeq:  seq,
	}
	timers := reader.viewTimerID.Load()
	reader.publishLoadedView(view)
	reader.publishLoadedView(view)
	if got := reader.viewTimerID.Load() - timers; got != 1 {
		t.Fatalf("one load published %d times", got)
	}
	if publishedSeq(reader) != seq+1 {
		t.Fatal("the newer view was not published")
	}
}

func TestRefreshRenewsUnchangedView(t *testing.T) {
	ctx, reader, _, _, _ := newRefreshTestReader(t)
	reader.mu.RLock()
	seq, expires := reader.viewSeq, reader.viewExpiresAt
	reader.mu.RUnlock()
	time.Sleep(10 * time.Millisecond)
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	if reader.viewSeq != seq {
		t.Fatalf("view moved from %d to %d with nothing committed", seq, reader.viewSeq)
	}
	if !reader.viewExpiresAt.After(expires) {
		t.Fatal("an unchanged refresh did not renew the view's expiry")
	}
}

func TestPublishLoadedViewLostRaceIsNotAFailure(t *testing.T) {
	_, reader, _, _, _ := newRefreshTestReader(t)
	makeViewDue(reader)
	seq := publishedSeq(reader)
	reader.publishLoadedView(loadedView{
		manifest: &manifestState{},
		current:  &manifest.Current{NextSeq: seq - 1},
		loadedAt: time.Now(),
		fromSeq:  seq - 1, // another reload published since this one started
	})
	if reader.stale.Load() || publishedSeq(reader) != seq {
		t.Fatalf("a lost race counted as a failure or replaced the view (stale=%v)", reader.stale.Load())
	}
}

func TestPublishLoadedViewStoreWentBackFails(t *testing.T) {
	_, reader, _, _, _ := newRefreshTestReader(t)
	makeViewDue(reader)
	seq := publishedSeq(reader)
	reader.publishLoadedView(loadedView{
		manifest: &manifestState{},
		current:  &manifest.Current{NextSeq: seq - 1},
		loadedAt: time.Now(),
		fromSeq:  seq,
	})
	if !reader.stale.Load() {
		t.Fatal("a store behind the published view was not a failed refresh")
	}
	if publishedSeq(reader) != seq {
		t.Fatal("an older view replaced the published one")
	}
}

func TestSharedReloadPublishesOnce(t *testing.T) {
	ctx, reader, storage, store, ms := newRefreshTestReader(t)
	commitKeyB(t, ctx, store, ms)
	seq := publishedSeq(reader)
	timers := reader.viewTimerID.Load()

	storage.arm()
	errs := make(chan error, 2)
	go func() { errs <- reader.Refresh(ctx) }()
	<-storage.read
	go func() { errs <- reader.refreshManifest(ctx, false) }()
	for waiters := 0; waiters < 2; time.Sleep(time.Millisecond) {
		reader.manifestLoads.mu.Lock()
		if call := reader.manifestLoads.calls["manifest"]; call != nil {
			waiters = call.waiters
		}
		reader.manifestLoads.mu.Unlock()
	}
	close(storage.release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("refresh: %v", err)
		}
	}
	if got := reader.viewTimerID.Load() - timers; got != 1 {
		t.Fatalf("one shared load published %d times", got)
	}
	if publishedSeq(reader) <= seq {
		t.Fatal("the newer view was not published")
	}
	assertReaderHasB(t, ctx, reader)
}
