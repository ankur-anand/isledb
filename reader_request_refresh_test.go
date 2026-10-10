package isledb

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestRequestRefreshLoadsWithoutWaitingForInterval(t *testing.T) {
	ctx, reader, _, store, ms := newRefreshTestReader(t)
	before := publishedSeq(reader)
	commitKeyB(t, ctx, store, ms)
	reader.RequestRefresh()
	waitForCondition(t, 3*time.Second, func() bool { return publishedSeq(reader) > before },
		"the requested refresh did not publish the newer view")
}

func TestRequestRefreshHoldsNewerViewInManualMode(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	before := publishedSeq(reader)
	commitKeyB(t, ctx, store, ms)
	reader.RequestRefresh()
	waitForCondition(t, 3*time.Second, func() bool { return nextViewSeq(reader) > before },
		"the requested refresh did not hold the newer view")
	if publishedSeq(reader) != before {
		t.Fatal("a requested refresh published in Manual mode")
	}
}

func TestRequestRefreshMergesBursts(t *testing.T) {
	_, reader, storage, _, _ := newRefreshTestReader(t)
	start := storage.currentReads()
	for range 50 {
		reader.RequestRefresh()
		time.Sleep(requestRefreshGap / 100)
	}
	time.Sleep(requestRefreshGap + requestRefreshGap/2)
	// 50 requests within one gap: one refresh at once and one at the end of
	// the gap, plus at most one the interval schedules meanwhile.
	if reads := storage.currentReads() - start; reads < 2 || reads > 3 {
		t.Fatalf("50 requests within one gap read CURRENT %d times, want 2 or 3", reads)
	}
}

func TestRequestRefreshInsideGapIsDeferredNotDropped(t *testing.T) {
	ctx, reader, storage, store, ms := newRefreshTestReader(t)
	start := storage.currentReads()
	reader.RequestRefresh()
	waitForCondition(t, 3*time.Second, func() bool {
		return storage.currentReads() > start && !reader.refreshing.Load()
	}, "the first request did not refresh")

	// The first refresh has finished, so only the second can load the commit.
	before := publishedSeq(reader)
	commitKeyB(t, ctx, store, ms)
	reader.RequestRefresh()
	waitForCondition(t, 3*requestRefreshGap, func() bool { return publishedSeq(reader) > before },
		"a request inside the gap was dropped")
}

func TestRequestRefreshAfterCloseDoesNothing(t *testing.T) {
	_, reader, storage, _, _ := newRefreshTestReader(t)
	opened := storage.currentReads()
	reader.RequestRefresh()
	reader.RequestRefresh() // deferred to the end of the gap
	waitForCondition(t, 3*time.Second, func() bool { return storage.currentReads() > opened },
		"the first request did not refresh")
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	reads := storage.currentReads()
	reader.RequestRefresh()
	time.Sleep(requestRefreshGap + requestRefreshGap/2)
	if got := storage.currentReads(); got != reads {
		t.Fatalf("CURRENT read %d more times after Close", got-reads)
	}
}

// TestRequestRefreshDuringCloseDoesNotDeadlock requests a refresh while Close
// waits for a read in progress: Close then holds the lifecycle lock and needs
// requestMu, so RequestRefresh must not wait for the lifecycle lock holding it.
func TestRequestRefreshDuringCloseDoesNotDeadlock(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("request-refresh-close")
	t.Cleanup(func() { _ = store.Close() })
	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("1")},
	}, 0, 1)
	// Not closed in cleanup: if Close deadlocks, a second Close would hang.
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	reader.lifecycleMu.RLock() // a read in progress
	closed := make(chan struct{})
	go func() {
		_ = reader.Close()
		close(closed)
	}()
	time.Sleep(50 * time.Millisecond) // Close now waits for the read
	go reader.RequestRefresh()
	waitForCondition(t, 3*time.Second, func() bool {
		if !reader.requestMu.TryLock() {
			return true // RequestRefresh holds it
		}
		defer reader.requestMu.Unlock()
		return !reader.requestLast.IsZero()
	}, "RequestRefresh never decided to refresh")
	time.Sleep(50 * time.Millisecond) // RequestRefresh now waits for the lifecycle lock
	reader.lifecycleMu.RUnlock()

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close deadlocked with RequestRefresh")
	}
}
