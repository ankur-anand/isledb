package isledb

import (
	"testing"
	"time"
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
