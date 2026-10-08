package isledb

import (
	"context"
	"errors"
	"testing"
	"time"
)

func takeNextView(t *testing.T, r *Reader) *NextView {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := r.NextView(ctx)
	if err != nil {
		t.Fatalf("NextView: %v", err)
	}
	return v
}

func noNextView(t *testing.T, r *Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if v, err := r.NextView(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("NextView = %v, %v; want it to wait", v, err)
	}
}

func TestNextViewRequiresManual(t *testing.T) {
	_, reader, _, _, _ := newRefreshTestReader(t)
	if _, err := reader.NextView(context.Background()); !errors.Is(err, ErrNotManual) {
		t.Fatalf("NextView outside Manual mode: %v, want ErrNotManual", err)
	}
}

func TestNextViewWaitsForNewerLoad(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	seq := publishedSeq(reader)
	noNextView(t, reader)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	v := takeNextView(t, reader)
	if v.Previous() != ViewPosition(seq) || v.Next() <= v.Previous() {
		t.Fatalf("Previous %d, Next %d; want %d and newer", v.Previous(), v.Next(), seq)
	}
	if len(v.Added()) != 1 || len(v.Removed()) != 0 {
		t.Fatalf("Added %d, Removed %d; want the one new SST", len(v.Added()), len(v.Removed()))
	}
}

func TestNextViewHandsOutEachLoadOnce(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	first := takeNextView(t, reader)
	first.Discard()
	noNextView(t, reader)
	fireViewTimer(reader) // reloads the same position
	again := takeNextView(t, reader)
	if again.Next() != first.Next() {
		t.Fatalf("next load at %d, want the same position %d", again.Next(), first.Next())
	}
	if err := again.Publish(); err != nil {
		t.Fatalf("publishing a reloaded view at the same position: %v", err)
	}
	if reader.ViewPosition() != again.Next() {
		t.Fatal("the reloaded view was not published")
	}
}

func TestNextViewNeverAtPublishedPosition(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	noNextView(t, reader)
}

func TestNextViewConcurrentCallersGetDifferentLoads(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	views := make(chan *NextView, 2)
	for range 2 {
		go func() {
			v, err := reader.NextView(ctx)
			if err != nil {
				t.Error(err)
			}
			views <- v
		}()
	}
	commitKey(t, ctx, store, ms, "b", 2)
	fireViewTimer(reader)
	first := <-views
	commitKey(t, ctx, store, ms, "c", 3)
	fireViewTimer(reader)
	second := <-views
	if first == nil || second == nil || first.Next() == second.Next() {
		t.Fatal("two callers did not receive two different loads")
	}
}

func TestNextViewSnapshotReadsUnpublishedView(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	v := takeNextView(t, reader)
	snap, err := v.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if _, found, err := snap.Get(ctx, []byte("b")); err != nil || !found {
		t.Fatalf("snapshot of the next view: found=%v err=%v", found, err)
	}
	if _, found, _ := reader.Get(ctx, []byte("b")); found {
		t.Fatal("reads saw the next view before it was published")
	}
	if err := v.Publish(); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := reader.Get(ctx, []byte("b")); !found {
		t.Fatal("reads did not switch to the published view")
	}
}

func TestNextViewDoneAfterPublishOrDiscard(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKey(t, ctx, store, ms, "b", 2)
	fireViewTimer(reader)
	published := takeNextView(t, reader)
	if err := published.Publish(); err != nil {
		t.Fatal(err)
	}
	if err := published.Publish(); !errors.Is(err, ErrNextViewDone) {
		t.Fatalf("second Publish: %v, want ErrNextViewDone", err)
	}
	commitKey(t, ctx, store, ms, "c", 3)
	fireViewTimer(reader)
	discarded := takeNextView(t, reader)
	discarded.Discard()
	if err := discarded.Publish(); !errors.Is(err, ErrNextViewDone) {
		t.Fatalf("Publish after Discard: %v, want ErrNextViewDone", err)
	}
	if _, err := discarded.Snapshot(); !errors.Is(err, ErrNextViewDone) {
		t.Fatalf("Snapshot after Discard: %v, want ErrNextViewDone", err)
	}
}

func TestNextViewPublishFailsAfterRefresh(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	v := takeNextView(t, reader)
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := v.Publish(); !errors.Is(err, ErrViewChanged) {
		t.Fatalf("Publish after Refresh published: %v, want ErrViewChanged", err)
	}
}

func TestNextViewSnapshotSurvivesSafetyNetPublish(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKey(t, ctx, store, ms, "b", 2)
	fireViewTimer(reader)
	v := takeNextView(t, reader)
	snap, err := v.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	commitKey(t, ctx, store, ms, "c", 3)
	fireViewTimer(reader)
	passMaxLag(reader)
	fireViewTimer(reader)
	if reader.ViewPosition() == v.Next() || reader.safetyPublishes.Load() != 1 {
		t.Fatal("the safety net did not publish the newer view")
	}
	if _, found, err := snap.Get(ctx, []byte("b")); err != nil || !found {
		t.Fatalf("next-view snapshot after a different view was published: found=%v err=%v", found, err)
	}
	if _, found, _ := snap.Get(ctx, []byte("c")); found {
		t.Fatal("the snapshot saw a later view")
	}
}

func TestNextViewReturnsWhenReaderCloses(t *testing.T) {
	_, reader, _, _, _ := newManualTestReader(t)
	errs := make(chan error, 1)
	go func() {
		_, err := reader.NextView(context.Background())
		errs <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = reader.Close()
	select {
	case err := <-errs:
		if !errors.Is(err, ErrReaderClosed) {
			t.Fatalf("NextView after Close: %v, want ErrReaderClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("NextView did not return when the reader closed")
	}
}

func TestNextViewAfterCloseWithPendingLoad(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	if nextViewSeq(reader) == 0 {
		t.Fatal("no load was kept")
	}
	_ = reader.Close()
	if _, err := reader.NextView(ctx); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("NextView on a closed reader with a kept load: %v, want ErrReaderClosed", err)
	}
}

func TestNextViewPublishAfterClose(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	v := takeNextView(t, reader)
	_ = reader.Close()
	if err := v.Publish(); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("Publish on a closed reader: %v, want ErrReaderClosed", err)
	}
}

func TestPublishAtAfterCloseRetiringSSTs(t *testing.T) {
	ctx, reader, _, _, _ := newManualTestReader(t)
	if _, _, err := reader.Get(ctx, []byte("a")); err != nil {
		t.Fatal(err)
	}
	seq := publishedSeq(reader)
	_ = reader.Close()
	if err := reader.publishAt(nextLoadedView(seq+1, time.Now()), seq); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("publishing a view that retires every SST on a closed reader: %v, want ErrReaderClosed", err)
	}
}
