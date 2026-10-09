package isledb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
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

func sstNamed(t *testing.T, m *manifestState, id string) sstMetadata {
	t.Helper()
	for _, sst := range sstMetadataOf(m) {
		if sst.ID == id {
			return sst
		}
	}
	t.Fatalf("SST %s not in the view", id)
	return sstMetadata{}
}

func TestNextViewPrefetchFetchesOnlyAdded(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	v := takeNextView(t, reader)
	stats, err := v.Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.MatchedSSTs != 1 || stats.CachedSSTs != 1 {
		t.Fatalf("stats %+v, want the one added SST cached", stats)
	}
	added := sstNamed(t, v.view.manifest, v.Added()[0].ID)
	if !reader.fetcher.resident(reader.fetcher.object(added)) {
		t.Fatal("the added SST is not on disk")
	}
	for _, sst := range sstMetadataOf(v.published) {
		if reader.fetcher.resident(reader.fetcher.object(sst)) {
			t.Fatalf("SST %s, shared with the published view, was fetched", sst.ID)
		}
	}
}

func TestNextViewPrefetchRangeAndMaxBytes(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	v := takeNextView(t, reader)
	if stats, err := v.Prefetch(ctx, PrefetchOptions{Range: KeyRange{Min: []byte("x"), Max: []byte("z")}}); err != nil || stats.MatchedSSTs != 0 {
		t.Fatalf("a range missing the added SST: %+v, %v", stats, err)
	}
	if stats, err := v.Prefetch(ctx, PrefetchOptions{All: true, MaxBytes: 1}); err != nil || stats.CachedSSTs != 0 || stats.SkippedSSTs != 1 {
		t.Fatalf("MaxBytes below the added SST: %+v, %v", stats, err)
	}
}

func TestNextViewPrefetchUsesOnlyFreeSpace(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("next-view-budget")
	t.Cleanup(func() { _ = store.Close() })
	ms := manifest.NewStore(store)
	a := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("1")},
	}, 0, 1).Meta
	commitKeyB(t, ctx, store, ms)

	// A data tier that holds either SST, but not both.
	var reader *Reader
	for size := int64(1024); ; size += 64 {
		r, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir(), DiskCacheSize: size})
		if err != nil {
			t.Fatal(err)
		}
		tier := r.diskCache.Stats(diskcache.TierData).MaxBytes
		m := r.currentManifest()
		b := m.L0SSTs[0]
		if b.ID == a.ID {
			b = m.L0SSTs[1]
		}
		if tier >= max(a.Size, b.Size) && tier < a.Size+b.Size {
			reader = r
			break
		}
		_ = r.Close()
	}
	t.Cleanup(func() { _ = reader.Close() })
	both := reader.currentManifest()
	a = sstNamed(t, both, a.ID)
	var b sstMetadata
	for _, sst := range both.L0SSTs {
		if sst.ID != a.ID {
			b = sst
		}
	}
	if _, err := reader.fetcher.prefetch(ctx, reader.fetcher.object(a)); err != nil {
		t.Fatal(err)
	}

	onlyA, onlyB := both.Clone(), both.Clone()
	onlyA.L0SSTs = []sstMetadata{a}
	onlyB.L0SSTs = []sstMetadata{b}
	current := reader.manifestStore.CurrentData()
	view := loadedView{manifest: onlyB, current: current, loadedAt: time.Now()}

	// Reads still use a, which the next view drops: b does not fit beside it.
	stats, err := newNextView(reader, view, 0, onlyA).Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil || stats.CachedSSTs != 0 || stats.SkippedSSTs != 1 {
		t.Fatalf("prefetch beside a published SST in use: %+v, %v; want b skipped", stats, err)
	}
	if !reader.fetcher.resident(reader.fetcher.object(a)) {
		t.Fatal("prefetching the next view evicted an SST reads use")
	}
	// Once a is deleted from disk, as a dead SST is, b fits.
	reader.fetcher.dropDeadObject(reader.fetcher.object(a))
	stats, err = newNextView(reader, view, 0, onlyA).Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil || stats.CachedSSTs != 1 {
		t.Fatalf("prefetch with nothing published in use: %+v, %v; want b cached", stats, err)
	}
}

// twoSSTReader opens a reader over a store with SSTs a and b, whose disk cache
// holds either but not both, and returns it with both SSTs.
func twoSSTReader(t *testing.T) (context.Context, *Reader, sstMetadata, sstMetadata) {
	t.Helper()
	ctx := context.Background()
	store := blobstore.NewMemory("next-view-two")
	t.Cleanup(func() { _ = store.Close() })
	ms := manifest.NewStore(store)
	aID := writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("1")},
	}, 0, 1).Meta.ID
	commitKeyB(t, ctx, store, ms)
	for size := int64(1024); ; size += 64 {
		r, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir(), DiskCacheSize: size})
		if err != nil {
			t.Fatal(err)
		}
		m := r.currentManifest()
		a, b := m.L0SSTs[0], m.L0SSTs[1]
		if a.ID != aID {
			a, b = b, a
		}
		tier := r.diskCache.Stats(diskcache.TierData).MaxBytes
		if tier >= max(a.Size, b.Size) && tier < a.Size+b.Size {
			t.Cleanup(func() { _ = r.Close() })
			return ctx, r, a, b
		}
		_ = r.Close()
	}
}

func TestNextViewPrefetchProtectsDeeperSharedSST(t *testing.T) {
	ctx, reader, a, b := twoSSTReader(t)
	if _, err := reader.fetcher.prefetch(ctx, reader.fetcher.object(a)); err != nil {
		t.Fatal(err)
	}
	published := reader.currentManifest().Clone()
	published.L0SSTs = nil
	published.AddLevelSSTs(1, []sstMetadata{a})
	next := published.Clone()
	next.AddL0SST(b)
	view := loadedView{manifest: next, current: reader.manifestStore.CurrentData(), loadedAt: time.Now()}
	stats, err := newNextView(reader, view, 0, published).Prefetch(ctx, PrefetchOptions{All: true})
	if err != nil || stats.CachedSSTs != 0 || stats.SkippedSSTs != 1 {
		t.Fatalf("added L0 SST beside a shared L1 SST on disk: %+v, %v; want it skipped", stats, err)
	}
	if !reader.fetcher.resident(reader.fetcher.object(a)) {
		t.Fatal("prefetching the next view evicted the shared SST")
	}
}

func waitDeadDrops(t *testing.T, r *Reader, n int64) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if r.dead.drops.Load() >= n {
			return
		}
	}
	t.Fatalf("%d dead SSTs dropped, want %d", r.dead.drops.Load(), n)
}

func TestPublishDropsRetiredSSTsFromDisk(t *testing.T) {
	ctx, reader, a, _ := twoSSTReader(t)
	if _, err := reader.fetcher.prefetch(ctx, reader.fetcher.object(a)); err != nil {
		t.Fatal(err)
	}
	seq := publishedSeq(reader)
	if err := reader.publishAt(nextLoadedView(seq+1, time.Now()), seq); err != nil {
		t.Fatal(err)
	}
	waitDeadDrops(t, reader, 2)
	if reader.fetcher.resident(reader.fetcher.object(a)) {
		t.Fatal("a retired SST is still on disk")
	}
}

func TestPublishDoesNotWaitForDrops(t *testing.T) {
	ctx, reader, a, _ := twoSSTReader(t)
	if _, err := reader.fetcher.prefetch(ctx, reader.fetcher.object(a)); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	reader.dead.gate = gate
	seq := publishedSeq(reader)
	published := make(chan error, 1)
	go func() { published <- reader.publishAt(nextLoadedView(seq+1, time.Now()), seq) }()
	select {
	case err := <-published:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publish waited for the dead SSTs to be deleted")
	}
	if !reader.fetcher.resident(reader.fetcher.object(a)) {
		t.Fatal("a dead SST was deleted before the gate opened")
	}
	close(gate)
	waitDeadDrops(t, reader, 2)
}

func TestCloseInterruptsDeletePause(t *testing.T) {
	rate := deadSSTDeleteRate
	deadSSTDeleteRate = 1
	t.Cleanup(func() { deadSSTDeleteRate = rate })
	ctx, reader, a, _ := twoSSTReader(t)
	if _, err := reader.fetcher.prefetch(ctx, reader.fetcher.object(a)); err != nil {
		t.Fatal(err)
	}
	seq := publishedSeq(reader)
	if err := reader.publishAt(nextLoadedView(seq+1, time.Now()), seq); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); reader.fetcher.resident(reader.fetcher.object(a)); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("a retired SST is still on disk")
		}
	}
	start := time.Now()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("Close took %v while the deleter paused", took)
	}
}
