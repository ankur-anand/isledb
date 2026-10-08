package isledb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func newManualTestReader(t *testing.T) (context.Context, *Reader, *pausingManifestStorage, *blobstore.Store, *manifest.Store) {
	t.Helper()
	ctx := context.Background()
	store := blobstore.NewMemory("reader-manual")
	t.Cleanup(func() { _ = store.Close() })
	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("1")},
	}, 0, 1)
	storage := &pausingManifestStorage{pagedManifestStorage: manifest.NewBlobStoreBackend(store)}
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(), ManifestStorage: storage, DisableManifestPageCache: true,
		ViewPolicy: ReaderViewPolicy{Manual: true, MaxLag: time.Minute},
	})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return ctx, reader, storage, store, ms
}

func commitKey(t *testing.T, ctx context.Context, store *blobstore.Store, ms *manifest.Store, key string, seq uint64) {
	t.Helper()
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte(key), Seq: seq, Kind: internal.OpPut, Value: []byte(key)},
	}, 0, seq)
}

func nextViewSeq(r *Reader) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.nextView == nil {
		return 0
	}
	return currentNextSeq(r.nextView.current)
}

func outdatedSince(r *Reader) time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.outdatedSince
}

// passMaxLag makes the published view outdated for longer than MaxLag.
func passMaxLag(r *Reader) {
	r.mu.Lock()
	r.outdatedSince = time.Now().Add(-2 * r.viewPolicy.MaxLag)
	r.mu.Unlock()
}

func TestManualKeepsNewerViewUnpublished(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	seq := publishedSeq(reader)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	if publishedSeq(reader) != seq || reader.ViewPosition() != ViewPosition(seq) {
		t.Fatal("a newer view was published in Manual mode")
	}
	if nextViewSeq(reader) <= seq {
		t.Fatal("the newer view was not kept as the next view")
	}
	if _, found, _ := reader.Get(ctx, []byte("b")); found {
		t.Fatal("reads saw the unpublished view")
	}
	if outdatedSince(reader).IsZero() {
		t.Fatal("outdatedSince was not set")
	}
}

func TestManualKeptLoadKeepsRefreshing(t *testing.T) {
	ctx, reader, storage, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	fireViewTimer(reader)
	reads := storage.currentReads()
	fireViewTimer(reader)
	if storage.currentReads() == reads {
		t.Fatal("no refresh ran after a kept load")
	}
	reader.mu.RLock()
	scheduled := !reader.viewRefreshAt.IsZero() && reader.viewRefreshAt.After(time.Now())
	reader.mu.RUnlock()
	if !scheduled {
		t.Fatal("a kept load did not schedule the next refresh")
	}
}

func TestManualSafetyNetPublishesNewestView(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	first := publishedSeq(reader)
	commitKey(t, ctx, store, ms, "b", 2)
	fireViewTimer(reader)
	reader.mu.RLock()
	held := *reader.nextView
	reader.mu.RUnlock()
	commitKey(t, ctx, store, ms, "c", 3)
	fireViewTimer(reader)
	newest := nextViewSeq(reader)
	if newest <= currentNextSeq(held.current) {
		t.Fatal("the next view did not move to the newest load")
	}
	passMaxLag(reader)
	fireViewTimer(reader)
	if publishedSeq(reader) != newest {
		t.Fatalf("safety net published %d, want the newest %d", publishedSeq(reader), newest)
	}
	if got := reader.safetyPublishes.Load(); got != 1 {
		t.Fatalf("%d safety publishes, want 1", got)
	}
	if err := reader.publishAt(held, first); !errors.Is(err, ErrViewChanged) {
		t.Fatalf("publishing the held view after the safety net: %v, want ErrViewChanged", err)
	}
	if !outdatedSince(reader).IsZero() || nextViewSeq(reader) != 0 {
		t.Fatal("publishing did not clear outdatedSince and the next view")
	}
}

func TestManualOutdatedSinceSurvivesLoadsAndClearsWhenNewestPublished(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	seq := publishedSeq(reader)
	commitKey(t, ctx, store, ms, "b", 2)
	fireViewTimer(reader)
	since := outdatedSince(reader)
	commitKey(t, ctx, store, ms, "c", 3)
	fireViewTimer(reader)
	if !outdatedSince(reader).Equal(since) {
		t.Fatal("a later load restarted outdatedSince")
	}
	reader.mu.RLock()
	next := *reader.nextView
	reader.mu.RUnlock()
	if err := reader.publishAt(next, seq); err != nil {
		t.Fatal(err)
	}
	if !outdatedSince(reader).IsZero() || nextViewSeq(reader) != 0 {
		t.Fatal("an application publish did not clear outdatedSince and the next view")
	}
}

func TestManualRefreshPublishesAtOnce(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	seq := publishedSeq(reader)
	commitKeyB(t, ctx, store, ms)
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if publishedSeq(reader) <= seq {
		t.Fatal("Refresh did not publish in Manual mode")
	}
	assertReaderHasB(t, ctx, reader)
}

func TestManualRenewsUnchangedView(t *testing.T) {
	_, reader, _, _, _ := newManualTestReader(t)
	reader.mu.RLock()
	expires := reader.viewExpiresAt
	reader.mu.RUnlock()
	time.Sleep(10 * time.Millisecond)
	fireViewTimer(reader)
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	if !reader.viewExpiresAt.After(expires) {
		t.Fatal("an unchanged view was not renewed in Manual mode")
	}
}

func TestManualExpiredViewPublishes(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	commitKeyB(t, ctx, store, ms)
	reader.mu.Lock()
	reader.viewExpiresAt = time.Now().Add(-time.Second)
	reader.mu.Unlock()
	if _, found, err := reader.Get(ctx, []byte("b")); err != nil || !found {
		t.Fatalf("read on an expired view: found=%v err=%v, want the newer view published", found, err)
	}
}

func TestManualKeptLoadEndsFailureRun(t *testing.T) {
	ctx, reader, storage, store, ms := newManualTestReader(t)
	makeViewDue(reader)
	storage.setFail(errors.New("storage down"))
	fireViewTimer(reader)
	if !reader.stale.Load() {
		t.Fatal("a failed refresh did not mark reads stale")
	}
	storage.setFail(nil)
	commitKeyB(t, ctx, store, ms)
	reader.mu.RLock()
	expires := reader.viewExpiresAt
	reader.mu.RUnlock()
	fireViewTimer(reader)
	if reader.stale.Load() {
		t.Fatal("a kept load did not end the failure run")
	}
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	if !reader.viewExpiresAt.Equal(expires) {
		t.Fatal("a kept load renewed the published view")
	}
}

func TestManualMaxLagCheckedAtOpen(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-manual-open")
	t.Cleanup(func() { _ = store.Close() })
	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("1")},
	}, 0, 1)
	_, err := newReader(ctx, store, readerOptions{
		CacheDir:   t.TempDir(),
		ViewPolicy: ReaderViewPolicy{Manual: true, MaxLag: manifest.DefaultMaxPinnedViewAge / 2},
	})
	if !errors.Is(err, ErrInvalidReaderOptions) {
		t.Fatalf("open with MaxLag at half the pinned age: %v, want ErrInvalidReaderOptions", err)
	}
}

func TestManualMaxLagClampedToPinnedAge(t *testing.T) {
	_, reader, _, _, _ := newManualTestReader(t)
	if got := reader.maxLag(&manifest.Current{MaxPinnedViewAge: time.Minute}); got != 30*time.Second {
		t.Fatalf("maxLag with a 1m pinned age = %s, want 30s", got)
	}
	if got := reader.maxLag(&manifest.Current{}); got != time.Minute {
		t.Fatalf("maxLag with the default pinned age = %s, want the policy's 1m", got)
	}
}

func TestManualPublishOfOlderViewKeepsClockOfNewer(t *testing.T) {
	ctx, reader, _, store, ms := newManualTestReader(t)
	first := publishedSeq(reader)
	commitKey(t, ctx, store, ms, "b", 2)
	fireViewTimer(reader)
	reader.mu.RLock()
	held := *reader.nextView
	reader.mu.RUnlock()
	commitKey(t, ctx, store, ms, "c", 3)
	fireViewTimer(reader)
	reader.mu.RLock()
	newer := *reader.nextView
	reader.mu.RUnlock()
	time.Sleep(50 * time.Millisecond)
	if err := reader.publishAt(held, first); err != nil {
		t.Fatal(err)
	}
	if nextViewSeq(reader) != currentNextSeq(newer.current) {
		t.Fatal("the newer next view did not survive publishing an older one")
	}
	if !outdatedSince(reader).Equal(newer.loadedAt) {
		t.Fatalf("outdatedSince = %v, want the newer view's load time %v", outdatedSince(reader), newer.loadedAt)
	}

	// MaxLag has passed since the newer view was loaded, not since the publish.
	reader.viewPolicy.MaxLag = time.Since(newer.loadedAt) - 10*time.Millisecond
	fireViewTimer(reader)
	if publishedSeq(reader) != currentNextSeq(newer.current) || reader.safetyPublishes.Load() != 1 {
		t.Fatalf("published %d with %d safety publishes, want the newer view by the safety net",
			publishedSeq(reader), reader.safetyPublishes.Load())
	}
}
