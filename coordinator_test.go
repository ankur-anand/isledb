package isledb

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
)

func TestCoordinatorRefreshPublishesImmutableViews(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("coordinator-refresh")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
	}, 0, 1)

	opts := DefaultCoordinatorOptions()
	opts.ReaderOptions.CacheDir = t.TempDir()

	coord, err := OpenCoordinator(ctx, store, opts)
	if err != nil {
		t.Fatalf("OpenCoordinator: %v", err)
	}
	defer coord.Close()

	view1 := coord.Current()
	if view1 == nil {
		t.Fatal("Current() = nil")
	}
	defer view1.Close()

	if value, found, err := view1.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("view1.Get(a): %v", err)
	} else if !found || !bytes.Equal(value, []byte("va")) {
		t.Fatalf("unexpected view1 value: %q found=%v", value, found)
	}

	version1 := view1.Version()
	if version1.IsZero() {
		t.Fatal("expected non-zero version")
	}

	sameView, changed, err := coord.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh no-op: %v", err)
	}
	if !sameVersion(version1, sameView.Version()) {
		t.Fatalf("expected same version, got before=%q after=%q", version1.String(), sameView.Version().String())
	}
	if changed {
		t.Fatal("expected changed=false on stable refresh")
	}
	_ = sameView.Close()

	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("vb")},
	}, 0, 1)

	view2, changed, err := coord.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh changed: %v", err)
	}
	defer view2.Close()

	if !changed {
		t.Fatal("expected changed=true after new SST")
	}
	if sameVersion(version1, view2.Version()) {
		t.Fatalf("expected version advance, still got %q", version1.String())
	}

	if value, found, err := view2.Get(ctx, []byte("b")); err != nil {
		t.Fatalf("view2.Get(b): %v", err)
	} else if !found || !bytes.Equal(value, []byte("vb")) {
		t.Fatalf("unexpected view2 value: %q found=%v", value, found)
	}

	if _, found, err := view1.Get(ctx, []byte("b")); err != nil {
		t.Fatalf("view1.Get(b) after refresh: %v", err)
	} else if found {
		t.Fatal("expected old view to remain immutable and not see b")
	}
}

func TestCoordinatorPublishesReaderManifestPointer(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("coordinator-pointer")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
	}, 0, 1)

	opts := DefaultCoordinatorOptions()
	opts.ReaderOptions.CacheDir = t.TempDir()

	coord, err := OpenCoordinator(ctx, store, opts)
	if err != nil {
		t.Fatalf("OpenCoordinator: %v", err)
	}
	defer coord.Close()

	view1, ok := coord.Current().(*coordinatorView)
	if !ok || view1 == nil {
		t.Fatal("Current() did not return coordinatorView")
	}
	defer view1.Close()

	if got, want := view1.state.manifest, coord.reader.currentManifest(); got != want {
		t.Fatal("current view should publish reader manifest pointer directly")
	}

	oldManifest := view1.state.manifest

	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("vb")},
	}, 0, 1)

	view2Raw, changed, err := coord.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh changed: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true after new SST")
	}

	view2, ok := view2Raw.(*coordinatorView)
	if !ok || view2 == nil {
		t.Fatal("Refresh() did not return coordinatorView")
	}
	defer view2.Close()

	if got, want := view2.state.manifest, coord.reader.currentManifest(); got != want {
		t.Fatal("refreshed view should publish current reader manifest pointer directly")
	}
	if view2.state.manifest == oldManifest {
		t.Fatal("expected refresh to publish a new manifest pointer")
	}
	if view1.state.manifest != oldManifest {
		t.Fatal("old view should retain its original manifest pointer")
	}
}

func TestCoordinatorViewCatchUpUsesFixedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("coordinator-catchup")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("vb")},
		{Key: []byte("c"), Seq: 3, Kind: internal.OpPut, Inline: true, Value: []byte("vc")},
	}, 0, 1)

	opts := DefaultCoordinatorOptions()
	opts.ReaderOptions.CacheDir = t.TempDir()

	coord, err := OpenCoordinator(ctx, store, opts)
	if err != nil {
		t.Fatalf("OpenCoordinator: %v", err)
	}
	defer coord.Close()

	view1 := coord.Current()
	if view1 == nil {
		t.Fatal("Current() = nil")
	}
	defer view1.Close()

	var got []string
	result, err := view1.CatchUp(ctx, CatchUpOptions{
		StartAfterKey: []byte("a"),
	}, func(kv KV) error {
		got = append(got, string(kv.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("view1.CatchUp: %v", err)
	}
	if want := []string{"b", "c"}; !equalStrings(got, want) {
		t.Fatalf("unexpected catch-up keys: got=%v want=%v", got, want)
	}
	if result.Count != 2 || result.Truncated {
		t.Fatalf("unexpected catch-up result: %+v", result)
	}
	if !bytes.Equal(result.LastKey, []byte("c")) {
		t.Fatalf("unexpected last key: %q", result.LastKey)
	}

	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("d"), Seq: 4, Kind: internal.OpPut, Inline: true, Value: []byte("vd")},
	}, 0, 1)

	got = got[:0]
	result, err = view1.CatchUp(ctx, CatchUpOptions{
		StartAfterKey: []byte("c"),
	}, func(kv KV) error {
		got = append(got, string(kv.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("view1.CatchUp after append: %v", err)
	}
	if len(got) != 0 || result.Count != 0 {
		t.Fatalf("expected old view to see no new records, got keys=%v result=%+v", got, result)
	}

	view2, changed, err := coord.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh after append: %v", err)
	}
	defer view2.Close()
	if !changed {
		t.Fatal("expected changed=true after append")
	}

	got = got[:0]
	result, err = view2.CatchUp(ctx, CatchUpOptions{
		StartAfterKey: []byte("c"),
	}, func(kv KV) error {
		got = append(got, string(kv.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("view2.CatchUp: %v", err)
	}
	if want := []string{"d"}; !equalStrings(got, want) {
		t.Fatalf("unexpected catch-up keys after refresh: got=%v want=%v", got, want)
	}
	if result.Count != 1 || result.Truncated {
		t.Fatalf("unexpected catch-up result after refresh: %+v", result)
	}
}

func TestCoordinatorViewScanLimitAndIterator(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("coordinator-view-readers")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("vb")},
		{Key: []byte("c"), Seq: 3, Kind: internal.OpPut, Inline: true, Value: []byte("vc")},
	}, 0, 1)

	opts := DefaultCoordinatorOptions()
	opts.ReaderOptions.CacheDir = t.TempDir()

	coord, err := OpenCoordinator(ctx, store, opts)
	if err != nil {
		t.Fatalf("OpenCoordinator: %v", err)
	}
	defer coord.Close()

	view := coord.Current()
	if view == nil {
		t.Fatal("Current() = nil")
	}
	defer view.Close()

	rows, err := view.ScanLimit(ctx, []byte("a"), []byte("z"), 2)
	if err != nil {
		t.Fatalf("ScanLimit: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("unexpected ScanLimit row count: got=%d want=2", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte("a")) || !bytes.Equal(rows[1].Key, []byte("b")) {
		t.Fatalf("unexpected ScanLimit keys: got=%q,%q", rows[0].Key, rows[1].Key)
	}

	iter, err := view.NewIterator(ctx, IteratorOptions{
		MinKey: []byte("b"),
		MaxKey: []byte("c"),
	})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer iter.Close()

	var got []string
	for iter.Next() {
		got = append(got, string(iter.Key()))
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator err: %v", err)
	}
	if want := []string{"b", "c"}; !equalStrings(got, want) {
		t.Fatalf("unexpected iterator keys: got=%v want=%v", got, want)
	}
}

func TestCoordinatorViewCloseIsIdempotentAndRejectsFurtherUse(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("coordinator-view-close")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
	}, 0, 1)

	opts := DefaultCoordinatorOptions()
	opts.ReaderOptions.CacheDir = t.TempDir()

	coord, err := OpenCoordinator(ctx, store, opts)
	if err != nil {
		t.Fatalf("OpenCoordinator: %v", err)
	}
	defer coord.Close()

	view := coord.Current()
	if view == nil {
		t.Fatal("Current() = nil")
	}

	if err := view.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if _, _, err := view.Get(ctx, []byte("a")); err != ErrViewClosed {
		t.Fatalf("Get after Close error = %v, want %v", err, ErrViewClosed)
	}
	if _, err := view.NewIterator(ctx, IteratorOptions{}); err != ErrViewClosed {
		t.Fatalf("NewIterator after Close error = %v, want %v", err, ErrViewClosed)
	}
	if _, err := view.ScanLimit(ctx, nil, nil, 1); err != ErrViewClosed {
		t.Fatalf("ScanLimit after Close error = %v, want %v", err, ErrViewClosed)
	}
	if _, err := view.CatchUp(ctx, CatchUpOptions{}, func(KV) error { return nil }); err != ErrViewClosed {
		t.Fatalf("CatchUp after Close error = %v, want %v", err, ErrViewClosed)
	}
}

func TestCoordinatorCloseWaitsForOpenViews(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("coordinator-close-waits")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
	}, 0, 1)

	opts := DefaultCoordinatorOptions()
	opts.ReaderOptions.CacheDir = t.TempDir()

	coord, err := OpenCoordinator(ctx, store, opts)
	if err != nil {
		t.Fatalf("OpenCoordinator: %v", err)
	}

	view := coord.Current()
	if view == nil {
		t.Fatal("Current() = nil")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- coord.Close()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned early while view was still open: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	if err := view.Close(); err != nil {
		t.Fatalf("view.Close: %v", err)
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for coordinator Close")
	}

	if got := coord.Current(); got != nil {
		_ = got.Close()
		t.Fatal("expected Current() to return nil after Close")
	}
	if _, _, err := coord.Refresh(ctx); err != ErrCoordinatorClosed {
		t.Fatalf("Refresh after Close error = %v, want %v", err, ErrCoordinatorClosed)
	}
}

func TestCoordinatorViewMaxCommittedLSN(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("coordinator-view-max-lsn")
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{
		CommittedLSNExtractor: BigEndianUint64LSNExtractor,
	})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	writer, err := db.OpenWriter(ctx, WriterOptions{FlushInterval: -1})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer writer.Close()

	putLSN := func(lsn uint64, value string) {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, lsn)
		if err := writer.Put(key, []byte(value)); err != nil {
			t.Fatalf("Put(%d): %v", lsn, err)
		}
	}

	putLSN(7, "v7")
	putLSN(42, "v42")
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	opts := DefaultCoordinatorOptions()
	opts.ReaderOptions.CacheDir = t.TempDir()

	coord, err := OpenCoordinator(ctx, store, opts)
	if err != nil {
		t.Fatalf("OpenCoordinator: %v", err)
	}
	defer coord.Close()

	view := coord.Current()
	if view == nil {
		t.Fatal("Current() = nil")
	}
	defer view.Close()

	lsn, found := view.MaxCommittedLSN()
	if !found {
		t.Fatal("expected max committed lsn to be found")
	}
	if lsn != 42 {
		t.Fatalf("unexpected max committed lsn: got=%d want=42", lsn)
	}

	low, found := view.LowWatermarkLSN()
	if !found {
		t.Fatal("expected low watermark lsn to be found")
	}
	if low != 7 {
		t.Fatalf("unexpected low watermark lsn: got=%d want=7", low)
	}
}

func sameVersion(left, right Version) bool {
	return left == right
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
