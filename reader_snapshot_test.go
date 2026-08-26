package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestReaderSnapshotPinsLoadedState(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-snapshot-refresh")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	snap1, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	defer snap1.Close()

	if value, found, err := snap1.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("snap1.Get(a): %v", err)
	} else if !found || !bytes.Equal(value, []byte("va")) {
		t.Fatalf("unexpected snap1 value: %q found=%v", value, found)
	}

	version1 := snap1.Version()
	if version1.IsZero() {
		t.Fatal("expected non-zero version")
	}

	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Value: []byte("vb")},
	}, 0, 1)

	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	snap2, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() after refresh: %v", err)
	}
	defer snap2.Close()

	if snap2.Version() == version1 {
		t.Fatalf("expected version advance, still got %q", version1.String())
	}

	if value, found, err := snap2.Get(ctx, []byte("b")); err != nil {
		t.Fatalf("snap2.Get(b): %v", err)
	} else if !found || !bytes.Equal(value, []byte("vb")) {
		t.Fatalf("unexpected snap2 value: %q found=%v", value, found)
	}

	if _, found, err := snap1.Get(ctx, []byte("b")); err != nil {
		t.Fatalf("snap1.Get(b) after refresh: %v", err)
	} else if found {
		t.Fatal("expected old snapshot to remain immutable and not see b")
	}
}

func TestReaderBootstrapViewResumesAfterItsSnapshot(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "reader-bootstrap-view")

	if err := writer.Put(ctx, []byte("before"), []byte("v1")); err != nil {
		t.Fatalf("Put(before): %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush(before): %v", err)
	}

	reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()

	view, err := reader.BootstrapView(ctx)
	if err != nil {
		t.Fatalf("BootstrapView: %v", err)
	}
	defer view.Snapshot.Close()

	if view.Version.IsZero() {
		t.Fatal("BootstrapView returned a zero version")
	}
	if view.Version != view.Snapshot.Version() {
		t.Fatalf("view version=%q snapshot version=%q",
			view.Version.String(), view.Snapshot.Version().String())
	}
	if view.Cursor.IsZero() {
		t.Fatal("BootstrapView returned the zero startup-policy cursor")
	}
	if value, found, err := view.Snapshot.Get(ctx, []byte("before")); err != nil {
		t.Fatalf("Snapshot.Get(before): %v", err)
	} else if !found || !bytes.Equal(value, []byte("v1")) {
		t.Fatalf("Snapshot.Get(before)=%q,%v want v1,true", value, found)
	}

	changes, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("OpenChangeReader: %v", err)
	}
	defer changes.Close()
	bounds, err := changes.Bounds(ctx)
	if err != nil {
		t.Fatalf("Bounds: %v", err)
	}
	if view.Cursor != bounds.Head {
		t.Fatalf("bootstrap cursor=%q current head=%q",
			view.Cursor.String(), bounds.Head.String())
	}

	if err := writer.Put(ctx, []byte("after"), []byte("v2")); err != nil {
		t.Fatalf("Put(after): %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush(after): %v", err)
	}
	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	if _, found, err := view.Snapshot.Get(ctx, []byte("after")); err != nil {
		t.Fatalf("Snapshot.Get(after): %v", err)
	} else if found {
		t.Fatal("bootstrap snapshot observed a write published after its cursor")
	}

	page, err := changes.Read(ctx, view.Cursor, DefaultChangeReadOptions())
	if err != nil {
		t.Fatalf("Read from bootstrap cursor: %v", err)
	}
	if len(page.Changes) != 1 {
		t.Fatalf("changes after bootstrap=%d want=1", len(page.Changes))
	}
	change := page.Changes[0]
	if change.Operation != ChangePut || !bytes.Equal(change.Key, []byte("after")) ||
		!change.HasValue || !bytes.Equal(change.Value, []byte("v2")) {
		t.Fatalf("unexpected change after bootstrap: %+v", change)
	}
}

func TestReaderBootstrapViewRequiresChangeFeed(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-bootstrap-disabled")
	defer store.Close()

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	if _, err := reader.BootstrapView(ctx); !errors.Is(err, ErrChangeFeedDisabled) {
		t.Fatalf("BootstrapView error=%v want=%v", err, ErrChangeFeedDisabled)
	}
}

func TestReaderBootstrapViewOnEmptyEnabledFeedReturnsExplicitHead(t *testing.T) {
	ctx := context.Background()
	_, db, _ := openChangeReaderTestDB(t, "reader-bootstrap-empty")

	reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()

	view, err := reader.BootstrapView(ctx)
	if err != nil {
		t.Fatalf("BootstrapView: %v", err)
	}
	defer view.Snapshot.Close()
	if view.Cursor.IsZero() {
		t.Fatal("empty bootstrap returned zero cursor instead of explicit feed head")
	}

	changes, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("OpenChangeReader: %v", err)
	}
	defer changes.Close()
	bounds, err := changes.Bounds(ctx)
	if err != nil {
		t.Fatalf("Bounds: %v", err)
	}
	if view.Cursor != bounds.Head {
		t.Fatalf("bootstrap cursor=%q current head=%q",
			view.Cursor.String(), bounds.Head.String())
	}
}

func TestReaderBootstrapViewCursorRoundTripsAcrossMultiChangeBatch(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "reader-bootstrap-cursor-round-trip")

	if err := writer.Put(ctx, []byte("keep"), []byte("old")); err != nil {
		t.Fatalf("Put(keep): %v", err)
	}
	if err := writer.Put(ctx, []byte("gone"), []byte("temporary")); err != nil {
		t.Fatalf("Put(gone): %v", err)
	}
	if err := writer.Put(ctx, []byte("empty"), []byte{}); err != nil {
		t.Fatalf("Put(empty): %v", err)
	}
	if err := writer.Delete(ctx, []byte("gone")); err != nil {
		t.Fatalf("Delete(gone): %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush(initial): %v", err)
	}

	reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()
	view, err := reader.BootstrapView(ctx)
	if err != nil {
		t.Fatalf("BootstrapView: %v", err)
	}
	defer view.Snapshot.Close()

	state := bootstrapSnapshotState(t, ctx, view.Snapshot)
	assertBootstrapValue(t, state, "keep", "old")
	assertBootstrapValue(t, state, "empty", "")
	if _, found := state["gone"]; found {
		t.Fatal("bootstrap snapshot retained a key deleted before capture")
	}

	encoded := view.Cursor.String()
	restored, err := ParseChangeCursor(encoded)
	if err != nil {
		t.Fatalf("ParseChangeCursor(%q): %v", encoded, err)
	}
	if restored != view.Cursor {
		t.Fatalf("restored cursor=%q want=%q", restored.String(), view.Cursor.String())
	}

	if err := writer.Put(ctx, []byte("keep"), []byte("new")); err != nil {
		t.Fatalf("Put(keep after capture): %v", err)
	}
	if err := writer.Delete(ctx, []byte("empty")); err != nil {
		t.Fatalf("Delete(empty after capture): %v", err)
	}
	if err := writer.Put(ctx, []byte("added"), []byte("value")); err != nil {
		t.Fatalf("Put(added after capture): %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush(after capture): %v", err)
	}

	changes, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("OpenChangeReader: %v", err)
	}
	defer changes.Close()
	replayBootstrapChanges(t, ctx, changes, restored, state, ChangeReadOptions{
		MaxChanges: 1,
		MaxBytes:   1 << 20,
	})

	assertBootstrapValue(t, state, "keep", "new")
	assertBootstrapValue(t, state, "added", "value")
	if _, found := state["empty"]; found {
		t.Fatal("replayed state retained a key deleted after capture")
	}
	if _, found := state["gone"]; found {
		t.Fatal("replayed state resurrected a key deleted before capture")
	}
}

func TestReaderBootstrapViewsCapturedDuringWritesHaveNoGaps(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "reader-bootstrap-concurrent")

	reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()

	const flushes = 24
	progress := make(chan int)
	writerResult := make(chan error, 1)
	go func() {
		defer close(progress)
		for i := 0; i < flushes; i++ {
			key := []byte(fmt.Sprintf("key-%02d", i%7))
			var err error
			if i%5 == 4 {
				err = writer.Delete(ctx, key)
			} else {
				err = writer.Put(ctx, key, []byte(fmt.Sprintf("value-%02d", i)))
			}
			if err != nil {
				writerResult <- fmt.Errorf("mutation %d: %w", i, err)
				return
			}
			if err := writer.Flush(ctx); err != nil {
				writerResult <- fmt.Errorf("flush %d: %w", i, err)
				return
			}
			progress <- i
		}
		writerResult <- nil
	}()

	views := make([]*BootstrapView, 0, flushes)
	var captureErr error
	for range progress {
		if captureErr != nil {
			continue
		}
		// The writer is free to start its next flush as soon as this receive
		// completes, so publication can overlap Refresh and BootstrapView.
		if err := reader.Refresh(ctx); err != nil {
			captureErr = fmt.Errorf("refresh: %w", err)
			continue
		}
		view, err := reader.BootstrapView(ctx)
		if err != nil {
			captureErr = fmt.Errorf("bootstrap view: %w", err)
			continue
		}
		if view.Version != view.Snapshot.Version() {
			_ = view.Snapshot.Close()
			captureErr = fmt.Errorf("bootstrap version %q differs from snapshot version %q",
				view.Version.String(), view.Snapshot.Version().String())
			continue
		}
		views = append(views, view)
	}
	if err := <-writerResult; err != nil {
		t.Fatal(err)
	}
	if captureErr != nil {
		t.Fatal(captureErr)
	}
	defer func() {
		for _, view := range views {
			_ = view.Snapshot.Close()
		}
	}()
	if len(views) != flushes {
		t.Fatalf("captured views=%d want=%d", len(views), flushes)
	}

	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("final Refresh: %v", err)
	}
	finalSnapshot, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("final Snapshot: %v", err)
	}
	finalState := bootstrapSnapshotState(t, ctx, finalSnapshot)
	_ = finalSnapshot.Close()

	changes, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("OpenChangeReader: %v", err)
	}
	defer changes.Close()
	for i, view := range views {
		state := bootstrapSnapshotState(t, ctx, view.Snapshot)
		replayBootstrapChanges(t, ctx, changes, view.Cursor, state, DefaultChangeReadOptions())
		assertBootstrapStatesEqual(t, state, finalState, fmt.Sprintf("view %d", i))
	}
}

func TestReaderBootstrapCursorCanExpireWhileSnapshotRemainsReadable(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "reader-bootstrap-retention")

	if err := writer.Put(ctx, []byte("before"), []byte("value")); err != nil {
		t.Fatalf("Put(before): %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush(before): %v", err)
	}
	reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()
	view, err := reader.BootstrapView(ctx)
	if err != nil {
		t.Fatalf("BootstrapView: %v", err)
	}
	defer view.Snapshot.Close()

	if err := writer.Put(ctx, []byte("after"), []byte("later")); err != nil {
		t.Fatalf("Put(after): %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush(after): %v", err)
	}

	changes, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("OpenChangeReader(before retention): %v", err)
	}
	bounds, err := changes.Bounds(ctx)
	if err != nil {
		t.Fatalf("Bounds: %v", err)
	}
	if err := changes.Close(); err != nil {
		t.Fatalf("Close change reader: %v", err)
	}

	token, err := db.manifestStore.ClaimCompactor(ctx, "bootstrap-retention")
	if err != nil {
		t.Fatalf("ClaimCompactor: %v", err)
	}
	if _, err := db.manifestStore.AdvanceChangeFeedLogStart(ctx, bounds.Head.entry, token); err != nil {
		t.Fatalf("AdvanceChangeFeedLogStart: %v", err)
	}

	changes, err = db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("OpenChangeReader(after retention): %v", err)
	}
	defer changes.Close()
	if _, err := changes.Read(ctx, view.Cursor, DefaultChangeReadOptions()); !errors.Is(err, ErrChangeCursorExpired) {
		t.Fatalf("Read expired bootstrap cursor error=%v want=%v", err, ErrChangeCursorExpired)
	}

	if value, found, err := view.Snapshot.Get(ctx, []byte("before")); err != nil {
		t.Fatalf("Snapshot.Get after cursor expiry: %v", err)
	} else if !found || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("Snapshot.Get after cursor expiry=%q,%v want value,true", value, found)
	}
}

func bootstrapSnapshotState(t *testing.T, ctx context.Context, snapshot *Snapshot) map[string][]byte {
	t.Helper()
	rows, err := snapshot.ScanLimit(ctx, nil, nil, 0)
	if err != nil {
		t.Fatalf("scan bootstrap snapshot: %v", err)
	}
	state := make(map[string][]byte, len(rows))
	for _, row := range rows {
		state[string(row.Key)] = append([]byte(nil), row.Value...)
	}
	return state
}

func replayBootstrapChanges(
	t *testing.T,
	ctx context.Context,
	reader *ChangeReader,
	cursor ChangeCursor,
	state map[string][]byte,
	opts ChangeReadOptions,
) {
	t.Helper()
	for pageNumber := 0; pageNumber < 10_000; pageNumber++ {
		page, err := reader.Read(ctx, cursor, opts)
		if err != nil {
			t.Fatalf("read bootstrap changes at page %d: %v", pageNumber, err)
		}
		for _, change := range page.Changes {
			switch change.Operation {
			case ChangePut:
				if !change.HasValue {
					t.Fatalf("change at sequence %d omitted the value", change.Sequence)
				}
				state[string(change.Key)] = append([]byte(nil), change.Value...)
			case ChangeDelete:
				delete(state, string(change.Key))
			default:
				t.Fatalf("change at sequence %d has operation %d", change.Sequence, change.Operation)
			}
		}
		cursor = page.Next
		if page.CaughtUp() {
			return
		}
	}
	t.Fatal("change replay did not reach the observed head")
}

func assertBootstrapValue(t *testing.T, state map[string][]byte, key, want string) {
	t.Helper()
	value, found := state[key]
	if !found || !bytes.Equal(value, []byte(want)) {
		t.Fatalf("state[%q]=%q,%v want %q,true", key, value, found, want)
	}
}

func assertBootstrapStatesEqual(t *testing.T, got, want map[string][]byte, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s rows=%d want=%d; got=%v want=%v", label, len(got), len(want), got, want)
	}
	for key, wantValue := range want {
		gotValue, found := got[key]
		if !found || !bytes.Equal(gotValue, wantValue) {
			t.Fatalf("%s key=%q value=%q,%v want=%q,true", label, key, gotValue, found, wantValue)
		}
	}
}

func TestReaderSnapshotScanLimitAndIterator(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-snapshot-readers")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Value: []byte("vb")},
		{Key: []byte("c"), Seq: 3, Kind: internal.OpPut, Value: []byte("vc")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	snap, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	defer snap.Close()

	rows, err := snap.ScanLimit(ctx, []byte("a"), []byte("z"), 2)
	if err != nil {
		t.Fatalf("ScanLimit: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("unexpected ScanLimit row count: got=%d want=2", len(rows))
	}
	if !bytes.Equal(rows[0].Key, []byte("a")) || !bytes.Equal(rows[1].Key, []byte("b")) {
		t.Fatalf("unexpected ScanLimit keys: got=%q,%q", rows[0].Key, rows[1].Key)
	}

	iter, err := snap.NewIterator(ctx, IteratorOptions{
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
	if want := []string{"b"}; !sameStrings(got, want) {
		t.Fatalf("unexpected iterator keys: got=%v want=%v", got, want)
	}
}

func TestReaderSnapshotCloseIsIdempotentAndRejectsFurtherUse(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-snapshot-close")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	snap, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if _, _, err := snap.Get(ctx, []byte("a")); err != ErrSnapshotClosed {
		t.Fatalf("Get after Close error = %v, want %v", err, ErrSnapshotClosed)
	}
	if _, err := snap.NewIterator(ctx, IteratorOptions{}); err != ErrSnapshotClosed {
		t.Fatalf("NewIterator after Close error = %v, want %v", err, ErrSnapshotClosed)
	}
	if _, err := snap.ScanLimit(ctx, nil, nil, 1); err != ErrSnapshotClosed {
		t.Fatalf("ScanLimit after Close error = %v, want %v", err, ErrSnapshotClosed)
	}
}

func TestReaderCloseRejectsFurtherUse(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-close")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	snap, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	iter, err := reader.NewIterator(ctx, IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator(): %v", err)
	}
	if !iter.Next() {
		t.Fatalf("Iterator.Next() before close: %v", iter.Err())
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if _, _, err := reader.Get(ctx, []byte("a")); err != ErrReaderClosed {
		t.Fatalf("Get after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.Scan(ctx, nil, nil); err != ErrReaderClosed {
		t.Fatalf("Scan after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.ScanLimit(ctx, nil, nil, 1); err != ErrReaderClosed {
		t.Fatalf("ScanLimit after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.NewIterator(ctx, IteratorOptions{}); err != ErrReaderClosed {
		t.Fatalf("NewIterator after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if err := reader.Refresh(ctx); err != ErrReaderClosed {
		t.Fatalf("Refresh after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.Prefetch(ctx, PrefetchOptions{All: true}); err != ErrReaderClosed {
		t.Fatalf("Prefetch after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.Snapshot(ctx); err != ErrReaderClosed {
		t.Fatalf("Snapshot after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, err := reader.BootstrapView(ctx); err != ErrReaderClosed {
		t.Fatalf("BootstrapView after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if _, _, err := snap.Get(ctx, []byte("a")); err != ErrReaderClosed {
		t.Fatalf("Snapshot.Get after Reader.Close error=%v, want %v", err, ErrReaderClosed)
	}
	if iter.Next() {
		t.Fatal("Iterator.Next succeeded after Reader.Close")
	}
	if iter.SeekGE([]byte("a")) {
		t.Fatal("Iterator.SeekGE succeeded after Reader.Close")
	}
	if !errors.Is(iter.Err(), ErrReaderClosed) {
		t.Fatalf("Iterator.Err after Reader.Close=%v, want %v", iter.Err(), ErrReaderClosed)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("Iterator.Close after Reader.Close: %v", err)
	}
}

func TestOpenReaderDefaultOptionsAreOpenable(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-default-options")
	defer store.Close()

	cacheDir := t.TempDir()
	reader := openReaderFromDBForTest(t, ctx, store, DefaultReaderOpenOptions(cacheDir))
	if reader.cacheDir != cacheDir {
		t.Fatalf("reader cacheDir=%q, want %q", reader.cacheDir, cacheDir)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOpenReaderRequiresExplicitCacheDir(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-cache-dir-required")
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	if _, err := db.OpenReader(ctx, DefaultReaderOpenOptions("")); err == nil {
		t.Fatal("OpenReader with empty cache dir succeeded, want error")
	}
}

func TestReaderRefreshesExpiredManifestBeforeRead(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-auto-refresh")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Value: []byte("vb")},
	}, 0, 1)

	if _, found, err := reader.Get(ctx, []byte("b")); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	} else if found {
		t.Fatal("Get before expiry observed unrefreshed key")
	}

	reader.viewExpired.Store(true)
	value, found, err := reader.Get(ctx, []byte("b"))
	if err != nil {
		t.Fatalf("Get after expiry: %v", err)
	}
	if !found || !bytes.Equal(value, []byte("vb")) {
		t.Fatalf("Get after expiry = %q, %v; want vb, true", value, found)
	}
}

func TestReaderSnapshotAndIteratorExpire(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-view-expiry")
	defer store.Close()

	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("va")},
	}, 0, 1)

	reader := openTestReader(t, ctx, store)
	defer reader.Close()

	snapshot, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	iterator, err := snapshot.NewIterator(ctx, IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	iterator.expiresAt = time.Now().Add(-time.Second)
	if iterator.Next() {
		t.Fatal("expired iterator advanced")
	}
	if !errors.Is(iterator.Err(), ErrIteratorExpired) {
		t.Fatalf("expired iterator error = %v, want %v", iterator.Err(), ErrIteratorExpired)
	}
	_ = iterator.Close()

	snapshot.expiresAt = time.Now().Add(-time.Second)
	if _, _, err := snapshot.Get(ctx, []byte("a")); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("expired snapshot Get error = %v, want %v", err, ErrSnapshotExpired)
	}
	if _, err := snapshot.NewIterator(ctx, IteratorOptions{}); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("expired snapshot NewIterator error = %v, want %v", err, ErrSnapshotExpired)
	}
	if _, err := snapshot.ScanLimit(ctx, nil, nil, 1); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("expired snapshot ScanLimit error = %v, want %v", err, ErrSnapshotExpired)
	}
}

func TestOpenReaderRejectsNegativeViewPolicy(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-invalid-views")
	defer store.Close()

	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.Views.RefreshAfter = -time.Second
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	if _, err := db.OpenReader(ctx, opts); !errors.Is(err, ErrInvalidReaderOptions) {
		t.Fatalf("OpenReader error = %v, want %v", err, ErrInvalidReaderOptions)
	}
}

func openTestReader(t *testing.T, ctx context.Context, store *blobstore.Store) *Reader {
	t.Helper()

	opts := DefaultReaderOpenOptions(t.TempDir())
	return openReaderFromDBForTest(t, ctx, store, opts)
}

func sameStrings(left, right []string) bool {
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
