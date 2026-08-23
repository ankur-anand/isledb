package isledb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestManifestPageCleanerRetiresCheckpointedPagesWithoutDataLoss(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-cleaner-checkpoint")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{storePolicy: StorePolicy{MaxPinnedViewAge: time.Millisecond}})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "page-cleaner-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := 0; i < 65; i++ {
		if _, err := db.manifestStore.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{
			ID:        fmt.Sprintf("page-cleaner-sst-%03d", i),
			Level:     0,
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("AppendAddSSTableWithFence(%d): %v", i, err)
		}
	}
	before, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(before): %v", err)
	}
	if len(before.IndexFrontier) != 1 {
		t.Fatalf("frontier pages=%d want=1", len(before.IndexFrontier))
	}
	retiredPage := before.IndexFrontier[0]

	now := time.Now().UTC()
	cleaner := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{
		OrphanGrace:  -1,
		SafetyMargin: -1,
		Now:          func() time.Time { return now },
	})
	reachableStats, err := cleaner.discover(ctx, now)
	if err != nil {
		t.Fatalf("discover reachable page: %v", err)
	}
	if reachableStats.Protected != 1 || reachableStats.PagesMarked != 0 {
		t.Fatalf("reachable discovery stats=%+v", reachableStats)
	}
	requireObjectExists(t, ctx, store, manifestPageRetirementMarkerPath(store, retiredPage.Path), false)

	opts := DefaultMaintenanceOptions()
	opts.SSTCompaction.L0TriggerSSTs = 1 << 20
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)
	checkpoint := stageSnapshotCheckpoint(t, ctx, db.manifestStore, maintenance)
	if _, err := db.manifestStore.ApplyPendingMaintenance(ctx); err != nil {
		t.Fatalf("ApplyPendingMaintenance: %v", err)
	}
	reconcileSnapshotCheckpoint(t, ctx, maintenance)
	after, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData(after): %v", err)
	}
	if after.Snapshot == nil || after.Snapshot.Path != checkpoint.Snapshot.Path {
		t.Fatalf("published snapshot=%+v want=%q", after.Snapshot, checkpoint.Snapshot.Path)
	}
	if after.LogSeqStart <= retiredPage.SeqHi || len(after.IndexFrontier) != 0 {
		t.Fatalf("checkpoint did not retire page: floor=%d page_hi=%d frontier=%+v",
			after.LogSeqStart, retiredPage.SeqHi, after.IndexFrontier)
	}

	marked, err := cleaner.discover(ctx, now)
	if err != nil {
		t.Fatalf("discover retired page: %v", err)
	}
	if marked.PagesMarked != 1 {
		t.Fatalf("retired discovery stats=%+v", marked)
	}
	markerPath := manifestPageRetirementMarkerPath(store, retiredPage.Path)
	mark, err := cleaner.readMark(ctx, markerPath)
	if err != nil {
		t.Fatalf("read page marker: %v", err)
	}
	if mark.Reason != "below_retained_floor" || mark.ObservedFloor != after.LogSeqStart {
		t.Fatalf("page marker=%+v", mark)
	}

	deferred, err := cleaner.sweep(ctx, mark.NotBefore.Add(-time.Nanosecond))
	if err != nil {
		t.Fatalf("sweep before deadline: %v", err)
	}
	if deferred.Deferred != 1 || deferred.PagesDeleted != 0 {
		t.Fatalf("pre-deadline sweep stats=%+v", deferred)
	}
	requireObjectExists(t, ctx, store, retiredPage.Path, true)

	deleted, err := cleaner.sweep(ctx, mark.NotBefore)
	if err != nil {
		t.Fatalf("sweep at deadline: %v", err)
	}
	if deleted.PagesDeleted != 1 || deleted.MarkersCleared != 1 {
		t.Fatalf("deadline sweep stats=%+v", deleted)
	}
	requireObjectExists(t, ctx, store, retiredPage.Path, false)
	requireObjectExists(t, ctx, store, markerPath, false)

	fresh := manifest.NewStore(store)
	replayed, err := fresh.Replay(ctx)
	if err != nil {
		t.Fatalf("fresh replay after page deletion: %v", err)
	}
	for i := 0; i < 65; i++ {
		if replayed.LookupSST(fmt.Sprintf("page-cleaner-sst-%03d", i)) == nil {
			t.Fatalf("SST %d missing after page reclamation", i)
		}
	}
}

func TestManifestPageCleanerRequiresCommittedFloor(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-cleaner-writer-proof")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{storePolicy: StorePolicy{MaxPinnedViewAge: time.Millisecond}})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	firstFence, err := db.manifestStore.ClaimWriter(ctx, "page-orphan-writer-1")
	if err != nil {
		t.Fatalf("ClaimWriter(first): %v", err)
	}
	createdAt := time.Now().UTC()
	if !createdAt.After(firstFence.ClaimedAt) {
		createdAt = firstFence.ClaimedAt.Add(time.Nanosecond)
	}
	page := writeStandaloneManifestPage(t, ctx, store, "current-writer-orphan", 1_000_000, createdAt)
	cleaner := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{
		OrphanGrace:  -1,
		SafetyMargin: -1,
	})

	unsafeStats, err := cleaner.discover(ctx, createdAt.Add(time.Second))
	if err != nil {
		t.Fatalf("discover current-writer page: %v", err)
	}
	if unsafeStats.PagesMarked != 0 || unsafeStats.Protected != 1 {
		t.Fatalf("current-writer discovery stats=%+v", unsafeStats)
	}
	requireObjectExists(t, ctx, store, manifestPageRetirementMarkerPath(store, page.Path), false)

	secondFence, err := db.manifestStore.ClaimWriter(ctx, "page-orphan-writer-2")
	if err != nil {
		t.Fatalf("ClaimWriter(second): %v", err)
	}
	if !secondFence.ClaimedAt.After(page.CreatedAt) {
		t.Fatalf("second fence=%s must be after page creation=%s", secondFence.ClaimedAt, page.CreatedAt)
	}
	stillUnsafe, err := cleaner.discover(ctx, secondFence.ClaimedAt)
	if err != nil {
		t.Fatalf("discover after new writer fence: %v", err)
	}
	if stillUnsafe.PagesMarked != 0 || stillUnsafe.Protected != 1 {
		t.Fatalf("new writer fence incorrectly authorized deletion: %+v", stillUnsafe)
	}
	requireObjectExists(t, ctx, store, manifestPageRetirementMarkerPath(store, page.Path), false)
}

func TestManifestPageCleanerRechecksReachabilityAndClearsMarker(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-cleaner-recheck")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "page-recheck-writer"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i := 0; i < 65; i++ {
		if _, err := db.manifestStore.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{ID: fmt.Sprintf("recheck-%d", i)}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil || len(current.IndexFrontier) != 1 {
		t.Fatalf("current=%+v error=%v", current, err)
	}
	page := current.IndexFrontier[0]
	now := time.Now().UTC()
	cleaner := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{OrphanGrace: -1, SafetyMargin: -1})
	// Simulate a page first observed as an orphan and then published before the
	// second observation. Sweep must trust fresh CURRENT, not the old marker.
	mark := manifestPageRetirementMark{
		Version:       manifestPageRetirementVersion,
		Page:          page,
		ObservedAt:    page.CreatedAt.Add(-2 * time.Hour),
		PinnedViewAge: time.Hour,
		SafetyMargin:  0,
		OrphanGrace:   0,
		NotBefore:     page.CreatedAt,
		ObservedFloor: page.SeqHi + 1,
		Reason:        "below_retained_floor",
	}
	payload, err := jsonMarshalPageMark(mark)
	if err != nil {
		t.Fatalf("marshal marker: %v", err)
	}
	markerPath := manifestPageRetirementMarkerPath(store, page.Path)
	if _, err := store.Write(ctx, markerPath, payload); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	stats, err := cleaner.sweep(ctx, now)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if stats.Protected != 1 || stats.MarkersCleared != 1 || stats.PagesDeleted != 0 {
		t.Fatalf("recheck stats=%+v", stats)
	}
	requireObjectExists(t, ctx, store, page.Path, true)
	requireObjectExists(t, ctx, store, markerPath, false)
}

type failKeyDeleter struct {
	base      objectDeleter
	failKey   string
	remaining int
	mu        sync.Mutex
}

func (d *failKeyDeleter) Delete(ctx context.Context, key string) error {
	d.mu.Lock()
	if key == d.failKey && d.remaining > 0 {
		d.remaining--
		d.mu.Unlock()
		return errors.New("injected delete failure")
	}
	d.mu.Unlock()
	return d.base.Delete(ctx, key)
}

func (d *failKeyDeleter) BatchDelete(ctx context.Context, keys []string) error {
	return d.base.BatchDelete(ctx, keys)
}

func TestManifestPageCleanerDeleteFailureAndRestartAreIdempotent(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-cleaner-restart")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{storePolicy: StorePolicy{MaxPinnedViewAge: time.Millisecond}})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	_, err = db.manifestStore.ClaimWriter(ctx, "restart-writer-1")
	if err != nil {
		t.Fatalf("ClaimWriter(first): %v", err)
	}
	for i := 0; i < 65; i++ {
		if _, err := db.manifestStore.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{ID: fmt.Sprintf("restart-sst-%d", i)}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	before, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil || len(before.IndexFrontier) != 1 {
		t.Fatalf("current before checkpoint=%+v error=%v", before, err)
	}
	page := before.IndexFrontier[0]
	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)
	stageSnapshotCheckpoint(t, ctx, db.manifestStore, maintenance)
	if _, err := db.manifestStore.ApplyPendingMaintenance(ctx); err != nil {
		t.Fatalf("ApplyPendingMaintenance: %v", err)
	}
	reconcileSnapshotCheckpoint(t, ctx, maintenance)
	now := time.Now().UTC()
	firstCleaner := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{
		OrphanGrace:  -1,
		SafetyMargin: -1,
		Deleter: &failKeyDeleter{
			base:      store,
			failKey:   page.Path,
			remaining: 1,
		},
	})
	stats, err := firstCleaner.discover(ctx, now)
	if err != nil || stats.PagesMarked != 1 {
		t.Fatalf("discover stats=%+v error=%v", stats, err)
	}
	mark, err := firstCleaner.readMark(ctx, manifestPageRetirementMarkerPath(store, page.Path))
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	failed, err := firstCleaner.sweep(ctx, mark.NotBefore)
	if err != nil {
		t.Fatalf("failed sweep: %v", err)
	}
	if failed.Failures != 1 || failed.PagesDeleted != 0 {
		t.Fatalf("failed sweep stats=%+v", failed)
	}
	requireObjectExists(t, ctx, store, page.Path, true)
	requireObjectExists(t, ctx, store, manifestPageRetirementMarkerPath(store, page.Path), true)

	restarted := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{OrphanGrace: -1, SafetyMargin: -1})
	completed, err := restarted.sweep(ctx, mark.NotBefore)
	if err != nil {
		t.Fatalf("restart sweep: %v", err)
	}
	if completed.PagesDeleted != 1 || completed.MarkersCleared != 1 {
		t.Fatalf("restart sweep stats=%+v", completed)
	}
	requireObjectExists(t, ctx, store, page.Path, false)
}

func TestManifestPageCleanerCorruptionAndBoundedScanningFailClosed(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-cleaner-corruption")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		path := store.ManifestPagePath(0, fmt.Sprintf("corrupt-%d", i))
		if _, err := store.Write(ctx, path, []byte("not-a-manifest-page")); err != nil {
			t.Fatalf("write corrupt page %d: %v", i, err)
		}
	}
	cleaner := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{PageScanLimit: 2})
	first, err := cleaner.discover(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("first discover: %v", err)
	}
	second, err := cleaner.discover(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("second discover: %v", err)
	}
	if first.ObjectsScanned != 2 || first.Failures != 2 || second.ObjectsScanned != 2 || second.Failures != 2 {
		t.Fatalf("bounded scans first=%+v second=%+v", first, second)
	}
	markers, err := store.List(ctx, blobstore.ListOptions{Prefix: manifestPageRetirementPrefix + "/"})
	if err != nil || len(markers.Objects) != 0 {
		t.Fatalf("corrupt pages produced markers=%+v error=%v", markers, err)
	}

	badMarkerPath := storeKey(store, manifestPageRetirementPrefix, "bad.json")
	if _, err := store.Write(ctx, badMarkerPath, []byte(`{"version":1}`)); err != nil {
		t.Fatalf("write bad marker: %v", err)
	}
	sweep, err := cleaner.sweep(ctx, time.Now().UTC().Add(48*time.Hour))
	if err != nil {
		t.Fatalf("sweep corrupt marker: %v", err)
	}
	if sweep.Failures != 1 || sweep.PagesDeleted != 0 {
		t.Fatalf("corrupt marker sweep=%+v", sweep)
	}
	requireObjectExists(t, ctx, store, badMarkerPath, true)
}

func TestManifestPageCleanerSpanningPageDoesNotEndListing(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-cleaner-spanning-page")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	createdAt := time.Now().UTC()
	wide := writeStandaloneManifestPageRange(t, ctx, store,
		fmt.Sprintf("%020d-%020d-wide", 0, 150), 0, 150, createdAt)
	narrow := writeStandaloneManifestPageRange(t, ctx, store,
		fmt.Sprintf("%020d-%020d-narrow", 50, 80), 50, 80, createdAt)
	if wide.Path >= narrow.Path {
		t.Fatalf("test requires wide page to list first: wide=%q narrow=%q", wide.Path, narrow.Path)
	}

	writeManifestFloorForPageCleanerTest(t, ctx, store, 100)
	cleaner := newManifestPageCleaner(store, db.manifestStore, manifestPageCleanerOptions{
		OrphanGrace:  -1,
		SafetyMargin: -1,
		Now:          func() time.Time { return createdAt },
	})

	first, err := cleaner.discover(ctx, createdAt)
	if err != nil {
		t.Fatalf("discover at floor 100: %v", err)
	}
	if first.ObjectsScanned != 2 || first.Protected != 1 || first.PagesMarked != 1 {
		t.Fatalf("discover at floor 100 stats=%+v", first)
	}
	requireObjectExists(t, ctx, store, manifestPageRetirementMarkerPath(store, wide.Path), false)
	requireObjectExists(t, ctx, store, manifestPageRetirementMarkerPath(store, narrow.Path), true)

	writeManifestFloorForPageCleanerTest(t, ctx, store, 151)
	second, err := cleaner.discover(ctx, createdAt.Add(time.Second))
	if err != nil {
		t.Fatalf("discover at floor 151: %v", err)
	}
	if second.ObjectsScanned != 2 || second.PagesMarked != 1 {
		t.Fatalf("discover at floor 151 stats=%+v", second)
	}
	requireObjectExists(t, ctx, store, manifestPageRetirementMarkerPath(store, wide.Path), true)
}

func writeStandaloneManifestPage(
	t *testing.T,
	ctx context.Context,
	store *blobstore.Store,
	id string,
	seq uint64,
	createdAt time.Time,
) manifest.PageRef {
	t.Helper()
	return writeStandaloneManifestPageRange(t, ctx, store, id, seq, seq, createdAt)
}

func writeStandaloneManifestPageRange(
	t *testing.T,
	ctx context.Context,
	store *blobstore.Store,
	id string,
	seqLo, seqHi uint64,
	createdAt time.Time,
) manifest.PageRef {
	t.Helper()
	entries := make([]manifest.ManifestLogEntry, 0, seqHi-seqLo+1)
	for seq := seqLo; seq <= seqHi; seq++ {
		entries = append(entries, manifest.ManifestLogEntry{Seq: seq})
	}
	page := &manifest.CommitPage{
		LayoutVersion: manifest.LayoutVersion,
		PageType:      manifest.CommitPageTypeLeaf,
		Level:         0,
		SeqLo:         seqLo,
		SeqHi:         seqHi,
		Count:         uint32(len(entries)),
		Entries:       entries,
		CreatedAt:     createdAt,
	}
	data, err := manifest.EncodeCommitPage(page)
	if err != nil {
		t.Fatalf("EncodeCommitPage: %v", err)
	}
	path := store.ManifestPagePath(0, id)
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("write page: %v", err)
	}
	ref, _, err := manifest.InspectCommitPage(path, data)
	if err != nil {
		t.Fatalf("InspectCommitPage: %v", err)
	}
	return ref
}

func writeManifestFloorForPageCleanerTest(
	t *testing.T,
	ctx context.Context,
	store *blobstore.Store,
	floor uint64,
) {
	t.Helper()
	backend := manifest.NewBlobStoreBackend(store)
	data, etag, err := backend.ReadCurrent(ctx)
	if err != nil && !errors.Is(err, manifest.ErrNotFound) {
		t.Fatalf("read CURRENT: %v", err)
	}
	current := &manifest.Current{}
	if len(data) > 0 {
		current, err = manifest.DecodeCurrent(data)
		if err != nil {
			t.Fatalf("decode CURRENT: %v", err)
		}
	}
	current.LogSeqStart = floor
	current.ChangeFeedLogStart = floor
	if current.NextSeq < floor {
		current.NextSeq = floor
	}
	encoded, err := manifest.EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode CURRENT: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, encoded, etag); err != nil {
		t.Fatalf("write CURRENT at floor %d: %v", floor, err)
	}
}

func jsonMarshalPageMark(mark manifestPageRetirementMark) ([]byte, error) {
	return json.Marshal(mark)
}

func requireObjectExists(t *testing.T, ctx context.Context, store *blobstore.Store, path string, exists bool) {
	t.Helper()
	_, err := store.Attributes(ctx, path)
	if exists && err != nil {
		t.Fatalf("object %q does not exist: %v", path, err)
	}
	if !exists && !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("object %q error=%v want not found", path, err)
	}
}
