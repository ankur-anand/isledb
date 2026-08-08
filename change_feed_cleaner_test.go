package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

type changeFeedCleanerScanStorage struct {
	manifest.Storage
	manifest.PageStorage

	currentReads atomic.Int64
	pageReads    atomic.Int64
}

func (s *changeFeedCleanerScanStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	s.currentReads.Add(1)
	return s.Storage.ReadCurrent(ctx)
}

func (s *changeFeedCleanerScanStorage) ReadPage(ctx context.Context, path string) ([]byte, error) {
	s.pageReads.Add(1)
	return s.PageStorage.ReadPage(ctx, path)
}

func TestChangeFeedCleanerRetiresOldBatches(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	now := time.Now().UTC()
	oldMeta := writeChangeBatchForCleanerTest(t, ctx, store, "old.chg", now.Add(-2*time.Hour))
	oldEntry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{ID: "old.sst", Epoch: oldMeta.Epoch, SeqLo: oldMeta.SeqLo, SeqHi: oldMeta.SeqHi, Level: 0, CreatedAt: oldMeta.CreatedAt}, &oldMeta)
	if err != nil {
		t.Fatalf("append old sst: %v", err)
	}
	recentMeta := writeChangeBatchForCleanerTest(t, ctx, store, "recent.chg", now)
	recentEntry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{ID: "recent.sst", Epoch: recentMeta.Epoch, SeqLo: recentMeta.SeqLo, SeqHi: recentMeta.SeqHi, Level: 0, CreatedAt: recentMeta.CreatedAt}, &recentMeta)
	if err != nil {
		t.Fatalf("append recent sst: %v", err)
	}

	cleaner, err := newChangeFeedCleaner(ctx, store, manifestStore, changeFeedCleanerOptions{
		RetentionPeriod:            time.Hour,
		KeepAtLeastManifestEntries: 1,
		SweepGracePeriod:           -1,
	})
	if err != nil {
		t.Fatalf("new change feed cleaner: %v", err)
	}
	if err := cleaner.RunOnce(ctx); err != nil {
		t.Fatalf("run cleaner: %v", err)
	}

	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if got, want := current.ChangeFeedLogStart, oldEntry.Seq+1; got != want {
		t.Fatalf("unexpected change-feed floor: got=%d want=%d", got, want)
	}
	if _, err := manifestStore.ReadEntry(ctx, oldEntry.Seq); err == nil {
		t.Fatalf("expected old manifest entry seq=%d to be retired", oldEntry.Seq)
	}
	if _, err := manifestStore.ReadEntry(ctx, recentEntry.Seq); err != nil {
		t.Fatalf("recent manifest entry should remain readable: %v", err)
	}
	if _, _, err := store.Read(ctx, oldMeta.Path); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("old change batch read error=%v, want ErrNotFound", err)
	}
	if _, _, err := store.Read(ctx, recentMeta.Path); err != nil {
		t.Fatalf("recent change batch should remain: %v", err)
	}
	marks, err := loadPendingChangeBatchDeleteMarks(ctx, store)
	if err != nil {
		t.Fatalf("load pending change marks: %v", err)
	}
	if len(marks) != 0 {
		t.Fatalf("expected pending change marks to be cleared after sweep, got=%+v", marks)
	}

	// Change-feed retention must not remove manifest entries still required to
	// rebuild current KV state after a restart.
	freshManifestStore := manifest.NewStore(store)
	replayed, err := freshManifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("fresh replay after change-feed cleanup: %v", err)
	}
	if replayed.LookupSST("old.sst") == nil || replayed.LookupSST("recent.sst") == nil {
		t.Fatalf("change-feed cleanup changed visible SST state: %+v", replayed.AllSSTIDs())
	}
}

func TestChangeFeedCleanerPlansInPageBatchesFromOneView(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-cleaner-page-scan")
	defer store.Close()

	backend := manifest.NewBlobStoreBackend(store)
	storage := &changeFeedCleanerScanStorage{Storage: backend, PageStorage: backend}
	manifestStore := manifest.NewStoreWithStorage(storage)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	writerFence, err := manifestStore.ClaimWriter(ctx, "writer-1")
	if err != nil {
		t.Fatalf("claim writer: %v", err)
	}
	if err := manifestStore.EnableChangeFeed(ctx, manifest.ChangeFeedPayloadFullValues); err != nil {
		t.Fatalf("enable change feed: %v", err)
	}

	now := time.Now().UTC()
	for i := 0; i < 130; i++ {
		appendChangeFeedCleanerManifestEntry(t, ctx, manifestStore, writerFence.Epoch, i, now.Add(-2*time.Hour))
	}
	recent := appendChangeFeedCleanerManifestEntry(t, ctx, manifestStore, writerFence.Epoch, 130, now)

	cleaner, err := newChangeFeedCleaner(ctx, store, manifestStore, changeFeedCleanerOptions{
		RetentionPeriod:            time.Hour,
		KeepAtLeastManifestEntries: 1,
	})
	if err != nil {
		t.Fatalf("new change feed cleaner: %v", err)
	}
	view, err := manifestStore.LoadChangeFeedView(ctx)
	if err != nil {
		t.Fatalf("load change-feed view: %v", err)
	}
	storage.currentReads.Store(0)
	storage.pageReads.Store(0)

	floor, candidates, err := cleaner.planRetentionFloor(ctx, view, now)
	if err != nil {
		t.Fatalf("plan retention floor: %v", err)
	}
	if floor != recent.Seq {
		t.Fatalf("floor=%d want recent seq=%d", floor, recent.Seq)
	}
	if len(candidates) != 130 {
		t.Fatalf("candidates=%d want=130", len(candidates))
	}
	if got := storage.currentReads.Load(); got != 0 {
		t.Fatalf("CURRENT reads during pinned scan=%d want=0", got)
	}
	if got := storage.pageReads.Load(); got != 2 {
		t.Fatalf("page reads=%d want=2", got)
	}
}

func TestPendingChangeBatchSweeperDoesNotDeleteRetainedBatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	meta := writeChangeBatchForCleanerTest(t, ctx, store, "retained.chg", time.Now().Add(-2*time.Hour))
	entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{ID: "retained.sst", Epoch: 1, SeqLo: meta.SeqLo, SeqHi: meta.SeqHi, Level: 0, CreatedAt: meta.CreatedAt}, &meta)
	if err != nil {
		t.Fatalf("append retained sst: %v", err)
	}
	if err := enqueuePendingChangeBatchDeleteMarks(ctx, store, []changeBatchDeleteCandidate{{Path: meta.Path, ID: meta.ID, Seq: entry.Seq}}, "test"); err != nil {
		t.Fatalf("enqueue pending change mark: %v", err)
	}

	stats, err := runPendingChangeBatchSweeper(ctx, store, manifestStore, 10, -1)
	if err != nil {
		t.Fatalf("run sweeper: %v", err)
	}
	if stats.Deleted != 0 || stats.BlockedRetained != 1 {
		t.Fatalf("unexpected sweep stats before floor advance: %+v", stats)
	}
	if _, _, err := store.Read(ctx, meta.Path); err != nil {
		t.Fatalf("retained change batch should not be deleted: %v", err)
	}

	compactorToken, err := manifestStore.ClaimCompactor(ctx, "change-feed-test")
	if err != nil {
		t.Fatalf("claim compactor: %v", err)
	}
	if _, err := manifestStore.AdvanceChangeFeedLogStart(ctx, entry.Seq+1, compactorToken); err != nil {
		t.Fatalf("advance change-feed floor: %v", err)
	}
	stats, err = runPendingChangeBatchSweeper(ctx, store, manifestStore, 10, -1)
	if err != nil {
		t.Fatalf("run sweeper after floor advance: %v", err)
	}
	if stats.Deleted != 1 {
		t.Fatalf("deleted=%d, want 1", stats.Deleted)
	}
	if _, _, err := store.Read(ctx, meta.Path); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("change batch read error=%v, want ErrNotFound", err)
	}
}

func TestChangeFeedCleanerBackgroundLoopStopsWhenFenced(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-cleaner-fenced")
	defer store.Close()

	manifestStore := manifest.NewStore(store)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}

	var cleanupErrCount atomic.Int32
	cleaner, err := newChangeFeedCleaner(ctx, store, manifestStore, changeFeedCleanerOptions{
		CheckInterval: 20 * time.Millisecond,
		OnCleanupError: func(error) {
			cleanupErrCount.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("new change feed cleaner: %v", err)
	}
	defer cleaner.Close(ctx)

	competingStore := manifest.NewStore(store)
	if _, err := competingStore.Replay(ctx); err != nil {
		t.Fatalf("competing replay: %v", err)
	}
	if _, err := competingStore.ClaimCompactor(ctx, "change-feed-cleaner-other"); err != nil {
		t.Fatalf("competing compactor claim: %v", err)
	}

	if err := cleaner.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for cleanupErrCount.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if cleanupErrCount.Load() == 0 {
		t.Fatal("expected the cleanup loop to report a fence error")
	}

	first := cleanupErrCount.Load()
	time.Sleep(100 * time.Millisecond)
	if after := cleanupErrCount.Load(); after != first {
		t.Fatalf("cleanup loop continued after fence loss: before=%d after=%d", first, after)
	}
	if cleaner.running.Load() {
		t.Fatal("cleanup loop still running after fence loss")
	}
}

func writeChangeBatchForCleanerTest(t *testing.T, ctx context.Context, store *blobstore.Store, id string, createdAt time.Time) manifest.ChangeBatchMeta {
	t.Helper()
	path := store.ChangeBatchPath(id)
	body := []byte("change-batch:" + id)
	if _, err := store.Write(ctx, path, body); err != nil {
		t.Fatalf("write change batch %s: %v", id, err)
	}
	return manifest.ChangeBatchMeta{
		ID:        id,
		Path:      path,
		Epoch:     1,
		SeqLo:     1,
		SeqHi:     1,
		Count:     1,
		Size:      int64(len(body)),
		Checksum:  "sha256:test",
		CreatedAt: createdAt,
		Version:   1,
		Payload:   manifest.ChangeFeedPayloadFullValues,
	}
}

func appendChangeFeedCleanerManifestEntry(
	t *testing.T,
	ctx context.Context,
	manifestStore *manifest.Store,
	epoch uint64,
	index int,
	createdAt time.Time,
) *manifest.ManifestLogEntry {
	t.Helper()
	seq := uint64(index + 1)
	change := manifest.ChangeBatchMeta{
		ID:        fmt.Sprintf("change-%03d", index),
		Path:      fmt.Sprintf("changes/change-%03d.batch", index),
		Epoch:     epoch,
		SeqLo:     seq,
		SeqHi:     seq,
		Count:     1,
		Size:      128,
		RawSize:   256,
		Checksum:  "sha256:test",
		CreatedAt: createdAt,
		Payload:   manifest.ChangeFeedPayloadFullValues,
	}
	entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{
		ID:        fmt.Sprintf("sst-%03d", index),
		Epoch:     epoch,
		SeqLo:     seq,
		SeqHi:     seq,
		CreatedAt: createdAt,
	}, &change)
	if err != nil {
		t.Fatalf("append entry %d: %v", index, err)
	}
	return entry
}
