package isledb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

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
	oldEntry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{ID: "old.sst", Epoch: 1, Level: 0, CreatedAt: oldMeta.CreatedAt}, &oldMeta)
	if err != nil {
		t.Fatalf("append old sst: %v", err)
	}
	recentMeta := writeChangeBatchForCleanerTest(t, ctx, store, "recent.chg", now)
	recentEntry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{ID: "recent.sst", Epoch: 2, Level: 0, CreatedAt: recentMeta.CreatedAt}, &recentMeta)
	if err != nil {
		t.Fatalf("append recent sst: %v", err)
	}

	cleaner, err := newChangeFeedCleaner(ctx, store, manifestStore, ChangeFeedCleanerOptions{
		RetentionPeriod:  time.Hour,
		RetentionCount:   1,
		SweepGracePeriod: -1,
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
	entry, err := manifestStore.AppendAddSSTableWithChangeBatchWithFence(ctx, manifest.SSTMeta{ID: "retained.sst", Epoch: 1, Level: 0, CreatedAt: meta.CreatedAt}, &meta)
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

	if _, err := manifestStore.AdvanceChangeFeedLogStart(ctx, entry.Seq+1); err != nil {
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
	}
}
