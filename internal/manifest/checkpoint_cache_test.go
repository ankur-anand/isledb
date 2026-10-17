package manifest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

// countingBackend counts snapshot reads and can refuse page reads.
type countingBackend struct {
	*BlobStoreBackend
	snapshotReads atomic.Int32
	refusePages   atomic.Bool
}

var errPageRefused = errors.New("page read refused")

func (b *countingBackend) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	b.snapshotReads.Add(1)
	return b.BlobStoreBackend.ReadSnapshot(ctx, path)
}

func (b *countingBackend) ReadPage(ctx context.Context, path string) ([]byte, error) {
	if b.refusePages.Load() {
		return nil, errPageRefused
	}
	return b.BlobStoreBackend.ReadPage(ctx, path)
}

// checkpointThenMerge builds an L0 of c, b, a, takes a checkpoint with four
// more SSTs saved after the snapshot was written, some of them in a page, then
// merges b and a. It returns how many snapshot reads the merge made,
// and L0 as a fresh store replays it, which the writer's cache must match.
func checkpointThenMerge(t *testing.T, refusePagesDuringCheckpoint bool) (mergeReads int32, l0 string) {
	t.Helper()
	ctx := context.Background()
	store := blobstore.NewMemory("checkpoint-cache")
	t.Cleanup(func() { store.Close() })
	backend := &countingBackend{BlobStoreBackend: NewBlobStoreBackend(store)}
	ms := NewStoreWithStorage(backend)
	ms.activeEntryLimit = 2 // entries after the snapshot land in pages
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer"); err != nil {
		t.Fatal(err)
	}
	ssts := l0Of("g", "f", "e", "d", "c", "b", "a")
	add := func(sst SSTMeta) {
		t.Helper()
		if _, err := ms.AppendAddSSTableWithFence(ctx, sst); err != nil {
			t.Fatal(err)
		}
	}
	add(ssts[6])
	add(ssts[5])
	add(ssts[4])
	command, err := ms.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 3; i >= 0; i-- {
		add(ssts[i])
	}

	backend.refusePages.Store(refusePagesDuringCheckpoint)
	stageAndApplyCheckpointForTest(t, ctx, ms, command)
	backend.refusePages.Store(false)

	token, err := ms.ClaimMaintenance(ctx, "maintenance")
	if err != nil {
		t.Fatal(err)
	}
	head, err := ms.StageMaintenance(ctx, MaintenanceCommand{ID: "merge", Kind: MaintenanceCommandCompaction,
		Compaction: &CompactionCommand{
			Payload: CompactionLogPayload{
				RemoveSSTableIDs: []string{"b", "a"},
				AddSSTables:      []SSTMeta{{ID: "ba", SeqLo: 10, SeqHi: 25, MinKey: []byte("a"), MaxKey: []byte("z")}},
			},
			RetiredObjects: []RetiredObject{
				{Kind: RetiredObjectSST, ID: "b", Key: "sstable/b"},
				{Kind: RetiredObjectSST, ID: "a", Key: "sstable/a"},
			},
		}}, token)
	if err != nil {
		t.Fatal(err)
	}
	before := backend.snapshotReads.Load()
	result, err := ms.ApplyMaintenanceCommand(ctx, *head.Pending)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != MaintenanceStatusApplied {
		t.Fatalf("merge %+v, want applied", result)
	}
	mergeReads = backend.snapshotReads.Load() - before

	fresh, err := NewStoreWithStorage(NewBlobStoreBackend(store)).Replay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := ms.Replay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := l0IDs(cached), l0IDs(fresh); got != want || len(cached.AllSSTIDs()) != len(fresh.AllSSTIDs()) {
		t.Fatalf("the writer's cached L0 %s differs from a fresh replay's %s", got, want)
	}
	return mergeReads, l0IDs(fresh)
}

func TestCheckpointSeedsTheReplayCache(t *testing.T) {
	reads, l0 := checkpointThenMerge(t, false)
	if reads != 0 {
		t.Fatalf("the merge after a checkpoint read the snapshot %d times, want 0: the cache was filled from the verified one", reads)
	}
	if l0 != "g,f,e,d,c,ba" {
		t.Fatalf("replayed L0 %s, want e,d,c,ba", l0)
	}
}

func TestCheckpointCacheFillFailureFallsBackToReplay(t *testing.T) {
	reads, l0 := checkpointThenMerge(t, true)
	if reads != 1 {
		t.Fatalf("the merge read the snapshot %d times, want 1: filling the cache failed, so it replays from storage", reads)
	}
	if l0 != "g,f,e,d,c,ba" {
		t.Fatalf("replayed L0 %s, want e,d,c,ba", l0)
	}
}
