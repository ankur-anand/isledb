package manifest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/segmentio/ksuid"
)

func writeLogEntry(t *testing.T, ctx context.Context, backend *BlobStoreBackend, entry *ManifestLogEntry) {
	t.Helper()
	if entry.ID.IsNil() {
		entry.ID = ksuid.New()
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}
	data, err := EncodeLogEntry(entry)
	if err != nil {
		t.Fatalf("encode entry: %v", err)
	}
	name := formatLogSeq(entry.Seq)
	if _, err := backend.WriteLog(ctx, name, data); err != nil {
		t.Fatalf("write log: %v", err)
	}
}

func writeSnapshot(t *testing.T, ctx context.Context, backend *BlobStoreBackend, id string, snap *Manifest) string {
	t.Helper()
	data, err := EncodeSnapshot(snap)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	path, err := backend.WriteSnapshot(ctx, id, data)
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	return path
}

func TestReplay_Snapshot_FenceClaimInWindow_PreservesEarlierEpochs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("replay-fence-window")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	ms := NewStoreWithStorage(backend)

	snap := &Manifest{
		Version:   2,
		NextEpoch: 2,
		L0SSTs: []SSTMeta{{
			ID:    "snap.sst",
			Epoch: 1,
			Level: 0,
		}},
	}
	snapPath := writeSnapshot(t, ctx, backend, "snap-1", snap)

	current := &Current{
		Snapshot:    snapPath,
		LogSeqStart: 10,
		NextSeq:     14,
		NextEpoch:   4,
		WriterFence: &FenceToken{Epoch: 3, Owner: "writer-3", ClaimedAt: time.Now().UTC()},
	}
	currentData, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	_, currentETag, err := backend.ReadCurrent(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("read current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, currentData, currentETag); err != nil {
		t.Fatalf("write current: %v", err)
	}

	// Log window:
	// seq=10: writer epoch=1 add (should apply)
	// seq=11: writer epoch=2 fence claim
	// seq=12: writer epoch=1 add (stale, should skip)
	// seq=13: writer epoch=2 add (should apply)

	writeLogEntry(t, ctx, backend, &ManifestLogEntry{
		Seq:     10,
		Role:    FenceRoleWriter,
		Epoch:   1,
		Op:      LogOpAddSSTable,
		SSTable: &SSTMeta{ID: "before-claim.sst", Epoch: 1, Level: 0},
	})
	writeLogEntry(t, ctx, backend, &ManifestLogEntry{
		Seq:   11,
		Role:  FenceRoleWriter,
		Epoch: 2,
		Op:    LogOpFenceClaim,
		FenceClaim: &FenceClaimPayload{
			Role:      FenceRoleWriter,
			Epoch:     2,
			Owner:     "writer-2",
			ClaimedAt: time.Now().UTC(),
		},
	})
	writeLogEntry(t, ctx, backend, &ManifestLogEntry{
		Seq:     12,
		Role:    FenceRoleWriter,
		Epoch:   1,
		Op:      LogOpAddSSTable,
		SSTable: &SSTMeta{ID: "stale-after-claim.sst", Epoch: 1, Level: 0},
	})
	writeLogEntry(t, ctx, backend, &ManifestLogEntry{
		Seq:     13,
		Role:    FenceRoleWriter,
		Epoch:   2,
		Op:      LogOpAddSSTable,
		SSTable: &SSTMeta{ID: "after-claim.sst", Epoch: 2, Level: 0},
	})

	m, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if m.LookupSST("snap.sst") == nil {
		t.Fatalf("expected snapshot sst to be present")
	}
	if m.LookupSST("before-claim.sst") == nil {
		t.Fatalf("expected before-claim.sst to be present")
	}
	if m.LookupSST("after-claim.sst") == nil {
		t.Fatalf("expected after-claim.sst to be present")
	}
	if m.LookupSST("stale-after-claim.sst") != nil {
		t.Fatalf("stale-after-claim.sst should be filtered")
	}
}

func TestReplay_Snapshot_NoFenceClaimInWindow_SeedsFromCurrent(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("replay-fence-seed")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	ms := NewStoreWithStorage(backend)

	snap := &Manifest{
		Version:   2,
		NextEpoch: 3,
		L0SSTs: []SSTMeta{{
			ID:    "snap-base.sst",
			Epoch: 1,
			Level: 0,
		}},
	}
	snapPath := writeSnapshot(t, ctx, backend, "snap-2", snap)

	current := &Current{
		Snapshot:       snapPath,
		LogSeqStart:    20,
		NextSeq:        24,
		NextEpoch:      3,
		WriterFence:    &FenceToken{Epoch: 2, Owner: "writer-2", ClaimedAt: time.Now().UTC()},
		CompactorFence: &FenceToken{Epoch: 2, Owner: "compactor-2", ClaimedAt: time.Now().UTC()},
	}
	currentData, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, currentData, ""); err != nil {
		t.Fatalf("write current: %v", err)
	}

	writeLogEntry(t, ctx, backend, &ManifestLogEntry{
		Seq:     20,
		Role:    FenceRoleWriter,
		Epoch:   1,
		Op:      LogOpAddSSTable,
		SSTable: &SSTMeta{ID: "writer-stale.sst", Epoch: 1, Level: 0},
	})
	writeLogEntry(t, ctx, backend, &ManifestLogEntry{
		Seq:     21,
		Role:    FenceRoleWriter,
		Epoch:   2,
		Op:      LogOpAddSSTable,
		SSTable: &SSTMeta{ID: "writer-valid.sst", Epoch: 2, Level: 0},
	})
	writeLogEntry(t, ctx, backend, &ManifestLogEntry{
		Seq:   22,
		Role:  FenceRoleCompactor,
		Epoch: 1,
		Op:    LogOpCompaction,
		Compaction: &CompactionLogPayload{
			AddSSTables: []SSTMeta{{ID: "compactor-stale.sst", Epoch: 1, Level: 1}},
		},
	})
	writeLogEntry(t, ctx, backend, &ManifestLogEntry{
		Seq:   23,
		Role:  FenceRoleCompactor,
		Epoch: 2,
		Op:    LogOpCompaction,
		Compaction: &CompactionLogPayload{
			AddSSTables: []SSTMeta{{ID: "compactor-valid.sst", Epoch: 2, Level: 1}},
		},
	})

	m, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if m.LookupSST("snap-base.sst") == nil {
		t.Fatalf("expected snapshot sst to be present")
	}
	if m.LookupSST("writer-stale.sst") != nil {
		t.Fatalf("writer-stale.sst should be filtered when seeding from CURRENT")
	}
	if m.LookupSST("writer-valid.sst") == nil {
		t.Fatalf("writer-valid.sst should be applied")
	}
	if m.LookupSST("compactor-stale.sst") != nil {
		t.Fatalf("compactor-stale.sst should be filtered when seeding from CURRENT")
	}
	if m.LookupSST("compactor-valid.sst") == nil {
		t.Fatalf("compactor-valid.sst should be applied")
	}
}
