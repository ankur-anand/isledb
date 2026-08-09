package manifest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/segmentio/ksuid"
)

type blockingSnapshotStorage struct {
	Storage
	PageStorage
	block   chan struct{}
	started chan struct{}
	once    sync.Once
}

func (s *blockingSnapshotStorage) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	s.once.Do(func() { close(s.started) })
	<-s.block
	return s.Storage.WriteSnapshot(ctx, id, data)
}

func commitEntriesForTest(t *testing.T, ctx context.Context, backend *BlobStoreBackend, entries []*ManifestLogEntry) {
	t.Helper()
	data, etag, err := backend.ReadCurrent(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("read current: %v", err)
	}
	var current *Current
	if len(data) > 0 {
		current, err = DecodeCurrent(data)
		if err != nil {
			t.Fatalf("decode current: %v", err)
		}
	}
	if current == nil {
		current = &Current{NextEpoch: 1}
	}
	normalizeCurrent(current)
	if len(entries) > 0 && current.NextSeq == 0 {
		current.LogSeqStart = entries[0].Seq
	}
	for _, entry := range entries {
		current.ActiveEntries = append(current.ActiveEntries, *entry)
		if entry.Seq >= current.NextSeq {
			current.NextSeq = entry.Seq + 1
		}
		current.NextEpoch = nextEpochFromEntry(current.NextEpoch, entry)
	}
	if current.ChangeFeedLogStart == 0 {
		current.ChangeFeedLogStart = current.LogSeqStart
	}
	encoded, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, encoded, etag); err != nil {
		t.Fatalf("write current: %v", err)
	}
}

func writeCurrentForTest(t *testing.T, ctx context.Context, backend *BlobStoreBackend, current *Current) {
	t.Helper()
	normalizeCurrent(current)
	encoded, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	_, etag, err := backend.ReadCurrent(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("read current etag: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, encoded, etag); err != nil {
		t.Fatalf("write current: %v", err)
	}
}

func testManifestEntry(seq uint64) ManifestLogEntry {
	return ManifestLogEntry{
		ID:      ksuid.New(),
		Seq:     seq,
		Role:    FenceRoleWriter,
		Epoch:   1,
		Op:      LogOpAddSSTable,
		SSTable: &SSTMeta{ID: fmt.Sprintf("%d.sst", seq), Epoch: 1, Level: 0},
	}
}

func testObjectRef(path string) ObjectRef {
	return ObjectRef{
		Path:         path,
		EncodedBytes: manifestObjectHeaderBytes + 1,
		Checksum:     "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		CreatedAt:    time.Unix(1_700_000_000, 0).UTC(),
	}
}

func testObjectRefPtr(path string) *ObjectRef {
	ref := testObjectRef(path)
	return &ref
}

func stageAndApplyCheckpointForTest(t *testing.T, ctx context.Context, store *Store, command CheckpointCommand) MaintenanceApplyResult {
	t.Helper()
	token, err := store.ClaimMaintenance(ctx, "maintenance-test")
	if err != nil {
		t.Fatalf("claim maintenance: %v", err)
	}
	staged, err := store.StageMaintenance(ctx, MaintenanceCommand{
		ID:         ksuid.New().String(),
		Kind:       MaintenanceCommandCheckpoint,
		Checkpoint: &command,
	}, token)
	if err != nil {
		t.Fatalf("stage checkpoint: %v", err)
	}
	result, err := store.ApplyPendingMaintenance(ctx)
	if err != nil {
		t.Fatalf("apply checkpoint: %v", err)
	}
	if result.Status != MaintenanceStatusApplied || !result.Changed {
		t.Fatalf("checkpoint result=%+v, want changed applied", result)
	}
	if _, err := store.ClearMaintenance(ctx, staged.Pending.ID, staged.Pending.Epoch, staged.Pending.Generation, token); err != nil {
		t.Fatalf("clear checkpoint: %v", err)
	}
	return result
}

func prepareAndApplyCheckpointForTest(t *testing.T, ctx context.Context, store *Store) CheckpointCommand {
	t.Helper()
	command, err := store.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("prepare checkpoint: %v", err)
	}
	stageAndApplyCheckpointForTest(t, ctx, store, command)
	return command
}

// applyManifestCheckpointForTest is test setup for hand-built CURRENT values.
// Production code creates the candidate with PrepareCheckpoint and still
// publishes it only through maintenance/HEAD and the active writer CAS.
func applyManifestCheckpointForTest(t *testing.T, ctx context.Context, store *Store, state *Manifest) CheckpointCommand {
	t.Helper()
	current, etag, err := store.readCurrentWithETag(ctx)
	if err != nil {
		t.Fatalf("read CURRENT for checkpoint: %v", err)
	}
	if current == nil {
		t.Fatal("checkpoint test requires CURRENT")
	}
	if current.WriterFence == nil {
		now := time.Now().UTC()
		current.WriterFence = &FenceToken{Epoch: current.NextEpoch, Owner: "writer-test", ClaimedAt: now}
		if current.WriterFence.Epoch == 0 {
			current.WriterFence.Epoch = 1
		}
		if current.NextEpoch <= current.WriterFence.Epoch {
			current.NextEpoch = current.WriterFence.Epoch + 1
		}
		if err := store.writeCurrentWithCAS(ctx, current, etag); err != nil {
			t.Fatalf("install test writer fence: %v", err)
		}
	}
	store.mu.Lock()
	store.writerFence = current.WriterFence.Clone()
	store.mu.Unlock()

	snapshot := state.Clone()
	if snapshot == nil {
		t.Fatal("nil checkpoint state")
	}
	snapshot.Version = 2
	if snapshot.NextEpoch < current.NextEpoch {
		snapshot.NextEpoch = current.NextEpoch
	}
	snapshot.WriterFence = current.WriterFence.Clone()
	snapshot.CompactorFence = current.CompactorFence.Clone()
	if current.NextSeq > 0 {
		snapshot.LogSeq = current.NextSeq - 1
	}
	ref, err := store.writeSnapshotObject(ctx, snapshot)
	if err != nil {
		t.Fatalf("write checkpoint candidate: %v", err)
	}
	command := CheckpointCommand{
		Snapshot:          ref,
		BaseSnapshot:      current.Snapshot.Clone(),
		BaseLogSeqStart:   current.LogSeqStart,
		SnapshotNextSeq:   current.NextSeq,
		FoldedReplayPages: current.StateReplayPages,
		FoldedReplayBytes: current.StateReplayBytes,
	}
	stageAndApplyCheckpointForTest(t, ctx, store, command)
	return command
}

func TestAppendWithWriterFence_FencedOut(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms1 := NewStore(store)
	ms2 := NewStore(store)

	if _, err := ms1.Replay(ctx); err != nil {
		t.Fatalf("replay ms1: %v", err)
	}
	if _, err := ms2.Replay(ctx); err != nil {
		t.Fatalf("replay ms2: %v", err)
	}

	if _, err := ms1.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer-1: %v", err)
	}
	if _, err := ms2.ClaimWriter(ctx, "writer-2"); err != nil {
		t.Fatalf("claim writer-2: %v", err)
	}

	if _, err := ms1.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}); !errors.Is(err, ErrFenced) {
		t.Fatalf("expected ErrFenced, got %v", err)
	}
	if _, err := ms2.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "b.sst", Epoch: 2, Level: 0}); err != nil {
		t.Fatalf("append writer-2: %v", err)
	}
}

func TestClaimWriter_WritesFenceClaimEntry(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}

	token, err := ms.ClaimWriter(ctx, "owner-1")
	if err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	logs, err := ms.ListEntries(ctx)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(logs))
	}

	entry, err := ms.ReadEntry(ctx, logs[0])
	if err != nil {
		t.Fatalf("read log entry: %v", err)
	}

	if entry.Op != LogOpFenceClaim {
		t.Errorf("expected op %s, got %s", LogOpFenceClaim, entry.Op)
	}
	if entry.Role != FenceRoleWriter {
		t.Errorf("expected role %d, got %d", FenceRoleWriter, entry.Role)
	}
	if entry.Epoch != token.Epoch {
		t.Errorf("expected epoch %d, got %d", token.Epoch, entry.Epoch)
	}
	if entry.FenceClaim == nil {
		t.Fatal("fence claim payload is nil")
	}
	if entry.FenceClaim.Owner != "owner-1" {
		t.Errorf("expected owner owner-1, got %s", entry.FenceClaim.Owner)
	}
}

func TestClaimCompactor_WritesFenceClaimEntry(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}

	token, err := ms.ClaimCompactor(ctx, "compactor-1")
	if err != nil {
		t.Fatalf("claim compactor: %v", err)
	}

	logs, err := ms.ListEntries(ctx)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(logs))
	}

	entry, err := ms.ReadEntry(ctx, logs[0])
	if err != nil {
		t.Fatalf("read log entry: %v", err)
	}

	if entry.Op != LogOpFenceClaim {
		t.Errorf("expected op %s, got %s", LogOpFenceClaim, entry.Op)
	}
	if entry.Role != FenceRoleCompactor {
		t.Errorf("expected role %d, got %d", FenceRoleCompactor, entry.Role)
	}
	if entry.Epoch != token.Epoch {
		t.Errorf("expected epoch %d, got %d", token.Epoch, entry.Epoch)
	}
}

func TestValidateCompactionPayloadRequiresAdjacentLevel(t *testing.T) {
	tests := []CompactionLogPayload{
		{
			RemoveSSTableIDs: []string{"l1-a"},
			SourceLevel:      1,
			DestinationLevel: 3,
			AddSSTables:      []SSTMeta{{ID: "l3-a", Level: 3, MinKey: []byte("a"), MaxKey: []byte("z")}},
		},
		{
			RemoveSSTableIDs: []string{"last"},
			SourceLevel:      ^uint32(0),
			DestinationLevel: 0,
		},
	}

	for _, payload := range tests {
		err := validateCompactionPayload(payload)
		if !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("validateCompactionPayload(%+v) error = %v, want %v", payload, err, ErrInvalidManifest)
		}
	}
}

func TestReplayRejectsNonAdjacentCompactionEntry(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)
	backend := NewBlobStoreBackend(store)
	commitEntriesForTest(t, ctx, backend, []*ManifestLogEntry{{
		ID:    ksuid.New(),
		Seq:   1,
		Role:  FenceRoleCompactor,
		Epoch: 1,
		Op:    LogOpCompaction,
		Compaction: &CompactionLogPayload{
			RemoveSSTableIDs: []string{"l1-a"},
			SourceLevel:      1,
			DestinationLevel: 3,
			AddSSTables:      []SSTMeta{{ID: "l3-a", Level: 3, MinKey: []byte("a"), MaxKey: []byte("z")}},
		},
	}})

	if _, err := ms.Replay(ctx); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("Replay error = %v, want %v", err, ErrInvalidManifest)
	}
}

func TestAppendWithWriterFence_SetsRoleAndEpoch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}

	token, err := ms.ClaimWriter(ctx, "owner-1")
	if err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	entry, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a.sst", Epoch: 1, Level: 0})
	if err != nil {
		t.Fatalf("append add sstable: %v", err)
	}

	if entry.Role != FenceRoleWriter {
		t.Errorf("expected role %d, got %d", FenceRoleWriter, entry.Role)
	}
	if entry.Epoch != token.Epoch {
		t.Errorf("expected epoch %d, got %d", token.Epoch, entry.Epoch)
	}
}

func TestReplay_FiltersStaleWriterEntries(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)
	backend := NewBlobStoreBackend(store)

	// Manually create log entries to simulate TOCTOU race scenario
	// seq=0: writer epoch=1 fence claim
	// seq=1: writer epoch=1 add_sstable (valid)
	// seq=2: writer epoch=2 fence claim (new writer takes over)
	// seq=3: writer epoch=1 add_sstable (stale - should be skipped)
	// seq=4: writer epoch=2 add_sstable (valid)

	entries := []*ManifestLogEntry{
		{
			ID:    ksuid.New(),
			Seq:   0,
			Role:  FenceRoleWriter,
			Epoch: 1,
			Op:    LogOpFenceClaim,
			FenceClaim: &FenceClaimPayload{
				Role:      FenceRoleWriter,
				Epoch:     1,
				Owner:     "owner-1",
				ClaimedAt: time.Now(),
			},
		},
		{
			ID:      ksuid.New(),
			Seq:     1,
			Role:    FenceRoleWriter,
			Epoch:   1,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "valid-1.sst", Epoch: 1, Level: 0},
		},
		{
			ID:    ksuid.New(),
			Seq:   2,
			Role:  FenceRoleWriter,
			Epoch: 2,
			Op:    LogOpFenceClaim,
			FenceClaim: &FenceClaimPayload{
				Role:      FenceRoleWriter,
				Epoch:     2,
				Owner:     "owner-2",
				ClaimedAt: time.Now(),
			},
		},
		{
			ID:      ksuid.New(),
			Seq:     3,
			Role:    FenceRoleWriter,
			Epoch:   1,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "stale.sst", Epoch: 1, Level: 0},
		},
		{
			ID:      ksuid.New(),
			Seq:     4,
			Role:    FenceRoleWriter,
			Epoch:   2,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "valid-2.sst", Epoch: 2, Level: 0},
		},
	}

	commitEntriesForTest(t, ctx, backend, entries)

	manifest, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(manifest.L0SSTs) != 2 {
		t.Fatalf("expected 2 L0 SSTs, got %d", len(manifest.L0SSTs))
	}

	ids := make(map[string]bool)
	for _, sst := range manifest.L0SSTs {
		ids[sst.ID] = true
	}

	if !ids["valid-1.sst"] {
		t.Error("expected valid-1.sst to be present")
	}
	if !ids["valid-2.sst"] {
		t.Error("expected valid-2.sst to be present")
	}
	if ids["stale.sst"] {
		t.Error("stale.sst should have been filtered out")
	}

	if manifest.NextEpoch != 3 {
		t.Errorf("expected NextEpoch 3, got %d", manifest.NextEpoch)
	}
}

func TestReplay_IndependentWriterCompactorFiltering(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)
	backend := NewBlobStoreBackend(store)

	// Scenario: Writer and Compactor operate independently
	// seq=0: writer epoch=1 fence claim
	// seq=1: compactor epoch=1 fence claim
	// seq=2: writer epoch=2 fence claim (new writer)
	// seq=3: writer epoch=1 add_sstable (stale writer - skip)
	// seq=4: compactor epoch=1 compaction (compactor still valid)

	entries := []*ManifestLogEntry{
		{
			ID:    ksuid.New(),
			Seq:   0,
			Role:  FenceRoleWriter,
			Epoch: 1,
			Op:    LogOpFenceClaim,
			FenceClaim: &FenceClaimPayload{
				Role:      FenceRoleWriter,
				Epoch:     1,
				Owner:     "writer-1",
				ClaimedAt: time.Now(),
			},
		},
		{
			ID:    ksuid.New(),
			Seq:   1,
			Role:  FenceRoleCompactor,
			Epoch: 1,
			Op:    LogOpFenceClaim,
			FenceClaim: &FenceClaimPayload{
				Role:      FenceRoleCompactor,
				Epoch:     1,
				Owner:     "compactor-1",
				ClaimedAt: time.Now(),
			},
		},
		{
			ID:    ksuid.New(),
			Seq:   2,
			Role:  FenceRoleWriter,
			Epoch: 2,
			Op:    LogOpFenceClaim,
			FenceClaim: &FenceClaimPayload{
				Role:      FenceRoleWriter,
				Epoch:     2,
				Owner:     "writer-2",
				ClaimedAt: time.Now(),
			},
		},
		{
			ID:      ksuid.New(),
			Seq:     3,
			Role:    FenceRoleWriter,
			Epoch:   1,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "stale-writer.sst", Epoch: 1, Level: 0},
		},
		{
			ID:      ksuid.New(),
			Seq:     4,
			Role:    FenceRoleWriter,
			Epoch:   2,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "valid-writer.sst", Epoch: 2, Level: 0},
		},
		{
			ID:    ksuid.New(),
			Seq:   5,
			Role:  FenceRoleCompactor,
			Epoch: 1,
			Op:    LogOpCompaction,
			Compaction: &CompactionLogPayload{
				RemoveSSTableIDs: []string{"compaction-input"},
				DestinationLevel: 1,
				AddSSTables:      []SSTMeta{{ID: "compacted.sst", Epoch: 1, Level: 1}},
			},
		},
	}

	commitEntriesForTest(t, ctx, backend, entries)

	manifest, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(manifest.L0SSTs) != 1 {
		t.Fatalf("expected 1 L0 SST, got %d", len(manifest.L0SSTs))
	}
	if manifest.L0SSTs[0].ID != "valid-writer.sst" {
		t.Errorf("expected valid-writer.sst, got %s", manifest.L0SSTs[0].ID)
	}

	if len(manifest.Levels) != 1 {
		t.Fatalf("expected 1 level, got %d", len(manifest.Levels))
	}
	if manifest.Levels[0].SSTs[0].ID != "compacted.sst" {
		t.Errorf("expected compacted.sst, got %s", manifest.Levels[0].SSTs[0].ID)
	}
}

func TestReplay_BackwardsCompatibleWithEntriesWithoutRoleEpoch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)
	backend := NewBlobStoreBackend(store)

	entries := []*ManifestLogEntry{
		{
			ID:      ksuid.New(),
			Seq:     0,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "old-entry.sst", Epoch: 1, Level: 0},
		},
	}

	commitEntriesForTest(t, ctx, backend, entries)

	manifest, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(manifest.L0SSTs) != 1 {
		t.Fatalf("expected 1 L0 SST, got %d", len(manifest.L0SSTs))
	}
	if manifest.L0SSTs[0].ID != "old-entry.sst" {
		t.Errorf("expected old-entry.sst, got %s", manifest.L0SSTs[0].ID)
	}
}

func TestReplay_SeedsEpochFromCurrentAfterSnapshot(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)
	backend := NewBlobStoreBackend(store)

	// Scenario: Simulating snapshotting that truncated logs.
	// CURRENT has WriterFence.Epoch=2, but the fence-claim log for epoch=2 is gone.
	// A stale entry (epoch=1) appears in the remaining logs and should be filtered.

	current := &Current{
		LogSeqStart: 10,
		NextSeq:     12,
		NextEpoch:   3,
		WriterFence: &FenceToken{
			Epoch: 2,
			Owner: "writer-2",
		},
	}
	currentData, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, currentData, ""); err != nil {
		t.Fatalf("write current: %v", err)
	}

	entries := []*ManifestLogEntry{
		{
			ID:      ksuid.New(),
			Seq:     10,
			Role:    FenceRoleWriter,
			Epoch:   1,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "stale-after-snapshot.sst", Epoch: 1, Level: 0},
		},
		{
			ID:      ksuid.New(),
			Seq:     11,
			Role:    FenceRoleWriter,
			Epoch:   2,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "valid-epoch2.sst", Epoch: 2, Level: 0},
		},
	}

	commitEntriesForTest(t, ctx, backend, entries)

	manifest, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(manifest.L0SSTs) != 1 {
		t.Fatalf("expected 1 L0 SST, got %d: %+v", len(manifest.L0SSTs), manifest.L0SSTs)
	}
	if manifest.L0SSTs[0].ID != "valid-epoch2.sst" {
		t.Errorf("expected valid-epoch2.sst, got %s", manifest.L0SSTs[0].ID)
	}

	for _, sst := range manifest.L0SSTs {
		if sst.ID == "stale-after-snapshot.sst" {
			t.Error("stale-after-snapshot.sst should have been filtered out")
		}
	}
}

func TestReplay_IgnoresLowerEpochFenceClaim(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)
	backend := NewBlobStoreBackend(store)

	// Scenario: Writer B claims epoch=5, updates CURRENT, but crashes before writing
	// its fence-claim log. Writer A's fence-claim log with epoch=3 still exists.
	// On replay:
	// 1. CURRENT seeds activeWriterEpoch=5
	// 2. Processing epoch=3 fence-claim should NOT downgrade to 3
	// 3. Entries with epoch=4 should still be filtered as stale

	current := &Current{
		LogSeqStart: 0,
		NextSeq:     3,
		NextEpoch:   6,
		WriterFence: &FenceToken{
			Epoch: 5,
			Owner: "writer-B",
		},
	}
	currentData, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, currentData, ""); err != nil {
		t.Fatalf("write current: %v", err)
	}

	entries := []*ManifestLogEntry{
		{
			ID:    ksuid.New(),
			Seq:   0,
			Role:  FenceRoleWriter,
			Epoch: 3,
			Op:    LogOpFenceClaim,
			FenceClaim: &FenceClaimPayload{
				Role:      FenceRoleWriter,
				Epoch:     3,
				Owner:     "writer-A",
				ClaimedAt: time.Now(),
			},
		},
		{
			ID:      ksuid.New(),
			Seq:     1,
			Role:    FenceRoleWriter,
			Epoch:   4,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "stale-epoch4.sst", Epoch: 4, Level: 0},
		},
		{
			ID:      ksuid.New(),
			Seq:     2,
			Role:    FenceRoleWriter,
			Epoch:   5,
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "valid-epoch5.sst", Epoch: 5, Level: 0},
		},
	}

	commitEntriesForTest(t, ctx, backend, entries)

	manifest, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(manifest.L0SSTs) != 1 {
		t.Fatalf("expected 1 L0 SST, got %d: %+v", len(manifest.L0SSTs), manifest.L0SSTs)
	}
	if manifest.L0SSTs[0].ID != "valid-epoch5.sst" {
		t.Errorf("expected valid-epoch5.sst, got %s", manifest.L0SSTs[0].ID)
	}

	for _, sst := range manifest.L0SSTs {
		if sst.ID == "stale-epoch4.sst" {
			t.Error("stale-epoch4.sst should have been filtered (epoch 4 < 5)")
		}
	}
}

func TestReplay_SeedsCompactorEpochFromCurrentAfterSnapshot(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)
	backend := NewBlobStoreBackend(store)

	current := &Current{
		LogSeqStart: 10,
		NextSeq:     12,
		NextEpoch:   4,
		CompactorFence: &FenceToken{
			Epoch: 3,
			Owner: "compactor-3",
		},
	}
	currentData, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, currentData, ""); err != nil {
		t.Fatalf("write current: %v", err)
	}

	entries := []*ManifestLogEntry{
		{
			ID:    ksuid.New(),
			Seq:   10,
			Role:  FenceRoleCompactor,
			Epoch: 2,
			Op:    LogOpCompaction,
			Compaction: &CompactionLogPayload{
				RemoveSSTableIDs: []string{"stale-input"},
				DestinationLevel: 1,
				AddSSTables:      []SSTMeta{{ID: "stale-compacted.sst", Epoch: 2, Level: 1}},
			},
		},
		{
			ID:    ksuid.New(),
			Seq:   11,
			Role:  FenceRoleCompactor,
			Epoch: 3,
			Op:    LogOpCompaction,
			Compaction: &CompactionLogPayload{
				RemoveSSTableIDs: []string{"valid-input"},
				DestinationLevel: 1,
				AddSSTables:      []SSTMeta{{ID: "valid-compacted.sst", Epoch: 3, Level: 1}},
			},
		},
	}

	commitEntriesForTest(t, ctx, backend, entries)

	manifest, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(manifest.Levels) != 1 {
		t.Fatalf("expected 1 level, got %d", len(manifest.Levels))
	}
	if manifest.Levels[0].SSTs[0].ID != "valid-compacted.sst" {
		t.Errorf("expected valid-compacted.sst, got %s", manifest.Levels[0].SSTs[0].ID)
	}
}

func TestReplay_SurvivesTransientCurrentCASConflictWithoutSequenceGap(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	inject := &casInjectStorage{base: base}
	ms := NewStoreWithStorage(inject)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	inject.mu.Lock()
	inject.failNextCAS = true
	inject.mu.Unlock()

	entry, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "applied.sst", Epoch: 1, Level: 0})
	if err != nil {
		t.Fatalf("append after transient CAS conflict: %v", err)
	}
	if entry.Seq != 1 {
		t.Fatalf("expected applied.sst seq=1 after fence claim seq=0, got %d", entry.Seq)
	}

	replayStore := NewStoreWithStorage(base)
	m, err := replayStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay after transient CAS conflict: %v", err)
	}
	if m.LookupSST("applied.sst") == nil {
		t.Fatalf("expected applied.sst to be present after replay")
	}
	if m.LogSeq != 1 {
		t.Fatalf("expected highest log seq=1, got %d", m.LogSeq)
	}
}

func TestCheckpointDoesNotRegressCurrentNextSeq(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	ms1 := NewStoreWithStorage(backend)

	if _, err := ms1.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms1.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	for i := 0; i < 3; i++ {
		_, err := ms1.AppendAddSSTableWithFence(ctx, SSTMeta{
			ID:    fmt.Sprintf("a-%d.sst", i),
			Epoch: uint64(i + 1),
			Level: 0,
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	beforeData, _, err := backend.ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("read current before snapshot: %v", err)
	}
	before, err := DecodeCurrent(beforeData)
	if err != nil {
		t.Fatalf("decode current before snapshot: %v", err)
	}

	prepareAndApplyCheckpointForTest(t, ctx, ms1)

	afterData, _, err := backend.ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("read current after snapshot: %v", err)
	}
	after, err := DecodeCurrent(afterData)
	if err != nil {
		t.Fatalf("decode current after snapshot: %v", err)
	}

	if after.NextSeq < before.NextSeq {
		t.Fatalf("next_seq regressed after snapshot: before=%d after=%d", before.NextSeq, after.NextSeq)
	}
}

func TestReadEntry_HonorsChangeFeedLogStart(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	writeCurrentForTest(t, ctx, backend, &Current{
		NextEpoch:          1,
		LogSeqStart:        10,
		ChangeFeedLogStart: 10,
		NextSeq:            12,
		ActiveEntries: []ManifestLogEntry{
			testManifestEntry(9),
			testManifestEntry(10),
			testManifestEntry(11),
		},
	})

	ms := NewStoreWithStorage(backend)
	if _, err := ms.ReadEntry(ctx, 9); err == nil {
		t.Fatal("expected seq below change-feed floor to be rejected")
	}
	entry, err := ms.ReadEntry(ctx, 10)
	if err != nil {
		t.Fatalf("read retained entry: %v", err)
	}
	if entry.Seq != 10 {
		t.Fatalf("unexpected entry seq: got=%d want=10", entry.Seq)
	}
	seqs, err := ms.ListEntries(ctx)
	if err != nil {
		t.Fatalf("list entries: %v", err)
	}
	if got, want := fmt.Sprint(seqs), "[10 11]"; got != want {
		t.Fatalf("unexpected listed entries: got=%s want=%s", got, want)
	}
}

func TestCheckpointPrunesRefsBelowChangeFeedFloor(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	writeCurrentForTest(t, ctx, backend, &Current{
		NextEpoch:          1,
		LogSeqStart:        0,
		ChangeFeedLogStart: 2,
		RetirementLogStart: 2,
		NextSeq:            4,
		ActiveEntries: []ManifestLogEntry{
			testManifestEntry(1),
			testManifestEntry(2),
			testManifestEntry(3),
		},
		IndexFrontier: []PageRef{
			{ObjectRef: testObjectRef("pages/l00/old"), Level: 0, SeqLo: 0, SeqHi: 1, Count: 2},
			{ObjectRef: testObjectRef("pages/l00/kept"), Level: 0, SeqLo: 2, SeqHi: 3, Count: 2},
		},
	})

	ms := NewStoreWithStorage(backend)
	applyManifestCheckpointForTest(t, ctx, ms, &Manifest{NextEpoch: 1, LogSeq: 3})
	current, err := ms.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if current.ChangeFeedLogStart != 2 {
		t.Fatalf("unexpected change-feed floor: got=%d want=2", current.ChangeFeedLogStart)
	}
	if got, want := len(current.ActiveEntries), 2; got != want {
		t.Fatalf("unexpected active entry count: got=%d want=%d", got, want)
	}
	if current.ActiveEntries[0].Seq != 2 || current.ActiveEntries[1].Seq != 3 {
		t.Fatalf("unexpected retained active entries: got=%d,%d want=2,3", current.ActiveEntries[0].Seq, current.ActiveEntries[1].Seq)
	}
	if got, want := len(current.IndexFrontier), 1; got != want {
		t.Fatalf("unexpected index frontier count: got=%d want=%d", got, want)
	}
	if current.IndexFrontier[0].Path != "pages/l00/kept" {
		t.Fatalf("unexpected retained page ref: got=%q", current.IndexFrontier[0].Path)
	}
}

func TestCheckpointAdvancesDefaultChangeFeedFloor(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	writeCurrentForTest(t, ctx, backend, &Current{
		NextEpoch:          1,
		LogSeqStart:        0,
		ChangeFeedLogStart: 0,
		RetirementLogStart: 2,
		NextSeq:            2,
		ActiveEntries: []ManifestLogEntry{
			testManifestEntry(0),
			testManifestEntry(1),
		},
	})

	ms := NewStoreWithStorage(backend)
	applyManifestCheckpointForTest(t, ctx, ms, &Manifest{NextEpoch: 1, LogSeq: 1})
	current, err := ms.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if current.ChangeFeedLogStart != 2 {
		t.Fatalf("unexpected default change-feed floor: got=%d want=2", current.ChangeFeedLogStart)
	}
	if len(current.ActiveEntries) != 0 {
		t.Fatalf("expected active entries below new floor to be pruned, got=%d", len(current.ActiveEntries))
	}
}

func TestCheckpointPreservesEnabledChangeFeedFloor(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-checkpoint")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	writeCurrentForTest(t, ctx, backend, &Current{
		NextEpoch:          1,
		LogSeqStart:        0,
		ChangeFeedEnabled:  true,
		ChangeFeedPayload:  ChangeFeedPayloadFullValues,
		ChangeFeedLogStart: 0,
		RetirementLogStart: 2,
		NextSeq:            2,
		ActiveEntries: []ManifestLogEntry{
			testManifestEntry(0),
			testManifestEntry(1),
		},
	})

	ms := NewStoreWithStorage(backend)
	applyManifestCheckpointForTest(t, ctx, ms, &Manifest{NextEpoch: 1, LogSeq: 1})
	current, err := ms.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if current.ChangeFeedLogStart != 0 {
		t.Fatalf("enabled change-feed floor=%d want=0", current.ChangeFeedLogStart)
	}
	if len(current.ActiveEntries) != 2 {
		t.Fatalf("retained active entries=%d want=2", len(current.ActiveEntries))
	}
}

func TestCheckpointKeepsRefsAtUnconsumedRetirementFloor(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	writeCurrentForTest(t, ctx, backend, &Current{
		NextEpoch:          1,
		LogSeqStart:        0,
		ChangeFeedLogStart: 2,
		RetirementLogStart: 0,
		NextSeq:            2,
		ActiveEntries: []ManifestLogEntry{
			testManifestEntry(0),
			testManifestEntry(1),
		},
	})

	ms := NewStoreWithStorage(backend)
	applyManifestCheckpointForTest(t, ctx, ms, &Manifest{NextEpoch: 1, LogSeq: 1})
	current, err := ms.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if got := len(current.ActiveEntries); got != 2 {
		t.Fatalf("active entries=%d, want 2 while retirement floor is unconsumed", got)
	}
}

func TestAdvanceChangeFeedLogStartKeepsRefsNeededForStateReplay(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	token := &FenceToken{Epoch: 7, Owner: "maintenance-1", ClaimedAt: time.Now().UTC()}
	writeCurrentForTest(t, ctx, backend, &Current{
		NextEpoch:          1,
		CompactorFence:     token,
		LogSeqStart:        0,
		ChangeFeedLogStart: 1,
		NextSeq:            4,
		ActiveEntries: []ManifestLogEntry{
			testManifestEntry(1),
			testManifestEntry(2),
			testManifestEntry(3),
		},
		IndexFrontier: []PageRef{
			{ObjectRef: testObjectRef("pages/l00/old"), Level: 0, SeqLo: 0, SeqHi: 1, Count: 2},
			{ObjectRef: testObjectRef("pages/l00/kept"), Level: 0, SeqLo: 2, SeqHi: 3, Count: 2},
		},
	})

	ms := NewStoreWithStorage(backend)
	updated, err := ms.AdvanceChangeFeedLogStart(ctx, 2, token)
	if err != nil {
		t.Fatalf("advance change-feed floor: %v", err)
	}
	if updated.ChangeFeedLogStart != 2 {
		t.Fatalf("unexpected change-feed floor: got=%d want=2", updated.ChangeFeedLogStart)
	}
	if got, want := len(updated.ActiveEntries), 3; got != want {
		t.Fatalf("unexpected active entry count: got=%d want=%d", got, want)
	}
	if updated.ActiveEntries[0].Seq != 1 || updated.ActiveEntries[1].Seq != 2 || updated.ActiveEntries[2].Seq != 3 {
		t.Fatalf("unexpected retained active entries: got=%d,%d,%d want=1,2,3", updated.ActiveEntries[0].Seq, updated.ActiveEntries[1].Seq, updated.ActiveEntries[2].Seq)
	}
	if got, want := len(updated.IndexFrontier), 2; got != want {
		t.Fatalf("unexpected index frontier count: got=%d want=%d", got, want)
	}
	if updated.IndexFrontier[0].Path != "pages/l00/old" || updated.IndexFrontier[1].Path != "pages/l00/kept" {
		t.Fatalf("unexpected retained page refs: got=%q,%q", updated.IndexFrontier[0].Path, updated.IndexFrontier[1].Path)
	}
}

func TestCheckFenceTokenRequiresExactIdentity(t *testing.T) {
	token := &FenceToken{Epoch: 7, Owner: "maintenance-1"}
	tests := []struct {
		name   string
		remote *FenceToken
		valid  bool
	}{
		{name: "same token", remote: &FenceToken{Epoch: 7, Owner: "maintenance-1"}, valid: true},
		{name: "newer epoch", remote: &FenceToken{Epoch: 8, Owner: "maintenance-2"}},
		{name: "older epoch", remote: &FenceToken{Epoch: 6, Owner: "maintenance-0"}},
		{name: "different owner", remote: &FenceToken{Epoch: 7, Owner: "maintenance-2"}},
		{name: "missing remote"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkFenceToken(token, tt.remote)
			if tt.valid && err != nil {
				t.Fatalf("checkFenceToken error=%v", err)
			}
			if !tt.valid && !errors.Is(err, ErrFenced) {
				t.Fatalf("checkFenceToken error=%v, want %v", err, ErrFenced)
			}
		})
	}
}

func TestReplay_ErrsWhenCurrentSnapshotIsMissing(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}
	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append: %v", err)
	}

	command := prepareAndApplyCheckpointForTest(t, ctx, ms)
	snapPath := command.Snapshot.Path

	if err := store.Delete(ctx, snapPath); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}

	ms2 := NewStore(store)
	if _, err := ms2.Replay(ctx); err == nil {
		t.Fatal("expected replay error when CURRENT points to a missing snapshot")
	}
}

func TestCheckpointDuringConcurrentAppendsNoSeqRegressionOrLostSSTs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	blocking := &blockingSnapshotStorage{
		Storage:     base,
		PageStorage: base,
		block:       make(chan struct{}),
		started:     make(chan struct{}),
	}
	ms := NewStoreWithStorage(blocking)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	expected := make(map[string]struct{})
	appendSST := func(id string, epoch uint64) {
		if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: id, Epoch: epoch, Level: 0}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
		expected[id] = struct{}{}
	}

	for i := 0; i < 5; i++ {
		appendSST(fmt.Sprintf("base-%02d.sst", i), 1)
	}

	checkpointCh := make(chan CheckpointCommand, 1)
	checkpointErrCh := make(chan error, 1)
	go func() {
		command, err := ms.PrepareCheckpoint(ctx)
		checkpointCh <- command
		checkpointErrCh <- err
	}()

	<-blocking.started

	for i := 0; i < 20; i++ {
		appendSST(fmt.Sprintf("during-%02d.sst", i), 1)
	}

	beforeData, _, err := base.ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("read current before unblocking snapshot: %v", err)
	}
	beforeCurrent, err := DecodeCurrent(beforeData)
	if err != nil {
		t.Fatalf("decode current before unblocking snapshot: %v", err)
	}

	close(blocking.block)
	command := <-checkpointCh
	if err := <-checkpointErrCh; err != nil {
		t.Fatalf("prepare checkpoint: %v", err)
	}
	stageAndApplyCheckpointForTest(t, ctx, ms, command)

	for i := 0; i < 10; i++ {
		appendSST(fmt.Sprintf("after-%02d.sst", i), 1)
	}

	afterData, _, err := base.ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("read current after snapshot: %v", err)
	}
	afterCurrent, err := DecodeCurrent(afterData)
	if err != nil {
		t.Fatalf("decode current after snapshot: %v", err)
	}
	if afterCurrent.NextSeq < beforeCurrent.NextSeq {
		t.Fatalf("next_seq regressed: before=%d after=%d", beforeCurrent.NextSeq, afterCurrent.NextSeq)
	}

	replayStore := NewStoreWithStorage(base)
	finalManifest, err := replayStore.Replay(ctx)
	if err != nil {
		t.Fatalf("final replay: %v", err)
	}
	for id := range expected {
		if finalManifest.LookupSST(id) == nil {
			t.Fatalf("missing sst after replay: %s", id)
		}
	}
}

func TestReplay_FallsBackToFullReplayWhenCurrentNextSeqRegresses(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	ms := NewStore(store)
	backend := NewBlobStoreBackend(store)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}
	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "regressed-window.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append: %v", err)
	}

	m, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay after append: %v", err)
	}
	if m.LookupSST("regressed-window.sst") == nil {
		t.Fatalf("expected regressed-window.sst before CURRENT regression")
	}

	currentData, currentETag, err := backend.ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	current, err := DecodeCurrent(currentData)
	if err != nil {
		t.Fatalf("decode current: %v", err)
	}
	if current.NextSeq == 0 {
		t.Fatal("expected current.NextSeq > 0")
	}
	current.NextSeq--

	updatedCurrentData, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, updatedCurrentData, currentETag); err != nil {
		t.Fatalf("write regressed current: %v", err)
	}

	m, err = ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay after CURRENT regression: %v", err)
	}
	if m.LookupSST("regressed-window.sst") != nil {
		t.Fatalf("expected regressed-window.sst to be absent after CURRENT.NextSeq regression")
	}
}

func TestReplay_IncrementalMatchesFullAfterFenceHandoff(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	observer := NewStoreWithStorage(base)
	writer1 := NewStoreWithStorage(base)

	if _, err := observer.Replay(ctx); err != nil {
		t.Fatalf("observer replay: %v", err)
	}
	if _, err := writer1.Replay(ctx); err != nil {
		t.Fatalf("writer1 replay: %v", err)
	}
	if _, err := writer1.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer1: %v", err)
	}
	if _, err := writer1.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "base.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append base.sst: %v", err)
	}

	if _, err := observer.Replay(ctx); err != nil {
		t.Fatalf("observer seed replay: %v", err)
	}

	writer2 := NewStoreWithStorage(base)
	if _, err := writer2.Replay(ctx); err != nil {
		t.Fatalf("writer2 replay: %v", err)
	}
	if _, err := writer2.ClaimWriter(ctx, "writer-2"); err != nil {
		t.Fatalf("claim writer2: %v", err)
	}

	if _, err := writer1.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "stale-after-fence.sst", Epoch: 1, Level: 0}); !errors.Is(err, ErrFenced) {
		t.Fatalf("expected ErrFenced for stale writer append, got %v", err)
	}

	if _, err := writer2.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "valid-epoch2.sst", Epoch: 2, Level: 0}); err != nil {
		t.Fatalf("append valid-epoch2.sst: %v", err)
	}

	mIncremental, err := observer.Replay(ctx)
	if err != nil {
		t.Fatalf("observer replay: %v", err)
	}
	mFull, err := NewStoreWithStorage(base).Replay(ctx)
	if err != nil {
		t.Fatalf("fresh full replay: %v", err)
	}

	if (mIncremental.LookupSST("base.sst") != nil) != (mFull.LookupSST("base.sst") != nil) {
		t.Fatalf("base.sst presence mismatch between incremental and full replay")
	}
	if mIncremental.LookupSST("stale-after-fence.sst") != nil || mFull.LookupSST("stale-after-fence.sst") != nil {
		t.Fatalf("stale-after-fence.sst should not be visible after stale writer append")
	}
	if (mIncremental.LookupSST("valid-epoch2.sst") != nil) != (mFull.LookupSST("valid-epoch2.sst") != nil) {
		t.Fatalf("valid-epoch2.sst presence mismatch between incremental and full replay")
	}
}

func TestReplay_DetectsCommittedPageChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	ms := NewStoreWithStorage(base)
	ms.activeEntryLimit = 2

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}
	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append a.sst: %v", err)
	}
	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "b.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append b.sst: %v", err)
	}

	current := ms.CurrentData()
	if current == nil || len(current.IndexFrontier) == 0 {
		t.Fatalf("expected committed page ref in CURRENT")
	}
	if _, err := store.Write(ctx, current.IndexFrontier[0].Path, []byte(`{"corrupt":true}`)); err != nil {
		t.Fatalf("overwrite committed page: %v", err)
	}

	_, err := NewStoreWithStorage(base).Replay(ctx)
	if err == nil {
		t.Fatalf("expected replay to fail on committed page checksum mismatch")
	}
}

func TestReplay_DetectsActiveEntrySequenceGap(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	current := &Current{
		LogSeqStart: 0,
		NextSeq:     3,
		NextEpoch:   2,
		ActiveEntries: []ManifestLogEntry{
			{ID: ksuid.New(), Seq: 0, Role: FenceRoleWriter, Epoch: 1, Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}},
			{ID: ksuid.New(), Seq: 2, Role: FenceRoleWriter, Epoch: 1, Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "c.sst", Epoch: 1, Level: 0}},
		},
	}
	data, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, data, ""); err != nil {
		t.Fatalf("write current: %v", err)
	}

	_, err = NewStoreWithStorage(backend).Replay(ctx)
	if err == nil {
		t.Fatalf("expected replay to fail on active entry sequence gap")
	}
}

func TestReplay_DetectsActiveEntryDuplicateSequence(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	current := &Current{
		LogSeqStart: 0,
		NextSeq:     2,
		NextEpoch:   2,
		ActiveEntries: []ManifestLogEntry{
			{ID: ksuid.New(), Seq: 0, Role: FenceRoleWriter, Epoch: 1, Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}},
			{ID: ksuid.New(), Seq: 0, Role: FenceRoleWriter, Epoch: 1, Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "dup.sst", Epoch: 1, Level: 0}},
		},
	}
	data, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, data, ""); err != nil {
		t.Fatalf("write current: %v", err)
	}

	_, err = NewStoreWithStorage(backend).Replay(ctx)
	if err == nil {
		t.Fatalf("expected replay to fail on duplicate active entry sequence")
	}
}

func TestReplay_DetectsCommittedPageShapeMismatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	ms := NewStoreWithStorage(backend)
	ms.activeEntryLimit = 2

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}
	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append a.sst: %v", err)
	}
	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "b.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append b.sst: %v", err)
	}

	current := ms.CurrentData()
	if current == nil || len(current.IndexFrontier) == 0 {
		t.Fatalf("expected committed page ref in CURRENT")
	}
	ref := current.IndexFrontier[0]
	page := &CommitPage{
		LayoutVersion: LayoutVersion,
		PageType:      CommitPageTypeLeaf,
		Level:         ref.Level,
		SeqLo:         ref.SeqLo,
		SeqHi:         ref.SeqHi,
		Count:         ref.Count + 1,
		Entries: []ManifestLogEntry{
			{ID: ksuid.New(), Seq: ref.SeqLo, Role: FenceRoleWriter, Epoch: 1, Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}},
		},
		CreatedAt: ref.CreatedAt,
	}
	pageData, err := EncodeCommitPage(page)
	if err != nil {
		t.Fatalf("encode page: %v", err)
	}
	tamperedRef, err := newManifestObjectRef(ref.Path, pageData, manifestObjectKindPage, ref.CreatedAt)
	if err != nil {
		t.Fatalf("create tampered page ref: %v", err)
	}
	current.IndexFrontier[0].ObjectRef = tamperedRef
	currentData, err := EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	_, etag, err := backend.ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if _, err := store.Write(ctx, ref.Path, pageData); err != nil {
		t.Fatalf("overwrite page: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, currentData, etag); err != nil {
		t.Fatalf("write current: %v", err)
	}

	_, err = NewStoreWithStorage(backend).Replay(ctx)
	if err == nil {
		t.Fatalf("expected replay to fail on committed page shape mismatch")
	}
}

func TestStoreDefaultPagePolicyRotatesAndPromotes(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("default-page-policy")
	defer store.Close()

	ms := NewStore(store)
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	// The fence claim is itself the first manifest entry. Sixty-three SST
	// entries therefore fill the default 64-entry active window.
	for i := 0; i < defaultActiveEntryLimit-1; i++ {
		id := fmt.Sprintf("sst-%04d", i)
		if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: id, Epoch: 1, Level: 0}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}
	current := ms.CurrentData()
	if got := len(current.ActiveEntries); got != defaultActiveEntryLimit {
		t.Fatalf("active entries=%d, want=%d", got, defaultActiveEntryLimit)
	}
	if got := len(current.IndexFrontier); got != 0 {
		t.Fatalf("frontier refs=%d before rollover, want=0", got)
	}

	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "sst-0063", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append rollover entry: %v", err)
	}
	current = ms.CurrentData()
	if got := len(current.ActiveEntries); got != 1 {
		t.Fatalf("active entries after rollover=%d, want=1", got)
	}
	if got := len(current.IndexFrontier); got != 1 {
		t.Fatalf("frontier refs after rollover=%d, want=1", got)
	}
	if ref := current.IndexFrontier[0]; ref.Level != 0 || ref.Count != defaultActiveEntryLimit {
		t.Fatalf("leaf ref=(level=%d count=%d), want=(0,%d)", ref.Level, ref.Count, defaultActiveEntryLimit)
	}

	// Complete 32 leaf rotations. The frontier must collapse those leaves into
	// one level-1 index page rather than growing CURRENT with 32 leaf refs.
	for i := defaultActiveEntryLimit; i < defaultActiveEntryLimit*defaultPageFanout; i++ {
		id := fmt.Sprintf("sst-%04d", i)
		if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: id, Epoch: 1, Level: 0}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}
	current = ms.CurrentData()
	if got := len(current.IndexFrontier); got != 1 {
		t.Fatalf("frontier refs after promotion=%d, want=1", got)
	}
	if ref := current.IndexFrontier[0]; ref.Level != 1 || ref.Count != defaultPageFanout {
		t.Fatalf("index ref=(level=%d count=%d), want=(1,%d)", ref.Level, ref.Count, defaultPageFanout)
	}

	replayed, err := NewStore(store).Replay(ctx)
	if err != nil {
		t.Fatalf("fresh replay: %v", err)
	}
	if got, want := len(replayed.L0SSTs), defaultActiveEntryLimit*defaultPageFanout; got != want {
		t.Fatalf("replayed L0 SSTs=%d, want=%d", got, want)
	}
}

func TestStoreCurrentByteLimitRotatesBeforeEntryLimit(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("current-byte-rollover")
	defer store.Close()

	ms := NewStore(store)
	ms.maxCurrentBytes = 2 << 10
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	var appended int
	for ; appended < defaultActiveEntryLimit; appended++ {
		id := fmt.Sprintf("sst-%02d-%s", appended, strings.Repeat("x", 320))
		if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: id, Epoch: 1, Level: 0}); err != nil {
			t.Fatalf("append %d: %v", appended, err)
		}
		current := ms.CurrentData()
		data, err := EncodeCurrent(current)
		if err != nil {
			t.Fatalf("encode CURRENT: %v", err)
		}
		if len(data) > ms.currentByteLimit() {
			t.Fatalf("CURRENT bytes=%d exceed limit=%d", len(data), ms.currentByteLimit())
		}
		if len(current.IndexFrontier) > 0 {
			if len(current.ActiveEntries) >= defaultActiveEntryLimit {
				t.Fatalf("byte rollover waited for entry limit: active=%d", len(current.ActiveEntries))
			}
			appended++
			break
		}
	}
	if appended >= defaultActiveEntryLimit {
		t.Fatal("expected byte-based rollover before active-entry limit")
	}

	replayed, err := NewStore(store).Replay(ctx)
	if err != nil {
		t.Fatalf("fresh replay: %v", err)
	}
	if got := len(replayed.L0SSTs); got != appended {
		t.Fatalf("replayed L0 SSTs=%d, want=%d", got, appended)
	}
}

func TestStorePagesEntryLargerThanCurrentLimit(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("oversized-current-entry")
	defer store.Close()

	ms := NewStore(store)
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}
	before := ms.CurrentData()
	ms.maxCurrentBytes = 4 << 10
	oversizedID := "oversized-" + strings.Repeat("x", ms.maxCurrentBytes*2)

	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{
		ID:    oversizedID,
		Epoch: 1,
		Level: 0,
	}); err != nil {
		t.Fatalf("append oversized entry: %v", err)
	}
	after := ms.CurrentData()
	if got, want := after.NextSeq, before.NextSeq+1; got != want {
		t.Fatalf("CURRENT next_seq=%d, want=%d", got, want)
	}
	if got := len(after.ActiveEntries); got != 0 {
		t.Fatalf("CURRENT active entries=%d, want=0", got)
	}
	afterData, err := EncodeCurrent(after)
	if err != nil {
		t.Fatalf("encode CURRENT after append: %v", err)
	}
	if got := len(afterData); got > ms.currentByteLimit() {
		t.Fatalf("CURRENT bytes=%d exceed limit=%d", got, ms.currentByteLimit())
	}
	if got := len(after.IndexFrontier); got != 1 {
		t.Fatalf("CURRENT frontier refs=%d, want=1", got)
	}

	ref := after.IndexFrontier[0]
	if ref.Level != 0 {
		t.Fatalf("page level=%d, want=0", ref.Level)
	}
	pages, ok := ms.storage.(PageStorage)
	if !ok {
		t.Fatal("manifest storage does not support pages")
	}
	pageData, err := pages.ReadPage(ctx, ref.Path)
	if err != nil {
		t.Fatalf("read oversized entry page: %v", err)
	}
	rawBytes, err := manifestObjectRawBytes(pageData, manifestObjectKindPage, maxManifestPageRawBytes)
	if err != nil {
		t.Fatalf("read page envelope: %v", err)
	}
	if got := rawBytes; got <= uint64(ms.currentByteLimit()) {
		t.Fatalf("page raw bytes=%d, want greater than CURRENT limit=%d", got, ms.currentByteLimit())
	}
	if got := uint64(len(pageData)); got != ref.EncodedBytes {
		t.Fatalf("page encoded bytes=%d, want=%d", got, ref.EncodedBytes)
	}
	sum := sha256.Sum256(pageData)
	if got, want := ref.Checksum, fmt.Sprintf("sha256:%x", sum[:]); got != want {
		t.Fatalf("page checksum=%q, want=%q", got, want)
	}
	page, err := DecodeCommitPage(pageData)
	if err != nil {
		t.Fatalf("decode oversized entry page: %v", err)
	}
	if err := validateCommitPage(page, ref.Path); err != nil {
		t.Fatalf("validate oversized entry page: %v", err)
	}
	if page.SeqLo != before.LogSeqStart || page.SeqHi != before.NextSeq || page.Count != uint32(len(before.ActiveEntries)+1) {
		t.Fatalf("page range/count=(%d,%d,%d), want=(%d,%d,%d)",
			page.SeqLo, page.SeqHi, page.Count,
			before.LogSeqStart, before.NextSeq, len(before.ActiveEntries)+1)
	}
	var matches int
	for i := range page.Entries {
		entry := &page.Entries[i]
		if entry.Seq == before.NextSeq && entry.SSTable != nil && entry.SSTable.ID == oversizedID {
			matches++
		}
	}
	if matches != 1 {
		t.Fatalf("oversized entry occurrences=%d, want=1", matches)
	}

	replayed, err := NewStore(store).Replay(ctx)
	if err != nil {
		t.Fatalf("fresh replay: %v", err)
	}
	if got := len(replayed.L0SSTs); got != 1 {
		t.Fatalf("replayed L0 SSTs=%d, want=1", got)
	}
	if got := replayed.L0SSTs[0].ID; got != oversizedID {
		t.Fatalf("replayed SST ID=%q, want=%q", got, oversizedID)
	}
}

func TestStoreRejectsFixedCurrentMetadataLargerThanLimit(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("oversized-fixed-current")
	defer store.Close()

	ms := NewStore(store)
	ms.maxCurrentBytes = 1
	current := &Current{NextSeq: 1, NextEpoch: 1}
	if err := ms.rotateActiveEntriesForCurrentSize(ctx, current); !errors.Is(err, ErrCurrentTooLarge) {
		t.Fatalf("rotate error=%v, want %v", err, ErrCurrentTooLarge)
	}
}
