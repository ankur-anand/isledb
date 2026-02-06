package manifest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/segmentio/ksuid"
)

type failNthWriteLogStorage struct {
	Storage
	failAt int
	writes int
}

func (s *failNthWriteLogStorage) WriteLog(ctx context.Context, name string, data []byte) (string, error) {
	s.writes++
	if s.failAt > 0 && s.writes == s.failAt {
		return "", fmt.Errorf("inject log write failure")
	}
	return s.Storage.WriteLog(ctx, name, data)
}

type blockingSnapshotStorage struct {
	Storage
	block   chan struct{}
	started chan struct{}
	once    sync.Once
}

func (s *blockingSnapshotStorage) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	s.once.Do(func() { close(s.started) })
	<-s.block
	return s.Storage.WriteSnapshot(ctx, id, data)
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

func TestWriteLog_DoesNotOverwriteExistingEntry(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	backend := NewBlobStoreBackend(store)
	entry := &ManifestLogEntry{
		ID:      ksuid.New(),
		Seq:     0,
		Role:    FenceRoleWriter,
		Epoch:   1,
		Op:      LogOpAddSSTable,
		SSTable: &SSTMeta{ID: "a.sst", Epoch: 1, Level: 0},
	}
	data, err := EncodeLogEntry(entry)
	if err != nil {
		t.Fatalf("encode entry: %v", err)
	}
	logName := formatLogSeq(entry.Seq)
	if _, err := backend.WriteLog(ctx, logName, data); err != nil {
		t.Fatalf("write log: %v", err)
	}
	if _, err := backend.WriteLog(ctx, logName, data); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
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

	logs, err := ms.List(ctx)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(logs))
	}

	entry, err := ms.Read(ctx, logs[0])
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

	logs, err := ms.List(ctx)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(logs))
	}

	entry, err := ms.Read(ctx, logs[0])
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

	for _, entry := range entries {
		data, err := EncodeLogEntry(entry)
		if err != nil {
			t.Fatalf("encode entry: %v", err)
		}
		logName := formatLogSeq(entry.Seq)
		if _, err := backend.WriteLog(ctx, logName, data); err != nil {
			t.Fatalf("write log: %v", err)
		}
	}

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
				AddSSTables: []SSTMeta{{ID: "compacted.sst", Epoch: 1, Level: 1}},
			},
		},
	}

	for _, entry := range entries {
		data, err := EncodeLogEntry(entry)
		if err != nil {
			t.Fatalf("encode entry: %v", err)
		}
		logName := formatLogSeq(entry.Seq)
		if _, err := backend.WriteLog(ctx, logName, data); err != nil {
			t.Fatalf("write log: %v", err)
		}
	}

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

	if len(manifest.SortedRuns) != 1 {
		t.Fatalf("expected 1 sorted run, got %d", len(manifest.SortedRuns))
	}
	if manifest.SortedRuns[0].SSTs[0].ID != "compacted.sst" {
		t.Errorf("expected compacted.sst, got %s", manifest.SortedRuns[0].SSTs[0].ID)
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

	for _, entry := range entries {
		data, err := EncodeLogEntry(entry)
		if err != nil {
			t.Fatalf("encode entry: %v", err)
		}
		logName := formatLogSeq(entry.Seq)
		if _, err := backend.WriteLog(ctx, logName, data); err != nil {
			t.Fatalf("write log: %v", err)
		}
	}

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

	for _, entry := range entries {
		data, err := EncodeLogEntry(entry)
		if err != nil {
			t.Fatalf("encode entry: %v", err)
		}
		logName := formatLogSeq(entry.Seq)
		if _, err := backend.WriteLog(ctx, logName, data); err != nil {
			t.Fatalf("write log: %v", err)
		}
	}

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

	for _, entry := range entries {
		data, err := EncodeLogEntry(entry)
		if err != nil {
			t.Fatalf("encode entry: %v", err)
		}
		logName := formatLogSeq(entry.Seq)
		if _, err := backend.WriteLog(ctx, logName, data); err != nil {
			t.Fatalf("write log: %v", err)
		}
	}

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
				AddSSTables: []SSTMeta{{ID: "stale-compacted.sst", Epoch: 2, Level: 1}},
			},
		},
		{
			ID:    ksuid.New(),
			Seq:   11,
			Role:  FenceRoleCompactor,
			Epoch: 3,
			Op:    LogOpCompaction,
			Compaction: &CompactionLogPayload{
				AddSSTables: []SSTMeta{{ID: "valid-compacted.sst", Epoch: 3, Level: 1}},
			},
		},
	}

	for _, entry := range entries {
		data, err := EncodeLogEntry(entry)
		if err != nil {
			t.Fatalf("encode entry: %v", err)
		}
		logName := formatLogSeq(entry.Seq)
		if _, err := backend.WriteLog(ctx, logName, data); err != nil {
			t.Fatalf("write log: %v", err)
		}
	}

	manifest, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(manifest.SortedRuns) != 1 {
		t.Fatalf("expected 1 sorted run, got %d", len(manifest.SortedRuns))
	}
	if manifest.SortedRuns[0].SSTs[0].ID != "valid-compacted.sst" {
		t.Errorf("expected valid-compacted.sst, got %s", manifest.SortedRuns[0].SSTs[0].ID)
	}
}

func TestReplay_SurvivesTransientWriteLogFailureWithoutSequenceGap(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	failStorage := &failNthWriteLogStorage{Storage: base, failAt: 2}
	ms := NewStoreWithStorage(failStorage)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "failed.sst", Epoch: 1, Level: 0}); err == nil {
		t.Fatal("expected injected append error")
	}

	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "applied.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append retry: %v", err)
	}

	replayStore := NewStoreWithStorage(base)
	m, err := replayStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay after transient write failure: %v", err)
	}
	if m.LookupSST("applied.sst") == nil {
		t.Fatalf("expected applied.sst to be present after replay")
	}
}

func TestWriteSnapshot_DoesNotRegressCurrentNextSeqOnFreshStore(t *testing.T) {
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

	m, err := ms1.Replay(ctx)
	if err != nil {
		t.Fatalf("replay before snapshot: %v", err)
	}

	ms2 := NewStoreWithStorage(backend)
	if _, err := ms2.WriteSnapshot(ctx, m); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

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

	m, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay before snapshot: %v", err)
	}
	snapPath, err := ms.WriteSnapshot(ctx, m)
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	if err := store.Delete(ctx, snapPath); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}

	ms2 := NewStore(store)
	if _, err := ms2.Replay(ctx); err == nil {
		t.Fatal("expected replay error when CURRENT points to a missing snapshot")
	}
}

func TestSnapshotDuringConcurrentAppends_NoSeqRegression_NoLostSSTs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	blocking := &blockingSnapshotStorage{
		Storage: base,
		block:   make(chan struct{}),
		started: make(chan struct{}),
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

	manifestBeforeSnapshot, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay before snapshot: %v", err)
	}

	snapshotErrCh := make(chan error, 1)
	go func() {
		_, err := ms.WriteSnapshot(ctx, manifestBeforeSnapshot)
		snapshotErrCh <- err
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
	firstSnapshotErr := <-snapshotErrCh
	if !errors.Is(firstSnapshotErr, ErrPreconditionFailed) {
		t.Fatalf("expected first snapshot attempt to fail with CAS conflict, got: %v", firstSnapshotErr)
	}

	latestManifest, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay before snapshot retry: %v", err)
	}
	if _, err := ms.WriteSnapshot(ctx, latestManifest); err != nil {
		t.Fatalf("snapshot retry: %v", err)
	}

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
