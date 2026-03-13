package manifest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

type countingStorage struct {
	Storage
	readLogCalls     atomic.Int64
	readCurrentCalls atomic.Int64
}

func (s *countingStorage) ReadLog(ctx context.Context, path string) ([]byte, error) {
	s.readLogCalls.Add(1)
	return s.Storage.ReadLog(ctx, path)
}

func (s *countingStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	s.readCurrentCalls.Add(1)
	return s.Storage.ReadCurrent(ctx)
}

func newCountingStore(t *testing.T) (*Store, *countingStorage) {
	t.Helper()
	bs := blobstore.NewMemory("test")
	t.Cleanup(func() { _ = bs.Close() })

	base := NewBlobStoreBackend(bs)
	cs := &countingStorage{Storage: base}
	return NewStoreWithStorage(cs), cs
}

func appendSSTEntry(t *testing.T, ctx context.Context, ms *Store, id string, epoch uint64) {
	t.Helper()

	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{
		ID:     id,
		Epoch:  epoch,
		Level:  0,
		MinKey: []byte(id),
		MaxKey: []byte(id + "z"),
	}); err != nil {
		t.Fatalf("append sst %s: %v", id, err)
	}
}

func TestIncrementalReplay_NoNewEntries(t *testing.T) {
	ctx := context.Background()
	ms, cs := newCountingStore(t)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("initial replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "w1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	for i := 0; i < 5; i++ {
		appendSSTEntry(t, ctx, ms, fmt.Sprintf("sst-%02d", i), 1)
	}

	m1, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("seed replay: %v", err)
	}
	if len(m1.L0SSTs) != 5 {
		t.Fatalf("expected 5 L0 SSTs, got %d", len(m1.L0SSTs))
	}

	cs.readLogCalls.Store(0)

	m2, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("incremental replay: %v", err)
	}
	if len(m2.L0SSTs) != 5 {
		t.Fatalf("expected 5 L0 SSTs, got %d", len(m2.L0SSTs))
	}

	logReads := cs.readLogCalls.Load()
	if logReads != 0 {
		t.Fatalf("expected 0 ReadLog calls for no-change replay, got %d", logReads)
	}
}

func TestIncrementalReplay_DeltaEntries(t *testing.T) {
	ctx := context.Background()
	ms, cs := newCountingStore(t)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("initial replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "w1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	for i := 0; i < 10; i++ {
		appendSSTEntry(t, ctx, ms, fmt.Sprintf("sst-%02d", i), 1)
	}

	m1, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("seed replay: %v", err)
	}
	if len(m1.L0SSTs) != 10 {
		t.Fatalf("expected 10 L0 SSTs, got %d", len(m1.L0SSTs))
	}

	for i := 10; i < 13; i++ {
		appendSSTEntry(t, ctx, ms, fmt.Sprintf("sst-%02d", i), 1)
	}

	cs.readLogCalls.Store(0)

	m2, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("incremental replay: %v", err)
	}
	if len(m2.L0SSTs) != 13 {
		t.Fatalf("expected 13 L0 SSTs, got %d", len(m2.L0SSTs))
	}

	logReads := cs.readLogCalls.Load()
	if logReads != 3 {
		t.Fatalf("expected 3 ReadLog calls for delta replay, got %d", logReads)
	}

	for i := 0; i < 13; i++ {
		id := fmt.Sprintf("sst-%02d", i)
		if m2.LookupSST(id) == nil {
			t.Fatalf("missing sst %s after incremental replay", id)
		}
	}
}

func TestIncrementalReplay_FallsBackAfterSnapshot(t *testing.T) {
	ctx := context.Background()
	ms, cs := newCountingStore(t)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("initial replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "w1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	for i := 0; i < 5; i++ {
		appendSSTEntry(t, ctx, ms, fmt.Sprintf("sst-%02d", i), 1)
	}
	m1, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("seed replay: %v", err)
	}

	if _, err := ms.WriteSnapshot(ctx, m1); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	for i := 5; i < 7; i++ {
		appendSSTEntry(t, ctx, ms, fmt.Sprintf("sst-%02d", i), 1)
	}

	cs.readLogCalls.Store(0)

	m2, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay after snapshot: %v", err)
	}
	if len(m2.L0SSTs) != 7 {
		t.Fatalf("expected 7 L0 SSTs, got %d", len(m2.L0SSTs))
	}

	logReads := cs.readLogCalls.Load()
	if logReads != 2 {
		t.Fatalf("expected 2 ReadLog calls after snapshot (new log window), got %d", logReads)
	}
}

func TestIncrementalReplay_ConsistencyWithFullReplay(t *testing.T) {
	ctx := context.Background()
	bs := blobstore.NewMemory("test")
	defer func() { _ = bs.Close() }()

	ms1 := NewStore(bs)
	if _, err := ms1.Replay(ctx); err != nil {
		t.Fatalf("initial replay: %v", err)
	}
	if _, err := ms1.ClaimWriter(ctx, "w1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	for i := 0; i < 8; i++ {
		appendSSTEntry(t, ctx, ms1, fmt.Sprintf("sst-%02d", i), 1)
	}

	if _, err := ms1.Replay(ctx); err != nil {
		t.Fatalf("seed replay: %v", err)
	}

	for i := 8; i < 12; i++ {
		appendSSTEntry(t, ctx, ms1, fmt.Sprintf("sst-%02d", i), 1)
	}

	mIncremental, err := ms1.Replay(ctx)
	if err != nil {
		t.Fatalf("incremental replay: %v", err)
	}

	ms2 := NewStore(bs)
	mFull, err := ms2.Replay(ctx)
	if err != nil {
		t.Fatalf("full replay: %v", err)
	}

	if len(mIncremental.L0SSTs) != len(mFull.L0SSTs) {
		t.Fatalf("L0 count mismatch: incremental=%d full=%d", len(mIncremental.L0SSTs), len(mFull.L0SSTs))
	}
	if mIncremental.NextEpoch != mFull.NextEpoch {
		t.Fatalf("NextEpoch mismatch: incremental=%d full=%d", mIncremental.NextEpoch, mFull.NextEpoch)
	}
	if mIncremental.LogSeq != mFull.LogSeq {
		t.Fatalf("LogSeq mismatch: incremental=%d full=%d", mIncremental.LogSeq, mFull.LogSeq)
	}
	if mIncremental.NextSortedRunID != mFull.NextSortedRunID {
		t.Fatalf("NextSortedRunID mismatch: incremental=%d full=%d", mIncremental.NextSortedRunID, mFull.NextSortedRunID)
	}

	for _, sst := range mFull.L0SSTs {
		if mIncremental.LookupSST(sst.ID) == nil {
			t.Fatalf("missing sst %s in incremental replay", sst.ID)
		}
	}
}

func TestIncrementalReplay_WithCompaction(t *testing.T) {
	ctx := context.Background()
	bs := blobstore.NewMemory("test")
	defer func() { _ = bs.Close() }()

	ms := NewStore(bs)
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("initial replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "w1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	for i := 0; i < 4; i++ {
		appendSSTEntry(t, ctx, ms, fmt.Sprintf("l0-%02d", i), 1)
	}

	m1, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("seed replay: %v", err)
	}
	if len(m1.L0SSTs) != 4 {
		t.Fatalf("expected 4 L0 SSTs, got %d", len(m1.L0SSTs))
	}

	if _, err := ms.ClaimCompactor(ctx, "c1"); err != nil {
		t.Fatalf("claim compactor: %v", err)
	}

	removeIDs := make([]string, 4)
	for i := 0; i < 4; i++ {
		removeIDs[i] = fmt.Sprintf("l0-%02d", i)
	}
	compactionPayload := CompactionLogPayload{
		RemoveSSTableIDs: removeIDs,
		AddSortedRun: &SortedRun{
			ID: m1.NextSortedRunID,
			SSTs: []SSTMeta{
				{ID: "compacted-0", Epoch: 1, MinKey: []byte("l0-00"), MaxKey: []byte("l0-03z")},
			},
		},
	}
	if _, err := ms.AppendCompactionWithFence(ctx, compactionPayload); err != nil {
		t.Fatalf("append compaction: %v", err)
	}

	m2, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("incremental replay after compaction: %v", err)
	}

	if len(m2.L0SSTs) != 0 {
		t.Fatalf("expected 0 L0 SSTs after compaction, got %d", len(m2.L0SSTs))
	}
	if len(m2.SortedRuns) != 1 {
		t.Fatalf("expected 1 sorted run after compaction, got %d", len(m2.SortedRuns))
	}
	if m2.LookupSST("compacted-0") == nil {
		t.Fatal("missing compacted-0 in sorted run")
	}

	ms2 := NewStore(bs)
	mFull, err := ms2.Replay(ctx)
	if err != nil {
		t.Fatalf("full replay: %v", err)
	}
	if len(mFull.L0SSTs) != len(m2.L0SSTs) {
		t.Fatalf("L0 mismatch: incremental=%d full=%d", len(m2.L0SSTs), len(mFull.L0SSTs))
	}
	if len(mFull.SortedRuns) != len(m2.SortedRuns) {
		t.Fatalf("SortedRuns mismatch: incremental=%d full=%d", len(m2.SortedRuns), len(mFull.SortedRuns))
	}
}

func TestIncrementalReplay_RepeatedCalls(t *testing.T) {
	ctx := context.Background()
	ms, cs := newCountingStore(t)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("initial replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "w1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	totalSSTs := 0
	for round := 0; round < 5; round++ {
		for i := 0; i < 3; i++ {
			appendSSTEntry(t, ctx, ms, fmt.Sprintf("r%d-sst-%d", round, i), 1)
			totalSSTs++
		}

		cs.readLogCalls.Store(0)

		m, err := ms.Replay(ctx)
		if err != nil {
			t.Fatalf("replay round %d: %v", round, err)
		}
		if len(m.L0SSTs) != totalSSTs {
			t.Fatalf("round %d: expected %d L0 SSTs, got %d", round, totalSSTs, len(m.L0SSTs))
		}

		logReads := cs.readLogCalls.Load()
		if round == 0 && logReads != 4 {
			t.Fatalf("round %d: expected 4 ReadLog calls, got %d", round, logReads)
		}
		if round > 0 && logReads != 3 {
			t.Fatalf("round %d: expected 3 ReadLog calls, got %d", round, logReads)
		}
	}
}
