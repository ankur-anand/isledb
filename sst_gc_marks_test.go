package isledb

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

func TestStoreGCMarkCheckpoint_MonotonicProgress(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	if err := storeGCMarkCheckpoint(ctx, store, &gcMarkCheckpoint{
		LastAppliedSeq:      20,
		LastSeenLogSeqStart: 10,
	}); err != nil {
		t.Fatalf("store initial checkpoint: %v", err)
	}

	if err := storeGCMarkCheckpoint(ctx, store, &gcMarkCheckpoint{
		LastAppliedSeq:      5,
		LastSeenLogSeqStart: 2,
	}); err != nil {
		t.Fatalf("store regressing checkpoint: %v", err)
	}

	got, err := loadGCMarkCheckpoint(ctx, store)
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if got.LastAppliedSeq < 20 {
		t.Fatalf("last_applied_seq regressed: got=%d want>=20", got.LastAppliedSeq)
	}
	if got.LastSeenLogSeqStart < 10 {
		t.Fatalf("last_seen_log_seq_start regressed: got=%d want>=10", got.LastSeenLogSeqStart)
	}
}

type fakeGCMarkStorage struct {
	pendingData []byte
	pendingETag string

	checkpointData []byte
	checkpointETag string

	pendingStoreCalls int
	pendingFailUntil  int
	pendingFailErr    error

	checkpointStoreCalls int
	checkpointFailUntil  int
	checkpointFailErr    error
}

func (f *fakeGCMarkStorage) LoadPendingDeleteMarks(context.Context) ([]byte, string, bool, error) {
	if len(f.pendingData) == 0 {
		return nil, "", false, nil
	}
	return append([]byte(nil), f.pendingData...), f.pendingETag, true, nil
}

func (f *fakeGCMarkStorage) StorePendingDeleteMarks(_ context.Context, data []byte, _ string, _ bool) error {
	f.pendingStoreCalls++
	if f.pendingStoreCalls <= f.pendingFailUntil {
		return f.pendingFailErr
	}
	f.pendingData = append([]byte(nil), data...)
	f.pendingETag = "pending-etag"
	return nil
}

func (f *fakeGCMarkStorage) LoadGCCheckpoint(context.Context) ([]byte, string, bool, error) {
	if len(f.checkpointData) == 0 {
		return nil, "", false, nil
	}
	return append([]byte(nil), f.checkpointData...), f.checkpointETag, true, nil
}

func (f *fakeGCMarkStorage) StoreGCCheckpoint(_ context.Context, data []byte, _ string, _ bool) error {
	f.checkpointStoreCalls++
	if f.checkpointStoreCalls <= f.checkpointFailUntil {
		return f.checkpointFailErr
	}
	f.checkpointData = append([]byte(nil), data...)
	f.checkpointETag = "etag"
	return nil
}

func TestStoreGCMarkCheckpointWithStorage_RetriesOnCASConflict(t *testing.T) {
	ctx := context.Background()
	storage := &fakeGCMarkStorage{
		checkpointFailUntil: 2,
		checkpointFailErr:   manifest.ErrPreconditionFailed,
	}

	err := storeGCMarkCheckpointWithStorage(ctx, storage, &gcMarkCheckpoint{
		LastAppliedSeq:      11,
		LastSeenLogSeqStart: 7,
	})
	if err != nil {
		t.Fatalf("store gc checkpoint with retries: %v", err)
	}
	if storage.checkpointStoreCalls != 3 {
		t.Fatalf("store attempts mismatch: got=%d want=3", storage.checkpointStoreCalls)
	}

	var got gcMarkCheckpoint
	if err := json.Unmarshal(storage.checkpointData, &got); err != nil {
		t.Fatalf("unmarshal stored checkpoint: %v", err)
	}
	if got.LastAppliedSeq != 11 || got.LastSeenLogSeqStart != 7 {
		t.Fatalf("unexpected stored checkpoint: %+v", got)
	}
}

func TestStoreGCMarkCheckpointWithStorage_ExhaustsRetriesOnCASConflict(t *testing.T) {
	ctx := context.Background()
	storage := &fakeGCMarkStorage{
		checkpointFailUntil: gcCASMaxRetries + 2,
		checkpointFailErr:   blobstore.ErrPreconditionFailed,
	}

	err := storeGCMarkCheckpointWithStorage(ctx, storage, &gcMarkCheckpoint{
		LastAppliedSeq:      1,
		LastSeenLogSeqStart: 1,
	})
	if err == nil {
		t.Fatal("expected retry exhaustion error")
	}
	if !strings.Contains(err.Error(), "store gc checkpoint after retries") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		t.Fatalf("expected wrapped precondition error, got: %v", err)
	}
	if storage.checkpointStoreCalls != gcCASMaxRetries {
		t.Fatalf("store attempts mismatch: got=%d want=%d", storage.checkpointStoreCalls, gcCASMaxRetries)
	}
}

func TestEnqueuePendingSSTDeleteMarksWithStorage_RetriesOnCASConflict(t *testing.T) {
	ctx := context.Background()
	storage := &fakeGCMarkStorage{
		pendingFailUntil: 1,
		pendingFailErr:   manifest.ErrPreconditionFailed,
	}

	if err := enqueuePendingSSTDeleteMarksWithStorage(ctx, storage, []string{"sst-a", "sst-b"}, "test_reason", 42); err != nil {
		t.Fatalf("enqueue pending marks with retries: %v", err)
	}
	if storage.pendingStoreCalls != 2 {
		t.Fatalf("pending store attempts mismatch: got=%d want=2", storage.pendingStoreCalls)
	}

	set, _, _, err := loadPendingSSTDeleteMarkSetWithStorageCAS(ctx, storage)
	if err != nil {
		t.Fatalf("load pending set: %v", err)
	}
	if len(set.Marks) != 2 {
		t.Fatalf("pending mark count mismatch: got=%d want=2", len(set.Marks))
	}
	byID := pendingMarkMapFromSet(set)
	if _, ok := byID["sst-a"]; !ok {
		t.Fatalf("missing mark for sst-a")
	}
	if _, ok := byID["sst-b"]; !ok {
		t.Fatalf("missing mark for sst-b")
	}
}

func TestEnqueuePendingSSTDeleteMarksWithStorage_ExhaustsRetriesOnCASConflict(t *testing.T) {
	ctx := context.Background()
	storage := &fakeGCMarkStorage{
		pendingFailUntil: gcCASMaxRetries + 2,
		pendingFailErr:   blobstore.ErrPreconditionFailed,
	}

	err := enqueuePendingSSTDeleteMarksWithStorage(ctx, storage, []string{"sst-a"}, "test_reason", 1)
	if err == nil {
		t.Fatal("expected retry exhaustion error")
	}
	if !strings.Contains(err.Error(), "store pending mark set after retries") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		t.Fatalf("expected wrapped precondition error, got: %v", err)
	}
	if storage.pendingStoreCalls != gcCASMaxRetries {
		t.Fatalf("pending store attempts mismatch: got=%d want=%d", storage.pendingStoreCalls, gcCASMaxRetries)
	}
}

func TestClearPendingSSTDeleteMarksWithStorage_RetriesOnCASConflict(t *testing.T) {
	ctx := context.Background()
	initial := &pendingSSTDeleteMarkSet{
		Version: gcMarkSchemaVersion,
		Marks: []pendingSSTDeleteMark{
			{Version: gcMarkSchemaVersion, SSTID: "sst-a"},
			{Version: gcMarkSchemaVersion, SSTID: "sst-b"},
		},
	}
	payload, err := json.Marshal(initial)
	if err != nil {
		t.Fatalf("marshal initial pending set: %v", err)
	}

	storage := &fakeGCMarkStorage{
		pendingData:      payload,
		pendingETag:      "etag-1",
		pendingFailUntil: 1,
		pendingFailErr:   manifest.ErrPreconditionFailed,
	}

	if err := clearPendingSSTDeleteMarksWithStorage(ctx, storage, []string{"sst-a"}); err != nil {
		t.Fatalf("clear pending marks with retries: %v", err)
	}
	if storage.pendingStoreCalls != 2 {
		t.Fatalf("pending store attempts mismatch: got=%d want=2", storage.pendingStoreCalls)
	}

	set, _, _, err := loadPendingSSTDeleteMarkSetWithStorageCAS(ctx, storage)
	if err != nil {
		t.Fatalf("load pending set: %v", err)
	}
	if len(set.Marks) != 1 || set.Marks[0].SSTID != "sst-b" {
		t.Fatalf("unexpected remaining pending marks: %+v", set.Marks)
	}
}
