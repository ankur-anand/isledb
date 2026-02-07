package isledb

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestRetentionCompactor_FIFO(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("wal:%d:%03d", batch, i)
			value := fmt.Sprintf("entry:%d:%03d", batch, i)
			if err := w.put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("put failed: %v", err)
			}
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	w.close()

	rOpts := DefaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	reader, _ := newReader(ctx, store, rOpts)
	m := reader.Manifest()
	if m.L0SSTCount() != 5 {
		t.Fatalf("Expected 5 L0 SSTs, got %d", m.L0SSTCount())
	}
	reader.Close()

	cleanerOpts := RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: time.Nanosecond,
		RetentionCount:  2,
		CheckInterval:   time.Hour,
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	rOpts2 := DefaultReaderOptions()
	rOpts2.CacheDir = t.TempDir()
	reader2, _ := newReader(ctx, store, rOpts2)
	m2 := reader2.Manifest()
	reader2.Close()

	total := m2.L0SSTCount()
	for _, sr := range m2.SortedRuns {
		total += len(sr.SSTs)
	}
	if total != 2 {
		t.Errorf("Expected 2 SSTs after cleanup, got %d", total)
	}
}

func TestRetentionCompactor_Segmented(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("log:%d:%03d", batch, i)
			value := fmt.Sprintf("data:%d:%03d", batch, i)
			if err := w.put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("put failed: %v", err)
			}
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	w.close()

	cleanerOpts := RetentionCompactorOptions{
		Mode:            CompactByTimeWindow,
		RetentionPeriod: time.Nanosecond,
		RetentionCount:  1,
		SegmentDuration: time.Hour,
		CheckInterval:   time.Hour,
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	statsBefore := cleaner.Stats()
	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	statsAfter := cleaner.Stats()

	totalAfter := statsAfter.L0SSTCount + statsAfter.SortedRunCount
	if totalAfter > statsBefore.L0SSTCount+statsBefore.SortedRunCount {
		t.Error("SST count should not increase after cleanup")
	}
}

func TestRetentionCompactor_NoDeleteWhenFresh(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key:%03d", i)
		if err := w.put([]byte(key), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	w.close()

	cleanerOpts := RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: 7 * 24 * time.Hour,
		RetentionCount:  1,
		CheckInterval:   time.Hour,
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	statsBefore := cleaner.Stats()

	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	statsAfter := cleaner.Stats()

	if statsAfter.L0SSTCount != statsBefore.L0SSTCount {
		t.Errorf("Expected no SSTs deleted, but L0 count changed from %d to %d",
			statsBefore.L0SSTCount, statsAfter.L0SSTCount)
	}
}

func TestRetentionCompactor_Callback(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 5; batch++ {
		if err := w.put([]byte(fmt.Sprintf("key:%d", batch)), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	w.close()

	var callbackCalled atomic.Bool
	var deletedCount int

	cleanerOpts := RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: time.Nanosecond,
		RetentionCount:  2,
		CheckInterval:   time.Hour,
		OnCleanup: func(stats CleanupStats) {
			callbackCalled.Store(true)
			deletedCount = stats.SSTsDeleted
		},
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	if !callbackCalled.Load() {
		t.Error("OnCleanup callback should have been called")
	}
	if deletedCount != 3 {
		t.Errorf("Expected 3 SSTs deleted, got %d", deletedCount)
	}
}

func TestRetentionCompactor_BackgroundLoop(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 5; batch++ {
		if err := w.put([]byte(fmt.Sprintf("key:%d", batch)), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	w.close()

	var cleanupCount atomic.Int32

	cleanerOpts := RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: time.Nanosecond,
		RetentionCount:  1,
		CheckInterval:   50 * time.Millisecond,
		OnCleanup: func(stats CleanupStats) {
			cleanupCount.Add(1)
		},
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}

	cleaner.Start()

	time.Sleep(150 * time.Millisecond)

	cleaner.Close()

	if cleanupCount.Load() == 0 {
		t.Error("Background cleanup should have run at least once")
	}
}

func TestDefaultRetentionCompactorOptions(t *testing.T) {
	opts := DefaultRetentionCompactorOptions()

	if opts.Mode != CompactByAge {
		t.Errorf("Default mode should be FIFO")
	}
	if opts.RetentionPeriod != 7*24*time.Hour {
		t.Errorf("Default retention should be 7 days")
	}
	if opts.RetentionCount != 10 {
		t.Errorf("Default retention count should be 10")
	}
	if opts.CheckInterval != time.Minute {
		t.Errorf("Default check interval should be 1 minute")
	}
	if opts.SegmentDuration != time.Hour {
		t.Errorf("Default segment duration should be 1 hour")
	}
}

func TestRetentionCompactor_BackgroundLoopStopsWhenFenced(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 6; batch++ {
		if err := w.put([]byte(fmt.Sprintf("fence-key:%d", batch)), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	if err := w.close(); err != nil {
		t.Fatalf("writer close failed: %v", err)
	}

	var cleanupErrCount atomic.Int32
	cleanerOpts := RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: time.Nanosecond,
		RetentionCount:  1,
		CheckInterval:   20 * time.Millisecond,
		OnCleanupError: func(err error) {
			cleanupErrCount.Add(1)
		},
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	competingStore := newManifestStore(store, nil)
	if _, err := competingStore.Replay(ctx); err != nil {
		t.Fatalf("competing replay failed: %v", err)
	}
	if _, err := competingStore.ClaimCompactor(ctx, "compactor-other"); err != nil {
		t.Fatalf("competing claim compactor failed: %v", err)
	}

	cleaner.Start()

	deadline := time.Now().Add(2 * time.Second)
	for cleanupErrCount.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if cleanupErrCount.Load() == 0 {
		t.Fatalf("expected cleanup loop to hit fence error at least once")
	}

	first := cleanupErrCount.Load()
	time.Sleep(120 * time.Millisecond)
	after := cleanupErrCount.Load()
	if after != first {
		t.Fatalf("expected cleanup loop to stop after fence error; errors before=%d after=%d", first, after)
	}
	if !cleaner.IsFenced() {
		t.Fatalf("expected retention compactor to remain fenced after fence error")
	}
}

func TestRetentionCompactor_FIFO_EnqueuesDeleteMarks_NoPhysicalDelete(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for batch := 0; batch < 5; batch++ {
		key := fmt.Sprintf("fifo-mark:%03d", batch)
		value := fmt.Sprintf("value:%03d", batch)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}

	sstBefore, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list ssts before cleanup: %v", err)
	}
	if len(sstBefore) != 5 {
		t.Fatalf("expected 5 SST files before cleanup, got %d", len(sstBefore))
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: time.Nanosecond,
		RetentionCount:  2,
		CheckInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	sstAfter, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list ssts after cleanup: %v", err)
	}
	if len(sstAfter) != len(sstBefore) {
		t.Fatalf("expected no physical SST deletion in phase-1, before=%d after=%d", len(sstBefore), len(sstAfter))
	}

	marks, err := loadPendingSSTDeleteMarks(ctx, store)
	if err != nil {
		t.Fatalf("load pending delete marks: %v", err)
	}
	expectedMarks := len(sstBefore) - 2
	if len(marks) != expectedMarks {
		t.Fatalf("expected %d pending delete marks, got %d", expectedMarks, len(marks))
	}
}

func TestRetentionCompactor_Segmented_EnqueuesDeleteMarks_NoPhysicalDelete(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for batch := 0; batch < 4; batch++ {
		key := fmt.Sprintf("seg-mark:%03d", batch)
		value := fmt.Sprintf("value:%03d", batch)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
		time.Sleep(1100 * time.Millisecond)
	}

	sstBefore, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list ssts before cleanup: %v", err)
	}
	if len(sstBefore) != 4 {
		t.Fatalf("expected 4 SST files before cleanup, got %d", len(sstBefore))
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, RetentionCompactorOptions{
		Mode:            CompactByTimeWindow,
		RetentionPeriod: time.Nanosecond,
		RetentionCount:  1,
		SegmentDuration: time.Second,
		CheckInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	sstAfter, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list ssts after cleanup: %v", err)
	}
	if len(sstAfter) != len(sstBefore) {
		t.Fatalf("expected no physical SST deletion in phase-1, before=%d after=%d", len(sstBefore), len(sstAfter))
	}

	marks, err := loadPendingSSTDeleteMarks(ctx, store)
	if err != nil {
		t.Fatalf("load pending delete marks: %v", err)
	}
	if len(marks) == 0 {
		t.Fatalf("expected pending delete marks to be enqueued")
	}
}

func TestRetentionCompactor_LogCatchup_RebuildsMissingMarkFromCompactionLog(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("catchup-key:%03d", i)
		if err := w.put([]byte(key), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}

	beforeCompaction, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay before compaction: %v", err)
	}
	if len(beforeCompaction.L0SSTs) == 0 {
		t.Fatalf("expected L0 SSTs before compaction")
	}
	targetSSTID := beforeCompaction.L0SSTs[0].ID

	compactor, err := newCompactor(ctx, store, manifestStore, CompactorOptions{
		L0CompactionThreshold: 2,
		CheckInterval:         time.Hour,
	})
	if err != nil {
		t.Fatalf("newCompactor failed: %v", err)
	}
	defer compactor.Close()

	if err := compactor.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction failed: %v", err)
	}

	found, err := hasPendingSSTDeleteMark(ctx, store, targetSSTID)
	if err != nil {
		t.Fatalf("lookup compactor mark for %s: %v", targetSSTID, err)
	}
	if !found {
		t.Fatalf("expected compactor mark for %s", targetSSTID)
	}
	if err := clearPendingSSTDeleteMarks(ctx, store, []string{targetSSTID}); err != nil {
		t.Fatalf("clear target mark: %v", err)
	}

	if err := store.Delete(ctx, gcCheckpointPath(store)); err != nil {
		t.Fatalf("delete gc checkpoint: %v", err)
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: 365 * 24 * time.Hour,
		RetentionCount:  1000,
		CheckInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	found, err = hasPendingSSTDeleteMark(ctx, store, targetSSTID)
	if err != nil {
		t.Fatalf("lookup recreated mark for %s: %v", targetSSTID, err)
	}
	if !found {
		t.Fatalf("expected catchup to recreate mark for %s", targetSSTID)
	}

	checkpoint, err := loadGCMarkCheckpoint(ctx, store)
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if checkpoint.LastAppliedSeq == 0 {
		t.Fatalf("expected checkpoint to advance")
	}
}

func TestRetentionCompactor_LogCatchup_FastForwardsFromSnapshotBoundary(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("snap-boundary-key:%03d", i)
		if err := w.put([]byte(key), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}

	m, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay before snapshot: %v", err)
	}
	if _, err := manifestStore.WriteSnapshot(ctx, m); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	if err := storeGCMarkCheckpoint(ctx, store, &gcMarkCheckpoint{
		LastAppliedSeq: 0,
	}); err != nil {
		t.Fatalf("write stale checkpoint: %v", err)
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: 365 * 24 * time.Hour,
		RetentionCount:  1000,
		CheckInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	current, err := cleaner.readManifestCurrent(ctx)
	if err != nil {
		t.Fatalf("read CURRENT: %v", err)
	}
	if current == nil {
		t.Fatalf("expected non-nil CURRENT")
	}

	checkpoint, err := loadGCMarkCheckpoint(ctx, store)
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if checkpoint.LastAppliedSeq < current.LogSeqStart {
		t.Fatalf("checkpoint did not fast-forward to snapshot boundary: got=%d log_seq_start=%d", checkpoint.LastAppliedSeq, current.LogSeqStart)
	}
	if checkpoint.LastSeenLogSeqStart != current.LogSeqStart {
		t.Fatalf("checkpoint log_seq_start mismatch: got=%d want=%d", checkpoint.LastSeenLogSeqStart, current.LogSeqStart)
	}
}

func TestRetentionCompactor_LogCatchup_SnapshotBoundaryRunsOrphanScan(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("orphan-scan-key:%03d", i)
		if err := w.put([]byte(key), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}

	m, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay before snapshot: %v", err)
	}
	if _, err := manifestStore.WriteSnapshot(ctx, m); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	orphanID := "orphan-manual-sst"
	if _, err := store.Write(ctx, store.SSTPath(orphanID), []byte("orphan-data")); err != nil {
		t.Fatalf("write orphan sst: %v", err)
	}

	if err := storeGCMarkCheckpoint(ctx, store, &gcMarkCheckpoint{
		LastAppliedSeq: 0,
	}); err != nil {
		t.Fatalf("write stale checkpoint: %v", err)
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: 365 * 24 * time.Hour,
		RetentionCount:  1000,
		CheckInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	found, err := hasPendingSSTDeleteMark(ctx, store, orphanID)
	if err != nil {
		t.Fatalf("lookup orphan mark: %v", err)
	}
	if !found {
		t.Fatalf("expected orphan sst to be marked: %s", orphanID)
	}
}

func TestRetentionCompactor_RunCleanup_MissingManifestLogReturnsError(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("missing-log-key:%03d", i)
		if err := w.put([]byte(key), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, RetentionCompactorOptions{
		Mode:            CompactByAge,
		RetentionPeriod: 365 * 24 * time.Hour,
		RetentionCount:  1000,
		CheckInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	current, err := cleaner.readManifestCurrent(ctx)
	if err != nil {
		t.Fatalf("read CURRENT: %v", err)
	}
	if current == nil || current.NextSeq == 0 {
		t.Fatalf("expected non-empty CURRENT")
	}
	missingSeq := current.LogSeqStart
	if missingSeq >= current.NextSeq {
		t.Fatalf("invalid log window: start=%d next=%d", current.LogSeqStart, current.NextSeq)
	}
	missingPath := manifestStore.Storage().LogPath(fmt.Sprintf("%020d", missingSeq))
	if err := store.Delete(ctx, missingPath); err != nil {
		t.Fatalf("delete manifest log %s: %v", missingPath, err)
	}

	orphanID := "orphan-after-missing-log"
	if _, err := store.Write(ctx, store.SSTPath(orphanID), []byte("orphan-data")); err != nil {
		t.Fatalf("write orphan sst: %v", err)
	}

	if err := cleaner.RunCleanup(ctx); err == nil {
		t.Fatalf("expected RunCleanup to fail when manifest log is missing")
	}

	found, err := hasPendingSSTDeleteMark(ctx, store, orphanID)
	if err != nil {
		t.Fatalf("lookup orphan mark: %v", err)
	}
	if found {
		t.Fatalf("orphan sst should not be marked when catchup fails before commit: %s", orphanID)
	}
}
