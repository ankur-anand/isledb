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

func TestRetentionCompactor_RejectsNilContext(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("retention-nil-context")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	if _, err := newRetentionCompactor(nil, store, manifestStore, retentionCompactorOptions{}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("newRetentionCompactor(nil) error=%v, want %v", err, ErrNilContext)
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, retentionCompactorOptions{})
	if err != nil {
		t.Fatalf("newRetentionCompactor: %v", err)
	}
	defer cleaner.Close(ctx)

	if err := cleaner.Start(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Start(nil) error=%v, want %v", err, ErrNilContext)
	}
	if err := cleaner.RunOnce(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("RunOnce(nil) error=%v, want %v", err, ErrNilContext)
	}
	if err := cleaner.Close(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Close(nil) error=%v, want %v", err, ErrNilContext)
	}
}

func TestRetentionCompactor_CloseTimeoutCanBeRetried(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("retention-close-retry")
	defer store.Close()

	storage := &blockingReadCurrentStorage{
		Storage: manifest.NewBlobStoreBackend(store),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manifestStore := manifest.NewStoreWithStorage(storage)

	opts := defaultRetentionCompactorOptions()
	opts.CheckInterval = 10 * time.Millisecond
	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newRetentionCompactor: %v", err)
	}

	storage.block.Store(true)
	if err := cleaner.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-storage.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background retention compactor did not reach blocking CURRENT read")
	}

	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	err = cleaner.Close(closeCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first close error=%v, want %v", err, context.DeadlineExceeded)
	}

	close(storage.release)

	retryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := cleaner.Close(retryCtx); err != nil {
		t.Fatalf("retry close: %v", err)
	}
}

func TestRetentionCompactor_CloseWaitsForManualRunOnce(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("retention-close-manual-runonce")
	defer store.Close()

	storage := &blockingReadCurrentStorage{
		Storage: manifest.NewBlobStoreBackend(store),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manifestStore := manifest.NewStoreWithStorage(storage)
	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, retentionCompactorOptions{})
	if err != nil {
		t.Fatalf("newRetentionCompactor: %v", err)
	}

	storage.block.Store(true)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- cleaner.RunOnce(ctx)
	}()

	select {
	case <-storage.started:
	case <-time.After(2 * time.Second):
		t.Fatal("manual RunOnce did not reach blocking CURRENT read")
	}

	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	err = cleaner.Close(closeCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first close error=%v, want %v", err, context.DeadlineExceeded)
	}

	close(storage.release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("manual RunOnce error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("manual RunOnce did not finish after release")
	}

	retryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := cleaner.Close(retryCtx); err != nil {
		t.Fatalf("retry close: %v", err)
	}
}

func TestRetentionCompactor_RunOnceAfterCloseReturnsClosed(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("retention-closed")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, retentionCompactorOptions{})
	if err != nil {
		t.Fatalf("newRetentionCompactor: %v", err)
	}
	if err := cleaner.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := cleaner.RunOnce(ctx); !errors.Is(err, errRetentionCompactorClosed) {
		t.Fatalf("RunOnce after Close error=%v, want %v", err, errRetentionCompactorClosed)
	}
}

func TestRetentionCompactor_RunOnceSerializesConcurrentCalls(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("retention-runonce-serial")
	defer store.Close()

	storage := &blockingReadCurrentStorage{
		Storage: manifest.NewBlobStoreBackend(store),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manifestStore := manifest.NewStoreWithStorage(storage)
	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, retentionCompactorOptions{})
	if err != nil {
		t.Fatalf("newRetentionCompactor: %v", err)
	}
	defer cleaner.Close(ctx)

	storage.block.Store(true)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- cleaner.RunOnce(ctx)
	}()

	select {
	case <-storage.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first RunOnce did not reach blocking CURRENT read")
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	err = cleaner.RunOnce(waitCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second RunOnce error=%v, want %v", err, context.DeadlineExceeded)
	}

	close(storage.release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first RunOnce error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first RunOnce did not finish after release")
	}
}

func TestRetentionCompactor_FIFO(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("wal:%d:%03d", batch, i)
			value := fmt.Sprintf("entry:%d:%03d", batch, i)
			if err := w.put(ctx, []byte(key), []byte(value)); err != nil {
				t.Fatalf("put failed: %v", err)
			}
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	w.close(ctx)

	rOpts := defaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	reader, _ := newReader(ctx, store, rOpts)
	m := reader.currentManifest()
	if m.L0SSTCount() != 5 {
		t.Fatalf("Expected 5 L0 SSTs, got %d", m.L0SSTCount())
	}
	reader.Close()

	cleanerOpts := retentionCompactorOptions{
		Mode:            compactByAge,
		RetentionPeriod: time.Nanosecond,
		KeepAtLeastSSTs: 2,
		CheckInterval:   time.Hour,
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close(ctx)

	if err := cleaner.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}
	stats := cleaner.Stats()
	if stats.L0SSTCount != 2 {
		t.Fatalf("retention stats L0SSTCount=%d, want 2 after cleanup", stats.L0SSTCount)
	}

	rOpts2 := defaultReaderOptions()
	rOpts2.CacheDir = t.TempDir()
	reader2, _ := newReader(ctx, store, rOpts2)
	m2 := reader2.currentManifest()
	reader2.Close()

	total := m2.L0SSTCount()
	for _, level := range m2.Levels {
		total += len(level.SSTs)
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
	wOpts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("log:%d:%03d", batch, i)
			value := fmt.Sprintf("data:%d:%03d", batch, i)
			if err := w.put(ctx, []byte(key), []byte(value)); err != nil {
				t.Fatalf("put failed: %v", err)
			}
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	w.close(ctx)

	cleanerOpts := retentionCompactorOptions{
		Mode:               compactByTimeWindow,
		RetentionPeriod:    time.Nanosecond,
		KeepAtLeastWindows: 1,
		SegmentDuration:    time.Hour,
		CheckInterval:      time.Hour,
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close(ctx)

	statsBefore := cleaner.Stats()
	if err := cleaner.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	statsAfter := cleaner.Stats()

	totalAfter := statsAfter.L0SSTCount + statsAfter.LevelCount
	if totalAfter > statsBefore.L0SSTCount+statsBefore.LevelCount {
		t.Error("SST count should not increase after cleanup")
	}
}

func TestRetentionCompactor_NoDeleteWhenFresh(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key:%03d", i)
		if err := w.put(ctx, []byte(key), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	w.close(ctx)

	cleanerOpts := retentionCompactorOptions{
		Mode:            compactByAge,
		RetentionPeriod: 7 * 24 * time.Hour,
		KeepAtLeastSSTs: 1,
		CheckInterval:   time.Hour,
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close(ctx)

	statsBefore := cleaner.Stats()

	if err := cleaner.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
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
	wOpts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 5; batch++ {
		if err := w.put(ctx, []byte(fmt.Sprintf("key:%d", batch)), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	w.close(ctx)

	var callbackCalled atomic.Bool
	var deletedCount int

	cleanerOpts := retentionCompactorOptions{
		Mode:            compactByAge,
		RetentionPeriod: time.Nanosecond,
		KeepAtLeastSSTs: 2,
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
	defer cleaner.Close(ctx)

	if err := cleaner.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
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
	wOpts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 5; batch++ {
		if err := w.put(ctx, []byte(fmt.Sprintf("key:%d", batch)), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	w.close(ctx)

	var cleanupCount atomic.Int32

	cleanerOpts := retentionCompactorOptions{
		Mode:            compactByAge,
		RetentionPeriod: time.Nanosecond,
		KeepAtLeastSSTs: 1,
		CheckInterval:   50 * time.Millisecond,
		OnCleanup: func(stats CleanupStats) {
			cleanupCount.Add(1)
		},
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}

	cleaner.Start(ctx)

	time.Sleep(150 * time.Millisecond)

	cleaner.Close(ctx)

	if cleanupCount.Load() == 0 {
		t.Error("Background cleanup should have run at least once")
	}
}

func TestDefaultRetentionCompactorOptions(t *testing.T) {
	opts := defaultRetentionCompactorOptions()

	if opts.Mode != compactByAge {
		t.Errorf("Default mode should be FIFO")
	}
	if opts.RetentionPeriod != 7*24*time.Hour {
		t.Errorf("Default retention should be 7 days")
	}
	if opts.KeepAtLeastSSTs != 10 {
		t.Errorf("default minimum SST count = %d, want 10", opts.KeepAtLeastSSTs)
	}
	if opts.KeepAtLeastWindows != 1 {
		t.Errorf("default minimum window count = %d, want 1", opts.KeepAtLeastWindows)
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
	wOpts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}

	for batch := 0; batch < 6; batch++ {
		if err := w.put(ctx, []byte(fmt.Sprintf("fence-key:%d", batch)), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}
	if err := w.close(ctx); err != nil {
		t.Fatalf("writer close failed: %v", err)
	}

	var cleanupErrCount atomic.Int32
	cleanerOpts := retentionCompactorOptions{
		Mode:            compactByAge,
		RetentionPeriod: time.Nanosecond,
		KeepAtLeastSSTs: 1,
		CheckInterval:   20 * time.Millisecond,
		OnCleanupError: func(err error) {
			cleanupErrCount.Add(1)
		},
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, cleanerOpts)
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close(ctx)

	competingStore := newManifestStore(store, nil)
	if _, err := competingStore.Replay(ctx); err != nil {
		t.Fatalf("competing replay failed: %v", err)
	}
	if _, err := competingStore.ClaimCompactor(ctx, "compactor-other"); err != nil {
		t.Fatalf("competing claim compactor failed: %v", err)
	}

	cleaner.Start(ctx)

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

func TestRetentionCompactorFIFOCommitsRetirementsWithoutEarlyDelete(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close(ctx)

	for batch := 0; batch < 5; batch++ {
		key := fmt.Sprintf("fifo-mark:%03d", batch)
		value := fmt.Sprintf("value:%03d", batch)
		if err := w.put(ctx, []byte(key), []byte(value)); err != nil {
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

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, retentionCompactorOptions{
		Mode:            compactByAge,
		RetentionPeriod: time.Nanosecond,
		KeepAtLeastSSTs: 2,
		CheckInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close(ctx)

	if err := cleaner.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	sstAfter, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list ssts after cleanup: %v", err)
	}
	if len(sstAfter) != len(sstBefore) {
		t.Fatalf("expected no physical SST deletion in phase-1, before=%d after=%d", len(sstBefore), len(sstAfter))
	}

	expectedRetirements := len(sstBefore) - 2
	if got := countRetiredSSTObjects(t, ctx, manifestStore); got != expectedRetirements {
		t.Fatalf("retirement records=%d, want %d", got, expectedRetirements)
	}
}

func TestRetentionCompactorSegmentedCommitsRetirementsWithoutEarlyDelete(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close(ctx)

	for batch := 0; batch < 4; batch++ {
		key := fmt.Sprintf("seg-mark:%03d", batch)
		value := fmt.Sprintf("value:%03d", batch)
		if err := w.put(ctx, []byte(key), []byte(value)); err != nil {
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

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, retentionCompactorOptions{
		Mode:               compactByTimeWindow,
		RetentionPeriod:    time.Nanosecond,
		KeepAtLeastWindows: 2,
		SegmentDuration:    time.Second,
		CheckInterval:      time.Hour,
	})
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close(ctx)

	if err := cleaner.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	sstAfter, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list ssts after cleanup: %v", err)
	}
	if len(sstAfter) != len(sstBefore) {
		t.Fatalf("expected no physical SST deletion in phase-1, before=%d after=%d", len(sstBefore), len(sstAfter))
	}
	manifestAfter, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest after cleanup: %v", err)
	}
	if got := manifestAfter.L0SSTCount(); got != 2 {
		t.Fatalf("retained L0 windows=%d, want 2", got)
	}

	if got := countRetiredSSTObjects(t, ctx, manifestStore); got != 2 {
		t.Fatalf("retirement records=%d, want 2", got)
	}
}

func countRetiredSSTObjects(t *testing.T, ctx context.Context, store *manifest.Store) int {
	t.Helper()
	seqs, err := store.ListEntries(ctx)
	if err != nil {
		t.Fatalf("list manifest entries: %v", err)
	}
	count := 0
	for _, seq := range seqs {
		entry, err := store.ReadEntry(ctx, seq)
		if err != nil {
			t.Fatalf("read manifest entry %d: %v", seq, err)
		}
		for _, retired := range entry.RetiredObjects {
			if retired.Kind == manifest.RetiredObjectSST {
				count++
			}
		}
	}
	return count
}

func TestRetentionCompactor_RunOnce_MissingManifestPageReturnsError(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	if _, err := manifestStore.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	// Force CURRENT.active_entries to rotate into an immutable committed page.
	for i := 0; i < 1024; i++ {
		_, err := manifestStore.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{
			ID:    fmt.Sprintf("missing-page-sst-%04d", i),
			Epoch: 1,
			Level: 0,
		})
		if err != nil {
			t.Fatalf("append manifest entry %d: %v", i, err)
		}
	}

	cleaner, err := newRetentionCompactor(ctx, store, manifestStore, retentionCompactorOptions{
		Mode:            compactByAge,
		RetentionPeriod: 365 * 24 * time.Hour,
		KeepAtLeastSSTs: 1000,
		CheckInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("newRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close(ctx)

	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read CURRENT: %v", err)
	}
	if current == nil || len(current.IndexFrontier) == 0 {
		t.Fatalf("expected CURRENT to reference at least one committed page")
	}
	missingPath := current.IndexFrontier[0].Path
	if err := store.Delete(ctx, missingPath); err != nil {
		t.Fatalf("delete manifest page %s: %v", missingPath, err)
	}

	if err := cleaner.RunOnce(ctx); err == nil {
		t.Fatalf("expected RunOnce to fail when a committed manifest page is missing")
	}
}
