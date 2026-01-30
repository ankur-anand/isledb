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

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
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

	reader, _ := NewReader(ctx, store, DefaultReaderOptions())
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

	cleaner, err := NewRetentionCompactor(ctx, store, cleanerOpts)
	if err != nil {
		t.Fatalf("NewRetentionCompactor failed: %v", err)
	}
	defer cleaner.Close()

	if err := cleaner.RunCleanup(ctx); err != nil {
		t.Fatalf("RunCleanup failed: %v", err)
	}

	reader2, _ := NewReader(ctx, store, DefaultReaderOptions())
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

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
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

	cleaner, err := NewRetentionCompactor(ctx, store, cleanerOpts)
	if err != nil {
		t.Fatalf("NewRetentionCompactor failed: %v", err)
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

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
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

	cleaner, err := NewRetentionCompactor(ctx, store, cleanerOpts)
	if err != nil {
		t.Fatalf("NewRetentionCompactor failed: %v", err)
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

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
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

	cleaner, err := NewRetentionCompactor(ctx, store, cleanerOpts)
	if err != nil {
		t.Fatalf("NewRetentionCompactor failed: %v", err)
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

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
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

	cleaner, err := NewRetentionCompactor(ctx, store, cleanerOpts)
	if err != nil {
		t.Fatalf("NewRetentionCompactor failed: %v", err)
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
