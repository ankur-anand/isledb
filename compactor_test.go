package isledb

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestCompactor_L0Compaction(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 1024

	writer, err := NewWriter(ctx, store, writerOpts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	for batch := 0; batch < 10; batch++ {
		for i := 0; i < 10; i++ {
			key := []byte{byte(batch), byte(i)}
			value := []byte("value")
			if err := writer.Put(key, value); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	writer.Close()

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.L0CompactionThreshold = 4
	compactorOpts.CheckInterval = time.Hour

	var compactionStarted, compactionEnded bool
	compactorOpts.OnCompactionStart = func(job CompactionJob) {
		compactionStarted = true
	}
	compactorOpts.OnCompactionEnd = func(job CompactionJob, err error) {
		compactionEnded = true
		if err != nil {
		} else if job.OutputRun != nil {
		}
	}

	compactor, err := NewCompactor(ctx, store, compactorOpts)
	if err != nil {
		t.Fatalf("NewCompactor: %v", err)
	}
	defer compactor.Close()

	if err := compactor.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}

	if !compactionStarted || !compactionEnded {
		t.Errorf("compaction callbacks not called: started=%v ended=%v", compactionStarted, compactionEnded)
	}

	if err := compactor.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	compactor.mu.Lock()
	m := compactor.manifest.Clone()
	compactor.mu.Unlock()

	if m.L0SSTCount() >= compactorOpts.L0CompactionThreshold {
		t.Errorf("L0 still has %d SSTs after compaction", m.L0SSTCount())
	}

	if m.SortedRunCount() == 0 {
		t.Error("no sorted runs created after compaction")
	}

}

func TestCompactor_DataIntegrity(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 512

	writer, err := NewWriter(ctx, store, writerOpts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	testData := make(map[string]string)
	for batch := 0; batch < 8; batch++ {
		for i := 0; i < 5; i++ {
			key := []byte{byte('a' + batch), byte('0' + i)}
			value := []byte{byte('v'), byte(batch), byte(i)}
			testData[string(key)] = string(value)
			if err := writer.Put(key, value); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	writer.Close()

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.L0CompactionThreshold = 4

	compactor, err := NewCompactor(ctx, store, compactorOpts)
	if err != nil {
		t.Fatalf("NewCompactor: %v", err)
	}

	if err := compactor.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}
	compactor.Close()

	reader, err := NewReader(ctx, store, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	for key, expectedValue := range testData {
		value, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			t.Errorf("Get(%q): %v", key, err)
			continue
		}
		if !found {
			t.Errorf("Get(%q): not found", key)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("Get(%q) = %q, want %q", key, value, expectedValue)
		}
	}
}

func TestCompactor_TombstoneHandling(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 512

	writer, err := NewWriter(ctx, store, writerOpts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	for batch := 0; batch < 4; batch++ {

		for i := 0; i < 5; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			value := []byte("value")
			if err := writer.Put(key, value); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		for i := 0; i < 3; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			if err := writer.Delete(key); err != nil {
				t.Fatalf("Delete: %v", err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	writer.Close()

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.L0CompactionThreshold = 4

	compactor, err := NewCompactor(ctx, store, compactorOpts)
	if err != nil {
		t.Fatalf("NewCompactor: %v", err)
	}

	if err := compactor.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}
	compactor.Close()

	reader, err := NewReader(ctx, store, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	for batch := 0; batch < 4; batch++ {

		for i := 0; i < 3; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			_, found, err := reader.Get(ctx, key)
			if err != nil {
				t.Errorf("Get(%q): %v", key, err)
				continue
			}
			if found {
				t.Errorf("Get(%q) should not be found (was deleted)", key)
			}
		}

		for i := 3; i < 5; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			value, found, err := reader.Get(ctx, key)
			if err != nil {
				t.Errorf("Get(%q): %v", key, err)
				continue
			}
			if !found {
				t.Errorf("Get(%q): not found", key)
				continue
			}
			if string(value) != "value" {
				t.Errorf("Get(%q) = %q, want %q", key, value, "value")
			}
		}
	}
}

func TestCompactor_BackgroundLoop(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.CheckInterval = 10 * time.Millisecond

	compactor, err := NewCompactor(ctx, store, compactorOpts)
	if err != nil {
		t.Fatalf("NewCompactor: %v", err)
	}

	compactor.Start()

	time.Sleep(50 * time.Millisecond)

	compactor.Stop()

	if err := compactor.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestCompactor_Refresh(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	compactor, err := NewCompactor(ctx, store, DefaultCompactorOptions())
	if err != nil {
		t.Fatalf("NewCompactor: %v", err)
	}
	defer compactor.Close()

	writer, err := NewWriter(ctx, store, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if err := writer.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	writer.Close()

	if err := compactor.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	compactor.mu.Lock()
	l0Count := compactor.manifest.L0SSTCount()
	compactor.mu.Unlock()

	if l0Count == 0 {
		t.Error("compactor didn't see new L0 SST after refresh")
	}
}
