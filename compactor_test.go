package isledb

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestCompactor_L0Compaction(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 1024

	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	for batch := 0; batch < 10; batch++ {
		for i := 0; i < 10; i++ {
			key := []byte{byte(batch), byte(i)}
			value := []byte("value")
			if err := writer.put(key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	writer.close()

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

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
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

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 512

	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	testData := make(map[string]string)
	for batch := 0; batch < 8; batch++ {
		for i := 0; i < 5; i++ {
			key := []byte{byte('a' + batch), byte('0' + i)}
			value := []byte{byte('v'), byte(batch), byte(i)}
			testData[string(key)] = string(value)
			if err := writer.put(key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	writer.close()

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.L0CompactionThreshold = 4

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}

	if err := compactor.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}
	compactor.Close()

	reader, err := newReader(ctx, store, ReaderOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
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

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 512

	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	for batch := 0; batch < 4; batch++ {

		for i := 0; i < 5; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			value := []byte("value")
			if err := writer.put(key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}

		for i := 0; i < 3; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			if err := writer.delete(key); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	writer.close()

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.L0CompactionThreshold = 4

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}

	if err := compactor.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}
	compactor.Close()

	reader, err := newReader(ctx, store, ReaderOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
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

	manifestStore := newManifestStore(store, nil)

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.CheckInterval = 10 * time.Millisecond

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}

	compactor.Start()

	time.Sleep(50 * time.Millisecond)

	compactor.Stop()

	if err := compactor.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestCompactor_Refresh(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	compactor, err := newCompactor(ctx, store, manifestStore, DefaultCompactorOptions())
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer compactor.Close()

	writer, err := newWriter(ctx, store, manifestStore, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := writer.put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := writer.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	writer.close()

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

func TestCompactor_MultipleSSTs(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 1024

	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	for batch := 0; batch < 10; batch++ {
		for i := 0; i < 100; i++ {
			key := make([]byte, 32)
			key[0] = byte(batch)
			key[1] = byte(i)
			value := make([]byte, 512)
			for j := range value {
				value[j] = byte(batch ^ i ^ j)
			}
			if err := writer.put(key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	writer.close()

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.L0CompactionThreshold = 4
	compactorOpts.TargetSSTSize = 4 * 1024
	compactorOpts.CheckInterval = time.Hour

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer compactor.Close()

	if err := compactor.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}

	if err := compactor.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	compactor.mu.Lock()
	m := compactor.manifest.Clone()
	compactor.mu.Unlock()

	if m.SortedRunCount() == 0 {
		t.Fatal("no sorted runs created after compaction")
	}

	totalSSTs := 0
	for _, sr := range m.SortedRuns {
		totalSSTs += len(sr.SSTs)
	}

	if totalSSTs <= 1 {
		t.Errorf("expected multiple SSTs in sorted runs, got %d", totalSSTs)
	}
}

func TestConsecutiveCompaction_Integration(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	compactorOpts := CompactorOptions{
		L0CompactionThreshold: 2,
		MinSources:            2,
		MaxSources:            4,
		SizeThreshold:         4,
		BloomBitsPerKey:       10,
		BlockSize:             1024,
		Compression:           "snappy",
		TargetSSTSize:         64 * 1024,
		CheckInterval:         time.Hour,
	}

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 512

	expectedData := make(map[string]string)

	t.Run("Phase1_L0CompactionCreatesSortedRun", func(t *testing.T) {
		writer, err := newWriter(ctx, store, manifestStore, writerOpts)
		if err != nil {
			t.Fatalf("newWriter: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := fmt.Sprintf("value-%03d-v1", i)
			expectedData[key] = value
			if err := writer.put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}

		for i := 20; i < 40; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := fmt.Sprintf("value-%03d-v1", i)
			expectedData[key] = value
			if err := writer.put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		writer.close()

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}

		if err := compactor.RunCompaction(ctx); err != nil {
			t.Fatalf("RunCompaction: %v", err)
		}

		if err := compactor.Refresh(ctx); err != nil {
			t.Fatalf("Refresh: %v", err)
		}

		compactor.mu.Lock()
		m := compactor.manifest.Clone()
		compactor.mu.Unlock()
		compactor.Close()

		if m.L0SSTCount() != 0 {
			t.Errorf("expected 0 L0 SSTs after compaction, got %d", m.L0SSTCount())
		}
		if m.SortedRunCount() != 1 {
			t.Errorf("expected 1 sorted run, got %d", m.SortedRunCount())
		}

	})

	t.Run("Phase2_ReadCorrectnessAfterCompaction", func(t *testing.T) {
		reader, err := newReader(ctx, store, ReaderOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		for key, expectedValue := range expectedData {
			value, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !found {
				t.Errorf("Get(%s): not found", key)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Get(%s) = %q, want %q", key, value, expectedValue)
			}
		}
	})

	t.Run("Phase3_NewerValuesOverrideOlder", func(t *testing.T) {
		writer, err := newWriter(ctx, store, manifestStore, writerOpts)
		if err != nil {
			t.Fatalf("newWriter: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := fmt.Sprintf("value-%03d-v2-UPDATED", i)
			expectedData[key] = value
			if err := writer.put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := fmt.Sprintf("value-%03d-v3-LATEST", i)
			expectedData[key] = value
			if err := writer.put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		writer.close()

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}

		if err := compactor.RunCompaction(ctx); err != nil {
			t.Fatalf("RunCompaction: %v", err)
		}

		if err := compactor.Refresh(ctx); err != nil {
			t.Fatalf("Refresh: %v", err)
		}

		compactor.Close()

		reader, err := newReader(ctx, store, ReaderOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		for key, expectedValue := range expectedData {
			value, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !found {
				t.Errorf("Get(%s): not found", key)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Get(%s) = %q, want %q", key, value, expectedValue)
			}
		}
	})

	t.Run("Phase4_ConsecutiveCompactionMergesSimilarRuns", func(t *testing.T) {
		for batch := 0; batch < 4; batch++ {
			writer, err := newWriter(ctx, store, manifestStore, writerOpts)
			if err != nil {
				t.Fatalf("newWriter: %v", err)
			}

			for i := 0; i < 30; i++ {
				key := fmt.Sprintf("batch%d-key-%03d", batch, i)
				value := fmt.Sprintf("batch%d-value-%03d", batch, i)
				expectedData[key] = value
				if err := writer.put([]byte(key), []byte(value)); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			if err := writer.flush(ctx); err != nil {
				t.Fatalf("flush: %v", err)
			}

			for i := 30; i < 60; i++ {
				key := fmt.Sprintf("batch%d-key-%03d", batch, i)
				value := fmt.Sprintf("batch%d-value-%03d", batch, i)
				expectedData[key] = value
				if err := writer.put([]byte(key), []byte(value)); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			if err := writer.flush(ctx); err != nil {
				t.Fatalf("flush: %v", err)
			}
			writer.close()

			compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
			if err != nil {
				t.Fatalf("newCompactor: %v", err)
			}

			if err := compactor.RunCompaction(ctx); err != nil {
				t.Fatalf("RunCompaction: %v", err)
			}
			compactor.Close()
		}

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}

		compactor.Close()

		reader, err := newReader(ctx, store, ReaderOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		var errors []string
		for key, expectedValue := range expectedData {
			value, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				errors = append(errors, fmt.Sprintf("Get(%s): %v", key, err))
				continue
			}
			if !found {
				errors = append(errors, fmt.Sprintf("Get(%s): not found", key))
				continue
			}
			if string(value) != expectedValue {
				errors = append(errors, fmt.Sprintf("Get(%s) = %q, want %q", key, value, expectedValue))
			}
		}

		if len(errors) > 0 {
			t.Errorf("Data integrity errors (%d total):", len(errors))
			for i, e := range errors {
				if i < 10 {
					t.Errorf("  %s", e)
				}
			}
			if len(errors) > 10 {
				t.Errorf("  ... and %d more errors", len(errors)-10)
			}
		}
	})

	t.Run("Phase5_DeletesRespected", func(t *testing.T) {
		writer, err := newWriter(ctx, store, manifestStore, writerOpts)
		if err != nil {
			t.Fatalf("newWriter: %v", err)
		}

		deletedKeys := make(map[string]bool)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%03d", i)
			deletedKeys[key] = true
			delete(expectedData, key)
			if err := writer.delete([]byte(key)); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("padding-key-%03d", i)
			value := fmt.Sprintf("padding-value-%03d", i)
			expectedData[key] = value
			if err := writer.put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		writer.close()

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}

		if err := compactor.RunCompaction(ctx); err != nil {
			t.Fatalf("RunCompaction: %v", err)
		}
		compactor.Close()

		reader, err := newReader(ctx, store, ReaderOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		for key := range deletedKeys {
			_, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if found {
				t.Errorf("Get(%s): should be deleted but was found", key)
			}
		}

		for key, expectedValue := range expectedData {
			value, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !found {
				t.Errorf("Get(%s): not found", key)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Get(%s) = %q, want %q", key, value, expectedValue)
			}
		}
	})

	t.Run("Phase6_ScanWorksAfterCompaction", func(t *testing.T) {
		reader, err := newReader(ctx, store, ReaderOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		results, err := reader.Scan(ctx, nil, nil)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}

		scanResults := make(map[string]string)
		for _, kv := range results {
			scanResults[string(kv.Key)] = string(kv.Value)
		}

		if len(scanResults) != len(expectedData) {
			t.Errorf("Scan returned %d keys, expected %d", len(scanResults), len(expectedData))
		}

		for key, expectedValue := range expectedData {
			if gotValue, ok := scanResults[key]; !ok {
				t.Errorf("Scan missing key: %s", key)
			} else if gotValue != expectedValue {
				t.Errorf("Scan(%s) = %q, want %q", key, gotValue, expectedValue)
			}
		}
	})
}

func TestConsecutiveCompaction_SequenceNumberCorrectness(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	compactorOpts := CompactorOptions{
		L0CompactionThreshold: 2,
		MinSources:            2,
		MaxSources:            4,
		SizeThreshold:         4,
		BloomBitsPerKey:       10,
		BlockSize:             512,
		Compression:           "snappy",
		TargetSSTSize:         64 * 1024,
		CheckInterval:         time.Hour,
	}

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 256

	writer1, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := writer1.put([]byte("foo"), []byte("v1-old")); err != nil {
		t.Fatalf("put: %v", err)
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("filler1-%03d", i)
		if err := writer1.put([]byte(key), []byte("filler-value-1")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := writer1.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	for i := 50; i < 100; i++ {
		key := fmt.Sprintf("filler1-%03d", i)
		if err := writer1.put([]byte(key), []byte("filler-value-1")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := writer1.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	writer1.close()

	compactor1, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	if err := compactor1.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}
	compactor1.Close()

	writer2, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := writer2.put([]byte("foo"), []byte("v2-new")); err != nil {
		t.Fatalf("put: %v", err)
	}
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("filler2-%03d", i)
		if err := writer2.put([]byte(key), []byte("short")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := writer2.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	for i := 30; i < 60; i++ {
		key := fmt.Sprintf("filler2-%03d", i)
		if err := writer2.put([]byte(key), []byte("short")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := writer2.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	writer2.close()

	compactor2, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	if err := compactor2.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}

	compactor2.Close()

	reader, err := newReader(ctx, store, ReaderOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	value, found, err := reader.Get(ctx, []byte("foo"))
	if err != nil {
		t.Fatalf("Get(foo): %v", err)
	}
	if !found {
		t.Fatal("Get(foo): not found")
	}
	if string(value) != "v2-new" {
		t.Errorf("Get(foo) = %q, want %q - SEQUENCE NUMBER NOT RESPECTED!", value, "v2-new")
	}
}

func TestCompactor_ValidateSSTChecksum(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("compactor-checksum")
	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := w.put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	w.close()

	m, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	if len(m.L0SSTs) == 0 {
		t.Fatalf("expected L0 SSTs in manifest")
	}

	newest := m.L0SSTs[0]
	path := store.SSTPath(newest.ID)
	data, _, err := store.Read(ctx, path)
	if err != nil {
		t.Fatalf("read sst: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("unexpected empty sst data")
	}
	data[0] ^= 0xFF
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("write corrupt sst: %v", err)
	}

	cOpts := DefaultCompactorOptions()
	cOpts.ValidateSSTChecksum = true
	cOpts.L0CompactionThreshold = 1
	c, err := newCompactor(ctx, store, manifestStore, cOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer c.Close()

	if err := c.RunCompaction(ctx); err == nil {
		t.Fatalf("expected compaction to fail on checksum mismatch")
	}
}

func TestConsecutiveCompaction_MergePreservesData(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	compactorOpts := CompactorOptions{
		L0CompactionThreshold: 1,
		MinSources:            2,
		MaxSources:            4,
		SizeThreshold:         4,
		BloomBitsPerKey:       10,
		BlockSize:             512,
		Compression:           "snappy",
		TargetSSTSize:         64 * 1024,
		CheckInterval:         time.Hour,
	}

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 4 * 1024

	expectedData := make(map[string]string)

	for batch := 0; batch < 4; batch++ {
		writer, err := newWriter(ctx, store, manifestStore, writerOpts)
		if err != nil {
			t.Fatalf("newWriter: %v", err)
		}

		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("batch%d-key-%05d", batch, i)
			value := fmt.Sprintf("batch%d-value-%05d", batch, i)
			expectedData[key] = value
			if err := writer.put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		writer.close()

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}
		if err := compactor.RunCompaction(ctx); err != nil {
			t.Fatalf("RunCompaction: %v", err)
		}

		compactor.Close()
	}

	reader, err := newReader(ctx, store, ReaderOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	var missing, wrong int
	for key, expectedValue := range expectedData {
		value, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			t.Errorf("Get(%s): %v", key, err)
			continue
		}
		if !found {
			missing++
			if missing <= 5 {
				t.Errorf("Get(%s): not found", key)
			}
			continue
		}
		if string(value) != expectedValue {
			wrong++
			if wrong <= 5 {
				t.Errorf("Get(%s) = %q, want %q", key, value, expectedValue)
			}
		}
	}

	if missing > 0 || wrong > 0 {
		t.Errorf("Data integrity issues: %d missing, %d wrong values out of %d total keys",
			missing, wrong, len(expectedData))
	}

	results, err := reader.Scan(ctx, nil, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(results) != len(expectedData) {
		t.Errorf("Scan returned %d keys, expected %d", len(results), len(expectedData))
	}

	for i := 1; i < len(results); i++ {
		if bytes.Compare(results[i-1].Key, results[i].Key) >= 0 {
			t.Errorf("Scan results not sorted at position %d: %q >= %q",
				i, results[i-1].Key, results[i].Key)
			break
		}
	}
}

func TestCompactor_EnqueuesPendingDeleteMarks(t *testing.T) {
	store := blobstore.NewMemory("")
	defer store.Close()
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer writer.close()

	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("mark-key-%03d", i)
		if err := writer.put([]byte(key), []byte("value")); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	before, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest before compaction: %v", err)
	}
	if len(before.L0SSTs) == 0 {
		t.Fatalf("expected L0 SSTs before compaction")
	}

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.L0CompactionThreshold = 2
	compactorOpts.CheckInterval = time.Hour

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer compactor.Close()

	if err := compactor.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}

	for _, sst := range before.L0SSTs {
		found, err := hasPendingSSTDeleteMark(ctx, store, sst.ID)
		if err != nil {
			t.Fatalf("lookup pending delete mark for %s: %v", sst.ID, err)
		}
		if !found {
			t.Fatalf("expected pending delete mark for %s", sst.ID)
		}
	}
}
