package isledb

import (
	"context"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestReader_RefreshAndPrefetch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key:%03d", i)
		value := fmt.Sprintf("value:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	rOpts := DefaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	r, err := newReader(ctx, store, rOpts)
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	statsBefore := r.SSTCacheStats()
	initialCount := statsBefore.EntryCount

	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key:%03d", i)
		value := fmt.Sprintf("value:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if err := r.RefreshAndPrefetchSSTs(ctx); err != nil {
		t.Fatalf("RefreshAndPrefetchSSTs failed: %v", err)
	}

	statsAfter := r.SSTCacheStats()
	if statsAfter.EntryCount <= initialCount {
		t.Errorf("Expected cache entry count to increase after prefetch, got %d (was %d)", statsAfter.EntryCount, initialCount)
	}

	val, found, err := r.Get(ctx, []byte("key:007"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Error("Expected to find key:007 after refresh")
	}
	if string(val) != "value:007" {
		t.Errorf("Expected value:007, got %s", val)
	}
}

func TestReader_RefreshAndPrefetch_NoNewSSTs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key:%03d", i)
		value := fmt.Sprintf("value:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	rOpts := DefaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	r, err := newReader(ctx, store, rOpts)
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	if err := r.RefreshAndPrefetchSSTs(ctx); err != nil {
		t.Fatalf("RefreshAndPrefetchSSTs failed: %v", err)
	}

	statsBefore := r.SSTCacheStats()

	if err := r.RefreshAndPrefetchSSTs(ctx); err != nil {
		t.Fatalf("RefreshAndPrefetchSSTs failed: %v", err)
	}

	statsAfter := r.SSTCacheStats()
	if statsAfter.EntryCount != statsBefore.EntryCount {
		t.Errorf("Cache entry count should not change when no new SSTs, got %d (was %d)", statsAfter.EntryCount, statsBefore.EntryCount)
	}
}

func TestReader_RefreshAndPrefetch_EmptyManifest(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	rOpts := DefaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	r, err := newReader(ctx, store, rOpts)
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	if err := r.RefreshAndPrefetchSSTs(ctx); err != nil {
		t.Fatalf("RefreshAndPrefetchSSTs failed on empty manifest: %v", err)
	}
}

func TestReader_RefreshAndPrefetch_MultipleFlushes(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = 0
	w, err := newWriter(ctx, store, wOpts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	rOpts := DefaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	r, err := newReader(ctx, store, rOpts)
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("batch%d:key:%03d", batch, i)
			value := fmt.Sprintf("batch%d:value:%03d", batch, i)
			if err := w.put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("put failed: %v", err)
			}
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush failed: %v", err)
		}

		if err := r.RefreshAndPrefetchSSTs(ctx); err != nil {
			t.Fatalf("RefreshAndPrefetchSSTs failed on batch %d: %v", batch, err)
		}
	}

	stats := r.SSTCacheStats()
	if stats.EntryCount != 3 {
		t.Errorf("Expected 3 SSTs in cache, got %d", stats.EntryCount)
	}

	for batch := 0; batch < 3; batch++ {
		key := fmt.Sprintf("batch%d:key:002", batch)
		expectedValue := fmt.Sprintf("batch%d:value:002", batch)
		val, found, err := r.Get(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !found {
			t.Errorf("Expected to find %s", key)
		}
		if string(val) != expectedValue {
			t.Errorf("Expected %s, got %s", expectedValue, val)
		}
	}
}
