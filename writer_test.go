package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

func TestWriter_FlushCreatesManifestAndFiles(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-test")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	w, err := newWriter(ctx, store, manifestStore, WriterOptions{
		MemtableSize:          1 << 20,
		FlushInterval:         0,
		BloomBitsPerKey:       0,
		BlockSize:             4096,
		Compression:           "none",
		MaxImmutableMemtables: 0,
	})
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := w.put([]byte("a"), []byte("value-1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	manifestKey := store.ManifestPath()
	if _, _, err := store.Read(ctx, manifestKey); err != nil {
		t.Fatalf("manifest CURRENT missing: %v", err)
	}

	logs, err := store.ListManifestLogs(ctx)
	if err != nil {
		t.Fatalf("ListManifestLogs: %v", err)
	}
	if len(logs) == 0 {
		t.Fatalf("expected manifest log entries")
	}

	ssts, err := store.List(ctx, blobstore.ListOptions{Prefix: "sstable/"})
	if err != nil {
		t.Fatalf("List sstable: %v", err)
	}
	if len(ssts.Objects) == 0 {
		t.Fatalf("expected at least one sstable object")
	}
}

func TestWriter_ReplaySeedsEpoch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-replay")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	w, err := newWriter(ctx, store, manifestStore, WriterOptions{
		MemtableSize:          1 << 20,
		FlushInterval:         0,
		BlockSize:             4096,
		Compression:           "none",
		MaxImmutableMemtables: 0,
	})
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	if err := w.put([]byte("a"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	w2, err := newWriter(ctx, store, manifestStore, WriterOptions{
		MemtableSize:          1 << 20,
		FlushInterval:         0,
		BlockSize:             4096,
		Compression:           "none",
		MaxImmutableMemtables: 0,
	})
	if err != nil {
		t.Fatalf("newWriter(2): %v", err)
	}
	defer w2.close()

	if err := w2.put([]byte("b"), []byte("v2")); err != nil {
		t.Fatalf("Put2: %v", err)
	}
	if err := w2.flush(ctx); err != nil {
		t.Fatalf("Flush2: %v", err)
	}

	logs, err := store.ListManifestLogs(ctx)
	if err != nil {
		t.Fatalf("ListManifestLogs: %v", err)
	}
	if len(logs) < 2 {
		t.Fatalf("expected at least 2 manifest log entries, got %d", len(logs))
	}
}

type failOnceStorage struct {
	manifest.Storage
	failOnce atomic.Bool
}

func (s *failOnceStorage) WriteLog(ctx context.Context, name string, data []byte) (string, error) {
	if s.failOnce.CompareAndSwap(false, true) {
		return "", errors.New("inject log write failure")
	}
	return s.Storage.WriteLog(ctx, name, data)
}

func TestWriter_Backpressure(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-backpressure")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	w, err := newWriter(ctx, store, manifestStore, WriterOptions{
		MemtableSize:          512,
		FlushInterval:         0,
		BlockSize:             4096,
		Compression:           "none",
		MaxImmutableMemtables: 1,
	})
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close()

	val := bytes.Repeat([]byte("v"), 128)
	var lastErr error
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		lastErr = w.put(key, val)
		if errors.Is(lastErr, ErrBackpressure) {
			break
		}
		if lastErr != nil {
			t.Fatalf("put: %v", lastErr)
		}
	}
	if !errors.Is(lastErr, ErrBackpressure) {
		t.Fatalf("expected ErrBackpressure, got %v", lastErr)
	}

	w.mu.Lock()
	queueLen := len(w.immQueue)
	w.mu.Unlock()
	if queueLen != 1 {
		t.Fatalf("expected immQueue length 1, got %d", queueLen)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := w.put([]byte("post"), []byte("v")); err != nil {
		t.Fatalf("put after flush: %v", err)
	}
}

func TestWriter_FlushRequeuesOnManifestFailure(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-fail")
	defer store.Close()

	baseStorage := manifest.NewBlobStoreBackend(store)
	failStorage := &failOnceStorage{Storage: baseStorage}
	manifestStore := manifest.NewStoreWithStorage(failStorage)

	w, err := newWriter(ctx, store, manifestStore, WriterOptions{
		MemtableSize:          1 << 20,
		FlushInterval:         0,
		BlockSize:             4096,
		Compression:           "none",
		MaxImmutableMemtables: 0,
	})
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close()

	if err := w.put([]byte("a"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	if err := w.flush(ctx); err == nil {
		t.Fatalf("expected flush error")
	}

	w.mu.Lock()
	queueLen := len(w.immQueue)
	w.mu.Unlock()
	if queueLen == 0 {
		t.Fatalf("expected immQueue to be requeued after failure")
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush retry: %v", err)
	}
}
