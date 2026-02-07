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
	writeCount  atomic.Int32
	failOnWrite int32
}

func (s *failOnceStorage) WriteLog(ctx context.Context, name string, data []byte) (string, error) {
	count := s.writeCount.Add(1)
	if count == s.failOnWrite {
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
	// Fail on second write (first write is the fence claim entry)
	failStorage := &failOnceStorage{Storage: baseStorage, failOnWrite: 2}
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

func TestWriter_DeleteBackpressureDoesNotAdvanceSeq(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-delete-backpressure")
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

	for i := 0; i < 10000; i++ {
		seqBefore := w.seq
		err := w.delete([]byte(fmt.Sprintf("k%06d", i)))
		if errors.Is(err, ErrBackpressure) {
			if w.seq != seqBefore {
				t.Fatalf("delete error should not advance seq: before=%d after=%d", seqBefore, w.seq)
			}
			return
		}
		if err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	t.Fatalf("expected ErrBackpressure from deletes")
}

func TestWriter_PutBlobWriteFailureDoesNotAdvanceSeq(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := blobstore.NewMemory("writer-putblob-seq-rollback")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	opts := DefaultWriterOptions()
	opts.FlushInterval = 0
	opts.ValueStorage.BlobThreshold = 1

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close()

	seqBefore := w.seq
	cancel()

	err = w.put([]byte("k"), []byte("v"))
	if err == nil {
		t.Fatalf("expected put blob error with canceled context")
	}
	if w.seq != seqBefore {
		t.Fatalf("blob write error should not advance seq: before=%d after=%d", seqBefore, w.seq)
	}
}
