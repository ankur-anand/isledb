package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestWriter_PutBlobWriteFailureLeavesSequenceHole(t *testing.T) {
	ctx := context.Background()
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
	blobCtx, cancel := context.WithCancel(context.Background())
	cancel()
	w.ctx = blobCtx

	err = w.put([]byte("k"), []byte("v"))
	if err == nil {
		t.Fatalf("expected put blob error with canceled blob write context")
	}
	if w.seq != seqBefore+1 {
		t.Fatalf("blob write error should leave a sequence hole: before=%d after=%d", seqBefore, w.seq)
	}
}

func TestWriter_OpenContextCancellationDoesNotBlockWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := blobstore.NewMemory("writer-open-context-cancel")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.FlushInterval = 0

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close()

	cancel()

	if err := w.put([]byte("k-inline"), []byte("v")); err != nil {
		t.Fatalf("put inline after opening ctx cancel: %v", err)
	}
	if err := w.delete([]byte("k-inline")); err != nil {
		t.Fatalf("delete after opening ctx cancel: %v", err)
	}
	w.valueConfig.BlobThreshold = 1
	if err := w.put([]byte("k-blob"), []byte("b")); err != nil {
		t.Fatalf("put blob after opening ctx cancel: %v", err)
	}
}

func TestWriter_PartialMetricsDoNotPanic(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-partial-metrics")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.FlushInterval = 0
	opts.MemtableSize = 512
	opts.MaxImmutableMemtables = 1
	opts.Metrics = &WriterMetrics{}

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close()

	val := bytes.Repeat([]byte("v"), 128)
	lastErr := error(nil)
	for i := 0; i < 10000; i++ {
		lastErr = w.put([]byte(fmt.Sprintf("k%06d", i)), val)
		if errors.Is(lastErr, ErrBackpressure) {
			break
		}
		if lastErr != nil {
			t.Fatalf("put %d: %v", i, lastErr)
		}
	}
	if !errors.Is(lastErr, ErrBackpressure) {
		t.Fatalf("expected ErrBackpressure, got %v", lastErr)
	}

	if err := w.delete([]byte("k-final")); err != nil && !errors.Is(err, ErrBackpressure) {
		t.Fatalf("delete: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestWriter_MetricsBlobFlushAndTTLPaths(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-metrics-coverage")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.FlushInterval = 0
	opts.ValueStorage.BlobThreshold = 1
	opts.Metrics = DefaultWriterMetrics(nil)

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close()

	blobValue := []byte("blob-value")
	if err := w.putWithTTL([]byte("k1"), blobValue, time.Second); err != nil {
		t.Fatalf("putWithTTL success: %v", err)
	}
	if err := w.putWithTTL(nil, []byte("bad"), time.Second); err == nil {
		t.Fatalf("expected putWithTTL error for empty key")
	}
	if err := w.deleteWithTTL([]byte("k1"), time.Second); err != nil {
		t.Fatalf("deleteWithTTL: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	metrics := opts.Metrics
	if got := testutil.ToFloat64(metrics.PutTotal); got != 2 {
		t.Fatalf("put_total mismatch: got %v want 2", got)
	}
	if got := testutil.ToFloat64(metrics.PutErrors); got != 1 {
		t.Fatalf("put_errors_total mismatch: got %v want 1", got)
	}
	if got := testutil.ToFloat64(metrics.PutBlobTotal); got != 1 {
		t.Fatalf("put_blob_total mismatch: got %v want 1", got)
	}
	if got := testutil.ToFloat64(metrics.PutBlobErrors); got != 0 {
		t.Fatalf("put_blob_errors_total mismatch: got %v want 0", got)
	}
	if got := testutil.ToFloat64(metrics.BlobBytesTotal); got != float64(len(blobValue)) {
		t.Fatalf("blob_bytes_total mismatch: got %v want %d", got, len(blobValue))
	}
	if got := testutil.ToFloat64(metrics.DeleteTotal); got != 1 {
		t.Fatalf("delete_total mismatch: got %v want 1", got)
	}
	if got := testutil.ToFloat64(metrics.FlushTotal); got != 1 {
		t.Fatalf("flush_total mismatch: got %v want 1", got)
	}
	if got := testutil.ToFloat64(metrics.FlushErrors); got != 0 {
		t.Fatalf("flush_errors mismatch: got %v want 0", got)
	}
	if got := testutil.ToFloat64(metrics.FlushBytes); got <= 0 {
		t.Fatalf("flush_bytes_total must be > 0, got %v", got)
	}
}
