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

	logs, err := manifestStore.List(ctx)
	if err != nil {
		t.Fatalf("manifest list: %v", err)
	}
	if len(logs) == 0 {
		t.Fatalf("expected committed manifest entries")
	}

	ssts, err := store.List(ctx, blobstore.ListOptions{Prefix: "sstable/"})
	if err != nil {
		t.Fatalf("List sstable: %v", err)
	}
	if len(ssts.Objects) == 0 {
		t.Fatalf("expected at least one sstable object")
	}
}

func TestWriter_FlushPublishesChangeBatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-change-feed")
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
	defer w.close()

	if err := w.put([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if err := w.delete([]byte("a")); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	if err := w.put([]byte("c"), []byte("vc")); err != nil {
		t.Fatalf("put c: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	logPaths, err := manifestStore.List(ctx)
	if err != nil {
		t.Fatalf("manifest log list: %v", err)
	}
	var meta *manifest.ChangeBatchMeta
	for _, logPath := range logPaths {
		entry, err := manifestStore.Read(ctx, logPath)
		if err != nil {
			t.Fatalf("read manifest log %s: %v", logPath, err)
		}
		if entry.Op == manifest.LogOpAddSSTable && entry.ChangeBatch != nil {
			meta = entry.ChangeBatch
			break
		}
	}
	if meta == nil {
		t.Fatal("committed add_sstable entry did not include change batch metadata")
	}
	if meta.SeqLo != 1 || meta.SeqHi != 3 || meta.Count != 3 || meta.Path == "" {
		t.Fatalf("change batch meta mismatch: %+v", *meta)
	}

	data, attrs, err := store.Read(ctx, meta.Path)
	if err != nil {
		t.Fatalf("read change batch: %v", err)
	}
	if attrs.Size != meta.Size {
		t.Fatalf("change batch size attr=%d meta=%d", attrs.Size, meta.Size)
	}
	batch, err := DecodeChangeBatch(data)
	if err != nil {
		t.Fatalf("DecodeChangeBatch: %v", err)
	}
	if batch.Epoch != meta.Epoch || batch.SeqLo != meta.SeqLo || batch.SeqHi != meta.SeqHi {
		t.Fatalf("batch header mismatch: %+v meta=%+v", batch, meta)
	}
	if got := len(batch.Changes); got != 3 {
		t.Fatalf("change count=%d want 3", got)
	}
	if batch.Changes[0].Seq != 1 || string(batch.Changes[0].Key) != "b" || string(batch.Changes[0].Value) != "vb" {
		t.Fatalf("change[0] mismatch: %+v", batch.Changes[0])
	}
	if batch.Changes[1].Seq != 2 || batch.Changes[1].Kind != ChangeDelete || string(batch.Changes[1].Key) != "a" {
		t.Fatalf("change[1] mismatch: %+v", batch.Changes[1])
	}
	if batch.Changes[2].Seq != 3 || string(batch.Changes[2].Key) != "c" || string(batch.Changes[2].Value) != "vc" {
		t.Fatalf("change[2] mismatch: %+v", batch.Changes[2])
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

	logs, err := manifestStore.List(ctx)
	if err != nil {
		t.Fatalf("manifest list: %v", err)
	}
	if len(logs) < 2 {
		t.Fatalf("expected at least 2 committed manifest entries, got %d", len(logs))
	}
}

type failOnceStorage struct {
	manifest.Storage
	writeCount  atomic.Int32
	failOnWrite int32
}

func (s *failOnceStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	count := s.writeCount.Add(1)
	if count == s.failOnWrite {
		return "", errors.New("inject current write failure")
	}
	return s.Storage.WriteCurrentCAS(ctx, data, expectedETag)
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
	// Fail the flush publish. The first two CURRENT writes claim the writer fence
	// and commit the fence-claim entry.
	failStorage := &failOnceStorage{Storage: baseStorage, failOnWrite: 3}
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

func TestWriter_PutBlobWriteFailureDoesNotAdvanceSequence(t *testing.T) {
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
	if w.seq != seqBefore {
		t.Fatalf("blob write error should not advance sequence: before=%d after=%d", seqBefore, w.seq)
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
