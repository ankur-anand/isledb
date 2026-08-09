package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func testWriterOptions(memtableBytes int64, maxPendingMemtables int) WriterOptions {
	return WriterOptions{
		Memtable: WriterMemtableOptions{
			TargetBytes:         memtableBytes,
			MaxPendingMemtables: maxPendingMemtables,
		},
	}
}

func testSSTOutput(compression string, blockBytes int) SSTOutputOptions {
	encoding := SSTEncodingOptions{
		Compression:     compression,
		BlockBytes:      blockBytes,
		BloomBitsPerKey: 10,
	}
	return SSTOutputOptions{L0: encoding, Compacted: encoding}
}

func TestWriter_FlushCreatesManifestAndFiles(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-test")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	w, err := newWriter(ctx, store, manifestStore, testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := w.put(ctx, []byte("a"), []byte("value-1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := w.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	manifestKey := store.ManifestPath()
	if _, _, err := store.Read(ctx, manifestKey); err != nil {
		t.Fatalf("manifest CURRENT missing: %v", err)
	}

	logs, err := manifestStore.ListEntries(ctx)
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

func TestWriter_RejectsNilContext(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-nil-context")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	if _, err := newWriter(nil, store, manifestStore, WriterOptions{}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("newWriter(nil) error=%v, want %v", err, ErrNilContext)
	}

	w, err := newWriter(ctx, store, manifestStore, WriterOptions{})
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	if err := w.put(nil, []byte("a"), []byte("v")); !errors.Is(err, ErrNilContext) {
		t.Fatalf("put(nil) error=%v, want %v", err, ErrNilContext)
	}
	if err := w.delete(nil, []byte("a")); !errors.Is(err, ErrNilContext) {
		t.Fatalf("delete(nil) error=%v, want %v", err, ErrNilContext)
	}
	if err := w.flush(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("flush(nil) error=%v, want %v", err, ErrNilContext)
	}
	if err := w.close(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("close(nil) error=%v, want %v", err, ErrNilContext)
	}
	if err := w.put(ctx, []byte("a"), []byte("v")); err != nil {
		t.Fatalf("writer should remain usable after nil-context errors: %v", err)
	}
}

func TestWriter_FlushPublishesChangeBatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-change-feed")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := testWriterOptions(1<<20, 0)
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	w.changeFeedPayload = ChangeFeedFullValues
	defer w.close(ctx)

	if err := w.put(ctx, []byte("b"), []byte("vb")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if err := w.delete(ctx, []byte("a")); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	if err := w.put(ctx, []byte("c"), []byte("vc")); err != nil {
		t.Fatalf("put c: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	entrySeqs, err := manifestStore.ListEntries(ctx)
	if err != nil {
		t.Fatalf("manifest entry list: %v", err)
	}
	var meta *manifest.ChangeBatchMeta
	for _, entrySeq := range entrySeqs {
		entry, err := manifestStore.ReadEntry(ctx, entrySeq)
		if err != nil {
			t.Fatalf("read manifest entry seq=%d: %v", entrySeq, err)
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
	if meta.Compression != changeBatchCompressionZstd {
		t.Fatalf("change batch compression=%q want=%q", meta.Compression, changeBatchCompressionZstd)
	}
	if meta.Payload != manifest.ChangeFeedPayloadFullValues {
		t.Fatalf("change batch payload=%q want=%q", meta.Payload, manifest.ChangeFeedPayloadFullValues)
	}
	if meta.Version != changeBatchVersion || meta.BlockCount == 0 || meta.RawSize <= 0 || meta.IndexChecksum == "" {
		t.Fatalf("incomplete indexed change batch metadata: %+v", *meta)
	}

	data, attrs, err := store.Read(ctx, meta.Path)
	if err != nil {
		t.Fatalf("read change batch: %v", err)
	}
	if attrs.Size != meta.Size {
		t.Fatalf("change batch size attr=%d meta=%d", attrs.Size, meta.Size)
	}
	batch, err := decodeChangeBatch(data)
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
	if batch.Changes[1].Seq != 2 || batch.Changes[1].Kind != changeDelete || string(batch.Changes[1].Key) != "a" {
		t.Fatalf("change[1] mismatch: %+v", batch.Changes[1])
	}
	if batch.Changes[2].Seq != 3 || string(batch.Changes[2].Key) != "c" || string(batch.Changes[2].Value) != "vc" {
		t.Fatalf("change[2] mismatch: %+v", batch.Changes[2])
	}
}

func TestWriter_ChangeFeedDisabledByDefault(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-change-feed-disabled")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	w, err := newWriter(ctx, store, manifestStore, testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	if err := w.put(ctx, []byte("a"), []byte("va")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	entrySeqs, err := manifestStore.ListEntries(ctx)
	if err != nil {
		t.Fatalf("manifest entry list: %v", err)
	}
	for _, entrySeq := range entrySeqs {
		entry, err := manifestStore.ReadEntry(ctx, entrySeq)
		if err != nil {
			t.Fatalf("read manifest entry seq=%d: %v", entrySeq, err)
		}
		if entry.Op == manifest.LogOpAddSSTable && entry.ChangeBatch != nil {
			t.Fatalf("change batch metadata present while change feed is disabled: %+v", entry.ChangeBatch)
		}
	}

	result, err := store.List(ctx, blobstore.ListOptions{Prefix: "changes/"})
	if err != nil {
		t.Fatalf("list changes: %v", err)
	}
	if len(result.Objects) != 0 {
		t.Fatalf("expected no change batch objects when disabled, got %d", len(result.Objects))
	}
}

func TestWriter_ChangeFeedBufferTriggersRotationForLargeValues(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-change-feed-rotation")
	defer store.Close()

	opts := testWriterOptions(1<<10, 0)
	w, err := newWriter(ctx, store, newManifestStore(store, nil), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	w.changeFeedPayload = ChangeFeedFullValues
	defer w.close(ctx)

	if err := w.put(ctx, []byte("large"), make([]byte, 2<<10)); err != nil {
		t.Fatalf("put large value: %v", err)
	}
	if err := w.put(ctx, []byte("next"), []byte("v")); err != nil {
		t.Fatalf("put next value: %v", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.immQueue) != 1 {
		t.Fatalf("immutable queue length=%d want=1", len(w.immQueue))
	}
	if w.immQueue[0].changes == nil || w.immQueue[0].changes.bodySize < opts.Memtable.TargetBytes {
		t.Fatalf("rotated change buffer=%+v", w.immQueue[0].changes)
	}
}

func TestWriter_ReplaySeedsEpoch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-replay")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	w, err := newWriter(ctx, store, manifestStore, testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	if err := w.put(ctx, []byte("a"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := w.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	w2, err := newWriter(ctx, store, manifestStore, testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter(2): %v", err)
	}
	defer w2.close(ctx)

	if err := w2.put(ctx, []byte("b"), []byte("v2")); err != nil {
		t.Fatalf("Put2: %v", err)
	}
	if err := w2.flush(ctx); err != nil {
		t.Fatalf("Flush2: %v", err)
	}

	logs, err := manifestStore.ListEntries(ctx)
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
	failErr     error
}

type applyThenFailOnceStorage struct {
	manifest.Storage
	writeCount  atomic.Int32
	failOnWrite int32
	failErr     error
}

func (s *applyThenFailOnceStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	etag, err := s.Storage.WriteCurrentCAS(ctx, data, expectedETag)
	if err != nil {
		return "", err
	}
	if s.writeCount.Add(1) == s.failOnWrite {
		return "", s.failErr
	}
	return etag, nil
}

func (s *failOnceStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	count := s.writeCount.Add(1)
	if count == s.failOnWrite {
		if s.failErr != nil {
			return "", s.failErr
		}
		return "", errors.New("inject current write failure")
	}
	return s.Storage.WriteCurrentCAS(ctx, data, expectedETag)
}

type blockingCurrentStorage struct {
	manifest.Storage
	block   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (s *blockingCurrentStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	if s.block.CompareAndSwap(true, false) {
		close(s.started)
		<-s.release
	}
	return s.Storage.WriteCurrentCAS(ctx, data, expectedETag)
}

func TestWriter_CloseTimeoutCanBeRetried(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-close-retry")
	defer store.Close()

	storage := &blockingCurrentStorage{
		Storage: manifest.NewBlobStoreBackend(store),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manifestStore := manifest.NewStoreWithStorage(storage)

	opts := testWriterOptions(1<<20, 0)
	opts.Flush.Interval = 10 * time.Millisecond
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := w.put(ctx, []byte("a"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	storage.block.Store(true)

	select {
	case <-storage.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background flush did not reach blocking CURRENT write")
	}
	w.mu.Lock()
	pending := w.pendingMemtables
	w.mu.Unlock()
	if pending != 1 {
		t.Fatalf("pending memtables during background flush=%d, want=1", pending)
	}

	for attempt := 0; attempt < 20; attempt++ {
		closeCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		err = w.close(closeCtx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close attempt %d error=%v, want %v", attempt, err, context.DeadlineExceeded)
		}
	}

	close(storage.release)

	retryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := w.close(retryCtx); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	w.mu.Lock()
	pending = w.pendingMemtables
	w.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending memtables after close=%d, want=0", pending)
	}
}

func TestWriter_Backpressure(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-backpressure")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	w, err := newWriter(ctx, store, manifestStore, testWriterOptions(512, 1))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	val := bytes.Repeat([]byte("v"), 128)
	var lastErr error
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		lastErr = w.put(ctx, key, val)
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
	pending := w.pendingMemtables
	w.mu.Unlock()
	if queueLen != 1 {
		t.Fatalf("expected immQueue length 1, got %d", queueLen)
	}
	if pending != 1 {
		t.Fatalf("pending memtables=%d, want=1", pending)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	w.mu.Lock()
	pending = w.pendingMemtables
	w.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending memtables after flush=%d, want=0", pending)
	}
	if err := w.put(ctx, []byte("post"), []byte("v")); err != nil {
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

	w, err := newWriter(ctx, store, manifestStore, testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	if err := w.put(ctx, []byte("a"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	firstErr := w.flush(ctx)
	if firstErr == nil {
		t.Fatalf("expected flush error")
	}
	if errors.Is(firstErr, ErrWriterFailed) {
		t.Fatalf("explicit flush failure must remain retryable: %v", firstErr)
	}
	if err := w.backgroundError(); err != nil {
		t.Fatalf("explicit flush stored terminal error: %v", err)
	}

	w.mu.Lock()
	queueLen := len(w.immQueue)
	pending := w.pendingMemtables
	w.mu.Unlock()
	if queueLen == 0 {
		t.Fatalf("expected immQueue to be requeued after failure")
	}
	if pending != 1 {
		t.Fatalf("pending memtables after failed flush=%d, want=1", pending)
	}
	sstsAfterFailure, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list SSTs after failure: %v", err)
	}
	if len(sstsAfterFailure) != 1 {
		t.Fatalf("SSTs after failed manifest publish=%d, want=1", len(sstsAfterFailure))
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush retry: %v", err)
	}
	w.mu.Lock()
	pending = w.pendingMemtables
	w.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending memtables after retry=%d, want=0", pending)
	}
	sstsAfterRetry, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list SSTs after retry: %v", err)
	}
	if len(sstsAfterRetry) != 1 {
		t.Fatalf("SSTs after manifest retry=%d, want=1", len(sstsAfterRetry))
	}
}

func TestWriter_ChangeBatchUploadRetryReusesObjectIdentity(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-change-batch-upload-retry")
	defer store.Close()

	w, err := newWriter(ctx, store, newManifestStore(store, nil), testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	w.changeFeedPayload = ChangeFeedFullValues
	defer w.close(ctx)

	if err := w.put(ctx, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	w.mu.Lock()
	pending := w.newPendingFlushLocked(w.memtable)
	w.memtable = w.newMemtable()
	w.mu.Unlock()
	if pending.changeBatchCreatedAt.IsZero() {
		t.Fatal("pending change batch has no stable creation time")
	}

	lostResponse := errors.New("lost change-batch upload response")
	objects := make(map[string][]byte)
	var uploadedIDs []string
	uploads := 0
	uploadFn := func(_ context.Context, id string, reader io.Reader) error {
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		uploadedIDs = append(uploadedIDs, id)
		objects[id] = data
		uploads++
		if uploads == 1 {
			return lostResponse
		}
		return nil
	}

	if _, err := writePendingChangeBatch(ctx, pending, uploadFn); !errors.Is(err, lostResponse) {
		t.Fatalf("first upload error=%v want=%v", err, lostResponse)
	}
	result, err := writePendingChangeBatch(ctx, pending, uploadFn)
	if err != nil {
		t.Fatalf("retry upload: %v", err)
	}
	if len(uploadedIDs) != 2 || uploadedIDs[0] != uploadedIDs[1] {
		t.Fatalf("uploaded IDs=%v want the same identity on retry", uploadedIDs)
	}
	if result.Meta.ID != uploadedIDs[0] {
		t.Fatalf("result ID=%q want=%q", result.Meta.ID, uploadedIDs[0])
	}
	if len(objects) != 1 {
		t.Fatalf("uploaded objects=%d want=1", len(objects))
	}
}

func TestWriter_FlushReconcilesAppliedManifestCASAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-ambiguous-manifest-cas")
	defer store.Close()

	lostResponse := errors.New("lost CURRENT response")
	storage := &applyThenFailOnceStorage{
		Storage:     manifest.NewBlobStoreBackend(store),
		failOnWrite: 3,
		failErr:     lostResponse,
	}
	manifestStore := manifest.NewStoreWithStorage(storage)
	opts := testWriterOptions(1<<20, 0)
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	w.changeFeedPayload = ChangeFeedFullValues
	defer w.close(ctx)

	if err := w.put(ctx, []byte("a"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); !errors.Is(err, lostResponse) {
		t.Fatalf("first flush error=%v, want %v", err, lostResponse)
	}

	w.mu.Lock()
	if len(w.immQueue) != 1 {
		w.mu.Unlock()
		t.Fatalf("pending queue length=%d, want=1", len(w.immQueue))
	}
	commitID := w.immQueue[0].commitID
	sstableID := w.immQueue[0].sstable.ID
	w.mu.Unlock()
	if commitID == "" || sstableID == "" {
		t.Fatalf("pending identity commit_id=%q sstable_id=%q", commitID, sstableID)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush retry: %v", err)
	}
	ssts, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("ListSSTFiles: %v", err)
	}
	if len(ssts) != 1 {
		t.Fatalf("SST object count=%d, want=1", len(ssts))
	}
	changes, err := store.List(ctx, blobstore.ListOptions{Prefix: "changes/"})
	if err != nil {
		t.Fatalf("list change batches: %v", err)
	}
	if len(changes.Objects) != 1 {
		t.Fatalf("change batch object count=%d, want=1", len(changes.Objects))
	}

	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.LastWriterCommit == nil || current.LastWriterCommit.CommitID != commitID ||
		current.LastWriterCommit.Fingerprint == "" {
		t.Fatalf("last writer commit=%+v", current.LastWriterCommit)
	}

	seqs, err := manifestStore.ListEntries(ctx)
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	commits := 0
	for _, seq := range seqs {
		entry, err := manifestStore.ReadEntry(ctx, seq)
		if err != nil {
			t.Fatalf("ReadEntry(%d): %v", seq, err)
		}
		if entry.CommitID == commitID {
			commits++
		}
	}
	if commits != 1 {
		t.Fatalf("manifest entries with commit_id=%q: got=%d want=1", commitID, commits)
	}
}

func TestWriter_BackgroundFlushFailureIsTerminal(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-background-failure")
	defer store.Close()

	rootCause := errors.New("injected background publish failure")
	storage := &failOnceStorage{
		Storage:     manifest.NewBlobStoreBackend(store),
		failOnWrite: 3,
		failErr:     rootCause,
	}
	callback := make(chan error, 2)
	opts := testWriterOptions(1<<20, 0)
	opts.Flush.Interval = time.Millisecond
	opts.OnFlushError = func(err error) {
		callback <- err
	}

	w, err := newWriter(ctx, store, manifest.NewStoreWithStorage(storage), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	if err := w.put(ctx, []byte("a"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	var terminalErr error
	select {
	case terminalErr = <-callback:
	case <-time.After(2 * time.Second):
		t.Fatal("background flush failure was not reported")
	}
	if !errors.Is(terminalErr, ErrWriterFailed) {
		t.Fatalf("background error=%v, want %v", terminalErr, ErrWriterFailed)
	}
	if !errors.Is(terminalErr, rootCause) {
		t.Fatalf("background error=%v, want root cause %v", terminalErr, rootCause)
	}
	select {
	case <-w.workerDone:
	default:
		t.Fatal("flush worker still running when OnFlushError was delivered")
	}
	// Drain any tick that raced with Stop, then verify no new ticks arrive.
	select {
	case <-w.flushTicker.C:
	default:
	}
	select {
	case <-w.flushTicker.C:
		t.Fatal("flush ticker remained active after terminal background failure")
	case <-time.After(10 * time.Millisecond):
	}

	w.mu.Lock()
	seqBefore := w.seq
	pending := w.pendingMemtables
	w.mu.Unlock()
	if pending != 1 {
		t.Fatalf("pending memtables after background failure=%d, want=1", pending)
	}

	operations := []struct {
		name string
		call func() error
	}{
		{name: "put", call: func() error { return w.put(ctx, []byte("b"), []byte("v")) }},
		{name: "delete", call: func() error { return w.delete(ctx, []byte("a")) }},
		{name: "flush", call: func() error { return w.flush(ctx) }},
	}
	for _, operation := range operations {
		err := operation.call()
		if err != terminalErr {
			t.Fatalf("%s error=%v, want stored error %v", operation.name, err, terminalErr)
		}
	}
	w.mu.Lock()
	seqAfter := w.seq
	w.mu.Unlock()
	if seqAfter != seqBefore {
		t.Fatalf("terminal operations advanced seq: before=%d after=%d", seqBefore, seqAfter)
	}

	if err := w.close(ctx); err != terminalErr {
		t.Fatalf("close error=%v, want stored error %v", err, terminalErr)
	}
	select {
	case err := <-callback:
		t.Fatalf("OnFlushError called more than once: %v", err)
	default:
	}
}

func TestWriter_OnFlushErrorCanClose(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-callback-close")
	defer store.Close()

	rootCause := errors.New("injected background publish failure")
	storage := &failOnceStorage{
		Storage:     manifest.NewBlobStoreBackend(store),
		failOnWrite: 3,
		failErr:     rootCause,
	}

	type callbackResult struct {
		flushErr error
		closeErr error
	}
	result := make(chan callbackResult, 1)
	var writerRef atomic.Pointer[writer]
	opts := testWriterOptions(1<<20, 0)
	opts.Flush.Interval = time.Millisecond
	opts.OnFlushError = func(flushErr error) {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		result <- callbackResult{
			flushErr: flushErr,
			closeErr: writerRef.Load().close(closeCtx),
		}
	}

	w, err := newWriter(ctx, store, manifest.NewStoreWithStorage(storage), opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	writerRef.Store(w)
	if err := w.put(ctx, []byte("a"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	select {
	case got := <-result:
		if !errors.Is(got.flushErr, ErrWriterFailed) || !errors.Is(got.flushErr, rootCause) {
			t.Fatalf("callback flush error=%v", got.flushErr)
		}
		if got.closeErr != got.flushErr {
			t.Fatalf("Close error=%v, want callback error %v", got.closeErr, got.flushErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnFlushError calling Close deadlocked")
	}
}

func TestWriter_InFlightMemtableCountsTowardBackpressure(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-inflight-backpressure")
	defer store.Close()

	storage := &blockingCurrentStorage{
		Storage: manifest.NewBlobStoreBackend(store),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manifestStore := manifest.NewStoreWithStorage(storage)
	opts := testWriterOptions(512, 1)
	opts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	if err := w.put(ctx, []byte("initial"), []byte("value")); err != nil {
		t.Fatalf("put initial: %v", err)
	}
	storage.block.Store(true)
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- w.flush(ctx)
	}()

	select {
	case <-storage.started:
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not reach blocking CURRENT write")
	}
	released := false
	defer func() {
		if !released {
			close(storage.release)
		}
	}()

	w.mu.Lock()
	if got := w.pendingMemtables; got != 1 {
		w.mu.Unlock()
		t.Fatalf("pending memtables during upload=%d, want=1", got)
	}
	if got := len(w.immQueue); got != 0 {
		w.mu.Unlock()
		t.Fatalf("queued memtables during upload=%d, want=0", got)
	}
	w.mu.Unlock()

	value := bytes.Repeat([]byte("v"), 128)
	var backpressureSeq uint64
	for i := 0; i < 10_000; i++ {
		w.mu.Lock()
		seqBefore := w.seq
		w.mu.Unlock()
		err := w.put(ctx, []byte(fmt.Sprintf("queued-%06d", i)), value)
		if errors.Is(err, ErrBackpressure) {
			w.mu.Lock()
			backpressureSeq = w.seq
			w.mu.Unlock()
			if backpressureSeq != seqBefore {
				t.Fatalf("backpressure advanced seq: before=%d after=%d", seqBefore, backpressureSeq)
			}
			break
		}
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if backpressureSeq == 0 {
		t.Fatal("expected backpressure while the only pending slot was in flight")
	}

	close(storage.release)
	released = true
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not finish after release")
	}
	w.mu.Lock()
	pending := w.pendingMemtables
	w.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending memtables after upload=%d, want=0", pending)
	}
	if err := w.put(ctx, []byte("after-flush"), []byte("value")); err != nil {
		t.Fatalf("put after capacity release: %v", err)
	}
}

func TestWriterOptions_Defaults(t *testing.T) {
	if got, want := DefaultWriterOptions().Memtable.MaxPendingMemtables, 4; got != want {
		t.Fatalf("default max pending memtables=%d, want=%d", got, want)
	}
	if got, want := DefaultWriterOptions().Values.MaxKeyBytes, 64<<10; got != want {
		t.Fatalf("default max key bytes=%d, want=%d", got, want)
	}
	if got, want := DefaultWriterOptions().Values.MaxValueBytes, int64(16<<20); got != want {
		t.Fatalf("default max value bytes=%d, want=%d", got, want)
	}

	normalized, err := normalizeWriterOptions(WriterOptions{})
	if err != nil {
		t.Fatalf("normalizeWriterOptions: %v", err)
	}
	defaults := DefaultWriterOptions()
	if normalized.Memtable != defaults.Memtable {
		t.Fatalf("normalized options=%+v defaults=%+v", normalized, defaults)
	}
	if normalized.Maintenance != defaults.Maintenance {
		t.Fatalf("normalized maintenance options=%+v defaults=%+v", normalized.Maintenance, defaults.Maintenance)
	}
	if normalized.Values != defaults.Values {
		t.Fatalf("normalized value options=%+v defaults=%+v", normalized.Values, defaults.Values)
	}
	if normalized.Flush.Interval != 0 {
		t.Fatalf("zero-value flush interval=%s, want disabled", normalized.Flush.Interval)
	}
	if normalized.Values.MaxKeyBytes <= 0 || normalized.Values.MaxValueBytes <= 0 {
		t.Fatalf("normalized value options=%+v", normalized.Values)
	}
}

func TestWriterOptions_MapValueOptions(t *testing.T) {
	opts := WriterOptions{
		Values: ValueOptions{
			MaxKeyBytes:   1024,
			MaxValueBytes: 4096,
		},
	}

	normalized, err := normalizeWriterOptions(opts)
	if err != nil {
		t.Fatalf("normalizeWriterOptions: %v", err)
	}
	if normalized.Values != opts.Values {
		t.Fatalf("normalized values=%+v, want=%+v", normalized.Values, opts.Values)
	}
}

func TestWriterOptions_RejectInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*WriterOptions)
	}{
		{name: "owner id too long", mutate: func(o *WriterOptions) { o.OwnerID = strings.Repeat("x", maxWriterOwnerIDBytes+1) }},
		{name: "negative target bytes", mutate: func(o *WriterOptions) { o.Memtable.TargetBytes = -1 }},
		{name: "negative pending memtables", mutate: func(o *WriterOptions) { o.Memtable.MaxPendingMemtables = -1 }},
		{name: "negative flush interval", mutate: func(o *WriterOptions) { o.Flush.Interval = -time.Nanosecond }},
		{name: "negative maintenance poll interval", mutate: func(o *WriterOptions) { o.Maintenance.PollInterval = -time.Nanosecond }},
		{name: "negative max key bytes", mutate: func(o *WriterOptions) { o.Values.MaxKeyBytes = -1 }},
		{name: "negative max value bytes", mutate: func(o *WriterOptions) { o.Values.MaxValueBytes = -1 }},
		{name: "target exceeds arena", mutate: func(o *WriterOptions) { o.Memtable.TargetBytes = maxMemtableArenaBytes + 1 }},
		{name: "inline entry exceeds arena", mutate: func(o *WriterOptions) {
			o.Values.MaxKeyBytes = int(maxMemtableArenaBytes / 2)
			o.Values.MaxValueBytes = maxMemtableArenaBytes
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := DefaultWriterOptions()
			test.mutate(&opts)
			_, err := normalizeWriterOptions(opts)
			if !errors.Is(err, ErrInvalidWriterOptions) {
				t.Fatalf("normalizeWriterOptions error=%v, want %v", err, ErrInvalidWriterOptions)
			}
		})
	}
}

type maintenanceReadCountingStorage struct {
	*manifest.BlobStoreBackend
	reads atomic.Int64
}

func (s *maintenanceReadCountingStorage) ReadMaintenanceHead(ctx context.Context) ([]byte, string, error) {
	s.reads.Add(1)
	return s.BlobStoreBackend.ReadMaintenanceHead(ctx)
}

func TestWriterMaintenancePollIntervalBoundsMailboxReads(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-maintenance-poll-bounds")
	defer store.Close()

	storage := &maintenanceReadCountingStorage{BlobStoreBackend: manifest.NewBlobStoreBackend(store)}
	manifestStore := newManifestStore(store, storage)
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	opts.Maintenance.PollInterval = time.Hour
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	for i := 0; i < 100; i++ {
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush(%d): %v", i, err)
		}
	}
	if got := storage.reads.Load(); got != 1 {
		t.Fatalf("maintenance HEAD reads=%d, want 1 within one poll interval", got)
	}
}

func TestWriterMaintenanceWakeBypassesPollInterval(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-maintenance-wake")
	defer store.Close()

	storage := &maintenanceReadCountingStorage{BlobStoreBackend: manifest.NewBlobStoreBackend(store)}
	db, err := openDB(ctx, store, dbOpenOptions{manifestStorage: storage})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writerOpts.Maintenance.PollInterval = time.Hour
	w, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("initial Flush: %v", err)
	}

	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	if err := maintenance.stageCommand(ctx, manifest.MaintenanceCommand{
		ID:              "same-process-wake",
		Kind:            manifest.MaintenanceCommandRetirementFloor,
		RetirementFloor: &manifest.AdvanceFloorCommand{Floor: 1},
	}); err != nil {
		t.Fatalf("stageCommand: %v", err)
	}

	readsBeforeFlush := storage.reads.Load()
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush after maintenance wake: %v", err)
	}
	if got := storage.reads.Load(); got != readsBeforeFlush+1 {
		t.Fatalf("maintenance HEAD reads=%d, want %d after wake", got, readsBeforeFlush+1)
	}

	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.MaintenanceReceipt == nil || current.MaintenanceReceipt.CommandID != "same-process-wake" {
		t.Fatalf("maintenance receipt=%+v, want same-process-wake", current.MaintenanceReceipt)
	}
}

func TestSSTOutputOptionsNormalizeCompression(t *testing.T) {
	opts := DefaultSSTOutputOptions()
	opts.L0.Compression = " ZSTD "
	normalized, err := normalizeSSTOutputOptions(opts)
	if err != nil {
		t.Fatalf("normalizeSSTOutputOptions: %v", err)
	}
	if normalized.L0.Compression != "zstd" {
		t.Fatalf("compression=%q, want zstd", normalized.L0.Compression)
	}
}

func TestWriter_DeleteBackpressureDoesNotAdvanceSeq(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-delete-backpressure")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	w, err := newWriter(ctx, store, manifestStore, testWriterOptions(512, 1))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	for i := 0; i < 10000; i++ {
		seqBefore := w.seq
		err := w.delete(ctx, []byte(fmt.Sprintf("k%06d", i)))
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

func TestWriter_CanceledPutDoesNotAdvanceSequence(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-putblob-seq-rollback")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)

	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	seqBefore := w.seq
	putCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = w.put(putCtx, []byte("k"), []byte("v"))
	if err == nil {
		t.Fatalf("expected put error with canceled context")
	}
	if w.seq != seqBefore {
		t.Fatalf("put error should not advance sequence: before=%d after=%d", seqBefore, w.seq)
	}
}

func TestWriter_OpenContextCancellationDoesNotBlockWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := blobstore.NewMemory("writer-open-context-cancel")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(context.Background())

	cancel()

	opCtx := context.Background()
	if err := w.put(opCtx, []byte("k-inline"), []byte("v")); err != nil {
		t.Fatalf("put inline after opening ctx cancel: %v", err)
	}
	if err := w.delete(opCtx, []byte("k-inline")); err != nil {
		t.Fatalf("delete after opening ctx cancel: %v", err)
	}
	if err := w.put(opCtx, []byte("k-large"), bytes.Repeat([]byte("b"), 256<<10)); err != nil {
		t.Fatalf("put large value after opening ctx cancel: %v", err)
	}
}

func TestWriter_PartialMetricsDoNotPanic(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-partial-metrics")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	opts.Memtable.TargetBytes = 512
	opts.Memtable.MaxPendingMemtables = 1
	opts.Metrics = &WriterMetrics{}

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	val := bytes.Repeat([]byte("v"), 128)
	lastErr := error(nil)
	for i := 0; i < 10000; i++ {
		lastErr = w.put(ctx, []byte(fmt.Sprintf("k%06d", i)), val)
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

	if err := w.delete(ctx, []byte("k-final")); err != nil && !errors.Is(err, ErrBackpressure) {
		t.Fatalf("delete: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestWriter_MetricsFlushAndTTLPaths(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-metrics-coverage")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	opts.Metrics = DefaultWriterMetrics(nil)

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	value := []byte("value")
	if err := w.putWithTTL(ctx, []byte("k1"), value, time.Second); err != nil {
		t.Fatalf("putWithTTL success: %v", err)
	}
	if err := w.putWithTTL(ctx, nil, []byte("bad"), time.Second); err == nil {
		t.Fatalf("expected putWithTTL error for empty key")
	}
	if err := w.delete(ctx, []byte("k1")); err != nil {
		t.Fatalf("delete: %v", err)
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

func TestWriterFlushAppliesPendingMaintenanceWithoutUserData(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-maintenance-poll")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	token, err := manifestStore.ClaimMaintenance(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}
	staged, err := manifestStore.StageMaintenance(ctx, manifest.MaintenanceCommand{
		ID:              "retirement-floor-1",
		Kind:            manifest.MaintenanceCommandRetirementFloor,
		RetirementFloor: &manifest.AdvanceFloorCommand{Floor: 1},
	}, token)
	if err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.RetirementLogStart != 1 {
		t.Fatalf("retirement_log_start=%d, want 1", current.RetirementLogStart)
	}
	if current.MaintenanceReceipt == nil ||
		current.MaintenanceReceipt.CommandID != staged.Pending.ID ||
		current.MaintenanceReceipt.Status != manifest.MaintenanceStatusApplied {
		t.Fatalf("maintenance_receipt=%+v, pending=%+v", current.MaintenanceReceipt, staged.Pending)
	}
}

func TestWriterRejectsInvalidMaintenanceWithoutBecomingTerminal(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-maintenance-rejection")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	token, err := manifestStore.ClaimMaintenance(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}
	if _, err := manifestStore.StageMaintenance(ctx, manifest.MaintenanceCommand{
		ID:   "invalid-compaction",
		Kind: manifest.MaintenanceCommandCompaction,
		Compaction: &manifest.CompactionCommand{Payload: manifest.CompactionLogPayload{
			SourceLevel:      0,
			DestinationLevel: 1,
		}},
	}, token); err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush rejected command: %v", err)
	}
	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.MaintenanceReceipt == nil || current.MaintenanceReceipt.Status != manifest.MaintenanceStatusRejected {
		t.Fatalf("maintenance_receipt=%+v, want rejected", current.MaintenanceReceipt)
	}
	if err := w.put(ctx, []byte("still-writable"), []byte("value")); err != nil {
		t.Fatalf("put after rejected maintenance: %v", err)
	}
}

func TestWriterBackgroundFlushPollsMaintenanceWhenIdle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := blobstore.NewMemory("writer-maintenance-background-poll")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 5 * time.Millisecond
	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(context.Background())

	token, err := manifestStore.ClaimMaintenance(ctx, "maintenance-1")
	if err != nil {
		t.Fatalf("ClaimMaintenance: %v", err)
	}
	staged, err := manifestStore.StageMaintenance(ctx, manifest.MaintenanceCommand{
		ID:              "idle-floor",
		Kind:            manifest.MaintenanceCommandRetirementFloor,
		RetirementFloor: &manifest.AdvanceFloorCommand{Floor: 1},
	}, token)
	if err != nil {
		t.Fatalf("StageMaintenance: %v", err)
	}

	for {
		current, err := manifestStore.ReadCurrentData(ctx)
		if err != nil {
			t.Fatalf("ReadCurrentData: %v", err)
		}
		if current.MaintenanceReceipt.Matches(staged.Pending) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("maintenance command was not applied: %v", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
