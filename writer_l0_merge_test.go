package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func testL0MergeWriterOptions(targetBytes int64, maxAge time.Duration) WriterOptions {
	opts := testWriterOptions(1<<20, 0)
	opts.L0Merge = WriterL0MergeOptions{TargetBytes: targetBytes, MaxAge: maxAge}
	return opts
}

// expectedEntry is what one key should hold after a merge: its newest
// version, a tombstone when deleted or expired.
type expectedEntry struct {
	kind  internal.OpKind
	value []byte
}

// publishedMerge is one settled merge as the committer reported it.
type publishedMerge struct {
	merged mergedL0Run
	err    error
}

// readMergedSST reads a merged L0 file back from the store and returns its
// entries in key order.
func readMergedSST(t *testing.T, store *blobstore.Store, meta sstMetadata) []internal.CompactionEntry {
	t.Helper()
	data, _, err := store.Read(context.Background(), store.SSTPath(meta.ID))
	if err != nil {
		t.Fatalf("read merged sst %s: %v", meta.ID, err)
	}
	if int64(len(data)) < meta.Size {
		t.Fatalf("merged sst %s: object %d bytes, manifest size %d", meta.ID, len(data), meta.Size)
	}
	requireBloomChecksum(t, meta, data)
	reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(t, meta, data)), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("open merged sst: %v", err)
	}
	defer reader.Close()
	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		t.Fatalf("iterate merged sst: %v", err)
	}
	merged := newMergeIterator([]sstable.Iterator{iter})
	defer merged.close()
	var entries []internal.CompactionEntry
	for merged.Next() {
		entry, err := merged.entry()
		if err != nil {
			t.Fatalf("decode merged entry: %v", err)
		}
		entry.Key = append([]byte(nil), entry.Key...)
		entry.Value = append([]byte(nil), entry.Value...)
		entries = append(entries, entry)
	}
	if err := merged.Err(); err != nil {
		t.Fatalf("merged iterator: %v", err)
	}
	return entries
}

func compactionNamedSSTs(t *testing.T, store *blobstore.Store) []string {
	t.Helper()
	listed, err := store.List(context.Background(), blobstore.ListOptions{Prefix: "sstable/"})
	if err != nil {
		t.Fatalf("list sstable/: %v", err)
	}
	var ids []string
	for _, object := range listed.Objects {
		if id := path.Base(object.Key); isCompactionSSTID(id) {
			ids = append(ids, id)
		}
	}
	return ids
}

func l0IDsOf(t *testing.T, manifestStore *manifest.Store) []string {
	t.Helper()
	m, err := manifestStore.Replay(context.Background())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	ids := make([]string, len(m.L0SSTs))
	for i, sst := range m.L0SSTs {
		ids[i] = sst.ID
	}
	return ids
}

// waitForL0Merge runs passes until the committer settles a merge.
func waitForL0Merge(t *testing.T, w *writer, merges <-chan publishedMerge) publishedMerge {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if err := w.flush(context.Background()); err != nil {
			t.Fatalf("flush: %v", err)
		}
		select {
		case merged := <-merges:
			return merged
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("no L0 merge within deadline; held=%d", w.l0Merge.heldBytes())
	return publishedMerge{}
}

func TestWriterL0Merge_OnByDefault(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-default")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)

	w, err := newWriter(ctx, store, manifestStore, testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)
	if w.l0Merge == nil || w.l0Merge.target != 64<<20 || w.l0Merge.maxAge != 5*time.Minute {
		t.Fatalf("merger with default options: %+v", w.l0Merge)
	}
	// A flush is held for merging without any option set.
	if _, err := w.put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if held := w.l0Merge.heldBytes(); held <= 0 {
		t.Fatalf("nothing held after a flush with default options")
	}
}

func TestWriterL0Merge_MergesAndPublishesRunAtTargetBytes(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-size")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)

	// A target the first few flushes reach; an age they never do.
	w, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(8<<10, time.Hour))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	merges := make(chan publishedMerge, 8)
	w.onL0MergePublished = func(merged mergedL0Run, err error) { merges <- publishedMerge{merged, err} }

	// Versions of the same keys across flushes, a delete, and an entry that
	// expires before the run is merged. Each flush is one L0 file; writes
	// are recorded per flush so the expectation can be built for exactly the
	// files the merge took.
	var perFlush []map[string]expectedEntry
	var flushIDs []string // the L0 file each flush committed, in order
	value := func(flush, key int) []byte {
		return bytes.Repeat([]byte(fmt.Sprintf("f%d-k%d-", flush, key)), 16)
	}
	for flush := 0; flush < 64 && len(compactionNamedSSTs(t, store)) == 0; flush++ {
		writes := map[string]expectedEntry{}
		for key := 0; key < 20; key++ {
			k := fmt.Sprintf("key-%03d", key)
			switch {
			case flush == 1 && key == 3:
				if _, err := w.delete(ctx, []byte(k)); err != nil {
					t.Fatalf("delete: %v", err)
				}
				writes[k] = expectedEntry{kind: internal.OpDelete}
			case flush == 0 && key == 7:
				if _, err := w.putWithTTL(ctx, []byte(k), value(flush, key), time.Millisecond); err != nil {
					t.Fatalf("putWithTTL: %v", err)
				}
				writes[k] = expectedEntry{kind: internal.OpDelete}
			default:
				if _, err := w.put(ctx, []byte(k), value(flush, key)); err != nil {
					t.Fatalf("put: %v", err)
				}
				writes[k] = expectedEntry{kind: internal.OpPut, value: value(flush, key)}
			}
		}
		// Let the TTL entry expire before its file is created, so the cutoff
		// is past its expiry.
		time.Sleep(5 * time.Millisecond)
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		perFlush = append(perFlush, writes)
		// The pass publishes merges before it flushes, so the newest L0 file
		// is this flush's.
		flushIDs = append(flushIDs, l0IDsOf(t, manifestStore)[0])
	}
	published := waitForL0Merge(t, w, merges)
	if published.err != nil {
		t.Fatalf("merge refused: %v", published.err)
	}
	merged := published.merged

	// The inputs are the oldest flushes, in commit order, one per flush.
	if len(merged.inputs) < 2 {
		t.Fatalf("merge of %d inputs; want at least 2", len(merged.inputs))
	}
	if len(flushIDs) < len(merged.inputs) {
		t.Fatalf("%d flushes, merge has %d inputs", len(flushIDs), len(merged.inputs))
	}
	for i, input := range merged.inputs {
		if input.ID != flushIDs[i] {
			t.Fatalf("input %d = %s, want flush %s", i, input.ID, flushIDs[i])
		}
	}
	expected := map[string]expectedEntry{}
	for _, writes := range perFlush[:len(merged.inputs)] {
		for k, e := range writes {
			expected[k] = e
		}
	}

	out := merged.output
	if !isCompactionSSTID(out.ID) {
		t.Fatalf("output %s does not use the compaction grammar", out.ID)
	}
	if out.Level != 0 {
		t.Fatalf("output level %d, want 0", out.Level)
	}
	// Only the newest version of each key survives, so the output's range
	// is that of the surviving entries: within the inputs', ending at the
	// newest input's end, since the newest write always survives.
	lastInput := merged.inputs[len(merged.inputs)-1]
	if out.SeqLo < merged.inputs[0].SeqLo || out.SeqHi != lastInput.SeqHi {
		t.Fatalf("output seq %d-%d, inputs %d-%d", out.SeqLo, out.SeqHi,
			merged.inputs[0].SeqLo, lastInput.SeqHi)
	}
	oldest := merged.inputs[0].CreatedAt
	for _, input := range merged.inputs {
		if input.CreatedAt.Before(oldest) {
			oldest = input.CreatedAt
		}
	}
	if !out.OldestDataAt.Equal(oldest) {
		t.Fatalf("output oldest data %s, want oldest input %s", out.OldestDataAt, oldest)
	}
	if out.Epoch == 0 {
		t.Fatalf("output has no epoch")
	}
	if ids := compactionNamedSSTs(t, store); len(ids) != 1 || ids[0] != out.ID {
		t.Fatalf("compaction-named objects %v, want only %s", ids, out.ID)
	}

	// The manifest: the run is gone, the output sits where it was, at the
	// old end, the later flushes are ahead of it, and CURRENT remembers the
	// merge apart from the last flush.
	wantL0 := []string{}
	for i := len(flushIDs) - 1; i >= len(merged.inputs); i-- {
		wantL0 = append(wantL0, flushIDs[i])
	}
	wantL0 = append(wantL0, out.ID)
	if l0After := l0IDsOf(t, manifestStore); strings.Join(l0After, ",") != strings.Join(wantL0, ",") {
		t.Fatalf("L0 after merge = %v, want %v", l0After, wantL0)
	}
	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if current.LastWriterL0Merge == nil || current.LastWriterL0Merge.CommitID != merged.commitID {
		t.Fatalf("last writer l0 merge=%+v, want commit %s", current.LastWriterL0Merge, merged.commitID)
	}
	if current.LastWriterCommit == nil || current.LastWriterCommit.CommitID == merged.commitID {
		t.Fatalf("last writer commit=%+v, must stay the last flush", current.LastWriterCommit)
	}

	// The output holds the newest version of every key the run saw: puts,
	// the tombstone, and the expired entry as a tombstone.
	entries := readMergedSST(t, store, out)
	if len(entries) != len(expected) {
		t.Fatalf("output has %d keys, want %d", len(entries), len(expected))
	}
	for _, entry := range entries {
		want, ok := expected[string(entry.Key)]
		if !ok {
			t.Fatalf("unexpected key %q in output", entry.Key)
		}
		if entry.Kind != want.kind {
			t.Fatalf("key %q kind %v, want %v", entry.Key, entry.Kind, want.kind)
		}
		if want.kind == internal.OpPut && !bytes.Equal(entry.Value, want.value) {
			t.Fatalf("key %q value %q, want %q", entry.Key, entry.Value, want.value)
		}
		if want.kind == internal.OpDelete && entry.ExpireAt != 0 {
			t.Fatalf("key %q tombstone keeps expiry %d", entry.Key, entry.ExpireAt)
		}
	}
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			t.Fatalf("output keys out of order: %q then %q", entries[i-1].Key, entries[i].Key)
		}
	}

	// A reader sees the newest version of every key across all of L0,
	// merged and not.
	latest := map[string]expectedEntry{}
	for _, writes := range perFlush {
		for k, e := range writes {
			latest[k] = e
		}
	}
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()
	for k, want := range latest {
		got, found, err := reader.Get(ctx, []byte(k))
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if want.kind == internal.OpDelete {
			if found {
				t.Fatalf("Get(%s) found a deleted or expired key", k)
			}
			continue
		}
		if !found || !bytes.Equal(got, want.value) {
			t.Fatalf("Get(%s) = %q found=%v, want %q", k, got, found, want.value)
		}
	}

	if err := w.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestWriterL0Merge_MergesRunAtMaxAge(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-age")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)

	// A target one flush never reaches; an age it passes at once.
	w, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(64<<20, time.Millisecond))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	merges := make(chan publishedMerge, 8)
	w.onL0MergePublished = func(merged mergedL0Run, err error) { merges <- publishedMerge{merged, err} }

	if _, err := w.put(ctx, []byte("only"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if held := w.l0Merge.heldBytes(); held <= 0 {
		t.Fatalf("nothing held after a committed flush")
	}
	time.Sleep(5 * time.Millisecond)
	published := waitForL0Merge(t, w, merges)
	if published.err != nil {
		t.Fatalf("merge refused: %v", published.err)
	}
	merged := published.merged
	if len(merged.inputs) != 1 {
		t.Fatalf("merge of %d inputs, want the one held file", len(merged.inputs))
	}
	if !isCompactionSSTID(merged.output.ID) {
		t.Fatalf("output %s does not use the compaction grammar", merged.output.ID)
	}
	entries := readMergedSST(t, store, merged.output)
	if len(entries) != 1 || string(entries[0].Key) != "only" || string(entries[0].Value) != "value" {
		t.Fatalf("output entries %+v", entries)
	}
	if held := w.l0Merge.heldBytes(); held != 0 {
		t.Fatalf("held %d bytes after the merge", held)
	}
	if l0 := l0IDsOf(t, manifestStore); len(l0) != 1 || l0[0] != merged.output.ID {
		t.Fatalf("L0 = %v, want only %s", l0, merged.output.ID)
	}
	if err := w.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// failMergeCASOnceStorage lets a CURRENT write that records an L0 merge land,
// then reports it failed, once: the response was lost.
type failMergeCASOnceStorage struct {
	manifest.Storage
	failed  atomic.Bool
	failErr error
}

func (s *failMergeCASOnceStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	etag, err := s.Storage.WriteCurrentCAS(ctx, data, expectedETag)
	if err != nil {
		return "", err
	}
	if bytes.Contains(data, []byte(`"last_writer_l0_merge"`)) && s.failed.CompareAndSwap(false, true) {
		return "", s.failErr
	}
	return etag, nil
}

func TestWriterL0Merge_PublishReconcilesAppliedCASAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-lost-cas")
	defer store.Close()
	lost := errors.New("lost CURRENT response")
	storage := &failMergeCASOnceStorage{Storage: manifest.NewBlobStoreBackend(store), failErr: lost}
	manifestStore := manifest.NewStoreWithStorage(storage)

	w, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(64<<20, time.Millisecond))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	merges := make(chan publishedMerge, 8)
	w.onL0MergePublished = func(merged mergedL0Run, err error) { merges <- publishedMerge{merged, err} }

	if _, err := w.put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	// Passes until the merge is uploaded and its publish loses its response.
	// Every accepted write is already committed, so Flush reports nothing;
	// the pass itself failed and the merge stays to be retried.
	deadline := time.Now().Add(20 * time.Second)
	for !storage.failed.Load() {
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("publish never lost its response")
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case published := <-merges:
		t.Fatalf("merge settled before its retry: %+v", published)
	default:
	}
	if w.l0Merge.nextToPublish() == nil {
		t.Fatalf("merge forgotten after a lost response")
	}
	// A write after the uncertain publish is committed only once the merge is
	// settled: the retry reconciles it from CURRENT and publishes nothing.
	if _, err := w.put(ctx, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush after lost response: %v", err)
	}
	select {
	case published := <-merges:
		if published.err != nil {
			t.Fatalf("merge refused: %v", published.err)
		}
		merged := published.merged
		seqs, err := manifestStore.ListEntries(ctx)
		if err != nil {
			t.Fatalf("ListEntries: %v", err)
		}
		count := 0
		for _, seq := range seqs {
			entry, err := manifestStore.ReadEntry(ctx, seq)
			if err != nil {
				t.Fatalf("ReadEntry(%d): %v", seq, err)
			}
			if entry.CommitID == merged.commitID {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("entries for the merge: %d, want 1", count)
		}
		l0 := l0IDsOf(t, manifestStore)
		if len(l0) != 2 || l0[1] != merged.output.ID {
			t.Fatalf("L0 = %v, want the new flush then %s", l0, merged.output.ID)
		}
	default:
		t.Fatalf("merge not settled by the retry")
	}
	if w.l0Merge.nextToPublish() != nil {
		t.Fatalf("merge still waiting after it was reconciled")
	}
	if err := w.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestWriterL0Merge_UncertainFlushIsRetriedBeforeAWaitingMerge loses the
// reply to a flush commit while a merge finishes and waits to be published.
// The manifest refuses every other commit until the flush is settled, so the
// pass must retry the flush first; publishing the merge first would fail
// every pass forever.
func TestWriterL0Merge_UncertainFlushIsRetriedBeforeAWaitingMerge(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-uncertain-flush")
	defer store.Close()
	lost := errors.New("lost CURRENT response")
	// Writes to CURRENT: the fence claim, then one per flush. The second flush
	// lands and loses its reply.
	storage := &applyThenFailOnceStorage{Storage: manifest.NewBlobStoreBackend(store), failOnWrite: 3, failErr: lost}
	manifestStore := manifest.NewStoreWithStorage(storage)
	w, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(64<<20, time.Hour))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	merges := make(chan publishedMerge, 8)
	w.onL0MergePublished = func(merged mergedL0Run, err error) { merges <- publishedMerge{merged, err} }

	if _, err := w.put(ctx, []byte("a"), []byte("v-a")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush a: %v", err)
	}
	if _, err := w.put(ctx, []byte("b"), []byte("v-b")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); !errors.Is(err, lost) {
		t.Fatalf("flush b error=%v, want %v", err, lost)
	}
	if !w.uncertainFlushPending() {
		t.Fatalf("flush b not pending as uncertain")
	}

	// A merge of the held run finishes while flush b is unsettled.
	w.l0Merge.mu.Lock()
	w.l0Merge.closeFillingLocked()
	w.l0Merge.startLocked()
	w.l0Merge.mu.Unlock()
	deadline := time.Now().Add(20 * time.Second)
	for w.l0Merge.nextToPublish() == nil {
		if time.Now().After(deadline) {
			t.Fatalf("merge did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// One pass settles the flush, then publishes the merge.
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush after lost response: %v", err)
	}
	select {
	case published := <-merges:
		if published.err != nil {
			t.Fatalf("merge refused: %v", published.err)
		}
		if l0 := l0IDsOf(t, manifestStore); len(l0) != 2 || l0[1] != published.merged.output.ID {
			t.Fatalf("L0 = %v, want flush b then the merged file", l0)
		}
	default:
		t.Fatalf("merge not published after the flush was settled")
	}
	if w.uncertainFlushPending() || w.manifestLog.PendingWriterCommitID() != "" {
		t.Fatalf("a commit is still pending after the pass")
	}
	// Flush b landed exactly once.
	seqs, err := manifestStore.ListEntries(ctx)
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	flushes := 0
	for _, seq := range seqs {
		entry, err := manifestStore.ReadEntry(ctx, seq)
		if err != nil {
			t.Fatalf("ReadEntry(%d): %v", seq, err)
		}
		if entry.Op == manifest.LogOpAddSSTable && entry.SSTable != nil && entry.SSTable.MinKey != nil && string(entry.SSTable.MinKey) == "b" {
			flushes++
		}
	}
	if flushes != 1 {
		t.Fatalf("flush b committed %d times", flushes)
	}
	if err := w.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestWriterL0Merge_RefusedMergeIsDropped(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-refused")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	w, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(64<<20, time.Hour))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)
	merges := make(chan publishedMerge, 8)
	w.onL0MergePublished = func(merged mergedL0Run, err error) { merges <- publishedMerge{merged, err} }

	if _, err := w.put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	before, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}

	// A merge whose inputs are not in L0: as after a drain took them.
	inputs := []sstMetadata{{ID: "gone-1.sst", Size: 10}, {ID: "gone-2.sst", Size: 10}}
	output := sstMetadata{ID: "compacted-" + strings.Repeat("0", 64) + "-0001.sst", SeqLo: 1, SeqHi: 2, Size: 10, Epoch: 1}
	intent, err := w.l0Merge.writeL0MergeIntent(ctx, inputs, output.ID)
	if err != nil {
		t.Fatalf("writeL0MergeIntent: %v", err)
	}
	w.l0Merge.mu.Lock()
	w.l0Merge.finished = append(w.l0Merge.finished, mergedL0Run{commitID: "stale-merge", intent: intent, inputs: inputs, output: output})
	w.l0Merge.mu.Unlock()

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	select {
	case published := <-merges:
		if !errors.Is(published.err, manifest.ErrInvalidManifest) {
			t.Fatalf("settled with %v, want %v", published.err, manifest.ErrInvalidManifest)
		}
	default:
		t.Fatalf("refused merge not settled")
	}
	if w.l0Merge.nextToPublish() != nil {
		t.Fatalf("refused merge kept for retry")
	}
	after, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	if after.NextSeq != before.NextSeq || after.LastWriterL0Merge != nil {
		t.Fatalf("refused merge wrote to CURRENT: next %d -> %d, marker %+v", before.NextSeq, after.NextSeq, after.LastWriterL0Merge)
	}
}

func TestWriterL0Merge_OutputNameIsStableAcrossAttempts(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-identity")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	w, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(64<<20, time.Hour))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	inputs := []sstMetadata{
		{ID: "1-1-5-10.sst", Checksum: "sha256:aa", Size: 100, CreatedAt: time.Unix(100, 0)},
		{ID: "2-6-9-20.sst", Checksum: "sha256:bb", Size: 200, CreatedAt: time.Unix(200, 0)},
	}
	opts := sstWriterOptions{BloomBitsPerKey: 10, BlockSize: 4096, Compression: "zstd"}
	first, err := w.l0Merge.identity(inputs, 7, time.Unix(1000, 0), opts, 100_000)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	// Another attempt: a later time and epoch, the same run.
	second, err := w.l0Merge.identity(inputs, 8, time.Unix(2000, 0), opts, 100_000)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	if first.OutputKey != second.OutputKey {
		t.Fatalf("output key changed between attempts: %s vs %s", first.OutputKey, second.OutputKey)
	}
	// A different run names a different output.
	other, err := w.l0Merge.identity(inputs[:1], 7, time.Unix(1000, 0), opts, 100_000)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	if other.OutputKey == first.OutputKey {
		t.Fatalf("different inputs share an output key")
	}
	output, err := first.output(1)
	if err != nil {
		t.Fatalf("output: %v", err)
	}
	if !isCompactionSSTID(output.ID) || !strings.HasSuffix(output.ID, "-0001.sst") {
		t.Fatalf("output id %s", output.ID)
	}
}

// TestL0Merger_HeldLimit drives the merger's bookkeeping without a writer
// loop: a cancelled context keeps merges from starting, so closed runs stay
// where add left them.
func TestL0Merger_HeldLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &writer{ctx: ctx}
	m := newL0Merger(w, WriterL0MergeOptions{TargetBytes: 100, MaxAge: time.Hour})

	file := func(id string, size int) (sstMetadata, []byte) {
		return sstMetadata{ID: id, Size: int64(size), CreatedAt: time.Now()}, make([]byte, size)
	}
	add := func(id string, size int) { m.add(file(id, size)) }

	// Fills to the target: the run closes.
	add("a", 40)
	add("b", 40)
	add("c", 40)
	if len(m.closed) != 1 || len(m.closed[0].ssts) != 3 || !m.filling.empty() {
		t.Fatalf("after 120 bytes: closed=%d filling=%d", len(m.closed), len(m.filling.ssts))
	}
	// A second run fills under the limit of two targets.
	add("d", 40)
	add("e", 40)
	if m.held != 200 || len(m.closed) != 1 || len(m.filling.ssts) != 2 {
		t.Fatalf("after 200 bytes: held=%d closed=%d filling=%d", m.held, len(m.closed), len(m.filling.ssts))
	}
	// Over the limit: the flush is not kept and the filling run closes, so
	// what is held stays adjacent.
	add("f", 40)
	if m.held != 200 || len(m.closed) != 2 || !m.filling.empty() {
		t.Fatalf("over the limit: held=%d closed=%d filling=%d", m.held, len(m.closed), len(m.filling.ssts))
	}
	// A file shorter than its manifest size is never kept.
	meta, data := file("g", 10)
	m.add(meta, data[:5])
	if m.held != 200 || !m.filling.empty() {
		t.Fatalf("short object kept: held=%d filling=%d", m.held, len(m.filling.ssts))
	}
	// Nothing started under a cancelled context.
	if m.running || m.done != nil {
		t.Fatalf("merge started under a cancelled context")
	}
	// Age closes a filling run on a pass.
	m2 := newL0Merger(w, WriterL0MergeOptions{TargetBytes: 1 << 20, MaxAge: time.Millisecond})
	m2.add(file("h", 10))
	m2.pass(time.Now().Add(time.Second))
	if len(m2.closed) != 1 || !m2.filling.empty() {
		t.Fatalf("age did not close the run: closed=%d filling=%d", len(m2.closed), len(m2.filling.ssts))
	}
}

func TestL0Merger_DropTouchingYieldsToMaintenance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := blobstore.NewMemory("l0-merger-drop")
	defer store.Close()
	w := &writer{ctx: ctx, store: store}
	m := newL0Merger(w, WriterL0MergeOptions{TargetBytes: 100, MaxAge: time.Hour})
	file := func(id string, size int) (sstMetadata, []byte) {
		return sstMetadata{ID: id, Size: int64(size), CreatedAt: time.Now()}, make([]byte, size)
	}
	// One closed run a,b,c; one filling run d; one finished merge of x,y.
	for _, id := range []string{"a", "b", "c", "d"} {
		m.add(file(id, 40))
	}
	m.finished = append(m.finished, mergedL0Run{commitID: "m-xy", inputs: []sstMetadata{{ID: "x"}, {ID: "y"}}})
	if len(m.closed) != 1 || len(m.filling.ssts) != 1 || m.held != 160 {
		t.Fatalf("setup: closed=%d filling=%d held=%d", len(m.closed), len(m.filling.ssts), m.held)
	}

	if dropped := m.dropTouching([]string{"unrelated"}); dropped != 0 || m.held != 160 {
		t.Fatalf("unrelated ids dropped %d, held=%d", dropped, m.held)
	}
	// A command over the filling run's file.
	if dropped := m.dropTouching([]string{"d"}); dropped != 1 || !m.filling.empty() || m.held != 120 {
		t.Fatalf("filling: dropped=%d filling=%d held=%d", dropped, len(m.filling.ssts), m.held)
	}
	// A command over one file of the closed run drops the whole run.
	if dropped := m.dropTouching([]string{"b"}); dropped != 1 || len(m.closed) != 0 || m.held != 0 {
		t.Fatalf("closed: dropped=%d closed=%d held=%d", dropped, len(m.closed), m.held)
	}
	// A command over an unpublished merge's input forgets the merge.
	if dropped := m.dropTouching([]string{"y"}); dropped != 1 || m.nextToPublish() != nil {
		t.Fatalf("finished: dropped=%d next=%v", dropped, m.nextToPublish())
	}
}

func TestMaintenanceCommandSSTIDs(t *testing.T) {
	if ids := maintenanceCommandSSTIDs(nil); ids != nil {
		t.Fatalf("nil command: %v", ids)
	}
	compaction := &manifest.MaintenanceCommand{Compaction: &manifest.CompactionCommand{
		Payload: manifest.CompactionLogPayload{RemoveSSTableIDs: []string{"a", "b"}}}}
	if ids := maintenanceCommandSSTIDs(compaction); len(ids) != 2 {
		t.Fatalf("compaction: %v", ids)
	}
	remove := &manifest.MaintenanceCommand{RemoveSSTables: &manifest.RemoveSSTablesCommand{SSTableIDs: []string{"c"}}}
	if ids := maintenanceCommandSSTIDs(remove); len(ids) != 1 || ids[0] != "c" {
		t.Fatalf("remove: %v", ids)
	}
	if ids := maintenanceCommandSSTIDs(&manifest.MaintenanceCommand{}); ids != nil {
		t.Fatalf("checkpoint: %v", ids)
	}
}

func TestWriterOptions_L0MergeValidation(t *testing.T) {
	if _, err := normalizeWriterOptions(WriterOptions{L0Merge: WriterL0MergeOptions{TargetBytes: -1}}); err == nil {
		t.Fatalf("negative target accepted")
	}
	if _, err := normalizeWriterOptions(WriterOptions{L0Merge: WriterL0MergeOptions{MaxAge: -time.Second}}); err == nil {
		t.Fatalf("negative max age accepted")
	}
	opts, err := normalizeWriterOptions(WriterOptions{L0Merge: WriterL0MergeOptions{TargetBytes: 1 << 20}})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if opts.L0Merge.MaxAge != 5*time.Minute {
		t.Fatalf("default max age %s, want 5m", opts.L0Merge.MaxAge)
	}
	if opts.L0Merge.TargetBytes != 1<<20 {
		t.Fatalf("target changed to %d", opts.L0Merge.TargetBytes)
	}
	zero, err := normalizeWriterOptions(WriterOptions{})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if zero.L0Merge.TargetBytes != 64<<20 {
		t.Fatalf("zero target selects %d, want the 64 MiB default", zero.L0Merge.TargetBytes)
	}
}
