package isledb

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
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

// waitForL0Merge runs passes until the committer takes a merge.
func waitForL0Merge(t *testing.T, w *writer, merges <-chan mergedL0Run) mergedL0Run {
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
	return mergedL0Run{}
}

func TestWriterL0Merge_OffByDefault(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-off")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)

	w, err := newWriter(ctx, store, manifestStore, testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	if w.l0Merge != nil {
		t.Fatalf("merger built with L0Merge off")
	}
	for i := 0; i < 4; i++ {
		if _, err := w.put(ctx, []byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := w.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	if err := w.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if ids := compactionNamedSSTs(t, store); len(ids) != 0 {
		t.Fatalf("merged files with merging off: %v", ids)
	}
}

func TestWriterL0Merge_MergesRunAtTargetBytes(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-l0-merge-size")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)

	// A target the first few flushes reach; an age they never do.
	w, err := newWriter(ctx, store, manifestStore, testL0MergeWriterOptions(8<<10, time.Hour))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	merges := make(chan mergedL0Run, 8)
	w.onL0Merged = func(merged mergedL0Run) { merges <- merged }

	// Versions of the same keys across flushes, a delete, and an entry that
	// expires before the run is merged. Each flush is one L0 file; writes
	// are recorded per flush so the expectation can be built for exactly the
	// files the merge took.
	var perFlush []map[string]expectedEntry
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
	}
	merged := waitForL0Merge(t, w, merges)

	// The inputs are the oldest files in L0, in commit order, one per flush.
	committed, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(merged.inputs) < 2 {
		t.Fatalf("merge of %d inputs; want at least 2", len(merged.inputs))
	}
	l0 := committed.L0SSTs // newest first
	if len(l0) < len(merged.inputs) || len(perFlush) < len(merged.inputs) {
		t.Fatalf("L0 has %d files from %d flushes, merge has %d inputs", len(l0), len(perFlush), len(merged.inputs))
	}
	for i, input := range merged.inputs {
		want := l0[len(l0)-1-i]
		if input.ID != want.ID {
			t.Fatalf("input %d = %s, want L0 file %s", i, input.ID, want.ID)
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
	if out.CreatedAt.Before(oldest) {
		t.Fatalf("output created %s before its oldest input %s", out.CreatedAt, oldest)
	}
	if out.Epoch == 0 {
		t.Fatalf("output has no epoch")
	}
	if ids := compactionNamedSSTs(t, store); len(ids) != 1 || ids[0] != out.ID {
		t.Fatalf("compaction-named objects %v, want only %s", ids, out.ID)
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
	merges := make(chan mergedL0Run, 8)
	w.onL0Merged = func(merged mergedL0Run) { merges <- merged }

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
	merged := waitForL0Merge(t, w, merges)
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
	if err := w.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
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
	if finished := m2.pass(time.Now().Add(time.Second)); finished != nil || len(m2.closed) != 1 {
		t.Fatalf("age did not close the run: closed=%d", len(m2.closed))
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
	off, err := normalizeWriterOptions(WriterOptions{})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if off.L0Merge.TargetBytes != 0 {
		t.Fatalf("merging on by default: %d", off.L0Merge.TargetBytes)
	}
}
