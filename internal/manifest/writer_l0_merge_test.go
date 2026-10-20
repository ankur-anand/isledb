package manifest

import (
	"context"
	"errors"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

// flushSST is one writer flush spanning the whole key range, as random keys
// produce.
func flushSST(id string, seqLo, seqHi uint64) SSTMeta {
	return SSTMeta{ID: id, Epoch: seqLo, SeqLo: seqLo, SeqHi: seqHi, Size: 100,
		MinKey: []byte("a"), MaxKey: []byte("z"), Level: 0}
}

func retiredSSTs(ids ...string) []RetiredObject {
	retired := make([]RetiredObject, len(ids))
	for i, id := range ids {
		retired[i] = RetiredObject{Kind: RetiredObjectSST, ID: id, Key: "sstable/000/" + id, Size: 100}
	}
	return retired
}

// writerWithFlushes claims the writer fence and commits a, b, c in that
// order, so L0 is c,b,a newest first with sequences 1-10, 11-20, 21-30.
func writerWithFlushes(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	for i, id := range []string{"a", "b", "c"} {
		lo := uint64(10*i + 1)
		if _, err := store.AppendWriterCommit(ctx, WriterCommit{ID: "flush-" + id, SSTable: flushSST(id, lo, lo+9)}); err != nil {
			t.Fatalf("AppendWriterCommit(%s): %v", id, err)
		}
	}
}

func currentOf(t *testing.T, store *Store) *Current {
	t.Helper()
	current, err := store.ReadCurrentData(context.Background())
	if err != nil {
		t.Fatalf("ReadCurrentData: %v", err)
	}
	return current
}

func TestAppendWriterL0MergeReplacesTheRunAndKeepsTheCommitMarker(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("writer-l0-merge")
	defer blob.Close()
	store := NewStore(blob)
	writerWithFlushes(t, store)
	before := currentOf(t, store)

	// Merge the two oldest files, b and a. The newest flush, c, stays, and
	// its marker must stay the writer's last commit.
	merge := WriterL0Merge{
		ID:       "merge-1",
		InputIDs: []string{"b", "a"},
		Output:   flushSST("compacted-ba", 1, 20),
		Retired:  retiredSSTs("b", "a"),
	}
	entry, err := store.AppendWriterL0Merge(ctx, merge)
	if err != nil {
		t.Fatalf("AppendWriterL0Merge: %v", err)
	}
	if entry.Op != LogOpCompaction || entry.Role != FenceRoleWriter || entry.CommitID != merge.ID ||
		entry.Compaction == nil || len(entry.Compaction.AddSSTables) != 1 || entry.Seq != before.NextSeq {
		t.Fatalf("merge entry=%+v", entry)
	}

	m, err := store.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if got := l0IDs(m); got != "c,compacted-ba" {
		t.Fatalf("L0 after merge = %s, want c,compacted-ba", got)
	}

	after := currentOf(t, store)
	if after.LayoutVersion != LayoutVersionL0Merge {
		t.Fatalf("layout version %d, want %d", after.LayoutVersion, LayoutVersionL0Merge)
	}
	if after.LastWriterL0Merge == nil || after.LastWriterL0Merge.CommitID != merge.ID ||
		after.LastWriterL0Merge.ManifestSeq != entry.Seq || after.LastWriterL0Merge.SeqHi != 20 {
		t.Fatalf("last writer l0 merge=%+v", after.LastWriterL0Merge)
	}
	if !writerCommitMarkersEqual(after.LastWriterCommit, before.LastWriterCommit) {
		t.Fatalf("last writer commit changed by a merge: before=%+v after=%+v", before.LastWriterCommit, after.LastWriterCommit)
	}
	if after.LastWriterCommit.SeqHi != 30 {
		t.Fatalf("last writer commit SeqHi=%d, want 30", after.LastWriterCommit.SeqHi)
	}

	// A successor's base sequence is where the writer's flushes ended, not
	// the merged file's range.
	successor := NewStore(blob)
	token, err := successor.ClaimWriter(ctx, "writer-2")
	if err != nil {
		t.Fatalf("successor ClaimWriter: %v", err)
	}
	if token.BaseSeq != 30 {
		t.Fatalf("successor BaseSeq=%d, want 30", token.BaseSeq)
	}
}

func TestAppendWriterL0MergeRetryWithTheSameIDPublishesOnce(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("writer-l0-merge-retry")
	defer blob.Close()
	store := NewStore(blob)
	writerWithFlushes(t, store)

	merge := WriterL0Merge{
		ID:       "merge-1",
		InputIDs: []string{"c", "b"},
		Output:   flushSST("compacted-cb", 11, 30),
		Retired:  retiredSSTs("c", "b"),
	}
	first, err := store.AppendWriterL0Merge(ctx, merge)
	if err != nil {
		t.Fatalf("AppendWriterL0Merge: %v", err)
	}
	second, err := store.AppendWriterL0Merge(ctx, merge)
	if err != nil {
		t.Fatalf("retry AppendWriterL0Merge: %v", err)
	}
	if second.Seq != first.Seq || second.ID != first.ID {
		t.Fatalf("retry published again: first seq=%d second seq=%d", first.Seq, second.Seq)
	}
	if next := currentOf(t, store).NextSeq; next != first.Seq+1 {
		t.Fatalf("NextSeq=%d after retry, want %d", next, first.Seq+1)
	}

	// The same ID with different contents is a conflict, not a publish.
	changed := merge
	changed.Output.Checksum = "sha256:other"
	if _, err := store.AppendWriterL0Merge(ctx, changed); !errors.Is(err, ErrWriterCommitConflict) {
		t.Fatalf("changed retry error=%v, want %v", err, ErrWriterCommitConflict)
	}
}

func TestAppendWriterL0MergeReconcilesAppliedCASAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("writer-l0-merge-lost-response")
	defer blob.Close()
	base := NewBlobStoreBackend(blob)
	storage := &appliedThenErrorStorage{Storage: base}
	store := NewStoreWithStorage(storage)
	writerWithFlushes(t, store)

	merge := WriterL0Merge{
		ID:       "merge-1",
		InputIDs: []string{"c", "b"},
		Output:   flushSST("compacted-cb", 11, 30),
		Retired:  retiredSSTs("c", "b"),
	}
	lost := errors.New("lost CURRENT response")
	storage.arm(lost)
	if _, err := store.AppendWriterL0Merge(ctx, merge); !errors.Is(err, lost) {
		t.Fatalf("first AppendWriterL0Merge error=%v, want %v", err, lost)
	}

	// The CAS landed. The retry must find it, publish nothing, and report
	// the committed entry.
	entry, err := store.AppendWriterL0Merge(ctx, merge)
	if err != nil {
		t.Fatalf("retry AppendWriterL0Merge: %v", err)
	}
	if entry.CommitID != merge.ID || entry.Compaction == nil {
		t.Fatalf("reconciled entry=%+v", entry)
	}
	m, err := store.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if got := l0IDs(m); got != "compacted-cb,a" {
		t.Fatalf("L0 = %s, want compacted-cb,a", got)
	}
	seqs, err := store.ListEntries(ctx)
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	merges := 0
	for _, seq := range seqs {
		e, err := store.ReadEntry(ctx, seq)
		if err != nil {
			t.Fatalf("ReadEntry(%d): %v", seq, err)
		}
		if e.CommitID == merge.ID {
			merges++
		}
	}
	if merges != 1 {
		t.Fatalf("entries with the merge's commit id: %d, want 1", merges)
	}
}

func TestAppendWriterL0MergeIsRefusedWhenTheRunDoesNotFit(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("writer-l0-merge-refused")
	defer blob.Close()
	store := NewStore(blob)
	writerWithFlushes(t, store)
	before := currentOf(t, store)

	for name, merge := range map[string]WriterL0Merge{
		"not adjacent":          {ID: "m-gap", InputIDs: []string{"c", "a"}, Output: flushSST("out", 1, 30), Retired: retiredSSTs("c", "a")},
		"missing input":         {ID: "m-missing", InputIDs: []string{"c", "x"}, Output: flushSST("out", 1, 30), Retired: retiredSSTs("c", "x")},
		"output outside inputs": {ID: "m-range", InputIDs: []string{"c", "b"}, Output: flushSST("out", 1, 30), Retired: retiredSSTs("c", "b")},
	} {
		if _, err := store.AppendWriterL0Merge(ctx, merge); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("%s: error=%v, want %v", name, err, ErrInvalidManifest)
		}
	}
	// Refused means nothing was written: no entry, no marker, L0 as before.
	after := currentOf(t, store)
	if after.NextSeq != before.NextSeq || after.LastWriterL0Merge != nil {
		t.Fatalf("refused merge changed CURRENT: before next=%d after next=%d marker=%+v",
			before.NextSeq, after.NextSeq, after.LastWriterL0Merge)
	}
	m, err := store.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if got := l0IDs(m); got != "c,b,a" {
		t.Fatalf("L0 = %s after refused merges, want c,b,a", got)
	}
}

func TestAppendWriterL0MergeRequiresRetirementRecords(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("writer-l0-merge-retired")
	defer blob.Close()
	store := NewStore(blob)
	writerWithFlushes(t, store)

	merge := WriterL0Merge{ID: "m", InputIDs: []string{"c", "b"}, Output: flushSST("out", 11, 30), Retired: retiredSSTs("c")}
	if _, err := store.AppendWriterL0Merge(ctx, merge); !errors.Is(err, ErrInvalidRetirement) {
		t.Fatalf("error=%v, want %v", err, ErrInvalidRetirement)
	}
}

func TestAppendWriterL0MergeValidatesItsInput(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("writer-l0-merge-validate")
	defer blob.Close()
	store := NewStore(blob)
	writerWithFlushes(t, store)

	for name, merge := range map[string]WriterL0Merge{
		"empty id":          {InputIDs: []string{"c"}, Output: flushSST("out", 21, 30), Retired: retiredSSTs("c")},
		"no inputs":         {ID: "m", Output: flushSST("out", 21, 30)},
		"empty output":      {ID: "m", InputIDs: []string{"c"}, Retired: retiredSSTs("c")},
		"output is input":   {ID: "m", InputIDs: []string{"c"}, Output: flushSST("c", 21, 30), Retired: retiredSSTs("c")},
		"inverted sequence": {ID: "m", InputIDs: []string{"c"}, Output: flushSST("out", 30, 21), Retired: retiredSSTs("c")},
	} {
		if _, err := store.AppendWriterL0Merge(ctx, merge); !errors.Is(err, ErrInvalidWriterCommit) {
			t.Fatalf("%s: error=%v, want %v", name, err, ErrInvalidWriterCommit)
		}
	}
}

func TestAppendWriterL0MergeFromAFencedWriterFails(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("writer-l0-merge-fenced")
	defer blob.Close()
	store := NewStore(blob)
	writerWithFlushes(t, store)
	if _, err := NewStore(blob).ClaimWriter(ctx, "writer-2"); err != nil {
		t.Fatalf("successor ClaimWriter: %v", err)
	}

	merge := WriterL0Merge{ID: "m", InputIDs: []string{"c", "b"}, Output: flushSST("out", 11, 30), Retired: retiredSSTs("c", "b")}
	if _, err := store.AppendWriterL0Merge(ctx, merge); !errors.Is(err, ErrFenced) {
		t.Fatalf("error=%v, want %v", err, ErrFenced)
	}
	m, err := NewStore(blob).Replay(ctx)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if got := l0IDs(m); got != "c,b,a" {
		t.Fatalf("L0 = %s after a fenced merge, want c,b,a", got)
	}
}

func TestWriterL0MergeMarkerSurvivesACheckpoint(t *testing.T) {
	ctx := context.Background()
	blob := blobstore.NewMemory("writer-l0-merge-checkpoint")
	defer blob.Close()
	store := NewStore(blob)
	writerWithFlushes(t, store)

	merge := WriterL0Merge{ID: "m", InputIDs: []string{"c", "b"}, Output: flushSST("out", 11, 30), Retired: retiredSSTs("c", "b")}
	if _, err := store.AppendWriterL0Merge(ctx, merge); err != nil {
		t.Fatalf("AppendWriterL0Merge: %v", err)
	}
	// Later flushes and a clone of CURRENT keep both markers.
	if _, err := store.AppendWriterCommit(ctx, WriterCommit{ID: "flush-d", SSTable: flushSST("d", 31, 40)}); err != nil {
		t.Fatalf("AppendWriterCommit: %v", err)
	}
	current := currentOf(t, store).Clone()
	if current.LastWriterL0Merge == nil || current.LastWriterL0Merge.CommitID != merge.ID {
		t.Fatalf("merge marker lost: %+v", current.LastWriterL0Merge)
	}
	if current.LastWriterCommit == nil || current.LastWriterCommit.CommitID != "flush-d" {
		t.Fatalf("commit marker=%+v, want flush-d", current.LastWriterCommit)
	}
	// A retry of the merge after more commits still reconciles from the
	// marker and publishes nothing.
	next := current.NextSeq
	if _, err := store.AppendWriterL0Merge(ctx, merge); err != nil {
		t.Fatalf("late retry: %v", err)
	}
	if currentOf(t, store).NextSeq != next {
		t.Fatalf("late retry published a second merge")
	}
}
