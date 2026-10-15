package isledb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

type commitStatusFixture struct {
	ctx   context.Context
	store *blobstore.Store
}

func newCommitStatusFixture(t *testing.T) *commitStatusFixture {
	t.Helper()
	store := blobstore.NewMemory("commit-status")
	t.Cleanup(func() { _ = store.Close() })
	return &commitStatusFixture{ctx: context.Background(), store: store}
}

// writer opens a Writer on a handle of its own, taking over from any other.
func (f *commitStatusFixture) writer(t *testing.T) *Writer {
	t.Helper()
	db, err := openDB(f.ctx, f.store, dbOpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	w, err := db.OpenWriter(f.ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func putKey(t *testing.T, w *Writer, key string) uint64 {
	t.Helper()
	seq, err := w.Put(context.Background(), []byte(key), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func wantStatus(t *testing.T, w *Writer, epoch, seq uint64, want CommitStatus) {
	t.Helper()
	got, err := w.CommitStatus(epoch, seq)
	if err != nil || got != want {
		t.Fatalf("CommitStatus(epoch %d, seq %d) = %v, %v; want %v", epoch, seq, got, err, want)
	}
}

func TestCommitStatusOfOwnWrites(t *testing.T) {
	f := newCommitStatusFixture(t)
	w := f.writer(t)
	seq := putKey(t, w, "a")
	wantStatus(t, w, w.Epoch(), seq, CommitPending)
	if err := w.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, w, w.Epoch(), seq, CommitDone)
	if _, err := w.CommitStatus(w.Epoch(), seq+1); err == nil {
		t.Fatal("a sequence not assigned yet was answered")
	}
	if _, err := w.CommitStatus(0, seq); err == nil {
		t.Fatal("epoch 0 was answered")
	}
}

func TestCommitStatusAcrossOneFailover(t *testing.T) {
	f := newCommitStatusFixture(t)
	a := f.writer(t)
	committed := putKey(t, a, "a1")
	if err := a.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	lost := putKey(t, a, "a2") // accepted by a, never committed

	b := f.writer(t)
	if b.Epoch() != a.Epoch()+1 {
		t.Fatalf("epochs %d then %d, want consecutive", a.Epoch(), b.Epoch())
	}
	reused := putKey(t, b, "b1")
	if reused != lost {
		t.Fatalf("b assigned %d, want %d again: the case CommitStatus exists for", reused, lost)
	}
	if err := b.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	// The same sequence, two writes: the epoch tells them apart.
	wantStatus(t, b, a.Epoch(), committed, CommitDone)
	wantStatus(t, b, a.Epoch(), lost, CommitNever)
	wantStatus(t, b, b.Epoch(), reused, CommitDone)

	// a learns it was fenced at its next commit, and then answers too.
	if err := a.Flush(f.ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("flush after takeover: %v, want ErrFenced", err)
	}
	wantStatus(t, a, a.Epoch(), lost, CommitNever)
	wantStatus(t, a, a.Epoch(), committed, CommitDone)
	if _, err := a.CommitStatus(b.Epoch(), reused); !errors.Is(err, ErrFenced) {
		t.Fatalf("a later writer's write asked of a: %v, want ErrFenced", err)
	}
}

func TestCommitStatusTwoFailoversBack(t *testing.T) {
	f := newCommitStatusFixture(t)
	a := f.writer(t)
	kept := putKey(t, a, "a1")
	if err := a.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	reused := putKey(t, a, "a2") // lost; b reuses its sequence and commits
	beyond := putKey(t, a, "a3") // lost; nothing reaches it before c
	b := f.writer(t)
	putKey(t, b, "b1")
	if err := b.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	c := f.writer(t)
	// c's fence keeps where b started, so a's writes are told apart from
	// b's under the same sequences.
	wantStatus(t, c, a.Epoch(), kept, CommitDone)
	wantStatus(t, c, a.Epoch(), reused, CommitNever)
	wantStatus(t, c, a.Epoch(), beyond, CommitNever)

	// Three back, only what is above every commit before d is known.
	d := f.writer(t)
	wantStatus(t, d, a.Epoch(), reused, CommitUnknown)
	wantStatus(t, d, a.Epoch(), beyond+5, CommitNever)
}

func TestClaimRecordsWhereEachWriterStopped(t *testing.T) {
	f := newCommitStatusFixture(t)
	a := f.writer(t)
	if a.w.fenceToken.BaseSeq != 0 || a.w.fenceToken.Prev != nil {
		t.Fatalf("first fence %+v, want base 0 and no previous", a.w.fenceToken)
	}
	putKey(t, a, "a1")
	last := putKey(t, a, "a2")
	if err := a.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	putKey(t, a, "a3") // never committed
	b := f.writer(t)
	if got := b.w.fenceToken; got.BaseSeq != last || got.Prev == nil || got.Prev.Epoch != a.Epoch() || got.Prev.BaseSeq != 0 {
		t.Fatalf("fence %+v, want base %d and previous {epoch %d, base 0}", got, last, a.Epoch())
	}
}

// TestFencedWriterLearnsItsLostCommitLanded: a commit lands but its response
// is lost; before the commit is retried, applying maintenance finds the fence
// taken. The successor's fence says the commit landed.
func TestFencedWriterLearnsItsLostCommitLanded(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, storage, _, _ := newHangingCommitWriter(t, opts, 200*time.Millisecond)
	// Passes are run here, in order, instead of by the committer, which
	// would retry the commit at once and learn of it that way.
	w.committerCancel()
	<-w.committerDone
	seq, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	storage.applyFirst.Store(true)
	storage.hang.Store(true)
	if err := w.commitPass(ctx, false); err == nil {
		t.Fatal("a commit with its response lost succeeded")
	}
	storage.hang.Store(false)
	if w.committed.Load() >= seq || storage.applied.Load() == 0 {
		t.Fatal("want the commit applied and the writer unaware of it")
	}
	if _, err := manifest.NewStoreWithStorage(storage.Storage).ClaimWriter(ctx, "successor"); err != nil {
		t.Fatal(err)
	}
	w.stagedMaintenance.Store(&manifest.MaintenanceCommand{
		ID: "floor-1", Epoch: 1, Generation: 1, CreatedAt: time.Now(),
		Kind: manifest.MaintenanceCommandChangeFeedFloor, ChangeFeedFloor: &manifest.AdvanceFloorCommand{Floor: 1},
	})
	if err := w.commitPass(ctx, false); !errors.Is(err, ErrFenced) {
		t.Fatalf("pass after takeover: %v, want ErrFenced from the maintenance apply", err)
	}
	position, err := w.waitCommittedAt(ctx, seq)
	if err != nil {
		t.Fatalf("WaitCommitted on the landed write: %v, want nil", err)
	}
	// From the CURRENT that showed the takeover: past the landed commit.
	data, _, err := storage.Storage.ReadCurrent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	current, err := manifest.DecodeCurrent(data)
	if err != nil {
		t.Fatal(err)
	}
	if position < 2 || position > current.NextSeq {
		t.Fatalf("position %d for a write settled by the successor's fence, want in [2, %d]", position, current.NextSeq)
	}
	if got, err := w.commitStatus(w.fenceToken.Epoch, seq); err != nil || got != CommitDone {
		t.Fatalf("CommitStatus: %v, %v; want committed", got, err)
	}
}

// TestReconciledCommitAfterTakeoverSettlesLaterWrites: the retry of a commit
// whose response was lost finds it landed, after a successor took the fence.
// The writer records it and learns where its commits ended, so a later write
// it never committed is CommitNever, not CommitUnknown.
func TestReconciledCommitAfterTakeoverSettlesLaterWrites(t *testing.T) {
	ctx := context.Background()
	opts := testWriterOptions(1<<20, 16)
	opts.Flush.Interval = 0
	w, storage, _, _ := newHangingCommitWriter(t, opts, 200*time.Millisecond)
	w.committerCancel()
	<-w.committerDone
	landed, err := w.put(ctx, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	storage.applyFirst.Store(true)
	storage.hang.Store(true)
	if err := w.commitPass(ctx, false); err == nil {
		t.Fatal("a commit with its response lost succeeded")
	}
	storage.hang.Store(false)
	later, err := w.put(ctx, []byte("b"), []byte("2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manifest.NewStoreWithStorage(storage.Storage).ClaimWriter(ctx, "successor"); err != nil {
		t.Fatal(err)
	}
	// The retry reconciles the landed commit, then the next one fails on the fence.
	if err := w.commitPass(ctx, false); !errors.Is(err, ErrFenced) {
		t.Fatalf("pass after takeover: %v, want ErrFenced", err)
	}
	if err := w.waitCommitted(ctx, landed); err != nil {
		t.Fatalf("WaitCommitted on the reconciled write: %v, want nil", err)
	}
	if got, err := w.commitStatus(w.fenceToken.Epoch, later); err != nil || got != CommitNever {
		t.Fatalf("CommitStatus of a later write: %v, %v; want never", got, err)
	}
}

func TestWriterStartsAboveTheLastCommitRecorded(t *testing.T) {
	f := newCommitStatusFixture(t)
	a := f.writer(t)
	last := putKey(t, a, "a1")
	if err := a.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	// The last commit recorded says more was committed than the SSTs show.
	storage := manifest.NewBlobStoreBackend(f.store)
	data, etag, err := storage.ReadCurrent(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	current, err := manifest.DecodeCurrent(data)
	if err != nil {
		t.Fatal(err)
	}
	current.LastWriterCommit.SeqHi = last + 10
	data, err = manifest.EncodeCurrent(current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.WriteCurrentCAS(f.ctx, data, etag); err != nil {
		t.Fatal(err)
	}
	b := f.writer(t)
	if seq := putKey(t, b, "b1"); seq != last+11 {
		t.Fatalf("first sequence %d after a recorded commit at %d, want %d", seq, last+10, last+11)
	}
}
