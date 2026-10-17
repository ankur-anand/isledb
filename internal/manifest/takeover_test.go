package manifest

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

type takeoverFixture struct {
	ctx context.Context
	bs  *blobstore.Store
	a   *Store
}

// newTakeoverFixture has writer A commit a0, checkpoint it, then commit a1,
// so the log window holds a1 and no writer claim.
func newTakeoverFixture(t *testing.T) *takeoverFixture {
	t.Helper()
	f := &takeoverFixture{ctx: context.Background(), bs: blobstore.NewMemory("takeover")}
	t.Cleanup(func() { _ = f.bs.Close() })
	f.a = NewStore(f.bs)
	if _, err := f.a.ClaimWriter(f.ctx, "A"); err != nil {
		t.Fatal(err)
	}
	f.commit(t, f.a, "a0.sst")
	f.checkpoint(t, f.a, 1)
	f.commit(t, f.a, "a1.sst")
	return f
}

func (f *takeoverFixture) commit(t *testing.T, s *Store, id string) {
	t.Helper()
	if _, err := s.AppendAddSSTableWithFence(f.ctx, SSTMeta{ID: id, Level: 0}); err != nil {
		t.Fatal(err)
	}
}

// checkpoint has a separate store prepare a checkpoint and writer apply it.
func (f *takeoverFixture) checkpoint(t *testing.T, writer *Store, generation uint64) {
	t.Helper()
	cp, err := NewStore(f.bs).PrepareCheckpoint(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := writer.ApplyMaintenanceCommand(f.ctx, MaintenanceCommand{ID: "cp", Epoch: 1, Generation: generation,
		Kind: MaintenanceCommandCheckpoint, CreatedAt: time.Now(), Checkpoint: &cp})
	if err != nil || res.Status != MaintenanceStatusApplied {
		t.Fatalf("checkpoint: %+v %v", res, err)
	}
}

func (f *takeoverFixture) has(t *testing.T, s *Store, id string) bool {
	t.Helper()
	m, err := s.Replay(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m.LookupSST(id) != nil
}

// TestTakeoverRecordsItsClaimWithItsFence: the claim entry is in the same
// CURRENT as the fence it records, so no reader sees one without the other.
func TestTakeoverRecordsItsClaimWithItsFence(t *testing.T) {
	f := newTakeoverFixture(t)
	b := NewStore(f.bs)
	token, err := b.ClaimWriter(f.ctx, "B")
	if err != nil {
		t.Fatal(err)
	}
	current, err := NewStore(f.bs).readCurrentData(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	last := current.ActiveEntries[len(current.ActiveEntries)-1]
	if current.WriterFence.Epoch != token.Epoch || last.Op != LogOpFenceClaim || last.Epoch != token.Epoch ||
		last.Seq+1 != current.NextSeq {
		t.Fatalf("fence epoch %d, last entry %+v, next seq %d: want the claim for epoch %d as the last entry",
			current.WriterFence.Epoch, last, current.NextSeq, token.Epoch)
	}
}

// TestTakeoverAfterCheckpointKeepsTheOldWritersCommits: A's a1, committed
// before B takes over, stays for a reader that replayed before the takeover,
// a fresh reader, and a checkpoint taken after it; A is fenced.
func TestTakeoverAfterCheckpointKeepsTheOldWritersCommits(t *testing.T) {
	f := newTakeoverFixture(t)
	reader := NewStore(f.bs)
	if !f.has(t, reader, "a1.sst") {
		t.Fatal("a1 missing before the takeover")
	}
	b := NewStore(f.bs)
	if _, err := b.ClaimWriter(f.ctx, "B"); err != nil {
		t.Fatal(err)
	}
	if !f.has(t, reader, "a1.sst") || !f.has(t, NewStore(f.bs), "a1.sst") {
		t.Fatal("a1 dropped after the takeover")
	}
	f.checkpoint(t, b, 2)
	f.commit(t, b, "b1.sst")
	for _, id := range []string{"a0.sst", "a1.sst", "b1.sst"} {
		if !f.has(t, reader, id) || !f.has(t, NewStore(f.bs), id) {
			t.Fatalf("%s missing after the checkpoint", id)
		}
	}
	if _, err := f.a.AppendAddSSTableWithFence(f.ctx, SSTMeta{ID: "a2.sst", Level: 0}); err == nil {
		t.Fatal("the replaced writer appended")
	}
}

// TestReplayKeepsCommitsWhenATakeoverLeftNoClaim: a database left by an
// earlier release mid-takeover, its fence moved with no claim in the log,
// keeps the commits made before it, through a checkpoint too.
func TestReplayKeepsCommitsWhenATakeoverLeftNoClaim(t *testing.T) {
	f := newTakeoverFixture(t)
	s := NewStore(f.bs)
	current, etag, err := s.readCurrentWithETag(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	current.WriterFence = &FenceToken{Epoch: current.WriterFence.Epoch + 1, Owner: "B", ClaimedAt: time.Now().UTC()}
	current.NextEpoch = current.WriterFence.Epoch + 1
	if err := s.writeCurrentWithCAS(f.ctx, current, etag); err != nil {
		t.Fatal(err)
	}
	if !f.has(t, NewStore(f.bs), "a1.sst") {
		t.Fatal("a1 dropped while the takeover has no claim")
	}
	b := NewStore(f.bs)
	if _, err := b.ClaimWriter(f.ctx, "C"); err != nil {
		t.Fatal(err)
	}
	f.checkpoint(t, b, 2)
	if !f.has(t, NewStore(f.bs), "a1.sst") {
		t.Fatal("a1 dropped by the checkpoint")
	}
}

// countingCASStorage counts the writes of CURRENT.
type countingCASStorage struct {
	Storage
	writes int
}

func (s *countingCASStorage) WriteCurrentCAS(ctx context.Context, data []byte, etag string) (string, error) {
	s.writes++
	return s.Storage.WriteCurrentCAS(ctx, data, etag)
}

// TestTakeoverIsOneWriteOfCurrent: taking the writer fence and recording the
// claim is one write, so there is no moment between them for a replay or a
// checkpoint to see one without the other.
func TestTakeoverIsOneWriteOfCurrent(t *testing.T) {
	f := newTakeoverFixture(t)
	counting := &countingCASStorage{Storage: NewBlobStoreBackend(f.bs)}
	if _, err := NewStoreWithStorage(counting).ClaimWriter(f.ctx, "B"); err != nil {
		t.Fatal(err)
	}
	if counting.writes != 1 {
		t.Fatalf("the takeover wrote CURRENT %d times, want 1", counting.writes)
	}
}
