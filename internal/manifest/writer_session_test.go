package manifest

import (
	"context"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

// TestReclaimedStoreForgetsTheLastSessionsSuccessor: a Store whose writer was
// replaced claims the fence again; the successor it reports after the next
// takeover is that one, not the first.
func TestReclaimedStoreForgetsTheLastSessionsSuccessor(t *testing.T) {
	ctx := context.Background()
	bs := blobstore.NewMemory("writer-session")
	defer bs.Close()
	a := NewStore(bs)
	if _, err := a.ClaimWriter(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	takeOver := func(owner string) *FenceToken {
		t.Helper()
		tok, err := NewStore(bs).ClaimWriter(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	first := takeOver("B")
	if _, err := a.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a1.sst", Level: 0, SeqLo: 1, SeqHi: 1}); err == nil {
		t.Fatal("the replaced writer appended")
	}
	if got, _ := a.SuccessorWriterFence(); got == nil || got.Epoch != first.Epoch {
		t.Fatalf("successor %+v, want epoch %d", got, first.Epoch)
	}

	if _, err := a.ClaimWriter(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.SuccessorWriterFence(); got != nil {
		t.Fatalf("a new session still reports the last one's successor %+v", got)
	}
	second := takeOver("C")
	if _, err := a.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a2.sst", Level: 0, SeqLo: 2, SeqHi: 2}); err == nil {
		t.Fatal("the replaced writer appended")
	}
	if got, _ := a.SuccessorWriterFence(); got == nil || got.Epoch != second.Epoch {
		t.Fatalf("successor %+v, want epoch %d", got, second.Epoch)
	}
}

// TestReclaimedStoreCommitsAfterAnUncertainCommit: a writer's commit is left
// uncertain when it is replaced; the Store, claimed again, still commits.
func TestReclaimedStoreCommitsAfterAnUncertainCommit(t *testing.T) {
	ctx := context.Background()
	bs := blobstore.NewMemory("writer-session-pending")
	defer bs.Close()
	storage := &loseResponseStorage{Storage: NewBlobStoreBackend(bs)}
	a := NewStoreWithStorage(storage)
	if _, err := a.ClaimWriter(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	storage.loseNext = true
	if _, err := a.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "lost.sst", Level: 0, SeqLo: 1, SeqHi: 1}); err == nil {
		t.Fatal("the lost response was not reported")
	}
	if _, err := NewStore(bs).ClaimWriter(ctx, "B"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ClaimWriter(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "next.sst", Level: 0, SeqLo: 2, SeqHi: 2}); err != nil {
		t.Fatalf("commit in the new session: %v", err)
	}
}
