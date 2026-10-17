package manifest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// loseResponseStorage applies the next write of CURRENT, then reports it
// failed, as a response lost on the way back would.
type loseResponseStorage struct {
	Storage
	loseNext bool
}

func (s *loseResponseStorage) WriteCurrentCAS(ctx context.Context, data []byte, etag string) (string, error) {
	tag, err := s.Storage.WriteCurrentCAS(ctx, data, etag)
	if err == nil && s.loseNext {
		s.loseNext = false
		return "", errors.New("response lost")
	}
	return tag, err
}

// TestLostCommitResponseThenCheckpointIsReconciled: a commit lands but its
// response is lost, and the writer applies a checkpoint that folds the commit
// away before retrying. The retry must find the commit landed, from CURRENT's
// record of the writer's last commit, and the writer must commit again.
func TestLostCommitResponseThenCheckpointIsReconciled(t *testing.T) {
	ctx := context.Background()
	bs := blobstore.NewMemory("lost-commit")
	defer bs.Close()
	storage := &loseResponseStorage{Storage: NewBlobStoreBackend(bs)}
	w := NewStoreWithStorage(storage)
	if _, err := w.ClaimWriter(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	sst := SSTMeta{ID: "x.sst", Level: 0, SeqLo: 1, SeqHi: 5}
	storage.loseNext = true
	if _, err := w.AppendAddSSTableWithFence(ctx, sst); err == nil {
		t.Fatal("the lost response was not reported")
	}

	cp, err := NewStore(bs).PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := w.ApplyMaintenanceCommand(ctx, MaintenanceCommand{ID: "cp", Epoch: 1, Generation: 1,
		Kind: MaintenanceCommandCheckpoint, CreatedAt: time.Now(), Checkpoint: &cp})
	if err != nil || res.Status != MaintenanceStatusApplied {
		t.Fatalf("checkpoint: %+v %v", res, err)
	}

	entry, err := w.AppendAddSSTableWithFence(ctx, sst)
	if err != nil {
		t.Fatalf("retry of the landed commit: %v", err)
	}
	if entry.SSTable == nil || entry.SSTable.ID != "x.sst" {
		t.Fatalf("retry returned %+v, want the landed x.sst", entry)
	}
	if _, err := w.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "y.sst", Level: 0, SeqLo: 6, SeqHi: 9}); err != nil {
		t.Fatalf("the next commit: %v", err)
	}
	m, err := NewStore(bs).Replay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.L0SSTs) != 2 || m.LookupSST("x.sst") == nil || m.LookupSST("y.sst") == nil {
		t.Fatalf("L0 %+v, want x.sst once and y.sst", m.L0SSTs)
	}
}
