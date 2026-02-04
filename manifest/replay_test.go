package manifest

import (
	"testing"
	"time"

	"github.com/segmentio/ksuid"
)

func TestReplaySnapshotAndLogs(t *testing.T) {

	snap := &Manifest{
		Version:   1,
		NextEpoch: 2,
		L0SSTs:    []SSTMeta{{ID: "a.sst", Epoch: 1, Level: 0}},
	}
	snapBytes, err := EncodeSnapshot(snap)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}

	entry := &ManifestLogEntry{
		ID:        ksuid.New(),
		Seq:       5,
		Timestamp: time.Unix(1700000100, 0).UTC(),
		Op:        LogOpAddSSTable,
		SSTable:   &SSTMeta{ID: "b.sst", Epoch: 2, Level: 0},
	}
	entryBytes, err := EncodeLogEntry(entry)
	if err != nil {
		t.Fatalf("encode entry: %v", err)
	}

	got, err := Replay(snapBytes, [][]byte{entryBytes})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(got.L0SSTs) != 2 {
		t.Fatalf("L0SSTs mismatch: %d", len(got.L0SSTs))
	}
	if got.NextEpoch != 3 {
		t.Fatalf("nextEpoch mismatch: %d", got.NextEpoch)
	}
	if got.LogSeq != entry.Seq {
		t.Fatalf("log seq mismatch: %d", got.LogSeq)
	}
}

func TestReplayEmpty(t *testing.T) {
	got, err := Replay(nil, nil)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got == nil || len(got.L0SSTs) != 0 || len(got.SortedRuns) != 0 {
		t.Fatalf("expected empty manifest")
	}
}
