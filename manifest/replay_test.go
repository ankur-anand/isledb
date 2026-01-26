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
		SSTables:  []SSTMeta{{ID: "a.sst", Epoch: 1}},
		VLogs:     []VLogMeta{{ID: ksuid.New(), Size: 10}},
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
		SSTable:   &SSTMeta{ID: "b.sst", Epoch: 2},
	}
	entryBytes, err := EncodeLogEntry(entry)
	if err != nil {
		t.Fatalf("encode entry: %v", err)
	}

	got, err := Replay(snapBytes, [][]byte{entryBytes})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got.SSTables) != 2 {
		t.Fatalf("sstables mismatch: %d", len(got.SSTables))
	}
	if got.NextEpoch != 3 {
		t.Fatalf("nextEpoch mismatch: %d", got.NextEpoch)
	}
}

func TestReplayEmpty(t *testing.T) {
	got, err := Replay(nil, nil)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got == nil || len(got.SSTables) != 0 {
		t.Fatalf("expected empty manifest")
	}
}
