package manifest

import (
	"reflect"
	"testing"

	"github.com/segmentio/ksuid"
)

func TestLogEntryRoundTrip(t *testing.T) {
	entry := &ManifestLogEntry{
		ID:    ksuid.New(),
		Seq:   7,
		Role:  FenceRoleWriter,
		Epoch: 2,
		Op:    LogOpAddSSTable,
		SSTable: &SSTMeta{
			ID: "sst-a", SeqLo: 1, SeqHi: 3,
		},
	}
	body, err := EncodeLogEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeLogEntry(body)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, entry) {
		t.Fatalf("round trip\n got=%+v\nwant=%+v", got, entry)
	}
}

func TestApplyLogEntryAddRemoveAndCompaction(t *testing.T) {
	m := ApplyLogEntry(nil, &ManifestLogEntry{Seq: 1, Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "a", Epoch: 1, MinKey: []byte("a"), MaxKey: []byte("c")}})
	m = ApplyLogEntry(m, &ManifestLogEntry{Seq: 2, Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "b", Epoch: 2, MinKey: []byte("d"), MaxKey: []byte("f")}})
	if len(m.L0SSTs) != 2 || m.L0SSTs[0].ID != "b" || m.NextEpoch != 3 {
		t.Fatalf("after add=%+v", m)
	}

	m = ApplyLogEntry(m, &ManifestLogEntry{Seq: 3, Op: LogOpCompaction, Compaction: &CompactionLogPayload{
		RemoveSSTableIDs: []string{"a", "b"},
		SourceLevel:      0,
		DestinationLevel: 1,
		AddSSTables:      []SSTMeta{{ID: "c", Epoch: 3, Level: 1, MinKey: []byte("a"), MaxKey: []byte("f")}},
	}})
	if len(m.L0SSTs) != 0 || len(m.Levels) != 1 || m.Levels[0].SSTs[0].ID != "c" {
		t.Fatalf("after compaction=%+v", m)
	}

	m = ApplyLogEntry(m, &ManifestLogEntry{Seq: 4, Op: LogOpCompaction, Compaction: &CompactionLogPayload{
		RemoveSSTableIDs: []string{"c"},
		SourceLevel:      1,
		DestinationLevel: 2,
		AddSSTables:      []SSTMeta{{ID: "c", Epoch: 3, Level: 2, MinKey: []byte("a"), MaxKey: []byte("f")}},
	}})
	if m.Level(1) != nil || m.Level(2) == nil || m.Level(2).SSTs[0].ID != "c" {
		t.Fatalf("after promotion=%+v", m)
	}

	m = ApplyLogEntry(m, &ManifestLogEntry{Seq: 5, Op: LogOpRemoveSSTable, RemoveSSTableIDs: []string{"c"}})
	if len(m.Levels) != 0 {
		t.Fatalf("after remove=%+v", m)
	}
}

func TestApplyCheckpoint(t *testing.T) {
	checkpoint := &Manifest{Version: 2, LogSeq: 4, Levels: []Level{{Number: 1, SSTs: []SSTMeta{{ID: "a"}}}}}
	got := ApplyLogEntry(&Manifest{}, &ManifestLogEntry{Seq: 9, Op: LogOpCheckpoint, Checkpoint: checkpoint})
	if got != checkpoint || got.LogSeq != 9 {
		t.Fatalf("checkpoint=%+v", got)
	}
}
