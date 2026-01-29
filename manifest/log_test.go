package manifest

import (
	"testing"
	"time"

	"github.com/segmentio/ksuid"
)

func TestLogEntryRoundTrip(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	created := time.Unix(1700000010, 0).UTC()

	entry := &ManifestLogEntry{
		ID:        ksuid.New(),
		Seq:       7,
		Timestamp: now,
		Op:        LogOpAddSSTable,
		SSTable: &SSTMeta{
			ID:        "1-1-2-abc.sst",
			Epoch:     1,
			SeqLo:     1,
			SeqHi:     2,
			MinKey:    []byte("a"),
			MaxKey:    []byte("b"),
			Size:      123,
			Checksum:  "sha256:abc",
			CreatedAt: created,
			Level:     0,
		},
	}

	data, err := EncodeLogEntry(entry)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeLogEntry(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Op != entry.Op || got.Seq != entry.Seq || !got.Timestamp.Equal(entry.Timestamp) {
		t.Fatalf("header mismatch")
	}
	if got.SSTable == nil || got.SSTable.ID != entry.SSTable.ID {
		t.Fatalf("sstable mismatch")
	}
	if !got.SSTable.CreatedAt.Equal(created) {
		t.Fatalf("createdAt mismatch")
	}
}

func TestApplyLogEntryAddSSTable(t *testing.T) {
	entry := &ManifestLogEntry{
		Op: LogOpAddSSTable,
		SSTable: &SSTMeta{
			ID:    "1-1-1-aaa.sst",
			Epoch: 2,
			SeqLo: 1,
			SeqHi: 1,
			Level: 0,
		},
	}

	m := &Manifest{Version: 2, NextEpoch: 1}
	got := ApplyLogEntry(m, entry)

	if got.NextEpoch != 3 {
		t.Fatalf("nextEpoch mismatch: %d", got.NextEpoch)
	}

	if len(got.L0SSTs) != 1 {
		t.Fatalf("L0SSTs mismatch: %d", len(got.L0SSTs))
	}
	if got.L0SSTs[0].ID != "1-1-1-aaa.sst" {
		t.Fatalf("L0 SST ID mismatch: %s", got.L0SSTs[0].ID)
	}
}

func TestApplyLogEntryAddSSTableL1(t *testing.T) {

	entry := &ManifestLogEntry{
		Op: LogOpAddSSTable,
		SSTable: &SSTMeta{
			ID:     "1-1-1-aaa.sst",
			Epoch:  2,
			SeqLo:  1,
			SeqHi:  1,
			Level:  1,
			MinKey: []byte("a"),
			MaxKey: []byte("z"),
		},
	}

	m := &Manifest{Version: 2, NextEpoch: 1}
	got := ApplyLogEntry(m, entry)

	if got.NextEpoch != 3 {
		t.Fatalf("nextEpoch mismatch: %d", got.NextEpoch)
	}
	if len(got.L0SSTs) != 0 {
		t.Fatalf("L0SSTs should be empty: %d", len(got.L0SSTs))
	}
	if len(got.SortedRuns) != 1 {
		t.Fatalf("SortedRuns mismatch: %d", len(got.SortedRuns))
	}
	if len(got.SortedRuns[0].SSTs) != 1 {
		t.Fatalf("SortedRun SSTs mismatch: %d", len(got.SortedRuns[0].SSTs))
	}
	if got.SortedRuns[0].SSTs[0].ID != "1-1-1-aaa.sst" {
		t.Fatalf("SortedRun SST ID mismatch: %s", got.SortedRuns[0].SSTs[0].ID)
	}
}

func TestApplyLogEntryRemoveSSTable(t *testing.T) {
	m := &Manifest{
		L0SSTs: []SSTMeta{
			{ID: "a.sst", Level: 0},
			{ID: "b.sst", Level: 0},
		},
	}

	entry := &ManifestLogEntry{
		Op:               LogOpRemoveSSTable,
		RemoveSSTableIDs: []string{"a.sst"},
	}
	got := ApplyLogEntry(m, entry)

	if len(got.L0SSTs) != 1 || got.L0SSTs[0].ID != "b.sst" {
		t.Fatalf("L0SSTs mismatch: got %d", len(got.L0SSTs))
	}
}

func TestApplyLogEntryRemoveSSTableFromSortedRun(t *testing.T) {
	m := &Manifest{
		SortedRuns: []SortedRun{
			{
				ID: 1,
				SSTs: []SSTMeta{
					{ID: "a.sst", MinKey: []byte("a"), MaxKey: []byte("m")},
					{ID: "b.sst", MinKey: []byte("n"), MaxKey: []byte("z")},
				},
			},
		},
	}

	entry := &ManifestLogEntry{
		Op:               LogOpRemoveSSTable,
		RemoveSSTableIDs: []string{"a.sst"},
	}
	got := ApplyLogEntry(m, entry)

	if len(got.SortedRuns) != 1 {
		t.Fatalf("SortedRuns count mismatch: %d", len(got.SortedRuns))
	}
	if len(got.SortedRuns[0].SSTs) != 1 || got.SortedRuns[0].SSTs[0].ID != "b.sst" {
		t.Fatalf("SortedRun SSTs mismatch")
	}
}

func TestApplyLogEntryCompaction(t *testing.T) {
	m := &Manifest{
		NextEpoch: 3,
		L0SSTs: []SSTMeta{
			{ID: "a.sst", Level: 0, Epoch: 1},
			{ID: "b.sst", Level: 0, Epoch: 2},
		},
	}

	entry := &ManifestLogEntry{
		Op: LogOpCompaction,
		Compaction: &CompactionLogPayload{
			RemoveSSTableIDs: []string{"a.sst", "b.sst"},
			AddSortedRun: &SortedRun{
				ID: 1,
				SSTs: []SSTMeta{
					{ID: "c.sst", Level: 1, Epoch: 5, MinKey: []byte("a"), MaxKey: []byte("z")},
				},
			},
		},
	}

	got := ApplyLogEntry(m, entry)

	if got.NextEpoch != 3 {
		t.Fatalf("nextEpoch mismatch: %d", got.NextEpoch)
	}
	if len(got.L0SSTs) != 0 {
		t.Fatalf("L0SSTs should be empty: %d", len(got.L0SSTs))
	}
	if len(got.SortedRuns) != 1 || got.SortedRuns[0].SSTs[0].ID != "c.sst" {
		t.Fatalf("SortedRuns mismatch")
	}
}

func TestApplyLogEntryCheckpoint(t *testing.T) {
	cp := &Manifest{
		Version:   9,
		NextEpoch: 10,
		L0SSTs:    []SSTMeta{{ID: "x.sst", Level: 0}},
	}
	entry := &ManifestLogEntry{
		Op:         LogOpCheckpoint,
		Checkpoint: cp,
	}

	got := ApplyLogEntry(&Manifest{Version: 1}, entry)
	if got.Version != 9 || got.NextEpoch != 10 {
		t.Fatalf("checkpoint mismatch")
	}
	if len(got.L0SSTs) != 1 || got.L0SSTs[0].ID != "x.sst" {
		t.Fatalf("checkpoint L0SSTs mismatch")
	}
}

func TestApplyLogEntries(t *testing.T) {
	m := &Manifest{Version: 2}
	entries := []*ManifestLogEntry{
		{Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}},
		{Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "b.sst", Epoch: 2, Level: 0}},
	}

	got := ApplyLogEntries(m, entries)

	if len(got.L0SSTs) != 2 {
		t.Fatalf("L0SSTs mismatch: %d", len(got.L0SSTs))
	}
	if got.NextEpoch != 3 {
		t.Fatalf("nextEpoch mismatch: %d", got.NextEpoch)
	}
}
