package manifest

import (
	"testing"
	"time"

	"github.com/segmentio/ksuid"
)

func TestLogEntryRoundTrip(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	created := time.Unix(1700000010, 0).UTC()
	vlogID := ksuid.New()

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
			VLogID:    vlogID,
			VLogSize:  456,
		},
		AddVLogs: []VLogMeta{
			{ID: ksuid.New(), Size: 100, ReferencedBy: []string{"1-1-2-abc.sst"}},
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
	if len(got.AddVLogs) != 1 || got.AddVLogs[0].Size != 100 {
		t.Fatalf("add_vlogs mismatch")
	}
}

func TestApplyLogEntryAddSSTable(t *testing.T) {
	vlogID := ksuid.New()
	extraVLog := ksuid.New()
	entry := &ManifestLogEntry{
		Op: LogOpAddSSTable,
		SSTable: &SSTMeta{
			ID:       "1-1-1-aaa.sst",
			Epoch:    2,
			SeqLo:    1,
			SeqHi:    1,
			Level:    0,
			VLogID:   vlogID,
			VLogSize: 50,
		},
		AddVLogs: []VLogMeta{
			{ID: extraVLog, Size: 10, ReferencedBy: []string{"1-1-1-aaa.sst"}},
		},
	}

	m := &Manifest{Version: 2, NextEpoch: 1}
	got := ApplyLogEntry(m, entry)

	if got.NextEpoch != 3 {
		t.Fatalf("nextEpoch mismatch: %d", got.NextEpoch)
	}
	if len(got.SSTables) != 1 {
		t.Fatalf("sstables mismatch: %d", len(got.SSTables))
	}
	if len(got.VLogs) != 2 {
		t.Fatalf("vlogs mismatch: %d", len(got.VLogs))
	}
	if got.VLogs[0].ID != vlogID && got.VLogs[1].ID != vlogID {
		t.Fatalf("missing vlog from sstable")
	}
}

func TestApplyLogEntryRemoveSSTable(t *testing.T) {
	v1 := ksuid.New()
	v2 := ksuid.New()
	m := &Manifest{
		SSTables: []SSTMeta{
			{ID: "a.sst", Level: 0, VLogID: v1},
			{ID: "b.sst", Level: 0, VLogID: v2},
		},
		VLogs: []VLogMeta{
			{ID: v1, Size: 10, ReferencedBy: []string{"a.sst"}},
			{ID: v2, Size: 20, ReferencedBy: []string{"b.sst"}},
		},
	}

	entry := &ManifestLogEntry{
		Op:               LogOpRemoveSSTable,
		RemoveSSTableIDs: []string{"a.sst"},
	}
	got := ApplyLogEntry(m, entry)

	if len(got.SSTables) != 1 || got.SSTables[0].ID != "b.sst" {
		t.Fatalf("sstables mismatch")
	}
	if len(got.VLogs) != 1 || got.VLogs[0].ID != v2 {
		t.Fatalf("vlogs mismatch")
	}
}

func TestApplyLogEntryCompaction(t *testing.T) {
	oldV1 := ksuid.New()
	oldV2 := ksuid.New()
	newV := ksuid.New()
	m := &Manifest{
		NextEpoch: 3,
		SSTables: []SSTMeta{
			{ID: "a.sst", Level: 0, Epoch: 1},
			{ID: "b.sst", Level: 0, Epoch: 2},
		},
		VLogs: []VLogMeta{
			{ID: oldV1, Size: 10},
			{ID: oldV2, Size: 20},
		},
	}

	entry := &ManifestLogEntry{
		Op: LogOpCompaction,
		Compaction: &CompactionLogPayload{
			RemoveSSTableIDs: []string{"a.sst", "b.sst"},
			AddSSTables: []SSTMeta{
				{ID: "c.sst", Level: 1, Epoch: 5},
			},
			AddVLogs:      []VLogMeta{{ID: newV, Size: 30}},
			RemoveVLogIDs: []string{oldV1.String()},
		},
	}

	got := ApplyLogEntry(m, entry)

	if got.NextEpoch != 6 {
		t.Fatalf("nextEpoch mismatch: %d", got.NextEpoch)
	}
	if len(got.SSTables) != 1 || got.SSTables[0].ID != "c.sst" {
		t.Fatalf("sstables mismatch")
	}
	if len(got.VLogs) != 2 {
		t.Fatalf("vlogs mismatch: %d", len(got.VLogs))
	}
}

func TestApplyLogEntryCheckpoint(t *testing.T) {
	cp := &Manifest{
		Version:   9,
		NextEpoch: 10,
		SSTables:  []SSTMeta{{ID: "x.sst"}},
	}
	entry := &ManifestLogEntry{
		Op:         LogOpCheckpoint,
		Checkpoint: cp,
	}

	got := ApplyLogEntry(&Manifest{Version: 1}, entry)
	if got.Version != 9 || got.NextEpoch != 10 {
		t.Fatalf("checkpoint mismatch")
	}
	if len(got.SSTables) != 1 || got.SSTables[0].ID != "x.sst" {
		t.Fatalf("checkpoint sstable mismatch")
	}
}

func TestApplyLogEntries(t *testing.T) {
	m := &Manifest{Version: 2}
	entries := []*ManifestLogEntry{
		{Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "a.sst", Epoch: 1}},
		{Op: LogOpAddSSTable, SSTable: &SSTMeta{ID: "b.sst", Epoch: 2}},
	}

	got := ApplyLogEntries(m, entries)
	if len(got.SSTables) != 2 {
		t.Fatalf("sstables mismatch: %d", len(got.SSTables))
	}
	if got.NextEpoch != 3 {
		t.Fatalf("nextEpoch mismatch: %d", got.NextEpoch)
	}
}
