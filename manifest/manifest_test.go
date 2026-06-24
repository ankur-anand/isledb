package manifest

import (
	"testing"
	"time"
)

func TestSnapshotRoundTrip(t *testing.T) {
	m := &Manifest{
		Version:         2,
		NextEpoch:       7,
		LogSeq:          42,
		NextSortedRunID: 3,
		CompactionConfig: CompactionConfig{
			L0CompactionThreshold: 8,
			MinSources:            4,
			MaxSources:            8,
			SizeThreshold:         4,
		},
		L0SSTs: []SSTMeta{
			{
				ID:        "1-1-2-deadbeef.sst",
				Epoch:     1,
				SeqLo:     1,
				SeqHi:     2,
				MinKey:    []byte("a"),
				MaxKey:    []byte("b"),
				Size:      123,
				Checksum:  "sha256:abc",
				CreatedAt: time.Unix(1700000000, 0).UTC(),
				Level:     0,
			},
		},
		SortedRuns: []SortedRun{
			{
				ID: 1,
				SSTs: []SSTMeta{
					{
						ID:     "2-1-5-abcdef.sst",
						MinKey: []byte("c"),
						MaxKey: []byte("z"),
					},
				},
			},
		},
	}

	data, err := EncodeSnapshot(m)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	got, err := DecodeSnapshot(data)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if got.Version != m.Version || got.NextEpoch != m.NextEpoch || got.LogSeq != m.LogSeq {
		t.Fatalf("manifest mismatch: version=%d nextEpoch=%d", got.Version, got.NextEpoch)
	}
	if got.NextSortedRunID != m.NextSortedRunID {
		t.Fatalf("NextSortedRunID mismatch: got %d want %d", got.NextSortedRunID, m.NextSortedRunID)
	}
	if len(got.L0SSTs) != 1 {
		t.Fatalf("manifest L0SSTs mismatch: got %d want 1", len(got.L0SSTs))
	}
	if got.L0SSTs[0].ID != m.L0SSTs[0].ID {
		t.Fatalf("L0 sst id mismatch: got %s want %s", got.L0SSTs[0].ID, m.L0SSTs[0].ID)
	}
	if got.L0SSTs[0].CreatedAt.IsZero() {
		t.Fatalf("createdAt should be preserved")
	}
	if len(got.SortedRuns) != 1 || got.SortedRuns[0].ID != 1 {
		t.Fatalf("SortedRuns mismatch")
	}
	if got.CompactionConfig.L0CompactionThreshold != 8 {
		t.Fatalf("CompactionConfig mismatch")
	}
}

func TestCurrentRoundTrip(t *testing.T) {
	c := &Current{
		Snapshot:    "snapshots/000000001.manifest",
		LogSeqStart: 7,
		NextSeq:     12,
		NextEpoch:   5,
	}

	data, err := EncodeCurrent(c)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	got, err := DecodeCurrent(data)
	if err != nil {
		t.Fatalf("decode current: %v", err)
	}
	if got.Snapshot != c.Snapshot || got.LogSeqStart != c.LogSeqStart || got.NextSeq != c.NextSeq || got.NextEpoch != c.NextEpoch {
		t.Fatalf("current mismatch")
	}
}
