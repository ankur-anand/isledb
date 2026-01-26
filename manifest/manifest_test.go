package manifest

import (
	"testing"
	"time"

	"github.com/segmentio/ksuid"
)

func TestSnapshotRoundTrip(t *testing.T) {
	m := &Manifest{
		Version:   2,
		NextEpoch: 7,
		LevelConfig: LevelConfig{
			MaxLevels:             4,
			L0CompactionThreshold: 4,
			L1TargetSize:          64 << 20,
			LevelSizeMultiplier:   10,
			VLogGarbageThreshold:  0.5,
		},
		SSTables: []SSTMeta{
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
				VLogID:    ksuid.New(),
				VLogSize:  456,
				VLogRefs: []VLogRef{
					{VLogID: ksuid.New(), LiveBytes: 100},
				},
			},
		},
		VLogs: []VLogMeta{
			{ID: ksuid.New(), Size: 999, ReferencedBy: []string{"1-1-2-deadbeef.sst"}},
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
	if got.Version != m.Version || got.NextEpoch != m.NextEpoch {
		t.Fatalf("manifest mismatch: version=%d nextEpoch=%d", got.Version, got.NextEpoch)
	}
	if len(got.SSTables) != 1 || len(got.VLogs) != 1 {
		t.Fatalf("manifest slices mismatch: sst=%d vlogs=%d", len(got.SSTables), len(got.VLogs))
	}
	if got.SSTables[0].ID != m.SSTables[0].ID {
		t.Fatalf("sst id mismatch: got %s want %s", got.SSTables[0].ID, m.SSTables[0].ID)
	}
	if got.SSTables[0].CreatedAt.IsZero() {
		t.Fatalf("createdAt should be preserved")
	}
}

func TestCurrentRoundTrip(t *testing.T) {
	c := &Current{
		Snapshot:  "snapshots/000000001.manifest",
		Logs:      []string{"log/000000002.log"},
		NextSeq:   12,
		NextEpoch: 5,
	}

	data, err := EncodeCurrent(c)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	got, err := DecodeCurrent(data)
	if err != nil {
		t.Fatalf("decode current: %v", err)
	}
	if got.Snapshot != c.Snapshot || got.NextSeq != c.NextSeq || got.NextEpoch != c.NextEpoch {
		t.Fatalf("current mismatch")
	}
	if len(got.Logs) != 1 || got.Logs[0] != c.Logs[0] {
		t.Fatalf("logs mismatch: %v", got.Logs)
	}
}
