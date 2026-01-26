package manifest

import (
	"math"
	"testing"

	"github.com/segmentio/ksuid"
)

func TestManifest_Level_L0Order(t *testing.T) {
	m := Manifest{
		SSTables: []SSTMeta{
			{ID: "a.sst", Level: 0, SeqHi: 3, MinKey: []byte("a"), MaxKey: []byte("b")},
			{ID: "b.sst", Level: 0, SeqHi: 10, MinKey: []byte("c"), MaxKey: []byte("d")},
			{ID: "c.sst", Level: 0, SeqHi: 7, MinKey: []byte("e"), MaxKey: []byte("f")},
			{ID: "l1.sst", Level: 1, SeqHi: 99, MinKey: []byte("a"), MaxKey: []byte("b")},
		},
	}

	got := m.Level(0)
	if len(got) != 3 {
		t.Fatalf("expected 3 L0 sstables, got %d", len(got))
	}
	if got[0].SeqHi != 10 || got[1].SeqHi != 7 || got[2].SeqHi != 3 {
		t.Fatalf("unexpected L0 order: %d %d %d", got[0].SeqHi, got[1].SeqHi, got[2].SeqHi)
	}
}

func TestManifest_Level_L1Order(t *testing.T) {
	m := Manifest{
		SSTables: []SSTMeta{
			{ID: "b.sst", Level: 1, MinKey: []byte("b"), MaxKey: []byte("c")},
			{ID: "a.sst", Level: 1, MinKey: []byte("a"), MaxKey: []byte("a")},
			{ID: "c.sst", Level: 1, MinKey: []byte("c"), MaxKey: []byte("d")},
		},
	}

	got := m.Level(1)
	if len(got) != 3 {
		t.Fatalf("expected 3 L1 sstables, got %d", len(got))
	}
	if string(got[0].MinKey) != "a" || string(got[1].MinKey) != "b" || string(got[2].MinKey) != "c" {
		t.Fatalf("unexpected L1 order: %s %s %s", got[0].MinKey, got[1].MinKey, got[2].MinKey)
	}
}

func TestManifest_LevelCountAndSize(t *testing.T) {
	m := Manifest{
		SSTables: []SSTMeta{
			{Level: 0, Size: 10},
			{Level: 1, Size: 20},
			{Level: 1, Size: 5},
		},
	}

	if got := m.LevelCount(1); got != 2 {
		t.Fatalf("expected L1 count 2, got %d", got)
	}
	if got := m.LevelSize(1); got != 25 {
		t.Fatalf("expected L1 size 25, got %d", got)
	}
}

func TestManifest_MaxLevel(t *testing.T) {
	m := Manifest{
		SSTables: []SSTMeta{
			{Level: 0},
			{Level: 2},
			{Level: 1},
		},
	}

	if got := m.MaxLevel(); got != 2 {
		t.Fatalf("expected max level 2, got %d", got)
	}
}

func TestManifest_VLogLivenessAndGarbageRatio(t *testing.T) {
	v1 := ksuid.New()
	v2 := ksuid.New()
	unknown := ksuid.New()

	m := Manifest{
		VLogs: []VLogMeta{
			{ID: v1, Size: 100},
			{ID: v2, Size: 200},
		},
		SSTables: []SSTMeta{
			{VLogRefs: []VLogRef{{VLogID: v1, LiveBytes: 30}, {VLogID: v2, LiveBytes: 50}}},
			{VLogRefs: []VLogRef{{VLogID: v1, LiveBytes: 10}}},
		},
	}

	live := m.ComputeVLogLiveness()
	if live[v1] != 40 || live[v2] != 50 {
		t.Fatalf("unexpected liveness: v1=%d v2=%d", live[v1], live[v2])
	}

	if got := m.VLogGarbageRatio(v1); math.Abs(got-0.6) > 1e-9 {
		t.Fatalf("unexpected v1 garbage ratio: %f", got)
	}
	if got := m.VLogGarbageRatio(v2); math.Abs(got-0.75) > 1e-9 {
		t.Fatalf("unexpected v2 garbage ratio: %f", got)
	}
	if got := m.VLogGarbageRatio(unknown); got != 1.0 {
		t.Fatalf("unexpected unknown garbage ratio: %f", got)
	}
}

func TestManifest_OverlappingSSTs(t *testing.T) {
	m := Manifest{
		SSTables: []SSTMeta{
			{ID: "a.sst", Level: 1, MinKey: []byte("a"), MaxKey: []byte("c")},
			{ID: "b.sst", Level: 1, MinKey: []byte("e"), MaxKey: []byte("g")},
			{ID: "c.sst", Level: 1, MinKey: []byte("i"), MaxKey: []byte("k")},
			{ID: "bad.sst", Level: 1, MinKey: nil, MaxKey: nil},
		},
	}

	got := m.OverlappingSSTs(1, []byte("b"), []byte("f"))
	if len(got) != 2 {
		t.Fatalf("expected 2 overlapping sstables, got %d", len(got))
	}
	seen := map[string]bool{}
	for _, sst := range got {
		seen[sst.ID] = true
	}
	if !seen["a.sst"] || !seen["b.sst"] {
		t.Fatalf("unexpected overlap set: %v", seen)
	}

	gotAll := m.OverlappingSSTs(1, nil, nil)
	if len(gotAll) != 3 {
		t.Fatalf("expected 3 overlapping sstables, got %d", len(gotAll))
	}
}
