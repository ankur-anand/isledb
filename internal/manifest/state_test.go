package manifest

import (
	"bytes"
	"testing"
)

func TestManifestAddAndRemoveSSTables(t *testing.T) {
	m := &Manifest{}
	m.AddL0SST(SSTMeta{ID: "new", MinKey: []byte("m"), MaxKey: []byte("z")})
	m.AddL0SST(SSTMeta{ID: "newest", MinKey: []byte("a"), MaxKey: []byte("f")})
	m.AddLevelSSTs(2, []SSTMeta{{ID: "l2", MinKey: []byte("g"), MaxKey: []byte("h")}})
	m.AddLevelSSTs(1, []SSTMeta{{ID: "l1-b", MinKey: []byte("n"), MaxKey: []byte("p")}, {ID: "l1-a", MinKey: []byte("i"), MaxKey: []byte("k")}})

	if got := m.L0SSTs[0].ID; got != "newest" {
		t.Fatalf("newest L0=%q", got)
	}
	if len(m.Levels) != 2 || m.Levels[0].Number != 1 || m.Levels[1].Number != 2 {
		t.Fatalf("levels=%+v", m.Levels)
	}
	if got := m.Levels[0].SSTs[0].ID; got != "l1-a" {
		t.Fatalf("first L1 SST=%q", got)
	}

	m.RemoveSSTables([]string{"new", "l1-a", "l2"})
	if len(m.L0SSTs) != 1 || m.L0SSTs[0].ID != "newest" {
		t.Fatalf("L0 after remove=%+v", m.L0SSTs)
	}
	if len(m.Levels) != 1 || len(m.Levels[0].SSTs) != 1 || m.Levels[0].SSTs[0].ID != "l1-b" {
		t.Fatalf("levels after remove=%+v", m.Levels)
	}
}

func TestManifestRemoveCompactionInputsOnlyTouchesAdjacentLevels(t *testing.T) {
	m := &Manifest{
		L0SSTs: []SSTMeta{{ID: "l0"}},
		Levels: []Level{
			{Number: 1, SSTs: []SSTMeta{{ID: "l1"}}},
			{Number: 2, SSTs: []SSTMeta{{ID: "same-id"}}},
			{Number: 3, SSTs: []SSTMeta{{ID: "same-id"}}},
		},
	}

	m.RemoveCompactionInputs(1, 2, []string{"l1", "same-id"})

	if m.Level(1) != nil || m.Level(2) != nil {
		t.Fatalf("source or destination level retained compaction inputs: %+v", m.Levels)
	}
	if level := m.Level(3); level == nil || len(level.SSTs) != 1 || level.SSTs[0].ID != "same-id" {
		t.Fatalf("unrelated level was modified: %+v", m.Levels)
	}
	if len(m.L0SSTs) != 1 || m.L0SSTs[0].ID != "l0" {
		t.Fatalf("L0 was modified: %+v", m.L0SSTs)
	}
}

func TestManifestAddLevelSSTsInsertsIntoSortedGap(t *testing.T) {
	m := &Manifest{}
	m.AddLevelSSTs(1, []SSTMeta{
		{ID: "a", MinKey: []byte("a"), MaxKey: []byte("c")},
		{ID: "c", MinKey: []byte("g"), MaxKey: []byte("i")},
	})
	m.AddLevelSSTs(1, []SSTMeta{{ID: "b", MinKey: []byte("d"), MaxKey: []byte("f")}})
	if err := m.ValidateLevels(); err != nil {
		t.Fatal(err)
	}
	if got := []string{m.Levels[0].SSTs[0].ID, m.Levels[0].SSTs[1].ID, m.Levels[0].SSTs[2].ID}; got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("order=%v", got)
	}
}

func TestLevelFindAndOverlap(t *testing.T) {
	level := Level{Number: 1, SSTs: []SSTMeta{
		{ID: "a", MinKey: []byte("a"), MaxKey: []byte("c")},
		{ID: "b", MinKey: []byte("f"), MaxKey: []byte("h")},
		{ID: "c", MinKey: []byte("k"), MaxKey: []byte("m")},
	}}
	if got := level.FindSST([]byte("g")); got == nil || got.ID != "b" {
		t.Fatalf("FindSST(g)=%+v", got)
	}
	if got := level.FindSST([]byte("e")); got != nil {
		t.Fatalf("FindSST(e)=%+v", got)
	}
	overlaps := level.OverlappingSSTs([]byte("c"), []byte("k"))
	if len(overlaps) != 3 {
		t.Fatalf("overlaps=%v", overlaps)
	}
}

func TestManifestValidateLevels(t *testing.T) {
	valid := &Manifest{
		L0SSTs: []SSTMeta{{ID: "l0"}},
		Levels: []Level{
			{Number: 1, SSTs: []SSTMeta{{ID: "a", Level: 1, MinKey: []byte("a"), MaxKey: []byte("c")}, {ID: "b", Level: 1, MinKey: []byte("d"), MaxKey: []byte("f")}}},
			{Number: 3, SSTs: []SSTMeta{{ID: "c", Level: 3, MinKey: []byte("a"), MaxKey: []byte("z")}}},
		},
	}
	if err := valid.ValidateLevels(); err != nil {
		t.Fatalf("ValidateLevels()=%v", err)
	}

	cases := []*Manifest{
		{Levels: []Level{{Number: 0, SSTs: []SSTMeta{{ID: "a"}}}}},
		{Levels: []Level{{Number: 2, SSTs: []SSTMeta{{ID: "a"}}}, {Number: 1, SSTs: []SSTMeta{{ID: "b"}}}}},
		{L0SSTs: []SSTMeta{{ID: "a"}}, Levels: []Level{{Number: 1, SSTs: []SSTMeta{{ID: "a"}}}}},
		{Levels: []Level{{Number: 1, SSTs: []SSTMeta{{ID: "a", MinKey: []byte("a"), MaxKey: []byte("d")}, {ID: "b", MinKey: []byte("d"), MaxKey: []byte("f")}}}}},
	}
	for i, m := range cases {
		if err := m.ValidateLevels(); err == nil {
			t.Fatalf("case %d unexpectedly valid", i)
		}
	}
}

func TestManifestCloneIsIndependent(t *testing.T) {
	m := &Manifest{
		WriterFence: &FenceToken{Epoch: 3, Owner: "writer"},
		L0SSTs:      []SSTMeta{{ID: "l0"}},
		Levels:      []Level{{Number: 1, SSTs: []SSTMeta{{ID: "l1"}}}},
	}
	clone := m.Clone()
	m.WriterFence.Owner = "changed"
	m.L0SSTs[0].ID = "changed"
	m.Levels[0].SSTs[0].ID = "changed"
	if clone.WriterFence.Owner != "writer" || clone.L0SSTs[0].ID != "l0" || clone.Levels[0].SSTs[0].ID != "l1" {
		t.Fatalf("clone changed with source: %+v", clone)
	}
}

func TestManifestSummaryHelpers(t *testing.T) {
	m := &Manifest{
		L0SSTs: []SSTMeta{{ID: "l0", SeqHi: 4, MinKey: []byte("m"), MaxKey: []byte("z")}},
		Levels: []Level{{Number: 1, SSTs: []SSTMeta{{ID: "l1", SeqHi: 9, MinKey: []byte("a"), MaxKey: []byte("f")}}}},
	}
	if got := m.MaxSeqNum(); got != 9 {
		t.Fatalf("MaxSeqNum=%d", got)
	}
	if got := m.AllSSTIDs(); len(got) != 2 || got[0] != "l0" || got[1] != "l1" {
		t.Fatalf("AllSSTIDs=%v", got)
	}
	if !bytes.Equal(m.MinKey(), []byte("a")) || !bytes.Equal(m.MaxKey(), []byte("z")) {
		t.Fatalf("range=%q..%q", m.MinKey(), m.MaxKey())
	}
}
