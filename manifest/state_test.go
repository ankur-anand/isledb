package manifest

import (
	"testing"
)

func TestDefaultTieredCompactionConfig(t *testing.T) {
	cfg := DefaultTieredConfig()
	if cfg.L0CompactionThreshold != 8 {
		t.Fatalf("expected L0CompactionThreshold 8, got %d", cfg.L0CompactionThreshold)
	}
	if cfg.TierCompactionThreshold != 8 {
		t.Fatalf("expected TierCompactionThreshold 8, got %d", cfg.TierCompactionThreshold)
	}
	if cfg.MaxTiers != 4 {
		t.Fatalf("expected MaxTiers 4, got %d", cfg.MaxTiers)
	}
	if !cfg.LazyLeveling {
		t.Fatalf("expected LazyLeveling true")
	}
}

func TestManifest_L0SSTCount(t *testing.T) {
	m := Manifest{
		L0SSTs: []SSTMeta{
			{ID: "a.sst"},
			{ID: "b.sst"},
			{ID: "c.sst"},
		},
	}

	if got := m.L0SSTCount(); got != 3 {
		t.Fatalf("expected 3 L0 SSTs, got %d", got)
	}
}

func TestManifest_SortedRunCount(t *testing.T) {
	m := Manifest{
		SortedRuns: []SortedRun{
			{ID: 1, SSTs: []SSTMeta{{ID: "a.sst"}}},
			{ID: 2, SSTs: []SSTMeta{{ID: "b.sst"}}},
		},
	}

	if got := m.SortedRunCount(); got != 2 {
		t.Fatalf("expected 2 sorted runs, got %d", got)
	}
}

func TestManifest_AddAndRemoveL0SST(t *testing.T) {
	m := &Manifest{}

	m.AddL0SST(SSTMeta{ID: "a.sst"})
	m.AddL0SST(SSTMeta{ID: "b.sst"})

	if len(m.L0SSTs) != 2 {
		t.Fatalf("expected 2 L0 SSTs, got %d", len(m.L0SSTs))
	}

	if m.L0SSTs[0].ID != "b.sst" {
		t.Fatalf("expected b.sst first (newest), got %s", m.L0SSTs[0].ID)
	}

	m.RemoveL0SSTs([]string{"b.sst"})
	if len(m.L0SSTs) != 1 || m.L0SSTs[0].ID != "a.sst" {
		t.Fatalf("expected only a.sst remaining")
	}
}

func TestManifest_AddAndRemoveSortedRun(t *testing.T) {
	m := &Manifest{}

	id1 := m.AddSortedRun([]SSTMeta{
		{ID: "a.sst", MinKey: []byte("a"), MaxKey: []byte("m")},
		{ID: "b.sst", MinKey: []byte("n"), MaxKey: []byte("z")},
	})

	id2 := m.AddSortedRun([]SSTMeta{
		{ID: "c.sst", MinKey: []byte("a"), MaxKey: []byte("z")},
	})

	if len(m.SortedRuns) != 2 {
		t.Fatalf("expected 2 sorted runs, got %d", len(m.SortedRuns))
	}

	if m.SortedRuns[0].ID != id2 {
		t.Fatalf("expected newest run first")
	}

	m.RemoveSortedRuns([]uint32{id1})
	if len(m.SortedRuns) != 1 || m.SortedRuns[0].ID != id2 {
		t.Fatalf("expected only run %d remaining", id2)
	}
}

func TestManifest_GetSortedRun(t *testing.T) {
	m := &Manifest{
		SortedRuns: []SortedRun{
			{ID: 5, SSTs: []SSTMeta{{ID: "a.sst"}}},
			{ID: 3, SSTs: []SSTMeta{{ID: "b.sst"}}},
		},
	}

	sr := m.GetSortedRun(5)
	if sr == nil || sr.ID != 5 {
		t.Fatalf("expected to find sorted run 5")
	}

	sr = m.GetSortedRun(999)
	if sr != nil {
		t.Fatalf("expected nil for non-existent sorted run")
	}
}

func TestManifest_MaxSeqNum(t *testing.T) {
	m := &Manifest{
		L0SSTs: []SSTMeta{
			{ID: "a.sst", SeqHi: 10},
			{ID: "b.sst", SeqHi: 5},
		},
		SortedRuns: []SortedRun{
			{
				ID: 1,
				SSTs: []SSTMeta{
					{ID: "c.sst", SeqHi: 20},
					{ID: "d.sst", SeqHi: 15},
				},
			},
		},
	}

	if got := m.MaxSeqNum(); got != 20 {
		t.Fatalf("expected max seq 20, got %d", got)
	}
}

func TestManifest_AllSSTIDs(t *testing.T) {
	m := &Manifest{
		L0SSTs: []SSTMeta{
			{ID: "a.sst"},
			{ID: "b.sst"},
		},
		SortedRuns: []SortedRun{
			{ID: 1, SSTs: []SSTMeta{{ID: "c.sst"}, {ID: "d.sst"}}},
		},
	}

	ids := m.AllSSTIDs()
	if len(ids) != 4 {
		t.Fatalf("expected 4 SST IDs, got %d", len(ids))
	}

	idSet := make(map[string]bool)
	for _, id := range ids {
		idSet[id] = true
	}
	for _, expected := range []string{"a.sst", "b.sst", "c.sst", "d.sst"} {
		if !idSet[expected] {
			t.Fatalf("missing expected ID: %s", expected)
		}
	}
}

func TestSortedRun_FindSST(t *testing.T) {
	sr := SortedRun{
		ID: 1,
		SSTs: []SSTMeta{
			{ID: "a.sst", MinKey: []byte("a"), MaxKey: []byte("c")},
			{ID: "b.sst", MinKey: []byte("e"), MaxKey: []byte("g")},
			{ID: "c.sst", MinKey: []byte("i"), MaxKey: []byte("k")},
		},
	}

	tests := []struct {
		key      []byte
		expected string
	}{
		{[]byte("a"), "a.sst"},
		{[]byte("b"), "a.sst"},
		{[]byte("c"), "a.sst"},
		{[]byte("e"), "b.sst"},
		{[]byte("f"), "b.sst"},
		{[]byte("i"), "c.sst"},
		{[]byte("k"), "c.sst"},
	}

	for _, tt := range tests {
		sst := sr.FindSST(tt.key)
		if sst == nil || sst.ID != tt.expected {
			var gotID string
			if sst != nil {
				gotID = sst.ID
			}
			t.Fatalf("FindSST(%s): expected %s, got %s", tt.key, tt.expected, gotID)
		}
	}

	if sst := sr.FindSST([]byte("d")); sst != nil {
		t.Fatalf("expected nil for key between SSTs")
	}
	if sst := sr.FindSST([]byte("z")); sst != nil {
		t.Fatalf("expected nil for key after all SSTs")
	}
}

func TestSortedRun_OverlappingSSTs(t *testing.T) {
	sr := SortedRun{
		ID: 1,
		SSTs: []SSTMeta{
			{ID: "a.sst", MinKey: []byte("a"), MaxKey: []byte("c")},
			{ID: "b.sst", MinKey: []byte("e"), MaxKey: []byte("g")},
			{ID: "c.sst", MinKey: []byte("i"), MaxKey: []byte("k")},
		},
	}

	got := sr.OverlappingSSTs([]byte("b"), []byte("f"))
	if len(got) != 2 {
		t.Fatalf("expected 2 overlapping SSTs, got %d", len(got))
	}
	seen := map[string]bool{}
	for _, sst := range got {
		seen[sst.ID] = true
	}
	if !seen["a.sst"] || !seen["b.sst"] {
		t.Fatalf("unexpected overlap set: %v", seen)
	}

	gotAll := sr.OverlappingSSTs(nil, nil)
	if len(gotAll) != 3 {
		t.Fatalf("expected 3 overlapping SSTs, got %d", len(gotAll))
	}
}

func TestSortedRun_TotalSize(t *testing.T) {
	sr := SortedRun{
		SSTs: []SSTMeta{
			{Size: 100},
			{Size: 200},
			{Size: 50},
		},
	}

	if got := sr.TotalSize(); got != 350 {
		t.Fatalf("expected total size 350, got %d", got)
	}
}

func TestSortedRun_MinMaxKey(t *testing.T) {
	sr := SortedRun{
		SSTs: []SSTMeta{
			{MinKey: []byte("a"), MaxKey: []byte("c")},
			{MinKey: []byte("d"), MaxKey: []byte("f")},
			{MinKey: []byte("g"), MaxKey: []byte("z")},
		},
	}

	if string(sr.MinKey()) != "a" {
		t.Fatalf("expected min key 'a', got '%s'", sr.MinKey())
	}
	if string(sr.MaxKey()) != "z" {
		t.Fatalf("expected max key 'z', got '%s'", sr.MaxKey())
	}

	empty := SortedRun{}
	if empty.MinKey() != nil || empty.MaxKey() != nil {
		t.Fatalf("expected nil keys for empty sorted run")
	}
}

func TestManifest_Clone(t *testing.T) {
	m := &Manifest{
		Version:         2,
		NextEpoch:       10,
		NextSortedRunID: 5,
		L0SSTs: []SSTMeta{
			{ID: "a.sst", MinKey: []byte("a"), MaxKey: []byte("z")},
		},
		SortedRuns: []SortedRun{
			{ID: 1, SSTs: []SSTMeta{{ID: "b.sst", MinKey: []byte("a"), MaxKey: []byte("m")}}},
		},
	}

	clone := m.Clone()

	m.L0SSTs[0].ID = "modified"
	m.SortedRuns[0].SSTs[0].ID = "modified"

	if clone.L0SSTs[0].ID != "a.sst" {
		t.Fatalf("clone L0SST was modified")
	}
	if clone.SortedRuns[0].SSTs[0].ID != "b.sst" {
		t.Fatalf("clone SortedRun SST was modified")
	}
}
