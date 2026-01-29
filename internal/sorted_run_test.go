package internal

import (
	"testing"

	"github.com/ankur-anand/isledb/manifest"
)

func TestSortedRun_FindSST(t *testing.T) {
	sr := NewSortedRun(1, []manifest.SSTMeta{
		{ID: "sst1", MinKey: []byte("a"), MaxKey: []byte("d")},
		{ID: "sst2", MinKey: []byte("e"), MaxKey: []byte("h")},
		{ID: "sst3", MinKey: []byte("i"), MaxKey: []byte("m")},
		{ID: "sst4", MinKey: []byte("n"), MaxKey: []byte("z")},
	})

	tests := []struct {
		name    string
		key     []byte
		wantSST string
		wantNil bool
	}{
		{"key in first SST", []byte("b"), "sst1", false},
		{"key at boundary", []byte("e"), "sst2", false},
		{"key in middle SST", []byte("j"), "sst3", false},
		{"key in last SST", []byte("x"), "sst4", false},
		{"key before all", []byte("0"), "", true},
		{"key after all", []byte("~"), "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sst := sr.FindSST(tt.key)
			if tt.wantNil {
				if sst != nil {
					t.Errorf("expected nil, got SST %s", sst.ID)
				}
				return
			}
			if sst == nil {
				t.Errorf("expected SST %s, got nil", tt.wantSST)
				return
			}
			if sst.ID != tt.wantSST {
				t.Errorf("expected SST %s, got %s", tt.wantSST, sst.ID)
			}
		})
	}
}

func TestSortedRun_OverlappingSSTs(t *testing.T) {
	sr := NewSortedRun(1, []manifest.SSTMeta{
		{ID: "sst1", MinKey: []byte("a"), MaxKey: []byte("d")},
		{ID: "sst2", MinKey: []byte("e"), MaxKey: []byte("h")},
		{ID: "sst3", MinKey: []byte("i"), MaxKey: []byte("m")},
		{ID: "sst4", MinKey: []byte("n"), MaxKey: []byte("z")},
	})

	tests := []struct {
		name      string
		minKey    []byte
		maxKey    []byte
		wantCount int
		wantIDs   []string
	}{
		{"full range", nil, nil, 4, []string{"sst1", "sst2", "sst3", "sst4"}},
		{"first half", []byte("a"), []byte("h"), 2, []string{"sst1", "sst2"}},
		{"middle", []byte("f"), []byte("k"), 2, []string{"sst2", "sst3"}},
		{"single SST", []byte("a"), []byte("c"), 1, []string{"sst1"}},
		{"no overlap before", []byte("0"), []byte("1"), 0, nil},
		{"no overlap after", []byte("~"), []byte("~~"), 0, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ssts := sr.OverlappingSSTs(tt.minKey, tt.maxKey)
			if len(ssts) != tt.wantCount {
				t.Errorf("expected %d SSTs, got %d", tt.wantCount, len(ssts))
				return
			}
			for i, sst := range ssts {
				if i < len(tt.wantIDs) && sst.ID != tt.wantIDs[i] {
					t.Errorf("expected SST[%d] = %s, got %s", i, tt.wantIDs[i], sst.ID)
				}
			}
		})
	}
}

func TestSortedRun_MinMaxKey(t *testing.T) {
	sr := NewSortedRun(1, []manifest.SSTMeta{
		{ID: "sst1", MinKey: []byte("c"), MaxKey: []byte("f")},
		{ID: "sst2", MinKey: []byte("a"), MaxKey: []byte("b")},
		{ID: "sst3", MinKey: []byte("g"), MaxKey: []byte("z")},
	})

	if string(sr.MinKey()) != "a" {
		t.Errorf("expected MinKey 'a', got '%s'", string(sr.MinKey()))
	}
	if string(sr.MaxKey()) != "z" {
		t.Errorf("expected MaxKey 'z', got '%s'", string(sr.MaxKey()))
	}
}

func TestSortedRun_TotalSize(t *testing.T) {
	sr := NewSortedRun(1, []manifest.SSTMeta{
		{ID: "sst1", Size: 100},
		{ID: "sst2", Size: 200},
		{ID: "sst3", Size: 300},
	})

	if sr.TotalSize() != 600 {
		t.Errorf("expected TotalSize 600, got %d", sr.TotalSize())
	}
}

func TestSortedRun_InRange(t *testing.T) {
	sr := NewSortedRun(1, []manifest.SSTMeta{
		{ID: "sst1", MinKey: []byte("b"), MaxKey: []byte("d")},
		{ID: "sst2", MinKey: []byte("e"), MaxKey: []byte("h")},
	})

	tests := []struct {
		key  []byte
		want bool
	}{
		{[]byte("a"), false},
		{[]byte("b"), true},
		{[]byte("c"), true},
		{[]byte("h"), true},
		{[]byte("i"), false},
	}

	for _, tt := range tests {
		got := sr.InRange(tt.key)
		if got != tt.want {
			t.Errorf("InRange(%s) = %v, want %v", string(tt.key), got, tt.want)
		}
	}
}

func TestSortedRun_Empty(t *testing.T) {
	sr := NewSortedRun(1, nil)

	if sr.FindSST([]byte("a")) != nil {
		t.Error("expected nil from empty sorted run")
	}
	if sr.MinKey() != nil {
		t.Error("expected nil MinKey from empty sorted run")
	}
	if sr.MaxKey() != nil {
		t.Error("expected nil MaxKey from empty sorted run")
	}
	if sr.TotalSize() != 0 {
		t.Error("expected 0 TotalSize from empty sorted run")
	}
	if sr.InRange([]byte("a")) {
		t.Error("expected false InRange from empty sorted run")
	}
}

func TestDefaultTieredConfig(t *testing.T) {
	cfg := manifest.DefaultTieredConfig()

	if cfg.L0CompactionThreshold != 8 {
		t.Errorf("expected L0CompactionThreshold 8, got %d", cfg.L0CompactionThreshold)
	}
	if cfg.TierCompactionThreshold != 8 {
		t.Errorf("expected TierCompactionThreshold 8, got %d", cfg.TierCompactionThreshold)
	}
	if cfg.TierMaxRuns != 16 {
		t.Errorf("expected TierMaxRuns 16, got %d", cfg.TierMaxRuns)
	}
	if cfg.MaxTiers != 4 {
		t.Errorf("expected MaxTiers 4, got %d", cfg.MaxTiers)
	}
	if !cfg.LazyLeveling {
		t.Error("expected LazyLeveling true")
	}
}
