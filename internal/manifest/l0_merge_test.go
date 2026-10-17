package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// l0Of returns an L0, newest first, with one SST per id spanning the same
// keys, and sequences rising toward the front.
func l0Of(ids ...string) []SSTMeta {
	l0 := make([]SSTMeta, len(ids))
	for i, id := range ids {
		seq := uint64(10 * (len(ids) - i))
		l0[i] = SSTMeta{ID: id, SeqLo: seq, SeqHi: seq + 5, MinKey: []byte("a"), MaxKey: []byte("z")}
	}
	return l0
}

func l0IDs(m *Manifest) string {
	ids := make([]string, len(m.L0SSTs))
	for i, sst := range m.L0SSTs {
		ids[i] = sst.ID
	}
	return strings.Join(ids, ",")
}

func TestReplaceL0RunKeepsThePosition(t *testing.T) {
	for _, c := range []struct {
		name string
		run  []string
		want string
	}{
		{"newest", []string{"e", "d"}, "ed,c,b,a"},
		{"middle", []string{"c", "b"}, "e,d,cb,a"},
		{"oldest", []string{"b", "a"}, "e,d,c,ba"},
		{"everything", []string{"e", "d", "c", "b", "a"}, "all"},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := &Manifest{L0SSTs: l0Of("e", "d", "c", "b", "a")}
			lo, hi := ^uint64(0), uint64(0)
			for _, sst := range m.L0SSTs {
				for _, id := range c.run {
					if sst.ID == id {
						lo, hi = min(lo, sst.SeqLo), max(hi, sst.SeqHi)
					}
				}
			}
			id := strings.Join(c.run, "")
			if len(c.run) == 5 {
				id = "all"
			}
			out := SSTMeta{ID: id, SeqLo: lo, SeqHi: hi, MinKey: []byte("a"), MaxKey: []byte("z"), Level: 7}
			if err := m.ReplaceL0Run(c.run, []SSTMeta{out}); err != nil {
				t.Fatal(err)
			}
			if got := l0IDs(m); got != c.want {
				t.Fatalf("L0 %s, want %s", got, c.want)
			}
			if m.L0SSTs[0].Level != 0 && len(c.run) == 5 {
				t.Fatal("output not placed at level 0")
			}
			for i := range m.L0SSTs {
				if err := checkL0Placement(m.L0SSTs, i); err != nil {
					t.Fatalf("order after a correct merge: %v", err)
				}
			}
		})
	}
}

func TestReplaceL0RunRefusesBadRuns(t *testing.T) {
	out := SSTMeta{ID: "out", SeqLo: 20, SeqHi: 35}
	for _, c := range []struct {
		name    string
		run     []string
		outputs []SSTMeta
		want    string
	}{
		{"split", []string{"d", "b"}, []SSTMeta{out}, "not adjacent"},
		{"missing", []string{"d", "x"}, []SSTMeta{out}, "not all in L0"},
		{"empty", nil, nil, "no inputs"},
		{"two outputs", []string{"c", "b"}, []SSTMeta{out, out}, "at most 1"},
		{"sequences outside", []string{"c", "b"}, []SSTMeta{{ID: "out", SeqLo: 20, SeqHi: 99}}, "outside its inputs"},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := &Manifest{L0SSTs: l0Of("e", "d", "c", "b", "a")}
			err := m.ReplaceL0Run(c.run, c.outputs)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want one saying %q", err, c.want)
			}
			if got := l0IDs(m); got != "e,d,c,b,a" {
				t.Fatalf("a refused merge changed L0 to %s", got)
			}
		})
	}
}

func TestReplaceL0RunWithNoOutputRemovesTheRun(t *testing.T) {
	m := &Manifest{L0SSTs: l0Of("c", "b", "a")}
	if err := m.ReplaceL0Run([]string{"b"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := l0IDs(m); got != "c,a" {
		t.Fatalf("L0 %s, want c,a", got)
	}
}

func TestReplaceL0RunRefusesAnOutputOutOfOrder(t *testing.T) {
	// An SST older than the run sits ahead of it: the merged output would hide
	// the run's newer data behind it.
	l0 := l0Of("c", "b", "a")
	l0[0].SeqLo, l0[0].SeqHi = 1, 2
	m := &Manifest{L0SSTs: l0}
	out := SSTMeta{ID: "ba", SeqLo: 10, SeqHi: 25, MinKey: []byte("a"), MaxKey: []byte("z")}
	if err := m.ReplaceL0Run([]string{"b", "a"}, []SSTMeta{out}); err == nil || !strings.Contains(err.Error(), "newer data") {
		t.Fatalf("error %v, want the output refused", err)
	}
	if got := l0IDs(m); got != "c,b,a" {
		t.Fatalf("a refused merge changed L0 to %s", got)
	}
	// SSTs that share no keys may sit in any order.
	out.MinKey, out.MaxKey = []byte("0"), []byte("1")
	l0[0].MinKey, l0[0].MaxKey = []byte("x"), []byte("z")
	if err := m.ReplaceL0Run([]string{"b", "a"}, []SSTMeta{out}); err != nil {
		t.Fatalf("disjoint output refused: %v", err)
	}
}

func TestValidateLevelsLeavesL0OrderToMerges(t *testing.T) {
	// Loading an existing database never fails on L0 order; only a merge is
	// checked, when it is applied.
	l0 := l0Of("c", "b", "a")
	l0[0], l0[2] = l0[2], l0[0]
	if err := (&Manifest{L0SSTs: l0}).ValidateLevels(); err != nil {
		t.Fatalf("ValidateLevels: %v", err)
	}
}

func TestApplyL0MergeEntry(t *testing.T) {
	m := &Manifest{L0SSTs: l0Of("c", "b", "a")}
	entry := &ManifestLogEntry{Op: LogOpCompaction, Compaction: &CompactionLogPayload{
		RemoveSSTableIDs: []string{"b", "a"},
		AddSSTables:      []SSTMeta{{ID: "ba", SeqLo: 10, SeqHi: 25, MinKey: []byte("a"), MaxKey: []byte("z")}},
	}}
	if err := validateCompactionPayload(*entry.Compaction); err != nil {
		t.Fatalf("payload refused: %v", err)
	}
	got, err := ApplyLogEntry(m, entry)
	if err != nil {
		t.Fatal(err)
	}
	if ids := l0IDs(got); ids != "c,ba" {
		t.Fatalf("L0 %s, want c,ba", ids)
	}

	// Replaying the same entry again finds its inputs gone.
	if _, err := ApplyLogEntry(got, entry); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("second apply: %v, want ErrInvalidManifest", err)
	}
}

func TestL0MergePayloadAllowsOneOutput(t *testing.T) {
	payload := CompactionLogPayload{
		RemoveSSTableIDs: []string{"b", "a"},
		AddSSTables:      []SSTMeta{{ID: "x", MinKey: []byte("a"), MaxKey: []byte("b")}, {ID: "y", MinKey: []byte("c"), MaxKey: []byte("d")}},
	}
	if err := validateCompactionPayload(payload); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("two outputs: %v, want ErrInvalidManifest", err)
	}
	payload.SourceLevel, payload.DestinationLevel = 1, 1
	if err := validateCompactionPayload(payload); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("L1 to L1: %v, want ErrInvalidManifest", err)
	}
}

func TestOldestData(t *testing.T) {
	created := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	oldest := created.Add(-time.Hour)
	if got := (SSTMeta{CreatedAt: created}).OldestData(); !got.Equal(created) {
		t.Fatalf("without OldestDataAt: %v, want CreatedAt", got)
	}
	meta := SSTMeta{CreatedAt: created, OldestDataAt: oldest}
	if got := meta.OldestData(); !got.Equal(oldest) {
		t.Fatalf("with OldestDataAt: %v", got)
	}
	body, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"oldest_data_at":"2026-10-17T11:00:00Z"`) {
		t.Fatalf("OldestDataAt not encoded: %s", body)
	}
}

func TestL0MergeCommandIsCheckedAndMovesTheLayout(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("l0-merge-command")
	defer store.Close()
	ms := NewStoreWithStorage(NewBlobStoreBackend(store))
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer"); err != nil {
		t.Fatal(err)
	}
	l0 := l0Of("c", "b", "a")
	for i := len(l0) - 1; i >= 0; i-- {
		if _, err := ms.AppendAddSSTableWithFence(ctx, l0[i]); err != nil {
			t.Fatal(err)
		}
	}
	token, err := ms.ClaimMaintenance(ctx, "maintenance")
	if err != nil {
		t.Fatal(err)
	}
	layout := func() int {
		t.Helper()
		current, err := ms.ReadCurrentData(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return current.LayoutVersion
	}
	merge := func(id string, inputs []string, out SSTMeta) MaintenanceApplyResult {
		t.Helper()
		var retired []RetiredObject
		for _, in := range inputs {
			retired = append(retired, RetiredObject{Kind: RetiredObjectSST, ID: in, Key: "sstable/" + in})
		}
		head, err := ms.StageMaintenance(ctx, MaintenanceCommand{ID: id, Kind: MaintenanceCommandCompaction,
			Compaction: &CompactionCommand{
				Payload:        CompactionLogPayload{RemoveSSTableIDs: inputs, AddSSTables: []SSTMeta{out}},
				RetiredObjects: retired,
			}}, token)
		if err != nil {
			t.Fatal(err)
		}
		staged := *head.Pending
		result, err := ms.ApplyMaintenanceCommand(ctx, staged)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ms.ClearMaintenance(ctx, staged.ID, staged.Epoch, staged.Generation, token); err != nil {
			t.Fatal(err)
		}
		return result
	}
	if got := layout(); got != LayoutVersion {
		t.Fatalf("layout %d before any merge, want %d", got, LayoutVersion)
	}

	// c and a are not adjacent: the writer rejects the merge instead of
	// committing an entry no reader could replay.
	before, err := ms.ReadCurrentData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	split := SSTMeta{ID: "ca", SeqLo: 10, SeqHi: 35, MinKey: []byte("a"), MaxKey: []byte("z")}
	if result := merge("split", []string{"c", "a"}, split); result.Status != MaintenanceStatusRejected {
		t.Fatalf("split merge %+v, want rejected", result)
	}
	after, err := ms.ReadCurrentData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.NextSeq != before.NextSeq || after.LayoutVersion != LayoutVersion {
		t.Fatalf("a rejected merge changed CURRENT: next_seq %d -> %d, layout %d", before.NextSeq, after.NextSeq, after.LayoutVersion)
	}

	good := SSTMeta{ID: "ba", SeqLo: 10, SeqHi: 25, MinKey: []byte("a"), MaxKey: []byte("z")}
	if result := merge("good", []string{"b", "a"}, good); result.Status != MaintenanceStatusApplied {
		t.Fatalf("merge %+v, want applied", result)
	}
	if got := layout(); got != LayoutVersionL0Merge {
		t.Fatalf("layout %d after a merge, want %d", got, LayoutVersionL0Merge)
	}
	replayed, err := NewStoreWithStorage(NewBlobStoreBackend(store)).Replay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ids := l0IDs(replayed); ids != "c,ba" {
		t.Fatalf("replayed L0 %s, want c,ba", ids)
	}
}

func TestCurrentLayoutVersions(t *testing.T) {
	for v, ok := range map[int]bool{1: false, LayoutVersion: true, LayoutVersionL0Merge: true, LayoutVersionL0Merge + 1: false} {
		err := validateCurrentFormat(&Current{LayoutVersion: v, Format: CurrentFormat})
		if (err == nil) != ok {
			t.Errorf("layout %d: %v, want accepted %v", v, err, ok)
		}
	}
}
