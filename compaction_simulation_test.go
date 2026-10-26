package isledb

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/manifest"
)

// compactionSim runs the planner and scheduler against a manifest with no
// storage behind it: every job is applied to the manifest as the files it
// would write, and new L0 files arrive in proportion to the bytes each job
// reads and writes, as ingest does while compaction runs.
type compactionSim struct {
	t       *testing.T
	c       *compactor
	m       *manifestState
	seq     uint64
	ids     int
	keys    int
	ingest  float64 // bytes of new L0 per byte of compaction I/O
	owed    float64
	written int64
	jobs    map[string]int
	state   manifest.MaintenanceSchedulerState
}

const simKeySpace = 10_000_000

func simKey(k int) []byte { return []byte(fmt.Sprintf("k%08d", k)) }

func simKeyOf(t *testing.T, key []byte) int {
	k, err := strconv.Atoi(string(key[1:]))
	if err != nil {
		t.Fatalf("key %q: %v", key, err)
	}
	return k
}

// newCompactionSim has the settings of a 4 GiB base level, levels 8 times
// larger each, 64 MiB files, and L0 drained at four times L1's overlap or 64
// files deep.
func newCompactionSim(t *testing.T, ingest float64) *compactionSim {
	c := plannerOnlyCompactor()
	c.opts.Trigger.BaseLevelBytes = 4 << 30
	c.opts.Trigger.LevelSizeMultiplier = 8
	c.opts.Trigger.L0ToBaseRatio = 4
	c.opts.Trigger.L0MaxDepth = 64
	c.opts.Trigger.L0MaxAge = 24 * time.Hour
	c.opts.Output.TargetSSTBytes = 64 << 20
	return &compactionSim{t: t, c: c, m: &manifestState{}, ingest: ingest, jobs: map[string]int{}}
}

// addL0 adds one writer merge of random keys: it spans the key space.
func (s *compactionSim) addL0() {
	s.ids++
	sst := sstMetadata{
		ID:        mergedSSTID(s.ids),
		MinKey:    simKey(0),
		MaxKey:    simKey(simKeySpace - 1),
		Size:      64 << 20,
		SeqLo:     s.seq + 1,
		SeqHi:     s.seq + 1000,
		CreatedAt: time.Now(),
	}
	s.seq += 1000
	s.m.AddL0SST(sst)
	s.written += sst.Size
}

// step plans, selects and applies one job, then adds the L0 files ingest
// would have written meanwhile. It reports false when there was nothing to do.
func (s *compactionSim) step() bool {
	s.t.Helper()
	candidates, err := s.c.planCompactionCandidates(s.m)
	if err != nil {
		s.t.Fatalf("plan: %v", err)
	}
	selected := selectCompactionCandidate(candidates, s.state)
	if selected == nil {
		return false
	}
	plan := selected.plan
	s.jobs[fmt.Sprintf("L%d->L%d", plan.sourceLevel, plan.destinationLevel)]++
	// As the writer records it when it publishes the job.
	if plan.sourceLevel == 0 {
		s.state.L0UnitsSinceLower++
	} else {
		s.state.L0UnitsSinceLower = 0
		s.state.NextLowerLevel = plan.sourceLevel + 1
	}
	io := s.apply(plan)
	for s.owed += float64(io) * s.ingest; s.owed >= 64<<20; s.owed -= 64 << 20 {
		s.addL0()
	}
	return true
}

// apply replaces plan's inputs with its outputs and returns the bytes it read
// and wrote: none for a move.
func (s *compactionSim) apply(plan *levelCompactionPlan) int64 {
	s.t.Helper()
	inputs := append(append([]sstMetadata(nil), plan.sourceSSTs...), plan.destinationSSTs...)
	ids := make([]string, 0, len(inputs))
	var bytes int64
	lo, hi := simKeySpace, -1
	seqLo, seqHi := ^uint64(0), uint64(0)
	for _, sst := range inputs {
		ids = append(ids, sst.ID)
		bytes += sst.Size
		lo, hi = min(lo, simKeyOf(s.t, sst.MinKey)), max(hi, simKeyOf(s.t, sst.MaxKey))
		seqLo, seqHi = min(seqLo, sst.SeqLo), max(seqHi, sst.SeqHi)
	}
	if plan.destinationLevel == 0 {
		s.ids++
		out := sstMetadata{ID: combinedSSTID(s.ids), MinKey: simKey(lo), MaxKey: simKey(hi), Size: bytes,
			SeqLo: seqLo, SeqHi: seqHi, CreatedAt: time.Now()}
		if err := s.m.ReplaceL0Run(ids, []sstMetadata{out}); err != nil {
			s.t.Fatalf("combine: %v", err)
		}
		return 2 * bytes
	}
	var outputs []sstMetadata
	if plan.metadataOnly {
		outputs = append(outputs, plan.sourceSSTs...)
	} else {
		target := s.c.opts.Output.TargetSSTBytes
		n := int((bytes + target - 1) / target)
		n = max(1, min(n, hi-lo+1))
		for i := 0; i < n; i++ {
			s.ids++
			a := lo + int(int64(hi-lo+1)*int64(i)/int64(n))
			b := lo + int(int64(hi-lo+1)*int64(i+1)/int64(n)) - 1
			outputs = append(outputs, sstMetadata{
				ID: fmt.Sprintf("sim-%d", s.ids), MinKey: simKey(a), MaxKey: simKey(b), Size: bytes / int64(n),
				SeqLo: seqLo, SeqHi: seqHi, CreatedAt: time.Now(),
			})
		}
	}
	s.m.RemoveCompactionInputs(plan.sourceLevel, plan.destinationLevel, ids)
	s.m.AddLevelSSTs(plan.destinationLevel, outputs)
	if plan.metadataOnly {
		return 0
	}
	return 2 * bytes
}

// fill is each level's bytes over its target.
func (s *compactionSim) fill() map[uint32]float64 {
	f := map[uint32]float64{}
	for i := range s.m.Levels {
		l := &s.m.Levels[i]
		f[l.Number] = float64(l.TotalSize()) / float64(s.c.levelTargetBytes(l.Number))
	}
	return f
}

// TestCompactionBoundsEveryLevelUnderRandomIngest writes 200 GiB of random
// keys, in L0 files spanning the whole key space, while compaction reads and
// writes 17 or 25 bytes for each byte written.
//
// At 25, compaction keeps up: L0 stays shallow and every level near its
// target. At 17, what one maintenance process managed against 24 MiB/s of
// ingest on S3-compatible storage, it cannot: L0 is due on every cycle and
// waits on L1, and deepens however the jobs are ordered. No level may then
// be held back for L1's sake past twice its target: L2 once grew to four
// times its target, and every job into it rewrote ever more of it.
func TestCompactionBoundsEveryLevelUnderRandomIngest(t *testing.T) {
	for _, test := range []struct {
		name       string
		ingest     float64
		maxL0Depth int // 0: no bound, compaction cannot keep up
	}{
		{"keeps up", 1.0 / 25, 200},
		{"falls behind", 1.0 / 17, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newCompactionSim(t, test.ingest)
			for s.written < 4<<30 {
				s.addL0()
			}
			worst := map[uint32]float64{}
			var deepest int
			for jobs := 0; s.written < 200<<30; jobs++ {
				if jobs > 100_000 {
					t.Fatal("no end to the jobs")
				}
				if !s.step() {
					s.addL0()
				}
				// Past the first levels' growth.
				if s.written > 50<<30 {
					for level, f := range s.fill() {
						worst[level] = max(worst[level], f)
					}
					deepest = max(deepest, l0Depth(s.m.L0SSTs))
				}
			}
			t.Logf("jobs %v; fill at worst %v; L0 at deepest %d", s.jobs, worst, deepest)
			for level, f := range worst {
				// A job past the bound adds up to one more job's worth.
				if f > maxFillWhileL0Waits+0.25 {
					t.Errorf("L%d reached %.2f times its target", level, f)
				}
			}
			if s.jobs["L2->L3"] == 0 {
				t.Error("no job from L2")
			}
			if test.maxL0Depth > 0 && deepest > test.maxL0Depth {
				t.Errorf("L0 reached %d deep, want at most %d", deepest, test.maxL0Depth)
			}
		})
	}
}
