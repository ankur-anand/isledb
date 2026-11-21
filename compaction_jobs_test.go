package isledb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func levelCandidate(source, destination uint32) compactionCandidate {
	return compactionCandidate{plan: &levelCompactionPlan{sourceLevel: source, destinationLevel: destination}, workUnits: 1}
}

func candidateLevels(candidates []compactionCandidate) [][2]uint32 {
	var levels [][2]uint32
	for _, c := range candidates {
		levels = append(levels, planLevels(c.plan))
	}
	return levels
}

func TestUnlockedLeavesOutHeldLevels(t *testing.T) {
	c := &compactor{locks: map[*levelLock]struct{}{}}
	candidates := []compactionCandidate{levelCandidate(1, 2), levelCandidate(2, 3), levelCandidate(3, 4)}
	if got := c.unlocked(candidates); len(got) != 3 {
		t.Fatalf("with nothing held, unlocked kept %v", candidateLevels(got))
	}
	// L2 into L3 runs: L1 into L2 and L3 into L4 each share a level with it.
	c.lock(levelCandidate(2, 3), true)
	if got := c.unlocked(candidates); len(got) != 0 {
		t.Fatalf("unlocked kept %v beside a job on L2 and L3", candidateLevels(got))
	}
	// A combine holds L0 alone, so L1 into L2 may run beside it.
	c = &compactor{locks: map[*levelLock]struct{}{}}
	c.lock(levelCandidate(0, 0), true)
	got := c.unlocked([]compactionCandidate{levelCandidate(1, 2), levelCandidate(3, 4)})
	if len(got) != 2 {
		t.Fatalf("beside a combine, unlocked kept %v, want both", candidateLevels(got))
	}
}

// While L0's drain waits for L1, no other job may take L1: one L1 job after
// another would keep it from L0 for ever.
func TestUnlockedKeepsLevelsForAWaitingL0(t *testing.T) {
	c := &compactor{locks: map[*levelLock]struct{}{}}
	c.lock(levelCandidate(1, 2), true)
	drain := levelCandidate(0, 1)
	drain.forL0 = true
	// Another L1 job would share L1 with the running one anyway; the test is
	// a job on L0's levels that the running job does not hold.
	combine := levelCandidate(0, 0)
	got := c.unlocked([]compactionCandidate{drain, combine, levelCandidate(3, 4)})
	if len(got) != 1 || got[0].plan.sourceLevel != 3 {
		t.Fatalf("unlocked kept %v, want only L3 into L4", candidateLevels(got))
	}
	// Once L1 is free, L0's drain goes ahead.
	c.release("")
	if got := c.unlocked([]compactionCandidate{drain}); len(got) != 1 {
		t.Fatal("L0's drain was still held back with L1 free")
	}
}

func TestReleaseFreesOnlyTheAnsweredJob(t *testing.T) {
	c := &compactor{locks: map[*levelLock]struct{}{}}
	a := c.lock(levelCandidate(0, 1), true)
	b := c.lock(levelCandidate(2, 3), true)
	a.commandID, b.commandID = "a", "b"
	c.release("a")
	if _, ok := c.locks[a]; ok {
		t.Fatal("the answered job still holds its levels")
	}
	if _, ok := c.locks[b]; !ok {
		t.Fatal("another job's levels were released")
	}
}

func TestMaintenanceMaxJobsDefaultsAndBounds(t *testing.T) {
	opts, err := normalizeMaintenanceOptions(MaintenanceOptions{})
	if err != nil || opts.sstCompaction.MaxJobs != 2 {
		t.Fatalf("default max jobs = %d, %v; want 2", opts.sstCompaction.MaxJobs, err)
	}
	for _, n := range []int{-1, maxCompactionJobs + 1} {
		_, err := normalizeMaintenanceOptions(MaintenanceOptions{SSTCompaction: SSTCompactionOptions{MaxJobs: n}})
		if !errors.Is(err, ErrInvalidMaintenanceOptions) {
			t.Errorf("max jobs %d: err = %v, want ErrInvalidMaintenanceOptions", n, err)
		}
	}
}

// With L0 due and every level over its target, two slots go to L0's drain
// and the first lower job that shares no level with it, in the order one
// job at a time would run them.
func TestPickJobsFillsSlotsOnSeparateLevels(t *testing.T) {
	drain := levelCandidate(0, 1)
	drain.forL0 = true
	candidates := func() []compactionCandidate {
		return []compactionCandidate{drain, levelCandidate(1, 2), levelCandidate(2, 3), levelCandidate(3, 4)}
	}
	selector := func(_ *manifest.Current, c []compactionCandidate) *compactionCandidate {
		return selectCompactionCandidate(c, manifest.MaintenanceSchedulerState{})
	}
	got := candidateLevels(pickJobs(nil, candidates(), 2, selector))
	if want := [][2]uint32{{0, 1}, {2, 3}}; !slices.Equal(got, want) {
		t.Fatalf("picked %v, want %v", got, want)
	}
	// Three slots: L3 into L4 shares L3 with L2 into L3, so still two.
	if got := pickJobs(nil, candidates(), 3, selector); len(got) != 2 {
		t.Fatalf("picked %v with three slots, want two", candidateLevels(got))
	}
	// One slot is one job at a time: L0 first, as today.
	if got := candidateLevels(pickJobs(nil, candidates(), 1, selector)); !slices.Equal(got, [][2]uint32{{0, 1}}) {
		t.Fatalf("picked %v with one slot", got)
	}
	// After an L0 job, the rotation gives the lower levels their turn first.
	turn := func(_ *manifest.Current, c []compactionCandidate) *compactionCandidate {
		return selectCompactionCandidate(c, manifest.MaintenanceSchedulerState{L0UnitsSinceLower: 1, NextLowerLevel: 2})
	}
	got = candidateLevels(pickJobs(nil, candidates(), 2, turn))
	if want := [][2]uint32{{2, 3}, {0, 1}}; !slices.Equal(got, want) {
		t.Fatalf("after an L0 job picked %v, want %v", got, want)
	}
}

// Merging carries every field, so a cycle that fails loses none of the jobs
// it counted. A field added to SSTCompactionStats must be merged too.
func TestMergeSSTCompactionStatsCarriesEveryField(t *testing.T) {
	var one SSTCompactionStats
	v := reflect.ValueOf(&one).Elem()
	for i := range v.NumField() {
		switch f := v.Field(i); f.Kind() {
		case reflect.Int, reflect.Int64:
			f.SetInt(1)
		case reflect.Uint32:
			f.SetUint(1)
		case reflect.Slice:
			f.Set(reflect.ValueOf([]CompactionRun{{SourceLevel: 1}}))
		default:
			t.Fatalf("field %s: kind %s not handled", v.Type().Field(i).Name, f.Kind())
		}
	}
	if got := mergeSSTCompactionStats(SSTCompactionStats{}, one); !reflect.DeepEqual(got, one) {
		t.Fatalf("merged %+v, want %+v", got, one)
	}
}

// While the writer has yet to answer one job's command, a job on other
// levels still starts: the slot does not wait for the writer.
func TestMaintenanceStartsJobsWhileTheWriterAnswers(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("maintenance-jobs-while-waiting")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "writer-jobs-while-waiting"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}
	// L0 files that share no keys move down the levels without being read.
	addL0 := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s-%02d", prefix, i))
			if _, err := db.manifestStore.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{
				ID: fmt.Sprintf("%s-sst-%02d", prefix, i), Level: 0, MinKey: key, MaxKey: key, Size: 1 << 20,
			}); err != nil {
				t.Fatalf("add L0 SST %d: %v", i, err)
			}
		}
	}
	addL0("a", 64)
	opts := DefaultMaintenanceOptions()
	opts.SSTCompaction.L0TriggerSSTs = 8
	opts.SSTCompaction.L0MaxAge = time.Nanosecond
	opts.SSTCompaction.BaseLevelBytes = 1 << 20
	opts.SSTCompaction.MaxJobs = 2
	opts.ManifestCheckpoint.TargetReplayPages = ^uint64(0)
	opts.ManifestCheckpoint.TargetReplayBytes = ^uint64(0)
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	defer maintenance.Close(ctx)

	// The 64 MiB moves from L0 to L1, L2 and L3, one level over its target
	// at a time; the last move is staged and left unanswered.
	var staged MaintenanceStats
	for i, from := range []uint32{0, 1, 2} {
		if i > 0 {
			if _, err := db.manifestStore.ApplyPendingMaintenance(ctx); err != nil {
				t.Fatalf("ApplyPendingMaintenance: %v", err)
			}
		}
		if staged, err = maintenance.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce(from L%d): %v", from, err)
		}
		if staged.State != MaintenanceWaitingForWriter || len(staged.SSTCompaction.Runs) != 1 || staged.SSTCompaction.Runs[0].SourceLevel != from {
			t.Fatalf("cycle %d: state %v, runs %+v; want the move from L%d staged", i, staged.State, staged.SSTCompaction.Runs, from)
		}
	}
	// New L0 files arrive while the writer has yet to answer.
	addL0("z", 8)
	waiting, err := maintenance.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce(waiting): %v", err)
	}
	if len(waiting.SSTCompaction.Runs) != 1 || waiting.SSTCompaction.Runs[0].SourceLevel != 0 {
		t.Fatalf("while waiting, runs %+v; want the L0 job started", waiting.SSTCompaction.Runs)
	}
	if !waiting.WaitingSince.Equal(staged.WaitingSince) || maintenance.inLine() != 1 {
		t.Fatalf("waiting since %v (want %v), %d in line (want 1)", waiting.WaitingSince, staged.WaitingSince, maintenance.inLine())
	}
	// Each answer lets the next command in line be staged, and none is rejected.
	for i := 0; i < 2; i++ {
		if _, err := db.manifestStore.ApplyPendingMaintenance(ctx); err != nil {
			t.Fatalf("ApplyPendingMaintenance: %v", err)
		}
		stats, err := maintenance.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce(answered %d): %v", i, err)
		}
		if stats.SSTCompaction.RejectedJobs != 0 {
			t.Fatalf("answer %d: %d jobs rejected", i, stats.SSTCompaction.RejectedJobs)
		}
	}
	state, err := db.manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(state.L0SSTs) != 0 || state.Level(1) == nil || len(state.Level(1).SSTs) != 8 ||
		state.Level(3) == nil || len(state.Level(3).SSTs) != 64 {
		t.Fatalf("L0 %d files, L1 %v, L3 %v; want the new files in L1 and the first 64 in L3",
			len(state.L0SSTs), state.Level(1), state.Level(3))
	}
}

// While an L0 job runs, the manifest still lists the files it merges. Planned
// on again, they would show L0 waiting for L1 and critical, and the levels
// below would be held back as for an L0 that waits, though L0's job runs.
func TestPlanSkipsL0WhileItsJobRuns(t *testing.T) {
	c, m := l0WaitingOnL1(t)
	c.locks = map[*levelLock]struct{}{}
	addLevelFile(m, 3, 120<<20) // over L3's target, under twice it

	// L0 waits for L1: only the drain it waits on is offered, as before.
	candidates, err := c.planCompactionCandidates(m)
	if err != nil {
		t.Fatalf("planCompactionCandidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].plan.sourceLevel != 1 || !candidates[0].forL0 {
		t.Fatalf("with L0 waiting, candidates %v; want only the L1 drain L0 waits on", candidateLevels(candidates))
	}

	// L0's drain runs: L0 is not planned, so L3 takes its turn, not urgent.
	c.lock(levelCandidate(0, 1), true)
	candidates, err = c.planCompactionCandidates(m)
	if err != nil {
		t.Fatalf("planCompactionCandidates: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.forL0 || candidate.plan.sourceLevel == 0 {
			t.Fatalf("L0 planned again while its job runs: %v", candidateLevels(candidates))
		}
	}
	got := c.unlocked(candidates)
	if len(got) != 1 || got[0].plan.sourceLevel != 3 || got[0].critical {
		t.Fatalf("beside L0's drain, unlocked %v (critical %v); want L3, not urgent",
			candidateLevels(got), len(got) == 1 && got[0].critical)
	}
}
