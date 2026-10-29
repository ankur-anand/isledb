package isledb

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// mergedSSTID names an L0 file as the writer's merges are named.
func mergedSSTID(i int) string {
	return fmt.Sprintf("merged-%064x-%04d.sst", i, 1)
}

// combinedSSTID names an L0 file as maintenance's outputs are named: a
// combine of the writer's merges.
func combinedSSTID(i int) string {
	return fmt.Sprintf("compacted-%064x-%04d.sst", i, 1)
}

// l0Planner builds an L0, given newest first by size, of merged SSTs that all
// span key-000..key-100 and were written now, over an L1 of l1Bytes covering
// the same keys.
func l0Planner(t *testing.T, sizes []int64, l1Bytes int64) (*compactor, *manifestState) {
	t.Helper()
	c := plannerOnlyCompactor()
	m := &manifestState{}
	for i := len(sizes) - 1; i >= 0; i-- {
		sst := plannerSST(0, 0, 100)
		sst.ID = mergedSSTID(i)
		sst.Size = sizes[i]
		sst.SeqLo, sst.SeqHi = uint64(100-i)*10, uint64(100-i)*10+5
		sst.CreatedAt = time.Now()
		m.AddL0SST(sst)
	}
	if l1Bytes > 0 {
		l1 := plannerSST(1, 0, 100)
		l1.Size = l1Bytes
		m.AddLevelSSTs(1, []sstMetadata{l1})
	}
	return c, m
}

// rawFlush is one writer flush file, named by the writer's grammar, written
// at createdAt.
func rawFlush(i int, size int64, createdAt time.Time) sstMetadata {
	sst := plannerSST(0, 0, 100)
	sst.ID = buildSSTIDWithTimestamp(uint64(1000+i), uint64(2000+10*i), uint64(2000+10*i+5), createdAt)
	sst.Size = size
	sst.SeqLo, sst.SeqHi = uint64(2000+10*i), uint64(2000+10*i+5)
	sst.CreatedAt = createdAt
	return sst
}

func repeat(size int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = size
	}
	return out
}

func l0IDList(ssts []sstMetadata) string {
	ids := make([]string, len(ssts))
	for i, sst := range ssts {
		ids[i] = sst.ID
	}
	return strings.Join(ids, ",")
}

func TestL0PlannerLeavesYoungFlushFilesToTheWriter(t *testing.T) {
	// Deep enough and large enough against L1 to drain, if these were
	// maintenance's to drain. They are the writer's: it merges them from
	// memory within its own age limit.
	c := plannerOnlyCompactor()
	m := &manifestState{}
	for i := 15; i >= 0; i-- {
		m.AddL0SST(rawFlush(i, 8<<20, time.Now()))
	}
	if got := c.drainableL0(m, time.Now()); len(got) != 0 {
		t.Fatalf("drainable %s, want nothing: every file is a young flush", l0IDList(got))
	}
	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan != nil {
		t.Fatalf("plan %+v, want none", plan)
	}
	if ssts, enough := (&compactor{opts: c.opts, manifest: m}).l0Work(); ssts != 0 || enough {
		t.Fatalf("l0Work = %d, %v; want nothing to do", ssts, enough)
	}
}

func TestL0PlannerDrainsFlushFilesPastTheAgeLimit(t *testing.T) {
	// A writer that does not merge, or died: its flush files are nobody's
	// once they are older than the age limit, and drain like any other.
	c := plannerOnlyCompactor()
	m := &manifestState{}
	old := time.Now().Add(-c.opts.Trigger.L0MaxAge - time.Minute)
	for i := 7; i >= 0; i-- {
		m.AddL0SST(rawFlush(i, 25<<10, old))
	}
	if got := c.drainableL0(m, time.Now()); len(got) != 8 {
		t.Fatalf("drainable %d files, want all 8", len(got))
	}
	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.destinationLevel != 1 || len(plan.sourceSSTs) != 8 {
		t.Fatalf("plan %+v, want all 8 into L1", plan)
	}
}

func TestL0PlannerDrainsEverythingBelowTheNewestMergedFile(t *testing.T) {
	c := plannerOnlyCompactor()
	m := &manifestState{}
	now := time.Now()
	// Oldest first as committed: two merged files, a flush the writer did not
	// keep a copy of, a merged file, another skipped flush, a merged file,
	// then the writer's current run.
	m.AddL0SST(plannerSSTNamed(mergedSSTID(1), 64<<20, now))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(2), 64<<20, now))
	m.AddL0SST(rawFlush(3, 2<<20, now))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(4), 64<<20, now))
	m.AddL0SST(rawFlush(5, 2<<20, now))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(6), 64<<20, now))
	m.AddL0SST(rawFlush(7, 2<<20, now))
	m.AddL0SST(rawFlush(8, 2<<20, now))

	// The writer merges oldest run first, so a flush file with a merged file
	// above it is one it skipped: everything from the newest merged file to
	// the old end drains, skipped files included. The run above stays.
	got := c.drainableL0(m, now)
	want := strings.Join([]string{mergedSSTID(6), m.L0SSTs[3].ID, mergedSSTID(4), m.L0SSTs[5].ID, mergedSSTID(2), mergedSSTID(1)}, ",")
	if l0IDList(got) != want {
		t.Fatalf("drainable %s, want %s", l0IDList(got), want)
	}
	// The run above the line drains only once it is older than the age
	// limit, as a dead writer's leftovers with no successor do.
	later := now.Add(c.opts.Trigger.L0MaxAge + time.Second)
	if got := c.drainableL0(m, later); len(got) != len(m.L0SSTs) {
		t.Fatalf("drainable %d of %d after the age limit", len(got), len(m.L0SSTs))
	}
	m.L0SSTs[0].CreatedAt = later // the newest flush is fresh again
	if got := c.drainableL0(m, later); len(got) != len(m.L0SSTs)-1 {
		t.Fatalf("drainable %d of %d with one fresh flush on top", len(got), len(m.L0SSTs))
	}
}

func TestL0PlannerLeavesARunWithNoMergedFileBelowIt(t *testing.T) {
	// A fresh database, or a dead writer's leftovers before a successor
	// adopts them: young flush files and nothing merged. Nothing drains.
	c := plannerOnlyCompactor()
	m := &manifestState{}
	now := time.Now()
	for i := 3; i >= 0; i-- {
		m.AddL0SST(rawFlush(i, 2<<20, now))
	}
	if got := c.drainableL0(m, now); len(got) != 0 {
		t.Fatalf("drainable %s, want nothing", l0IDList(got))
	}
}

func plannerSSTNamed(id string, size int64, createdAt time.Time) sstMetadata {
	sst := plannerSST(0, 0, 100)
	sst.ID = id
	sst.Size = size
	sst.CreatedAt = createdAt
	return sst
}

func TestL0PlannerWaitsBelowTheDrainRules(t *testing.T) {
	// Eight merged files under the overlapped L1, not deep enough, not old,
	// and the ratio within the depth limit's 16 of them: nothing to do until
	// more arrive. Merging within L0 is the writer's, so no L0 job is
	// planned.
	c, m := l0Planner(t, repeat(25<<10, 8), 384<<10)
	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan != nil {
		t.Fatalf("plan %+v, want no L0 job until a drain rule fires", plan)
	}
}

func TestL0PlannerDrainsIntoL1(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(*compactor, *manifestState)
		l0    []int64
		l1    int64
	}{
		{"as large as the overlapped L1", nil, repeat(8<<20, 8), 64 << 20},
		{"nothing to overlap in L1", nil, repeat(25<<10, 8), 0},
		{"deep enough", nil, repeat(25<<10, 16), 256 << 20},
		{"old data", func(c *compactor, m *manifestState) {
			m.L0SSTs[len(m.L0SSTs)-1].CreatedAt = time.Now().Add(-31 * time.Minute)
		}, repeat(25<<10, 8), 256 << 20},
		{"old data in a merged SST", func(c *compactor, m *manifestState) {
			// Merged just now, but holding data from 31 minutes ago.
			m.L0SSTs[len(m.L0SSTs)-1].OldestDataAt = time.Now().Add(-31 * time.Minute)
		}, repeat(25<<10, 8), 256 << 20},
	} {
		t.Run(c.name, func(t *testing.T) {
			comp, m := l0Planner(t, c.l0, c.l1)
			if c.setup != nil {
				c.setup(comp, m)
			}
			plan, err := plannedCandidateForLevel(comp, m, 0)
			if err != nil {
				t.Fatal(err)
			}
			if plan == nil || plan.destinationLevel != 1 {
				t.Fatalf("plan %+v, want L0 into L1", plan)
			}
		})
	}
}

func TestL0PlannerMovesDisjointFilesWithoutRewrite(t *testing.T) {
	// Keys written in order: L0 SSTs don't overlap each other or L1, so they
	// move to L1 as they are.
	c := plannerOnlyCompactor()
	m := &manifestState{}
	for i := 0; i < 8; i++ {
		sst := plannerSST(0, 200+10*i, 205+10*i)
		sst.ID = mergedSSTID(i)
		sst.Size = 25 << 10
		sst.CreatedAt = time.Now()
		m.AddL0SST(sst)
	}
	l1 := plannerSST(1, 0, 100)
	m.AddLevelSSTs(1, []sstMetadata{l1})
	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.destinationLevel != 1 || !plan.metadataOnly {
		t.Fatalf("plan %+v, want a move into L1", plan)
	}
}

func TestL0Depth(t *testing.T) {
	ssts := []sstMetadata{
		plannerSST(0, 0, 10),
		plannerSST(0, 5, 15),
		plannerSST(0, 10, 20), // meets the first at key-010: ranges are inclusive
		plannerSST(0, 30, 40),
	}
	if got := l0Depth(ssts); got != 3 {
		t.Fatalf("depth %d, want 3", got)
	}
	if got := l0Depth(nil); got != 0 {
		t.Fatalf("depth of nothing %d", got)
	}
}

func TestL0IntervalIsShorterThanIdle(t *testing.T) {
	store := blobstore.NewMemory("l0-interval")
	defer store.Close()
	m := &Maintenance{compactor: &compactor{store: store}}
	if got := m.l0Interval(); got != maintenanceL0Interval || got >= defaultMaintenanceIdleInterval {
		t.Fatalf("L0 interval %s, want %s, under the idle %s", got, maintenanceL0Interval, defaultMaintenanceIdleInterval)
	}
}

func TestMaintenanceRevisitsL0OnlyWhileThereIsProgress(t *testing.T) {
	c, m := l0Planner(t, repeat(25<<10, 8), 256<<20)
	c.manifest = m
	maint := &Maintenance{compactor: c}
	if !maint.revisitL0(MaintenanceStats{}) {
		t.Fatal("L0 grew to 8 SSTs: want an early cycle")
	}
	if maint.revisitL0(MaintenanceStats{}) {
		t.Fatal("no job and no new SSTs: want the idle interval")
	}
	var ran MaintenanceStats
	ran.SSTCompaction.Jobs = 1
	if !maint.revisitL0(ran) {
		t.Fatal("a job ran: want an early cycle")
	}
	m.L0SSTs = m.L0SSTs[:4]
	if maint.revisitL0(ran) {
		t.Fatal("L0 under the trigger: want the idle interval")
	}
}

func TestL0PlannerCombinesMergedFilesForALargeL1(t *testing.T) {
	// L1 of 4 GiB: the ratio needs 64 files of 64 MiB, the depth limit is 16,
	// so merged files are combined four at a time. Eight 64 MiB files: not
	// worth a drain (512 MiB against 4 GiB), not deep, not old; the four
	// oldest are combined into one.
	c, m := l0Planner(t, repeat(64<<20, 8), 4<<30)
	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.destinationLevel != 0 || len(plan.sourceSSTs) != 4 {
		t.Fatalf("plan %+v, want an L0 combine of 4 files", plan)
	}
	want := strings.Join([]string{mergedSSTID(4), mergedSSTID(5), mergedSSTID(6), mergedSSTID(7)}, ",")
	if got := l0IDList(plan.sourceSSTs); got != want {
		t.Fatalf("combine %s, want the four oldest newest first %s", got, want)
	}
}

func TestL0PlannerDoesNotCombineForTheDefaultL1(t *testing.T) {
	// L1 of 512 MiB: the ratio needs 8 files, within the depth limit. Eight
	// files of 64 MiB meet the ratio and drain; seven wait, uncombined.
	c, m := l0Planner(t, repeat(64<<20, 8), 512<<20)
	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.destinationLevel != 1 {
		t.Fatalf("plan %+v, want a drain into L1", plan)
	}
	c, m = l0Planner(t, repeat(64<<20, 7), 512<<20)
	c.opts.Trigger.L0SSTCount = 2
	plan, err = plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan != nil {
		t.Fatalf("plan %+v, want to wait: 448 MiB against 512 MiB, 7 deep, no combine needed", plan)
	}
}

func TestL0PlannerCombinesOnlyWhenItRewritesLess(t *testing.T) {
	// At a ratio of 1 a combine costs two rewrites a byte: its own, then the
	// L1 a drain at the ratio rewrites. Draining at the depth limit of 16
	// instead rewrites L1 over 16 merged files' worth, which is less until L1
	// passes twice that. Over 1.5 GiB of L1 it does not: 1.5 a byte with
	// 64 MiB files, 1.92 with 50 MiB ones, as compression leaves them.
	for _, size := range []int64{64 << 20, 50 << 20} {
		c, m := l0Planner(t, repeat(size, 8), 3<<29)
		c.opts.Trigger.L0SSTCount = 2
		if plan := c.planL0Combine(c.drainableL0(m, time.Now()), m, time.Now()); plan != nil {
			t.Fatalf("%d MiB files over 1.5 GiB: combine %+v, want none", size>>20, plan)
		}
	}
	// Just past twice the depth limit's worth, draining at it rewrites 2.06 a
	// byte, more than a combine's two, and a group of three reaches the
	// ratio.
	c, m := l0Planner(t, repeat(64<<20, 8), 33*64<<20)
	plan := c.planL0Combine(c.drainableL0(m, time.Now()), m, time.Now())
	if plan == nil || len(plan.sourceSSTs) != 3 {
		t.Fatalf("plan %+v, want a combine of three", plan)
	}
}

func TestL0PlannerCombineGroupIsCapped(t *testing.T) {
	// L1 of 64 GiB wants groups of 64; one combine never writes more than
	// eight files' worth.
	c, m := l0Planner(t, repeat(64<<20, 12), 64<<30)
	plan := c.planL0Combine(c.drainableL0(m, time.Now()), m, time.Now())
	if plan == nil || len(plan.sourceSSTs) != maxL0CombineGroup {
		t.Fatalf("plan %+v, want a combine of %d", plan, maxL0CombineGroup)
	}
}

func TestL0PlannerCombineSkipsSlowWriters(t *testing.T) {
	// L1 of 4 GiB, four merged files of 64 MiB. Their oldest data is 10
	// minutes old: 256 MiB in 10 minutes projects to 768 MiB at the 30-minute
	// age limit, short of the depth limit's 1 GiB, so the age rule drains L0
	// before the depth rule ever would and a combine saves nothing. At 5
	// minutes the projection is 1.5 GiB and the combine goes ahead.
	now := time.Now()
	for _, tc := range []struct {
		age  time.Duration
		want bool
	}{
		{10 * time.Minute, false},
		{5 * time.Minute, true},
	} {
		c, m := l0Planner(t, repeat(64<<20, 4), 4<<30)
		c.opts.Trigger.L0SSTCount = 2
		for i := range m.L0SSTs {
			m.L0SSTs[i].CreatedAt = now.Add(-tc.age)
		}
		plan := c.planL0Combine(c.drainableL0(m, now), m, now)
		if (plan != nil) != tc.want {
			t.Fatalf("oldest data %s old: plan %+v, want combine=%v", tc.age, plan, tc.want)
		}
	}
}

func TestL0PlannerCombineSkipsCombinedFilesAndStopsAtFlushFiles(t *testing.T) {
	c := plannerOnlyCompactor()
	c.opts.Trigger.L0SSTCount = 2
	m := &manifestState{}
	now := time.Now()
	l1 := plannerSST(1, 0, 100)
	l1.Size = 4 << 30
	m.AddLevelSSTs(1, []sstMetadata{l1})
	// Oldest first: one combine maintenance made, two writer merges, a
	// skipped flush below newer merged files, then four writer merges. All a
	// minute old, so the fill rate projects well past the depth limit.
	at := now.Add(-time.Minute)
	m.AddL0SST(plannerSSTNamed(combinedSSTID(1), 256<<20, at))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(2), 64<<20, at))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(3), 64<<20, at))
	m.AddL0SST(rawFlush(4, 2<<20, at))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(5), 64<<20, at))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(6), 64<<20, at))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(7), 64<<20, at))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(8), 64<<20, at))

	plan := c.planL0Combine(c.drainableL0(m, now), m, now)
	if plan == nil || len(plan.sourceSSTs) != 4 {
		t.Fatalf("plan %+v, want the run of four above the flush file", plan)
	}
	want := strings.Join([]string{mergedSSTID(8), mergedSSTID(7), mergedSSTID(6), mergedSSTID(5)}, ",")
	if got := l0IDList(plan.sourceSSTs); got != want {
		t.Fatalf("combine %s, want %s", got, want)
	}
	// Without the run of four, the pair below the flush is too short and the
	// combine is known by its name, whatever its size: nothing to combine.
	m.L0SSTs = m.L0SSTs[2:]
	if plan := c.planL0Combine(c.drainableL0(m, now), m, now); plan != nil {
		t.Fatalf("plan %+v, want none: no adjacent run of four uncombined files", plan)
	}
}

func TestL0PlannerCombinesTheWritersRealMergedFiles(t *testing.T) {
	// A run closes when its held bytes reach the target, so the writer's
	// merged files land a little above 64 MiB, about 66 MiB; a combine is
	// known by its name, not its size, so they are combined all the same.
	c, m := l0Planner(t, repeat(66<<20, 8), 4<<30)
	plan := c.planL0Combine(c.drainableL0(m, time.Now()), m, time.Now())
	if plan == nil || len(plan.sourceSSTs) != 4 {
		t.Fatalf("plan %+v, want a combine of four 66 MiB merged files", plan)
	}
	// A combine that came out small, as heavy deduplication leaves it, is
	// still a combine and is never combined again.
	c, m = l0Planner(t, repeat(66<<20, 8), 4<<30)
	for i := range m.L0SSTs {
		m.L0SSTs[i].ID = combinedSSTID(i)
		m.L0SSTs[i].Size = 40 << 20
	}
	if plan := c.planL0Combine(c.drainableL0(m, time.Now()), m, time.Now()); plan != nil {
		t.Fatalf("plan %+v, want none: every file is a combine already", plan)
	}
}

func TestL0PlannerCombineCountsTheWritersFilesNotTheOutputSize(t *testing.T) {
	// Merged files of 67 MiB over 4 GiB of L1, as a 1 TiB run's. Whether
	// they are worth combining is the same whatever size compaction writes
	// its own files.
	for _, tc := range []struct {
		ratio float64
		depth int
		want  bool
	}{
		// 123 files reach the ratio, the depth limit is 32: combined four at a
		// time, each byte is rewritten once more but rewrites half as much L1.
		{2, 32, true},
		// 245 files against 64: four at a time again, but at four times L1 a
		// drain rewrites only a quarter as much, less than the combine's own
		// rewrite saves.
		{4, 64, false},
	} {
		for _, target := range []int64{64 << 20, 256 << 20} {
			c, m := l0Planner(t, repeat(67<<20, 8), 4<<30)
			c.opts.Trigger.L0ToBaseRatio = tc.ratio
			c.opts.Trigger.L0MaxDepth = tc.depth
			c.opts.Output.TargetSSTBytes = target
			plan := c.planL0Combine(c.drainableL0(m, time.Now()), m, time.Now())
			if (plan != nil) != tc.want {
				t.Fatalf("ratio %v, depth %d, %d MiB outputs: plan %+v, want combine=%v",
					tc.ratio, tc.depth, target>>20, plan, tc.want)
			}
			if plan != nil && len(plan.sourceSSTs) != 4 {
				t.Fatalf("ratio %v, depth %d: combine of %d, want 4", tc.ratio, tc.depth, len(plan.sourceSSTs))
			}
		}
	}
}

func TestL0PlannerCombinesSmallFilesAtAHighRatio(t *testing.T) {
	// Merged files of 8 MiB at four times 4 GiB of L1: draining at the depth
	// limit of 64 rewrites eight bytes of L1 for each of L0. Combined eight at
	// a time, each byte is rewritten twice instead.
	c, m := l0Planner(t, repeat(8<<20, 8), 4<<30)
	c.opts.Trigger.L0ToBaseRatio = 4
	c.opts.Trigger.L0MaxDepth = 64
	plan := c.planL0Combine(c.drainableL0(m, time.Now()), m, time.Now())
	if plan == nil || len(plan.sourceSSTs) != maxL0CombineGroup {
		t.Fatalf("plan %+v, want a combine of %d", plan, maxL0CombineGroup)
	}
}

func TestL0PlannerCombineGroupFitsUnderTheDepthLimit(t *testing.T) {
	// A depth limit of four drains L0 at four files, so a group of eight,
	// which the ratio would want, never gathers; three is the most that does.
	c, m := l0Planner(t, repeat(1<<20, 3), 4<<30)
	c.opts.Trigger.L0MaxDepth = 4
	plan := c.planL0Combine(c.drainableL0(m, time.Now()), m, time.Now())
	if plan == nil || len(plan.sourceSSTs) != 3 {
		t.Fatalf("plan %+v, want a combine of three", plan)
	}
}

func TestL0PlannerCombineGroupLeavesRoomForEarlierCombines(t *testing.T) {
	// A depth limit of four, a combine already in L0 and two merged files
	// after it: a group of three would need four files, which the depth rule
	// drains first, so the two are combined. With two combines in L0, a
	// group of one is no combine at all.
	c := plannerOnlyCompactor()
	c.opts.Trigger.L0SSTCount = 2
	c.opts.Trigger.L0MaxDepth = 4
	m := &manifestState{}
	l1 := plannerSST(1, 0, 100)
	l1.Size = 4 << 30
	m.AddLevelSSTs(1, []sstMetadata{l1})
	now := time.Now()
	at := now.Add(-time.Minute)
	m.AddL0SST(plannerSSTNamed(combinedSSTID(1), 3<<20, at))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(2), 1<<20, at))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(3), 1<<20, at))
	plan := c.planL0Combine(c.drainableL0(m, now), m, now)
	if plan == nil || len(plan.sourceSSTs) != 2 {
		t.Fatalf("plan %+v, want the two merged files combined", plan)
	}
	want := strings.Join([]string{mergedSSTID(3), mergedSSTID(2)}, ",")
	if got := l0IDList(plan.sourceSSTs); got != want {
		t.Fatalf("combine %s, want %s", got, want)
	}

	m.L0SSTs = m.L0SSTs[:1]
	m.AddL0SST(plannerSSTNamed(combinedSSTID(4), 2<<20, at))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(5), 1<<20, at))
	if plan := c.planL0Combine(c.drainableL0(m, now), m, now); plan != nil {
		t.Fatalf("plan %+v, want none: one place left under the depth limit", plan)
	}
}
