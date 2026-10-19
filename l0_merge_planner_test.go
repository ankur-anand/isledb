package isledb

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// mergedSSTID names an L0 file as the writer's merges and compaction outputs
// are named.
func mergedSSTID(i int) string {
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

func TestL0PlannerDrainsOnlyBelowTheWritersFiles(t *testing.T) {
	c := plannerOnlyCompactor()
	m := &manifestState{}
	now := time.Now()
	// Oldest first as committed: two merged files, a flush the writer did not
	// keep a copy of, a merged file, then the writer's current run.
	m.AddL0SST(plannerSSTNamed(mergedSSTID(1), 64<<20, now))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(2), 64<<20, now))
	m.AddL0SST(rawFlush(3, 2<<20, now))
	m.AddL0SST(plannerSSTNamed(mergedSSTID(4), 64<<20, now))
	m.AddL0SST(rawFlush(5, 2<<20, now))
	m.AddL0SST(rawFlush(6, 2<<20, now))

	// The drain stops below the first young flush file from the old end: the
	// merged file above it cannot move to L1 while an older version of a key
	// may sit in that flush.
	got := c.drainableL0(m, now)
	if want := mergedSSTID(2) + "," + mergedSSTID(1); l0IDList(got) != want {
		t.Fatalf("drainable %s, want %s", l0IDList(got), want)
	}
	// Once that flush ages past the limit, it and the merged file above it
	// are drainable too; the writer's run still is not.
	later := now.Add(c.opts.Trigger.L0MaxAge + time.Second)
	m.L0SSTs[1].CreatedAt = later // the writer's run stays young
	m.L0SSTs[0].CreatedAt = later
	got = c.drainableL0(m, later)
	if want := strings.Join([]string{mergedSSTID(4), m.L0SSTs[3].ID, mergedSSTID(2), mergedSSTID(1)}, ","); l0IDList(got) != want {
		t.Fatalf("drainable %s, want %s", l0IDList(got), want)
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
	// Eight merged files well under the overlapped L1, not deep enough, not
	// old: nothing to do until more arrive. Merging within L0 is the
	// writer's, so no L0 job is planned.
	c, m := l0Planner(t, repeat(25<<10, 8), 256<<20)
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
