package isledb

import (
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// l0Planner builds an L0, given newest first by size, of SSTs that all span
// key-000..key-100 and were written now, over an L1 of l1Bytes covering the
// same keys.
func l0Planner(t *testing.T, sizes []int64, l1Bytes int64) (*compactor, *manifestState) {
	t.Helper()
	c := plannerOnlyCompactor()
	m := &manifestState{}
	for i := len(sizes) - 1; i >= 0; i-- {
		sst := plannerSST(0, 0, 100)
		sst.ID = fmt.Sprintf("l0-%02d", i)
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

func repeat(size int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = size
	}
	return out
}

func TestL0PlannerMergesNewFilesWithEachOtherFirst(t *testing.T) {
	c, m := l0Planner(t, repeat(25<<10, 8), 256<<20)
	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.destinationLevel != 0 {
		t.Fatalf("plan %+v, want an L0 merge: 200 KiB of L0 is not worth rewriting 256 MiB of L1", plan)
	}
	if len(plan.sourceSSTs) != 8 || plan.sourceSSTs[0].ID != m.L0SSTs[0].ID {
		t.Fatalf("merged %d SSTs starting at %q, want all 8 from the newest", len(plan.sourceSSTs), plan.sourceSSTs[0].ID)
	}
}

func TestL0PlannerTakesOlderFilesNoLargerThanTheTotal(t *testing.T) {
	// 8 × 25 KiB = 200 KiB takes the 200 KiB file (400 KiB), then the 400 KiB
	// one (800 KiB), and stops at the 4 MiB one.
	sizes := append(repeat(25<<10, 8), 200<<10, 400<<10, 4<<20)
	c, m := l0Planner(t, sizes, 256<<20)
	plan := c.planL0Merge(m)
	if plan == nil || len(plan.sourceSSTs) != 10 {
		t.Fatalf("plan %+v, want the 10 newest SSTs", plan)
	}
}

func TestL0PlannerWaitsWhenNoRunQualifies(t *testing.T) {
	// The newest SST is smaller than the next, so there is no run of two;
	// L0 must not fall back to rewriting L1.
	sizes := []int64{25 << 10, 200 << 10, 400 << 10, 800 << 10, 1600 << 10, 3200 << 10, 6400 << 10, 12800 << 10}
	c, m := l0Planner(t, sizes, 256<<20)
	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan != nil {
		t.Fatalf("plan %+v, want no L0 job until more SSTs arrive", plan)
	}
}

func TestL0PlannerRespectsTheOutputSize(t *testing.T) {
	c, m := l0Planner(t, repeat(20<<20, 8), 512<<20)
	plan := c.planL0Merge(m)
	if plan == nil || len(plan.sourceSSTs) != 3 {
		t.Fatalf("plan %+v, want 3 SSTs of 20 MiB: a fourth passes the 64 MiB output", plan)
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
