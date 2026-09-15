package isledb

import (
	"fmt"
	"testing"
)

func TestLevelPlannerPromotesDisjointL0WithoutRewrite(t *testing.T) {
	c := plannerOnlyCompactor()
	m := &manifestState{}
	for i := 0; i < 8; i++ {
		m.AddL0SST(plannerSST(0, i, i))
	}
	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.sourceLevel != 0 || plan.destinationLevel != 1 || !plan.metadataOnly {
		t.Fatalf("plan=%+v", plan)
	}
	if len(plan.sourceSSTs) != 8 || len(plan.destinationSSTs) != 0 {
		t.Fatalf("sources=%d destination=%d", len(plan.sourceSSTs), len(plan.destinationSSTs))
	}
}

func TestLevelPlannerRewritesOverlappingL0AndDestination(t *testing.T) {
	c := plannerOnlyCompactor()
	m := &manifestState{}
	for i := 0; i < 8; i++ {
		m.AddL0SST(plannerSST(0, i, i+2))
	}
	m.AddLevelSSTs(1, []sstMetadata{plannerSST(1, 0, 20)})

	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.metadataOnly {
		t.Fatalf("plan=%+v", plan)
	}
	if len(plan.destinationSSTs) != 1 || plan.destinationSSTs[0].ID != "l1-000-020" {
		t.Fatalf("destination=%+v", plan.destinationSSTs)
	}
}

func TestLevelPlannerPromotesWideL0Directly(t *testing.T) {
	c := plannerOnlyCompactor()
	m := &manifestState{}
	for i := 0; i < c.opts.Trigger.L0SSTCount; i++ {
		sst := plannerSST(0, 0, 100)
		sst.ID = fmt.Sprintf("l0-wide-%03d", i)
		sst.Size = 1 << 20
		m.AddL0SST(sst)
	}
	destination := plannerSST(1, 0, 100)
	destination.Size = 64 << 20
	m.AddLevelSSTs(1, []sstMetadata{destination})

	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.sourceLevel != 0 || plan.destinationLevel != 1 {
		t.Fatalf("plan=%+v, want direct L0-to-L1 promotion", plan)
	}
	if got := len(plan.sourceSSTs); got != c.opts.Trigger.L0SSTCount {
		t.Fatalf("promotion sources=%d want=%d", got, c.opts.Trigger.L0SSTCount)
	}
	if len(plan.destinationSSTs) != 1 {
		t.Fatalf("promotion destination=%+v, want the complete overlap", plan.destinationSSTs)
	}
}

func TestLevelPlannerLeavesOrdinaryShallowL0Alone(t *testing.T) {
	c := plannerOnlyCompactor()
	m := &manifestState{}
	m.AddL0SST(plannerSST(0, 0, 0))

	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan != nil {
		t.Fatalf("ordinary L0 below trigger unexpectedly planned: %+v", plan)
	}
}

func TestLevelPlannerMovesOverBudgetLevelDown(t *testing.T) {
	c := plannerOnlyCompactor()
	c.opts.Trigger.BaseLevelBytes = 1
	m := &manifestState{}
	m.AddLevelSSTs(1, []sstMetadata{plannerSST(1, 0, 0), plannerSST(1, 2, 2)})

	plan, err := plannedCandidateForLevel(c, m, 1)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.sourceLevel != 1 || plan.destinationLevel != 2 || !plan.metadataOnly {
		t.Fatalf("plan=%+v", plan)
	}
}

func TestLevelPlannerSelectsWidestValidSourceBatch(t *testing.T) {
	c := plannerOnlyCompactor()
	c.opts.Trigger.L0SSTCount = 3
	m := &manifestState{}
	for i := 0; i < 3; i++ {
		m.AddL0SST(plannerSST(0, i, i+2))
	}
	m.AddLevelSSTs(1, []sstMetadata{plannerSST(1, 0, 10)})

	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatalf("planCompactionCandidates: %v", err)
	}
	if got := len(plan.sourceSSTs); got != 3 {
		t.Fatalf("source SSTs=%d, want widest valid batch of 3", got)
	}
}

func TestLevelPlannerLargeDestinationDoesNotShrinkSourceBatch(t *testing.T) {
	c := plannerOnlyCompactor()
	m := &manifestState{}
	for i := 0; i < 8; i++ {
		sst := plannerSST(0, 0, 100)
		sst.ID = fmt.Sprintf("wide-source-%d", i)
		sst.Size = 16 << 20
		m.AddL0SST(sst)
	}
	destination := plannerSST(1, 0, 100)
	destination.Size = 512 << 20
	m.AddLevelSSTs(1, []sstMetadata{destination})

	plan, err := c.buildLevelPlan(m, 0, 1, m.L0SSTs)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(plan.sourceSSTs); got != 8 {
		t.Fatalf("source SSTs=%d want=8; an indivisible destination must not shrink the source batch", got)
	}
}

func plannerOnlyCompactor() *compactor {
	return &compactor{opts: normalizeCompactorOptions(defaultCompactorOptions())}
}

func plannedCandidateForLevel(c *compactor, m *manifestState, sourceLevel uint32) (*levelCompactionPlan, error) {
	candidates, err := c.planCompactionCandidates(m)
	if err != nil {
		return nil, err
	}
	for i := range candidates {
		if candidates[i].plan.sourceLevel == sourceLevel {
			return candidates[i].plan, nil
		}
	}
	return nil, nil
}

func plannerSST(level uint32, lo, hi int) sstMetadata {
	return sstMetadata{
		ID:     fmt.Sprintf("l%d-%03d-%03d", level, lo, hi),
		Level:  level,
		MinKey: []byte(fmt.Sprintf("key-%03d", lo)),
		MaxKey: []byte(fmt.Sprintf("key-%03d", hi)),
		Size:   64 << 20,
	}
}

// Checksum validation must not cost a rewrite. A disjoint L0 with nothing
// overlapping in the destination is still a move; validation only means the
// sources are read and verified before it is committed.
func TestLevelPlannerStillMovesWhenChecksumValidationIsOn(t *testing.T) {
	c := plannerOnlyCompactor()
	c.opts.Safety.ValidateSSTChecksum = true
	m := &manifestState{}
	for i := 0; i < 8; i++ {
		m.AddL0SST(plannerSST(0, i, i))
	}

	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || !plan.metadataOnly {
		t.Fatalf("validation turned a movable plan into a rewrite: %+v", plan)
	}
}

func TestLevelPlannerDrainsDestinationBeforeBlockedPromotion(t *testing.T) {
	c := plannerOnlyCompactor()
	var blocked []string
	c.opts.OnPlanningBlocked = func(sourceLevel uint32, sstCount int, critical bool, err error) {
		blocked = append(blocked, fmt.Sprintf("L%d", sourceLevel))
	}

	m := &manifestState{}
	// L0 files that each span the whole keyspace, deep enough to be critical.
	l0Count := c.opts.Trigger.L0SSTCount * l0CriticalTriggerMultiplier
	for i := 0; i < l0Count; i++ {
		sst := plannerSST(0, 0, 999)
		sst.ID = fmt.Sprintf("l0-wide-%03d", i)
		m.AddL0SST(sst)
	}
	// More L1 files under that span than a single job may retire, so shrinking
	// the source count can never bring the plan under the limit.
	l1 := make([]sstMetadata, 0, maxCompactionSSTsPerJob)
	for i := 0; i < maxCompactionSSTsPerJob; i++ {
		l1 = append(l1, plannerSST(1, i, i))
	}
	m.AddLevelSSTs(1, l1)

	candidates, err := c.planCompactionCandidates(m)
	if err != nil {
		t.Fatalf("plan destination drain: %v", err)
	}
	if len(candidates) == 0 {
		t.Fatal("no compaction candidate")
	}
	plan := candidates[0].plan
	if plan.sourceLevel != 1 || plan.destinationLevel != 2 || !plan.metadataOnly {
		t.Fatalf("plan=%+v, want L1-to-L2 drain", plan)
	}
	if len(plan.sourceSSTs) != maxCompactionSSTsPerJob {
		t.Fatalf("drain sources=%d, want %d", len(plan.sourceSSTs), maxCompactionSSTsPerJob)
	}
	if len(blocked) != 0 {
		t.Fatalf("recoverable L0 pressure reported blocked: %v", blocked)
	}

	ids := make([]string, len(plan.sourceSSTs))
	for i := range plan.sourceSSTs {
		ids[i] = plan.sourceSSTs[i].ID
	}
	m.RemoveCompactionInputs(1, 2, ids)
	m.AddLevelSSTs(2, plan.sourceSSTs)
	next, err := c.buildLevelPlanWithDrain(m, 0, 1, m.L0SSTs)
	if err != nil {
		t.Fatalf("plan L0 after drain: %v", err)
	}
	if next.sourceLevel != 0 || next.destinationLevel != 1 {
		t.Fatalf("next plan=%+v, want original L0-to-L1 promotion", next)
	}
}

func TestLevelPlannerRecursivelyDrainsDeepestBlockingLevel(t *testing.T) {
	c := plannerOnlyCompactor()
	m := &manifestState{}
	for i := 0; i < c.opts.Trigger.L0SSTCount; i++ {
		sst := plannerSST(0, 0, 0)
		sst.ID = fmt.Sprintf("l0-wide-%03d", i)
		sst.MinKey = []byte("a000")
		sst.MaxKey = []byte("z999")
		m.AddL0SST(sst)
	}

	l1 := make([]sstMetadata, 0, maxCompactionSSTsPerJob)
	first := plannerSST(1, 0, 0)
	first.ID = "l1-wide-first"
	first.MinKey = []byte("a000")
	first.MaxKey = []byte("a999")
	l1 = append(l1, first)
	for i := 1; i < maxCompactionSSTsPerJob; i++ {
		key := []byte(fmt.Sprintf("b%03d", i))
		sst := plannerSST(1, i, i)
		sst.MinKey = key
		sst.MaxKey = key
		l1 = append(l1, sst)
	}
	m.AddLevelSSTs(1, l1)

	l2 := make([]sstMetadata, 0, maxCompactionSSTsPerJob)
	for i := 0; i < maxCompactionSSTsPerJob; i++ {
		key := []byte(fmt.Sprintf("a%03d", i))
		sst := plannerSST(2, i, i)
		sst.MinKey = key
		sst.MaxKey = key
		l2 = append(l2, sst)
	}
	m.AddLevelSSTs(2, l2)

	candidates, err := c.planCompactionCandidates(m)
	if err != nil {
		t.Fatalf("plan recursive drain: %v", err)
	}
	if len(candidates) == 0 {
		t.Fatal("no compaction candidate")
	}
	plan := candidates[0].plan
	if plan.sourceLevel != 2 || plan.destinationLevel != 3 || !plan.metadataOnly {
		t.Fatalf("plan=%+v, want deepest L2-to-L3 drain", plan)
	}
}
