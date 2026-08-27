package isledb

import (
	"fmt"
	"strings"
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

func TestLevelPlannerRejectsUnboundedOverlap(t *testing.T) {
	c := plannerOnlyCompactor()
	c.opts.Trigger.MaxInputSSTs = 2
	m := &manifestState{}
	for i := 0; i < 8; i++ {
		m.AddL0SST(plannerSST(0, 0, 100))
	}
	m.AddLevelSSTs(1, []sstMetadata{plannerSST(1, 0, 49), plannerSST(1, 50, 100)})

	if _, err := plannedCandidateForLevel(c, m, 0); err == nil {
		t.Fatal("expected bounded-input error")
	}
}

func TestLevelPlannerBoundsRewriteByInputBytes(t *testing.T) {
	c := plannerOnlyCompactor()
	c.opts.Trigger.L0SSTCount = 3
	c.opts.Trigger.MaxInputBytes = 100 << 20
	m := &manifestState{}
	for i := 0; i < 3; i++ {
		m.AddL0SST(plannerSST(0, i, i+2))
	}
	m.AddLevelSSTs(1, []sstMetadata{plannerSST(1, 0, 10)})

	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatalf("planCompactionCandidates: %v", err)
	}
	if got := len(plan.sourceSSTs); got != 1 {
		t.Fatalf("source SSTs=%d, want one indivisible oversized source", got)
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

func TestLevelPlannerBoundsVerifiedMoveBytesWithoutForcingRewrite(t *testing.T) {
	c := plannerOnlyCompactor()
	c.opts.Safety.ValidateSSTChecksum = true
	c.opts.Trigger.L0SSTCount = 1
	c.opts.Trigger.MaxInputBytes = 2 * (64 << 20)
	m := &manifestState{}
	for i := 0; i < 4; i++ {
		m.AddL0SST(plannerSST(0, i, i))
	}

	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || !plan.metadataOnly {
		t.Fatalf("verified move became a rewrite: %+v", plan)
	}
	if got := len(plan.sourceSSTs); got != 2 {
		t.Fatalf("verified move sources = %d, want 2 within byte target", got)
	}

	// Without verification the same move performs no object I/O, so the byte
	// target must not fragment it into extra manifest-only jobs.
	c.opts.Safety.ValidateSSTChecksum = false
	plan, err = plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(plan.sourceSSTs); got != 4 {
		t.Fatalf("unchecked move sources = %d, want all 4", got)
	}
}

// A level that cannot be planned must say so. The scheduler otherwise sees an
// absent candidate, finds work at another level, and reports a healthy cycle
// while the blocked level never drains.
func TestLevelPlannerReportsBlockedL0(t *testing.T) {
	c := plannerOnlyCompactor()
	var blocked []string
	var sawCritical bool
	var sawCount int
	var reason string
	c.opts.OnPlanningBlocked = func(sourceLevel uint32, sstCount int, critical bool, err error) {
		blocked = append(blocked, fmt.Sprintf("L%d", sourceLevel))
		sawCritical = critical
		sawCount = sstCount
		reason = err.Error()
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
	l1 := make([]sstMetadata, 0, c.opts.Trigger.MaxInputSSTs+1)
	for i := 0; i <= c.opts.Trigger.MaxInputSSTs; i++ {
		l1 = append(l1, plannerSST(1, i, i))
	}
	m.AddLevelSSTs(1, l1)

	plan, err := plannedCandidateForLevel(c, m, 0)
	if err != nil {
		t.Fatalf("planning should not fail the cycle when another level has work: %v", err)
	}
	if plan != nil {
		t.Fatalf("expected no L0 candidate, got %+v", plan)
	}
	if len(blocked) != 1 || blocked[0] != "L0" {
		t.Fatalf("blocked levels = %v, want [L0]", blocked)
	}
	if !sawCritical {
		t.Fatal("a critically deep L0 reported as not critical because planning failed")
	}
	if sawCount != l0Count {
		t.Fatalf("blocked sst count = %d, want %d", sawCount, l0Count)
	}
	wantReason := fmt.Sprintf("one source plus %d destination SSTs requires %d inputs",
		c.opts.Trigger.MaxInputSSTs+1, c.opts.Trigger.MaxInputSSTs+2)
	if !strings.Contains(reason, wantReason) {
		t.Fatalf("blocked reason = %q, want it to contain %q", reason, wantReason)
	}
}
