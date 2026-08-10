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
	return &compactor{opts: normalizeCompactorOptions(defaultCompactorOptions(), nil)}
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
