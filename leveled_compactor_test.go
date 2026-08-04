package isledb

import (
	"fmt"
	"testing"
)

func TestLevelPlannerPromotesDisjointL0WithoutRewrite(t *testing.T) {
	c := plannerOnlyCompactor()
	m := &Manifest{}
	for i := 0; i < 8; i++ {
		m.AddL0SST(plannerSST(0, i, i))
	}
	plan, err := c.planCompaction(m)
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
	m := &Manifest{}
	for i := 0; i < 8; i++ {
		m.AddL0SST(plannerSST(0, i, i+2))
	}
	m.AddLevelSSTs(1, []SSTMeta{plannerSST(1, 0, 20)})

	plan, err := c.planCompaction(m)
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
	m := &Manifest{}
	m.AddLevelSSTs(1, []SSTMeta{plannerSST(1, 0, 0), plannerSST(1, 2, 2)})

	plan, err := c.planCompaction(m)
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
	m := &Manifest{}
	for i := 0; i < 8; i++ {
		m.AddL0SST(plannerSST(0, 0, 100))
	}
	m.AddLevelSSTs(1, []SSTMeta{plannerSST(1, 0, 49), plannerSST(1, 50, 100)})

	if _, err := c.planCompaction(m); err == nil {
		t.Fatal("expected bounded-input error")
	}
}

func plannerOnlyCompactor() *compactor {
	return &compactor{opts: normalizeCompactorOptions(defaultCompactorOptions(), nil)}
}

func plannerSST(level uint32, lo, hi int) SSTMeta {
	return SSTMeta{
		ID:     fmt.Sprintf("l%d-%03d-%03d", level, lo, hi),
		Level:  level,
		MinKey: []byte(fmt.Sprintf("key-%03d", lo)),
		MaxKey: []byte(fmt.Sprintf("key-%03d", hi)),
		Size:   64 << 20,
	}
}
