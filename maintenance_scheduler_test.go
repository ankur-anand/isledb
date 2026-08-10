package isledb

import (
	"context"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

var schedulerDecisionSink maintenanceDecision

func TestSelectMaintenancePrimaryBoundsBothDirections(t *testing.T) {
	compaction := schedulerCandidate(0, true, 1)
	checkpoint := checkpointPressure{eligible: true}
	state := manifest.MaintenanceSchedulerState{}

	compactionUnitsSinceCheckpoint := uint32(0)
	lastTask := MaintenanceTaskNone
	for turn := 0; turn < 100; turn++ {
		decision := selectMaintenancePrimary(&compaction, checkpoint, state)
		switch decision.task {
		case MaintenanceTaskSSTCompaction:
			if lastTask == MaintenanceTaskManifestCheckpoint {
				compactionUnitsSinceCheckpoint = 0
			}
			compactionUnitsSinceCheckpoint += compaction.workUnits
			if compactionUnitsSinceCheckpoint > maxPrimaryCompactionBurstUnits {
				t.Fatalf("checkpoint waited for %d compaction units", compactionUnitsSinceCheckpoint)
			}
		case MaintenanceTaskManifestCheckpoint:
			if lastTask == MaintenanceTaskManifestCheckpoint {
				t.Fatal("selected consecutive checkpoints while compaction was eligible")
			}
			compactionUnitsSinceCheckpoint = 0
		default:
			t.Fatal("selected no primary while both were eligible")
		}
		applySchedulerDecisionForTest(&state, decision)
		lastTask = decision.task
	}
}

func TestSelectMaintenancePrimaryUrgentCheckpointStillAllowsCompaction(t *testing.T) {
	compaction := schedulerCandidate(0, true, 1)
	checkpoint := checkpointPressure{eligible: true, urgent: true}

	decision := selectMaintenancePrimary(&compaction, checkpoint, manifest.MaintenanceSchedulerState{})
	if decision.task != MaintenanceTaskManifestCheckpoint {
		t.Fatalf("first task=%v, want checkpoint", decision.task)
	}
	state := manifest.MaintenanceSchedulerState{LastPrimary: manifest.MaintenanceCommandCheckpoint}
	decision = selectMaintenancePrimary(&compaction, checkpoint, state)
	if decision.task != MaintenanceTaskSSTCompaction {
		t.Fatalf("task after checkpoint=%v, want compaction", decision.task)
	}
}

func TestSelectCompactionCandidateFairAcrossLevels(t *testing.T) {
	candidates := []compactionCandidate{
		schedulerCandidate(0, true, 1),
		schedulerCandidate(1, false, 1),
		schedulerCandidate(2, false, 1),
		schedulerCandidate(3, false, 1),
	}
	state := manifest.MaintenanceSchedulerState{}
	selected := make(map[uint32]int)
	lastSelected := map[uint32]int{1: -1, 2: -1, 3: -1}
	maxGap := make(map[uint32]int)
	for turn := 0; turn < 300; turn++ {
		candidate := selectCompactionCandidate(candidates, state)
		if candidate == nil {
			t.Fatal("no compaction candidate")
		}
		level := candidate.plan.sourceLevel
		selected[level]++
		if level > 0 {
			if lastSelected[level] >= 0 {
				maxGap[level] = max(maxGap[level], turn-lastSelected[level])
			}
			lastSelected[level] = turn
		}
		applyCompactionCandidateForTest(&state, candidate)
	}
	for level := uint32(1); level <= 3; level++ {
		if selected[level] == 0 {
			t.Fatalf("L%d starved: selected=%v", level, selected)
		}
		if maxGap[level] > 5*3 {
			t.Fatalf("L%d max gap=%d, want <=15", level, maxGap[level])
		}
	}
	if selected[0] <= selected[1] {
		t.Fatalf("critical L0 did not receive its larger share: selected=%v", selected)
	}
}

func TestPlanCompactionCandidatesIncludesEveryOverBudgetLevel(t *testing.T) {
	c := plannerOnlyCompactor()
	c.opts.Trigger.L0SSTCount = 2
	c.opts.Trigger.BaseLevelBytes = 1
	c.opts.Trigger.LevelSizeMultiplier = 2
	m := &manifestState{}
	for i := 0; i < 2; i++ {
		m.AddL0SST(plannerSST(0, i, i))
	}
	for level := uint32(1); level <= 3; level++ {
		key := int(level * 100)
		m.AddLevelSSTs(level, []sstMetadata{plannerSST(level, key, key)})
	}
	candidates, err := c.planCompactionCandidates(m)
	if err != nil {
		t.Fatalf("planCompactionCandidates: %v", err)
	}
	if len(candidates) != 4 {
		t.Fatalf("candidates=%d, want L0 and three lower levels", len(candidates))
	}
	for i, candidate := range candidates {
		if candidate.plan.sourceLevel != uint32(i) {
			t.Fatalf("candidate[%d] source=L%d", i, candidate.plan.sourceLevel)
		}
	}
}

func TestCompactionPlanWorkUnits(t *testing.T) {
	plan := &levelCompactionPlan{
		sourceSSTs:      []sstMetadata{{Size: 300}, {Size: 300}},
		destinationSSTs: []sstMetadata{{Size: 300}},
	}
	bytes, units := compactionPlanWorkUnits(plan, 400)
	if bytes != 900 || units != 3 {
		t.Fatalf("bytes=%d units=%d, want 900 and 3", bytes, units)
	}
	plan.metadataOnly = true
	_, units = compactionPlanWorkUnits(plan, 400)
	if units != 1 {
		t.Fatalf("metadata-only units=%d, want 1", units)
	}
	plan.metadataOnly = false
	plan.sourceSSTs[0].Size = 10_000
	_, units = compactionPlanWorkUnits(plan, 400)
	if units != maxPrimaryCompactionBurstUnits {
		t.Fatalf("oversized units=%d, want saturation=%d", units, maxPrimaryCompactionBurstUnits)
	}
}

func TestCheckpointPressureUsesPagesAndBytes(t *testing.T) {
	opts := ManifestCheckpointOptions{TargetReplayPages: 64, TargetReplayBytes: 1 << 20}
	for _, test := range []struct {
		name         string
		current      manifest.Current
		wantEligible bool
		wantUrgent   bool
	}{
		{name: "below", current: manifest.Current{StateReplayPages: 63, StateReplayBytes: (1 << 20) - 1}},
		{name: "pages", current: manifest.Current{StateReplayPages: 64}, wantEligible: true},
		{name: "bytes", current: manifest.Current{StateReplayBytes: 1 << 20}, wantEligible: true},
		{name: "urgent pages", current: manifest.Current{StateReplayPages: 128}, wantEligible: true, wantUrgent: true},
		{name: "urgent bytes", current: manifest.Current{StateReplayBytes: 2 << 20}, wantEligible: true, wantUrgent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := calculateCheckpointPressure(&test.current, opts)
			if got.eligible != test.wantEligible || got.urgent != test.wantUrgent {
				t.Fatalf("pressure=%+v, want eligible=%v urgent=%v", got, test.wantEligible, test.wantUrgent)
			}
		})
	}
}

func BenchmarkMaintenanceSchedulerPolicy(b *testing.B) {
	candidates := []compactionCandidate{
		schedulerCandidate(0, true, 1),
		schedulerCandidate(1, false, 1),
		schedulerCandidate(2, false, 1),
		schedulerCandidate(3, false, 1),
		schedulerCandidate(4, false, 1),
		schedulerCandidate(5, false, 1),
		schedulerCandidate(6, false, 1),
	}
	checkpoint := checkpointPressure{eligible: true}
	b.ReportAllocs()
	state := manifest.MaintenanceSchedulerState{}
	var checkpointSelections, deepLevelSelections, checkpointWait, maxCheckpointWait int
	for i := 0; i < b.N; i++ {
		candidate := selectCompactionCandidate(candidates, state)
		decision := selectMaintenancePrimary(candidate, checkpoint, state)
		if decision.task == MaintenanceTaskManifestCheckpoint {
			checkpointSelections++
			checkpointWait = 0
		} else {
			checkpointWait += int(candidate.workUnits)
			maxCheckpointWait = max(maxCheckpointWait, checkpointWait)
			if candidate.plan.sourceLevel >= 2 {
				deepLevelSelections++
			}
		}
		applySchedulerDecisionForTest(&state, decision)
		if decision.task == MaintenanceTaskSSTCompaction {
			applyCompactionCandidateForTest(&state, candidate)
		}
		schedulerDecisionSink = decision
	}
	reportSchedulerTraceMetrics(b, checkpointSelections, deepLevelSelections, maxCheckpointWait)
}

func BenchmarkMaintenanceSchedulerExecution(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ctx := context.Background()
		store := blobstore.NewMemory("maintenance-scheduler-execution")
		db, err := openDB(ctx, store, dbOpenOptions{})
		if err != nil {
			b.Fatal(err)
		}
		writerOpts := DefaultWriterOptions()
		writerOpts.Flush.Interval = 0
		writerOpts.Memtable.TargetBytes = 32 << 10
		writer, err := db.OpenWriter(ctx, writerOpts)
		if err != nil {
			b.Fatal(err)
		}
		value := make([]byte, 100)
		for generation := 0; generation < 8; generation++ {
			for key := 0; key < 100; key++ {
				if err := writer.Put(ctx, []byte(fmt.Sprintf("key-%016d", key)), value); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Flush(ctx); err != nil {
				b.Fatal(err)
			}
		}
		if err := writer.Close(ctx); err != nil {
			b.Fatal(err)
		}
		opts := DefaultMaintenanceOptions()
		opts.SSTCompaction.L0TriggerSSTs = 4
		opts.ManifestCheckpoint.TargetReplayPages = ^uint64(0)
		opts.ManifestCheckpoint.TargetReplayBytes = ^uint64(0)
		maintenance, err := db.OpenMaintenance(ctx, opts)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if _, err := maintenance.RunOnce(ctx); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		if err := maintenance.Close(ctx); err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func reportSchedulerTraceMetrics(b *testing.B, checkpoints, deepLevels, maxCheckpointWait int) {
	b.Helper()
	if b.N == 0 {
		return
	}
	b.ReportMetric(100*float64(checkpoints)/float64(b.N), "checkpoint_%")
	b.ReportMetric(100*float64(deepLevels)/float64(b.N), "deep-level_%")
	b.ReportMetric(float64(maxCheckpointWait), "max-checkpoint-wait-units")
}

func schedulerCandidate(level uint32, critical bool, units uint32) compactionCandidate {
	return compactionCandidate{
		plan: &levelCompactionPlan{
			sourceLevel:      level,
			destinationLevel: level + 1,
			workUnits:        units,
		},
		workUnits: units,
		critical:  critical,
	}
}

func applySchedulerDecisionForTest(state *manifest.MaintenanceSchedulerState, decision maintenanceDecision) {
	switch decision.task {
	case MaintenanceTaskManifestCheckpoint:
		state.LastPrimary = manifest.MaintenanceCommandCheckpoint
		state.CompactionUnitsSinceCheckpoint = 0
	case MaintenanceTaskSSTCompaction:
		state.LastPrimary = manifest.MaintenanceCommandCompaction
		state.CompactionUnitsSinceCheckpoint = min(
			^uint32(0), state.CompactionUnitsSinceCheckpoint+decision.compaction.workUnits)
	}
}

func applyCompactionCandidateForTest(state *manifest.MaintenanceSchedulerState, candidate *compactionCandidate) {
	if candidate.plan.sourceLevel == 0 {
		state.L0UnitsSinceLower = min(^uint32(0), state.L0UnitsSinceLower+candidate.workUnits)
		return
	}
	state.L0UnitsSinceLower = 0
	state.NextLowerLevel = candidate.plan.sourceLevel + 1
}
