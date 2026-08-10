package isledb

import (
	"math"

	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	maxPrimaryCompactionBurstUnits uint32 = 4
	maxCriticalL0BurstUnits        uint32 = 4
	checkpointUrgentMultiplier     uint64 = 2
	l0CriticalTriggerMultiplier           = 4
)

type checkpointPressure struct {
	eligible    bool
	urgent      bool
	replayPages uint64
	replayBytes uint64
}

type compactionCandidate struct {
	plan       *levelCompactionPlan
	inputBytes int64
	workUnits  uint32
	critical   bool
}

type maintenanceDecision struct {
	task       MaintenanceTask
	compaction *compactionCandidate
	checkpoint checkpointPressure
}

func calculateCheckpointPressure(current *manifest.Current, opts ManifestCheckpointOptions) checkpointPressure {
	if current == nil {
		return checkpointPressure{}
	}
	pressure := checkpointPressure{
		replayPages: current.StateReplayPages,
		replayBytes: current.StateReplayBytes,
	}
	pressure.eligible = current.StateReplayPages >= opts.TargetReplayPages ||
		current.StateReplayBytes >= opts.TargetReplayBytes
	pressure.urgent = current.StateReplayPages >= saturatingMultiply(opts.TargetReplayPages, checkpointUrgentMultiplier) ||
		current.StateReplayBytes >= saturatingMultiply(opts.TargetReplayBytes, checkpointUrgentMultiplier)
	return pressure
}

func selectMaintenancePrimary(
	compaction *compactionCandidate,
	checkpoint checkpointPressure,
	state manifest.MaintenanceSchedulerState,
) maintenanceDecision {
	decision := maintenanceDecision{checkpoint: checkpoint}
	if compaction == nil {
		if checkpoint.eligible {
			decision.task = MaintenanceTaskManifestCheckpoint
		}
		return decision
	}
	if !checkpoint.eligible {
		decision.task = MaintenanceTaskSSTCompaction
		decision.compaction = compaction
		return decision
	}

	// A continuously due checkpoint cannot run twice while compaction waits.
	if state.LastPrimary == manifest.MaintenanceCommandCheckpoint {
		decision.task = MaintenanceTaskSSTCompaction
		decision.compaction = compaction
		return decision
	}
	if checkpoint.urgent {
		decision.task = MaintenanceTaskManifestCheckpoint
		return decision
	}
	if compaction.critical && state.CompactionUnitsSinceCheckpoint < maxPrimaryCompactionBurstUnits {
		decision.task = MaintenanceTaskSSTCompaction
		decision.compaction = compaction
		return decision
	}
	decision.task = MaintenanceTaskManifestCheckpoint
	return decision
}

func selectCompactionCandidate(
	candidates []compactionCandidate,
	state manifest.MaintenanceSchedulerState,
) *compactionCandidate {
	var l0 *compactionCandidate
	hasLower := false
	for i := range candidates {
		candidate := &candidates[i]
		if candidate.plan.sourceLevel == 0 {
			l0 = candidate
			continue
		}
		hasLower = true
	}
	if l0 == nil {
		return nextLowerCompaction(candidates, state.NextLowerLevel)
	}
	if !hasLower {
		return l0
	}

	limit := uint32(1)
	if l0.critical {
		limit = maxCriticalL0BurstUnits
	}
	if state.L0UnitsSinceLower < limit {
		return l0
	}
	return nextLowerCompaction(candidates, state.NextLowerLevel)
}

func nextLowerCompaction(candidates []compactionCandidate, next uint32) *compactionCandidate {
	if next == 0 {
		next = 1
	}
	var first *compactionCandidate
	for i := range candidates {
		candidate := &candidates[i]
		if candidate.plan.sourceLevel == 0 {
			continue
		}
		if first == nil {
			first = candidate
		}
		if candidate.plan.sourceLevel >= next {
			return candidate
		}
	}
	return first
}

func compactionPlanWorkUnits(plan *levelCompactionPlan, maxInputBytes int64) (int64, uint32) {
	if plan == nil {
		return 0, 0
	}
	var inputBytes int64
	for _, sst := range plan.sourceSSTs {
		inputBytes = saturatingAddInt64(inputBytes, sst.Size)
	}
	for _, sst := range plan.destinationSSTs {
		inputBytes = saturatingAddInt64(inputBytes, sst.Size)
	}
	if plan.metadataOnly || maxInputBytes <= 0 || inputBytes <= maxInputBytes {
		return inputBytes, 1
	}
	units := uint64(inputBytes / maxInputBytes)
	if inputBytes%maxInputBytes != 0 {
		units++
	}
	if units > uint64(maxPrimaryCompactionBurstUnits) {
		units = uint64(maxPrimaryCompactionBurstUnits)
	}
	return inputBytes, uint32(units)
}

func l0CompactionCritical(l0Count, trigger int) bool {
	if trigger <= 0 {
		return false
	}
	if trigger > math.MaxInt/l0CriticalTriggerMultiplier {
		return l0Count >= trigger
	}
	return l0Count >= trigger*l0CriticalTriggerMultiplier
}

func saturatingMultiply(value, multiplier uint64) uint64 {
	if multiplier != 0 && value > math.MaxUint64/multiplier {
		return math.MaxUint64
	}
	return value * multiplier
}

func saturatingAddInt64(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	if b < 0 && a < math.MinInt64-b {
		return math.MinInt64
	}
	return a + b
}
