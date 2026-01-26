package manifest

import (
	"bytes"
	"math"
	"sort"

	"github.com/segmentio/ksuid"
)

// DefaultLevelConfig returns sensible defaults for level configuration.
func DefaultLevelConfig() LevelConfig {
	return LevelConfig{
		MaxLevels:             7,
		L0CompactionThreshold: 8,
		L1TargetSize:          256 << 20,
		LevelSizeMultiplier:   10,
		VLogGarbageThreshold:  0.5,
	}
}

// TargetSize returns the target size for a given level.
// for l0 we will use file count not size.
func (c LevelConfig) TargetSize(level int) int64 {
	if level == 0 {
		return 0
	}
	// lowest has no limit.
	if level >= c.MaxLevels-1 {
		return math.MaxInt64
	}

	multiplier := int64(1)
	for i := 1; i < level; i++ {
		multiplier *= int64(c.LevelSizeMultiplier)
	}
	return c.L1TargetSize * multiplier
}

// Clone returns a deep copy of the manifest.
// Safe to use concurrently after refresh.
func (m *Manifest) Clone() *Manifest {
	if m == nil {
		return nil
	}
	clone := &Manifest{
		Version:     m.Version,
		NextEpoch:   m.NextEpoch,
		LevelConfig: m.LevelConfig,
	}

	if len(m.SSTables) > 0 {
		clone.SSTables = make([]SSTMeta, len(m.SSTables))
		for i, sst := range m.SSTables {
			clone.SSTables[i] = sst
			if len(sst.MinKey) > 0 {
				clone.SSTables[i].MinKey = append([]byte(nil), sst.MinKey...)
			}
			if len(sst.MaxKey) > 0 {
				clone.SSTables[i].MaxKey = append([]byte(nil), sst.MaxKey...)
			}
			if len(sst.VLogRefs) > 0 {
				clone.SSTables[i].VLogRefs = append([]VLogRef(nil), sst.VLogRefs...)
			}
		}
	}

	if len(m.VLogs) > 0 {
		clone.VLogs = make([]VLogMeta, len(m.VLogs))
		for i, vlog := range m.VLogs {
			clone.VLogs[i] = vlog
			if len(vlog.ReferencedBy) > 0 {
				clone.VLogs[i].ReferencedBy = append([]string(nil), vlog.ReferencedBy...)
			}
		}
	}

	return clone
}

// Level returns all SSTables at the given level, properly sorted.
func (m *Manifest) Level(n int) []SSTMeta {
	var result []SSTMeta
	for _, sst := range m.SSTables {
		if sst.Level == n {
			result = append(result, sst)
		}
	}

	if n == 0 {
		// L0: newest first (by SeqHi descending).
		sort.Slice(result, func(i, j int) bool {
			return result[i].SeqHi > result[j].SeqHi
		})
	} else {
		// L1+: by MinKey ascending
		sort.Slice(result, func(i, j int) bool {
			return bytes.Compare(result[i].MinKey, result[j].MinKey) < 0
		})
	}

	return result
}

// LevelCount returns the number of SSTables at the given level.
func (m *Manifest) LevelCount(n int) int {
	count := 0
	for _, sst := range m.SSTables {
		if sst.Level == n {
			count++
		}
	}
	return count
}

// LevelSize returns the total size of SSTables at the given level.
func (m *Manifest) LevelSize(n int) int64 {
	var total int64
	for _, sst := range m.SSTables {
		if sst.Level == n {
			total += sst.Size
		}
	}
	return total
}

// MaxLevel returns the highest level number that contains any SSTables.
func (m *Manifest) MaxLevel() int {
	maxLevel := 0
	for _, sst := range m.SSTables {
		if sst.Level > maxLevel {
			maxLevel = sst.Level
		}
	}
	return maxLevel
}

// ComputeVLogLiveness calculates live bytes for each VLog by summing VLogRefs.
func (m *Manifest) ComputeVLogLiveness() map[ksuid.KSUID]int64 {
	live := make(map[ksuid.KSUID]int64)
	for _, sst := range m.SSTables {
		for _, ref := range sst.VLogRefs {
			live[ref.VLogID] += ref.LiveBytes
		}
	}
	return live
}

// VLogGarbageRatio returns the garbage ratio for a VLog (1 - live/total).
func (m *Manifest) VLogGarbageRatio(vlogID ksuid.KSUID) float64 {
	var totalSize int64
	for _, vlog := range m.VLogs {
		if vlog.ID == vlogID {
			totalSize = vlog.Size
			break
		}
	}
	if totalSize == 0 {
		return 1.0
	}

	liveness := m.ComputeVLogLiveness()
	liveBytes := liveness[vlogID]

	return 1.0 - (float64(liveBytes) / float64(totalSize))
}

// OverlappingSSTs returns SSTables at the given level that overlap with [minKey, maxKey].
func (m *Manifest) OverlappingSSTs(level int, minKey, maxKey []byte) []SSTMeta {
	levelSSTs := m.Level(level)
	var result []SSTMeta

	for _, sst := range levelSSTs {
		if overlapsRange(sst.MinKey, sst.MaxKey, minKey, maxKey) {
			result = append(result, sst)
		}
	}

	return result
}

// overlapsRange returns true if [aMin, aMax] overlaps with [bMin, bMax].
func overlapsRange(aMin, aMax, bMin, bMax []byte) bool {
	if len(aMin) == 0 || len(aMax) == 0 {
		return false
	}
	if len(bMin) == 0 && len(bMax) == 0 {
		return true
	}
	if len(bMin) == 0 {
		return bytes.Compare(aMin, bMax) <= 0
	}
	if len(bMax) == 0 {
		return bytes.Compare(bMin, aMax) <= 0
	}
	return bytes.Compare(aMin, bMax) <= 0 && bytes.Compare(bMin, aMax) <= 0
}
