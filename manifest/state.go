package manifest

import (
	"bytes"
	"sort"

	"github.com/ankur-anand/isledb/internal"
)

func DefaultCompactionConfig() CompactionConfig {
	return CompactionConfig{
		L0CompactionThreshold: 8,
		MinSources:            4,
		MaxSources:            8,
		SizeThreshold:         4,
	}
}

func (m *Manifest) Clone() *Manifest {
	if m == nil {
		return nil
	}
	clone := &Manifest{
		Version:          m.Version,
		NextEpoch:        m.NextEpoch,
		LogSeq:           m.LogSeq,
		NextSortedRunID:  m.NextSortedRunID,
		CompactionConfig: m.CompactionConfig,
	}

	if len(m.L0SSTs) > 0 {
		clone.L0SSTs = make([]SSTMeta, len(m.L0SSTs))
		for i, sst := range m.L0SSTs {
			clone.L0SSTs[i] = sst
			if len(sst.MinKey) > 0 {
				clone.L0SSTs[i].MinKey = append([]byte(nil), sst.MinKey...)
			}
			if len(sst.MaxKey) > 0 {
				clone.L0SSTs[i].MaxKey = append([]byte(nil), sst.MaxKey...)
			}
		}
	}

	if len(m.SortedRuns) > 0 {
		clone.SortedRuns = make([]SortedRun, len(m.SortedRuns))
		for i, sr := range m.SortedRuns {
			clone.SortedRuns[i] = SortedRun{
				ID:   sr.ID,
				SSTs: make([]SSTMeta, len(sr.SSTs)),
			}
			for j, sst := range sr.SSTs {
				clone.SortedRuns[i].SSTs[j] = sst
				if len(sst.MinKey) > 0 {
					clone.SortedRuns[i].SSTs[j].MinKey = append([]byte(nil), sst.MinKey...)
				}
				if len(sst.MaxKey) > 0 {
					clone.SortedRuns[i].SSTs[j].MaxKey = append([]byte(nil), sst.MaxKey...)
				}
			}
		}
	}

	return clone
}

func (m *Manifest) L0SSTCount() int {
	return len(m.L0SSTs)
}

func (m *Manifest) AddL0SST(sst SSTMeta) {
	m.L0SSTs = append([]SSTMeta{sst}, m.L0SSTs...)
}

func (m *Manifest) RemoveL0SSTs(ids []string) {
	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	newL0 := make([]SSTMeta, 0, len(m.L0SSTs))
	for _, sst := range m.L0SSTs {
		if !idSet[sst.ID] {
			newL0 = append(newL0, sst)
		}
	}
	m.L0SSTs = newL0
}

func (m *Manifest) SortedRunCount() int {
	return len(m.SortedRuns)
}

// LookupSST returns the SST meta by ID if present in L0 or sorted runs.
func (m *Manifest) LookupSST(id string) *SSTMeta {
	if m == nil {
		return nil
	}

	for i := range m.L0SSTs {
		if m.L0SSTs[i].ID == id {
			return &m.L0SSTs[i]
		}
	}

	for i := range m.SortedRuns {
		for j := range m.SortedRuns[i].SSTs {
			if m.SortedRuns[i].SSTs[j].ID == id {
				return &m.SortedRuns[i].SSTs[j]
			}
		}
	}

	return nil
}

// FindConsecutiveSimilarRuns finds consecutive runs with similar sizes.
func (m *Manifest) FindConsecutiveSimilarRuns(minSources, maxSources, sizeThreshold int) []SortedRun {
	if minSources < 2 {
		minSources = 2
	}
	if maxSources < minSources {
		maxSources = minSources
	}
	if sizeThreshold < 1 {
		sizeThreshold = 1
	}
	if len(m.SortedRuns) < minSources {
		return nil
	}

	for start := 0; start <= len(m.SortedRuns)-minSources; start++ {
		baseSize := m.SortedRuns[start].TotalSize()
		group := []SortedRun{m.SortedRuns[start]}

		for i := start + 1; i < len(m.SortedRuns) && len(group) < maxSources; i++ {
			size := m.SortedRuns[i].TotalSize()

			ratio := float64(size) / float64(baseSize)
			if ratio > float64(sizeThreshold) || ratio < 1.0/float64(sizeThreshold) {
				break
			}
			group = append(group, m.SortedRuns[i])
		}

		if len(group) >= minSources {
			return group
		}
	}

	return nil
}

func (m *Manifest) GetSortedRun(id uint32) *SortedRun {
	for i := range m.SortedRuns {
		if m.SortedRuns[i].ID == id {
			return &m.SortedRuns[i]
		}
	}
	return nil
}

func (m *Manifest) AddSortedRun(ssts []SSTMeta) uint32 {
	id := m.NextSortedRunID
	m.NextSortedRunID++

	sr := SortedRun{
		ID:   id,
		SSTs: make([]SSTMeta, len(ssts)),
	}
	copy(sr.SSTs, ssts)

	sort.Slice(sr.SSTs, func(i, j int) bool {
		return bytes.Compare(sr.SSTs[i].MinKey, sr.SSTs[j].MinKey) < 0
	})

	m.SortedRuns = append([]SortedRun{sr}, m.SortedRuns...)
	return id
}

func (m *Manifest) RemoveSortedRuns(ids []uint32) {
	idSet := make(map[uint32]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	newRuns := make([]SortedRun, 0, len(m.SortedRuns))
	for _, sr := range m.SortedRuns {
		if !idSet[sr.ID] {
			newRuns = append(newRuns, sr)
		}
	}
	m.SortedRuns = newRuns
}

func (m *Manifest) RemoveSSTsFromSortedRuns(sstIDs []string) {
	idSet := make(map[string]bool, len(sstIDs))
	for _, id := range sstIDs {
		idSet[id] = true
	}

	for i := range m.SortedRuns {
		newSSTs := make([]SSTMeta, 0, len(m.SortedRuns[i].SSTs))
		for _, sst := range m.SortedRuns[i].SSTs {
			if !idSet[sst.ID] {
				newSSTs = append(newSSTs, sst)
			}
		}
		m.SortedRuns[i].SSTs = newSSTs
	}

	newRuns := make([]SortedRun, 0, len(m.SortedRuns))
	for _, sr := range m.SortedRuns {
		if len(sr.SSTs) > 0 {
			newRuns = append(newRuns, sr)
		}
	}
	m.SortedRuns = newRuns
}

func (sr *SortedRun) FindSST(key []byte) *SSTMeta {
	if len(sr.SSTs) == 0 {
		return nil
	}

	idx := sort.Search(len(sr.SSTs), func(i int) bool {
		return bytes.Compare(sr.SSTs[i].MaxKey, key) >= 0
	})

	if idx >= len(sr.SSTs) {
		return nil
	}

	sst := &sr.SSTs[idx]
	if bytes.Compare(key, sst.MinKey) >= 0 {
		return sst
	}

	return nil
}

func (sr *SortedRun) OverlappingSSTs(minKey, maxKey []byte) []SSTMeta {
	if len(sr.SSTs) == 0 {
		return nil
	}

	var result []SSTMeta
	for _, sst := range sr.SSTs {
		if internal.OverlapsRange(sst.MinKey, sst.MaxKey, minKey, maxKey) {
			result = append(result, sst)
		}
	}
	return result
}

func (sr *SortedRun) TotalSize() int64 {
	var total int64
	for _, sst := range sr.SSTs {
		total += sst.Size
	}
	return total
}

func (sr *SortedRun) MinKey() []byte {
	if len(sr.SSTs) == 0 {
		return nil
	}
	return sr.SSTs[0].MinKey
}

func (sr *SortedRun) MaxKey() []byte {
	if len(sr.SSTs) == 0 {
		return nil
	}
	return sr.SSTs[len(sr.SSTs)-1].MaxKey
}

func (sr *SortedRun) InRange(key []byte) bool {
	if len(sr.SSTs) == 0 {
		return false
	}
	minKey := sr.SSTs[0].MinKey
	maxKey := sr.SSTs[len(sr.SSTs)-1].MaxKey

	return bytes.Compare(key, minKey) >= 0 && bytes.Compare(key, maxKey) <= 0
}

func (m *Manifest) MaxSeqNum() uint64 {
	var maxSeq uint64

	for _, sst := range m.L0SSTs {
		if sst.SeqHi > maxSeq {
			maxSeq = sst.SeqHi
		}
	}

	for _, sr := range m.SortedRuns {
		for _, sst := range sr.SSTs {
			if sst.SeqHi > maxSeq {
				maxSeq = sst.SeqHi
			}
		}
	}

	return maxSeq
}

func (m *Manifest) AllSSTIDs() []string {
	var ids []string

	for _, sst := range m.L0SSTs {
		ids = append(ids, sst.ID)
	}

	for _, sr := range m.SortedRuns {
		for _, sst := range sr.SSTs {
			ids = append(ids, sst.ID)
		}
	}

	return ids
}
