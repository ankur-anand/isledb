package manifest

import (
	"bytes"
	"fmt"
	"sort"
)

func (m *Manifest) Clone() *Manifest {
	if m == nil {
		return nil
	}
	clone := &Manifest{
		Version:        m.Version,
		NextEpoch:      m.NextEpoch,
		LogSeq:         m.LogSeq,
		WriterFence:    m.WriterFence.Clone(),
		CompactorFence: m.CompactorFence.Clone(),
	}
	clone.L0SSTs = append(clone.L0SSTs, m.L0SSTs...)
	if len(m.Levels) > 0 {
		clone.Levels = make([]Level, len(m.Levels))
		for i := range m.Levels {
			clone.Levels[i].Number = m.Levels[i].Number
			clone.Levels[i].SSTs = append(clone.Levels[i].SSTs, m.Levels[i].SSTs...)
		}
	}
	return clone
}

func (f *FenceToken) Clone() *FenceToken {
	if f == nil {
		return nil
	}
	clone := *f
	return &clone
}

func (c *Current) Clone() *Current {
	if c == nil {
		return nil
	}

	clone := &Current{
		LayoutVersion:        c.LayoutVersion,
		Format:               c.Format,
		Snapshot:             c.Snapshot.Clone(),
		LogSeqStart:          c.LogSeqStart,
		NextSeq:              c.NextSeq,
		NextEpoch:            c.NextEpoch,
		ChangeFeedEnabled:    c.ChangeFeedEnabled,
		ChangeFeedPayload:    c.ChangeFeedPayload,
		ChangeFeedLogStart:   c.ChangeFeedLogStart,
		StateReplayPages:     c.StateReplayPages,
		StateReplayBytes:     c.StateReplayBytes,
		MaxPinnedViewAge:     c.MaxPinnedViewAge,
		WriterFence:          c.WriterFence.Clone(),
		CompactorFence:       c.CompactorFence.Clone(),
		LastWriterCommit:     c.LastWriterCommit.Clone(),
		MaintenanceReceipt:   c.MaintenanceReceipt.Clone(),
		MaintenanceScheduler: c.MaintenanceScheduler,
	}
	clone.ActiveEntries = append(clone.ActiveEntries, c.ActiveEntries...)
	clone.IndexFrontier = append(clone.IndexFrontier, c.IndexFrontier...)
	return clone
}

func (r *MaintenanceReceipt) Clone() *MaintenanceReceipt {
	if r == nil {
		return nil
	}
	clone := *r
	return &clone
}

func (m *WriterCommitMarker) Clone() *WriterCommitMarker {
	if m == nil {
		return nil
	}
	clone := *m
	return &clone
}

func (m *Manifest) L0SSTCount() int {
	return len(m.L0SSTs)
}

func (m *Manifest) AddL0SST(sst SSTMeta) {
	sst.Level = 0
	m.L0SSTs = append(m.L0SSTs, SSTMeta{})
	copy(m.L0SSTs[1:], m.L0SSTs[:len(m.L0SSTs)-1])
	m.L0SSTs[0] = sst
}

func (m *Manifest) RemoveSSTables(ids []string) {
	if len(ids) == 0 {
		return
	}
	idSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	m.L0SSTs = removeSSTs(m.L0SSTs, idSet)
	for i := range m.Levels {
		m.Levels[i].SSTs = removeSSTs(m.Levels[i].SSTs, idSet)
	}
	m.removeEmptyLevels()
}

func (m *Manifest) RemoveCompactionInputs(sourceLevel, destinationLevel uint32, ids []string) {
	if len(ids) == 0 {
		return
	}
	idSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	m.removeSSTablesFromLevel(sourceLevel, idSet)
	if destinationLevel != sourceLevel {
		m.removeSSTablesFromLevel(destinationLevel, idSet)
	}
	m.removeEmptyLevels()
}

func (m *Manifest) removeSSTablesFromLevel(level uint32, ids map[string]struct{}) {
	if level == 0 {
		m.L0SSTs = removeSSTs(m.L0SSTs, ids)
		return
	}
	if target := m.Level(level); target != nil {
		target.SSTs = removeSSTs(target.SSTs, ids)
	}
}

func removeSSTs(ssts []SSTMeta, ids map[string]struct{}) []SSTMeta {
	n := 0
	for _, sst := range ssts {
		if _, remove := ids[sst.ID]; remove {
			continue
		}
		ssts[n] = sst
		n++
	}
	clear(ssts[n:])
	return ssts[:n]
}

func (m *Manifest) removeEmptyLevels() {
	n := 0
	for _, level := range m.Levels {
		if len(level.SSTs) == 0 {
			continue
		}
		m.Levels[n] = level
		n++
	}
	clear(m.Levels[n:])
	m.Levels = m.Levels[:n]
}

func (m *Manifest) Level(number uint32) *Level {
	i := sort.Search(len(m.Levels), func(i int) bool {
		return m.Levels[i].Number >= number
	})
	if i == len(m.Levels) || m.Levels[i].Number != number {
		return nil
	}
	return &m.Levels[i]
}

func (m *Manifest) AddLevelSSTs(number uint32, ssts []SSTMeta) {
	if number == 0 || len(ssts) == 0 {
		return
	}
	i := sort.Search(len(m.Levels), func(i int) bool {
		return m.Levels[i].Number >= number
	})
	if i == len(m.Levels) || m.Levels[i].Number != number {
		m.Levels = append(m.Levels, Level{})
		copy(m.Levels[i+1:], m.Levels[i:])
		m.Levels[i] = Level{Number: number}
	}
	additions := append([]SSTMeta(nil), ssts...)
	for j := range additions {
		additions[j].Level = number
	}
	sort.Slice(additions, func(a, b int) bool {
		return bytes.Compare(additions[a].MinKey, additions[b].MinKey) < 0
	})
	m.Levels[i].SSTs = insertLevelSSTs(m.Levels[i].SSTs, additions)
}

func insertLevelSSTs(existing, additions []SSTMeta) []SSTMeta {
	if len(existing) == 0 {
		return additions
	}
	insertAt := sort.Search(len(existing), func(i int) bool {
		return bytes.Compare(existing[i].MinKey, additions[0].MinKey) >= 0
	})
	combined := make([]SSTMeta, len(existing)+len(additions))
	copy(combined, existing[:insertAt])
	copy(combined[insertAt:], additions)
	copy(combined[insertAt+len(additions):], existing[insertAt:])
	return combined
}

func (m *Manifest) LookupSST(id string) *SSTMeta {
	if m == nil {
		return nil
	}
	for i := range m.L0SSTs {
		if m.L0SSTs[i].ID == id {
			return &m.L0SSTs[i]
		}
	}
	for i := range m.Levels {
		for j := range m.Levels[i].SSTs {
			if m.Levels[i].SSTs[j].ID == id {
				return &m.Levels[i].SSTs[j]
			}
		}
	}
	return nil
}

func (m *Manifest) ValidateLevels() error {
	var previous uint32
	totalSSTs := len(m.L0SSTs)
	for i := range m.Levels {
		totalSSTs += len(m.Levels[i].SSTs)
	}
	seen := make(map[string]struct{}, totalSSTs)
	for _, sst := range m.L0SSTs {
		if sst.ID == "" {
			return fmt.Errorf("empty L0 SST id")
		}
		if _, ok := seen[sst.ID]; ok {
			return fmt.Errorf("duplicate SST id %q", sst.ID)
		}
		seen[sst.ID] = struct{}{}
		if sst.Level != 0 {
			return fmt.Errorf("L0 SST %q has level %d", sst.ID, sst.Level)
		}
	}
	for i := range m.Levels {
		level := &m.Levels[i]
		if level.Number == 0 || (i > 0 && level.Number <= previous) {
			return fmt.Errorf("levels are not strictly ordered at level %d", level.Number)
		}
		previous = level.Number
		for j := range level.SSTs {
			sst := &level.SSTs[j]
			if sst.ID == "" {
				return fmt.Errorf("empty SST id in L%d", level.Number)
			}
			if _, ok := seen[sst.ID]; ok {
				return fmt.Errorf("duplicate SST id %q", sst.ID)
			}
			seen[sst.ID] = struct{}{}
			if sst.Level != level.Number {
				return fmt.Errorf("SST %q has level %d, want %d", sst.ID, sst.Level, level.Number)
			}
			if bytes.Compare(sst.MinKey, sst.MaxKey) > 0 {
				return fmt.Errorf("invalid key range for SST %q", sst.ID)
			}
			if j > 0 && bytes.Compare(level.SSTs[j-1].MaxKey, sst.MinKey) >= 0 {
				return fmt.Errorf("overlapping SSTs %q and %q in L%d", level.SSTs[j-1].ID, sst.ID, level.Number)
			}
		}
	}
	return nil
}

func (l *Level) FindSST(key []byte) *SSTMeta {
	if l == nil || len(l.SSTs) == 0 {
		return nil
	}
	i := sort.Search(len(l.SSTs), func(i int) bool {
		return bytes.Compare(l.SSTs[i].MaxKey, key) >= 0
	})
	if i == len(l.SSTs) || bytes.Compare(key, l.SSTs[i].MinKey) < 0 {
		return nil
	}
	return &l.SSTs[i]
}

func (l *Level) OverlappingSSTs(minKey, maxKey []byte) []SSTMeta {
	if l == nil || len(l.SSTs) == 0 {
		return nil
	}
	lo := 0
	if len(minKey) > 0 {
		lo = sort.Search(len(l.SSTs), func(i int) bool {
			return bytes.Compare(l.SSTs[i].MaxKey, minKey) >= 0
		})
	}
	hi := len(l.SSTs)
	if len(maxKey) > 0 {
		hi = lo + sort.Search(len(l.SSTs)-lo, func(i int) bool {
			return bytes.Compare(l.SSTs[lo+i].MinKey, maxKey) > 0
		})
	}
	return l.SSTs[lo:hi]
}

func (l *Level) TotalSize() int64 {
	var total int64
	if l != nil {
		for _, sst := range l.SSTs {
			total += sst.Size
		}
	}
	return total
}

func (l *Level) MinKey() []byte {
	if l == nil || len(l.SSTs) == 0 {
		return nil
	}
	return l.SSTs[0].MinKey
}

func (l *Level) MaxKey() []byte {
	if l == nil || len(l.SSTs) == 0 {
		return nil
	}
	return l.SSTs[len(l.SSTs)-1].MaxKey
}

func (m *Manifest) MaxSeqNum() uint64 {
	var maxSeq uint64
	for _, sst := range m.L0SSTs {
		if sst.SeqHi > maxSeq {
			maxSeq = sst.SeqHi
		}
	}
	for _, level := range m.Levels {
		for _, sst := range level.SSTs {
			if sst.SeqHi > maxSeq {
				maxSeq = sst.SeqHi
			}
		}
	}
	return maxSeq
}

func (m *Manifest) AllSSTIDs() []string {
	total := len(m.L0SSTs)
	for _, level := range m.Levels {
		total += len(level.SSTs)
	}
	if total == 0 {
		return nil
	}
	ids := make([]string, 0, total)
	for _, sst := range m.L0SSTs {
		ids = append(ids, sst.ID)
	}
	for _, level := range m.Levels {
		for _, sst := range level.SSTs {
			ids = append(ids, sst.ID)
		}
	}
	return ids
}

func (m *Manifest) MaxKey() []byte {
	if m == nil {
		return nil
	}
	var maxKey []byte
	for _, sst := range m.L0SSTs {
		if bytes.Compare(sst.MaxKey, maxKey) > 0 {
			maxKey = sst.MaxKey
		}
	}
	for i := range m.Levels {
		if key := m.Levels[i].MaxKey(); bytes.Compare(key, maxKey) > 0 {
			maxKey = key
		}
	}
	return append([]byte(nil), maxKey...)
}

func (m *Manifest) MinKey() []byte {
	if m == nil {
		return nil
	}
	var minKey []byte
	found := false
	for _, sst := range m.L0SSTs {
		if !found || bytes.Compare(sst.MinKey, minKey) < 0 {
			minKey = sst.MinKey
			found = true
		}
	}
	for i := range m.Levels {
		if key := m.Levels[i].MinKey(); len(key) > 0 && (!found || bytes.Compare(key, minKey) < 0) {
			minKey = key
			found = true
		}
	}
	if !found {
		return nil
	}
	return append([]byte(nil), minKey...)
}
