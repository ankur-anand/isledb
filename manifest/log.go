package manifest

import (
	"encoding/json"
	"time"

	"github.com/segmentio/ksuid"
)

type LogOpType string

const (
	LogOpAddSSTable    LogOpType = "add_sstable"
	LogOpRemoveSSTable LogOpType = "remove_sstables"
	LogOpCheckpoint    LogOpType = "checkpoint"
	LogOpCompaction    LogOpType = "compaction"
)

type ManifestLogEntry struct {
	ID               ksuid.KSUID           `json:"id"`
	Seq              uint64                `json:"seq"`
	Timestamp        time.Time             `json:"ts"`
	Op               LogOpType             `json:"op"`
	SSTable          *SSTMeta              `json:"sstable,omitempty"`
	RemoveSSTableIDs []string              `json:"remove_sstable_ids,omitempty"`
	Checkpoint       *Manifest             `json:"checkpoint,omitempty"`
	Compaction       *CompactionLogPayload `json:"compaction,omitempty"`
}

type CompactionLogPayload struct {
	RemoveSSTableIDs   []string   `json:"remove_sstable_ids"`
	RemoveSortedRunIDs []uint32   `json:"remove_sorted_run_ids,omitempty"`
	AddSSTables        []SSTMeta  `json:"add_sstables"`
	AddSortedRun       *SortedRun `json:"add_sorted_run,omitempty"`
}

func EncodeLogEntry(entry *ManifestLogEntry) ([]byte, error) {
	return json.Marshal(entry)
}

func DecodeLogEntry(data []byte) (*ManifestLogEntry, error) {
	var entry ManifestLogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func ApplyLogEntries(m *Manifest, entries []*ManifestLogEntry) *Manifest {
	current := m
	for _, entry := range entries {
		current = ApplyLogEntry(current, entry)
	}
	return current
}

func ApplyLogEntry(m *Manifest, entry *ManifestLogEntry) *Manifest {
	if m == nil {
		m = &Manifest{}
	}
	if entry == nil {
		return m
	}

	switch entry.Op {
	case LogOpAddSSTable:
		if entry.SSTable != nil {
			sst := *entry.SSTable

			if sst.Level == 0 {

				m.L0SSTs = append([]SSTMeta{sst}, m.L0SSTs...)
			} else {

				sr := SortedRun{
					ID:   m.NextSortedRunID,
					SSTs: []SSTMeta{sst},
				}
				m.NextSortedRunID++

				m.SortedRuns = append([]SortedRun{sr}, m.SortedRuns...)
			}

			if sst.Epoch >= m.NextEpoch {
				m.NextEpoch = sst.Epoch + 1
			}
		}

	case LogOpRemoveSSTable:
		if len(entry.RemoveSSTableIDs) > 0 {
			removeSet := make(map[string]bool, len(entry.RemoveSSTableIDs))
			for _, id := range entry.RemoveSSTableIDs {
				removeSet[id] = true
			}

			var newL0 []SSTMeta
			for _, sst := range m.L0SSTs {
				if !removeSet[sst.ID] {
					newL0 = append(newL0, sst)
				}
			}
			m.L0SSTs = newL0

			for i := range m.SortedRuns {
				var newSSTs []SSTMeta
				for _, sst := range m.SortedRuns[i].SSTs {
					if !removeSet[sst.ID] {
						newSSTs = append(newSSTs, sst)
					}
				}
				m.SortedRuns[i].SSTs = newSSTs
			}

			var newRuns []SortedRun
			for _, sr := range m.SortedRuns {
				if len(sr.SSTs) > 0 {
					newRuns = append(newRuns, sr)
				}
			}
			m.SortedRuns = newRuns
		}

	case LogOpCheckpoint:
		if entry.Checkpoint != nil {
			return entry.Checkpoint
		}

	case LogOpCompaction:
		if entry.Compaction != nil {
			c := entry.Compaction

			if len(c.RemoveSSTableIDs) > 0 {
				removeSet := make(map[string]bool, len(c.RemoveSSTableIDs))
				for _, id := range c.RemoveSSTableIDs {
					removeSet[id] = true
				}

				var newL0 []SSTMeta
				for _, sst := range m.L0SSTs {
					if !removeSet[sst.ID] {
						newL0 = append(newL0, sst)
					}
				}
				m.L0SSTs = newL0

				m.RemoveSSTsFromSortedRuns(c.RemoveSSTableIDs)
			}

			if len(c.RemoveSortedRunIDs) > 0 {
				m.RemoveSortedRuns(c.RemoveSortedRunIDs)
			}

			if c.AddSortedRun != nil {

				sr := *c.AddSortedRun
				if sr.ID >= m.NextSortedRunID {
					m.NextSortedRunID = sr.ID + 1
				}
				m.SortedRuns = append([]SortedRun{sr}, m.SortedRuns...)
			}

			for _, sst := range c.AddSSTables {
				if sst.Level == 0 {
					m.L0SSTs = append([]SSTMeta{sst}, m.L0SSTs...)
				} else {
					sr := SortedRun{
						ID:   m.NextSortedRunID,
						SSTs: []SSTMeta{sst},
					}
					m.NextSortedRunID++
					m.SortedRuns = append([]SortedRun{sr}, m.SortedRuns...)
				}
				if sst.Epoch >= m.NextEpoch {
					m.NextEpoch = sst.Epoch + 1
				}
			}
		}
	}

	return m
}
