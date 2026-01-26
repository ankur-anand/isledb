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
	AddVLogs         []VLogMeta            `json:"add_vlogs,omitempty"`
	RemoveSSTableIDs []string              `json:"remove_sstable_ids,omitempty"`
	Checkpoint       *Manifest             `json:"checkpoint,omitempty"`
	Compaction       *CompactionLogPayload `json:"compaction,omitempty"`
}

type CompactionLogPayload struct {
	RemoveSSTableIDs []string   `json:"remove_sstable_ids"`
	AddSSTables      []SSTMeta  `json:"add_sstables"`
	AddVLogs         []VLogMeta `json:"add_vlogs,omitempty"`
	RemoveVLogIDs    []string   `json:"remove_vlog_ids,omitempty"`
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
			m.SSTables = append(m.SSTables, *entry.SSTable)
			if entry.SSTable.Epoch >= m.NextEpoch {
				m.NextEpoch = entry.SSTable.Epoch + 1
			}
			if entry.SSTable.HasVLog() && entry.SSTable.VLogSize > 0 {
				addOrMergeVLog(m, VLogMeta{
					ID:           entry.SSTable.VLogID,
					Size:         entry.SSTable.VLogSize,
					ReferencedBy: []string{entry.SSTable.ID},
				})
			}
		}
		for _, vlog := range entry.AddVLogs {
			addOrMergeVLog(m, vlog)
		}

	case LogOpRemoveSSTable:
		if len(entry.RemoveSSTableIDs) > 0 {
			removeSet := make(map[string]bool, len(entry.RemoveSSTableIDs))
			for _, id := range entry.RemoveSSTableIDs {
				removeSet[id] = true
			}

			vlogRemoveSet := make(map[string]bool)
			var newSSTables []SSTMeta
			for _, sst := range m.SSTables {
				if removeSet[sst.ID] {
					if sst.HasVLog() {
						vlogRemoveSet[sst.VLogID.String()] = true
					}
				} else {
					newSSTables = append(newSSTables, sst)
				}
			}
			m.SSTables = newSSTables

			if len(vlogRemoveSet) > 0 {
				var newVLogs []VLogMeta
				for _, vlog := range m.VLogs {
					if !vlogRemoveSet[vlog.ID.String()] {
						newVLogs = append(newVLogs, vlog)
					}
				}
				m.VLogs = newVLogs
			}
		}

	case LogOpCheckpoint:
		if entry.Checkpoint != nil {
			return entry.Checkpoint
		}

	case LogOpCompaction:
		if entry.Compaction != nil {
			c := entry.Compaction
			removeSet := make(map[string]bool, len(c.RemoveSSTableIDs))
			for _, id := range c.RemoveSSTableIDs {
				removeSet[id] = true
			}

			var newSSTables []SSTMeta
			for _, sst := range m.SSTables {
				if !removeSet[sst.ID] {
					newSSTables = append(newSSTables, sst)
				}
			}

			for i := len(c.AddSSTables) - 1; i >= 0; i-- {
				sst := c.AddSSTables[i]
				newSSTables = append([]SSTMeta{sst}, newSSTables...)
				if sst.Epoch >= m.NextEpoch {
					m.NextEpoch = sst.Epoch + 1
				}
			}
			m.SSTables = newSSTables

			for _, vlog := range c.AddVLogs {
				addOrMergeVLog(m, vlog)
			}

			if len(c.RemoveVLogIDs) > 0 {
				removeVLogSet := make(map[string]bool, len(c.RemoveVLogIDs))
				for _, id := range c.RemoveVLogIDs {
					removeVLogSet[id] = true
				}
				var newVLogs []VLogMeta
				for _, vlog := range m.VLogs {
					if !removeVLogSet[vlog.ID.String()] {
						newVLogs = append(newVLogs, vlog)
					}
				}
				m.VLogs = newVLogs
			}
		}
	}

	return m
}

func (m SSTMeta) HasVLog() bool {
	return !m.VLogID.IsNil()
}

func addOrMergeVLog(m *Manifest, vlog VLogMeta) {
	for i := range m.VLogs {
		if m.VLogs[i].ID == vlog.ID {
			if vlog.Size != 0 {
				m.VLogs[i].Size = vlog.Size
			}
			if len(vlog.ReferencedBy) > 0 {
				m.VLogs[i].ReferencedBy = appendUniqueStrings(m.VLogs[i].ReferencedBy, vlog.ReferencedBy)
			}
			return
		}
	}
	m.VLogs = append(m.VLogs, vlog)
}

func appendUniqueStrings(dst []string, src []string) []string {
	if len(src) == 0 {
		return dst
	}
	seen := make(map[string]struct{}, len(dst)+len(src))
	for _, v := range dst {
		seen[v] = struct{}{}
	}
	for _, v := range src {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		dst = append(dst, v)
	}
	return dst
}
