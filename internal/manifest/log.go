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
	LogOpFenceClaim    LogOpType = "fence_claim"
)

const MaxRetiredObjectsPerEntry = 128

type RetiredObjectKind string

const (
	RetiredObjectSST RetiredObjectKind = "sst"
)

// RetiredObject records one immutable object made unreachable by this
// manifest commit. Key is the exact backend object key used for deletion.
type RetiredObject struct {
	Kind RetiredObjectKind `json:"kind"`
	ID   string            `json:"id"`
	Key  string            `json:"key"`
	Size int64             `json:"size,omitempty"`
}

type ManifestLogEntry struct {
	ID                   ksuid.KSUID           `json:"id"`
	CommitID             string                `json:"commit_id,omitempty"`
	MaintenanceCommandID string                `json:"maintenance_command_id,omitempty"`
	Seq                  uint64                `json:"seq"`
	Role                 FenceRole             `json:"role"`
	Epoch                uint64                `json:"epoch"`
	Timestamp            time.Time             `json:"ts"`
	Op                   LogOpType             `json:"op"`
	SSTable              *SSTMeta              `json:"sstable,omitempty"`
	ChangeBatch          *ChangeBatchMeta      `json:"change_batch,omitempty"`
	RemoveSSTableIDs     []string              `json:"remove_sstable_ids,omitempty"`
	Checkpoint           *Manifest             `json:"checkpoint,omitempty"`
	Compaction           *CompactionLogPayload `json:"compaction,omitempty"`
	RetiredObjects       []RetiredObject       `json:"retired_objects,omitempty"`
	FenceClaim           *FenceClaimPayload    `json:"fence_claim,omitempty"`
}

type FenceClaimPayload struct {
	Role      FenceRole `json:"role"`
	Epoch     uint64    `json:"epoch"`
	Owner     string    `json:"owner"`
	ClaimedAt time.Time `json:"claimed_at"`
}

type CompactionLogPayload struct {
	RemoveSSTableIDs []string  `json:"remove_sstable_ids"`
	SourceLevel      uint32    `json:"source_level"`
	DestinationLevel uint32    `json:"destination_level"`
	AddSSTables      []SSTMeta `json:"add_sstables,omitempty"`
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

	if entry.Seq > m.LogSeq {
		m.LogSeq = entry.Seq
	}

	switch entry.Op {
	case LogOpAddSSTable:
		if entry.SSTable != nil {
			sst := *entry.SSTable
			m.AddL0SST(sst)

			if sst.Epoch >= m.NextEpoch {
				m.NextEpoch = sst.Epoch + 1
			}
		}

	case LogOpRemoveSSTable:
		m.RemoveSSTables(entry.RemoveSSTableIDs)

	case LogOpCheckpoint:
		if entry.Checkpoint != nil {
			if entry.Seq > entry.Checkpoint.LogSeq {
				entry.Checkpoint.LogSeq = entry.Seq
			}
			return entry.Checkpoint
		}

	case LogOpCompaction:
		if entry.Compaction != nil {
			c := entry.Compaction
			m.RemoveCompactionInputs(c.SourceLevel, c.DestinationLevel, c.RemoveSSTableIDs)
			if c.DestinationLevel == 0 {
				for i := len(c.AddSSTables) - 1; i >= 0; i-- {
					m.AddL0SST(c.AddSSTables[i])
				}
			} else {
				m.AddLevelSSTs(c.DestinationLevel, c.AddSSTables)
			}
			for _, sst := range c.AddSSTables {
				if sst.Epoch >= m.NextEpoch {
					m.NextEpoch = sst.Epoch + 1
				}
			}
		}
	}

	return m
}
