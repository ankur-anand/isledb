package manifest

import (
	"encoding/json"
	"time"

	"github.com/segmentio/ksuid"
)

type Manifest struct {
	Version     int        `json:"version"`
	SSTables    []SSTMeta  `json:"sstables"`
	VLogs       []VLogMeta `json:"vlogs,omitempty"`
	NextEpoch   uint64     `json:"next_epoch"`
	LevelConfig LevelConfig `json:"level_config,omitempty"`
}

type LevelConfig struct {
	MaxLevels             int     `json:"max_levels,omitempty"`
	L0CompactionThreshold int     `json:"l0_compaction_threshold,omitempty"`
	L1TargetSize          int64   `json:"l1_target_size,omitempty"`
	LevelSizeMultiplier   int     `json:"level_size_multiplier,omitempty"`
	VLogGarbageThreshold  float64 `json:"vlog_garbage_threshold,omitempty"`
}

type VLogRef struct {
	VLogID    ksuid.KSUID `json:"vlog_id"`
	LiveBytes int64       `json:"live_bytes"`
}

type SSTSignature struct {
	Algorithm string
	KeyID     string
	Hash      string
	Signature []byte
}

type BloomMeta struct {
	BitsPerKey int
	K          int
}

type SSTMeta struct {
	ID        string
	Epoch     uint64
	SeqLo     uint64
	SeqHi     uint64
	MinKey    []byte
	MaxKey    []byte
	Size      int64
	Checksum  string
	Signature *SSTSignature
	Bloom     BloomMeta
	CreatedAt time.Time

	Level int

	VLogID   ksuid.KSUID
	VLogSize int64

	VLogRefs []VLogRef
}

type VLogMeta struct {
	ID           ksuid.KSUID `json:"id"`
	Size         int64       `json:"size"`
	ReferencedBy []string    `json:"referenced_by"`
}

type Current struct {
	Snapshot  string   `json:"snapshot"`
	Logs      []string `json:"logs,omitempty"`
	NextSeq   uint64   `json:"next_seq"`
	NextEpoch uint64   `json:"next_epoch"`
}

func EncodeSnapshot(m *Manifest) ([]byte, error) {
	return json.Marshal(m)
}

func DecodeSnapshot(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func EncodeCurrent(c *Current) ([]byte, error) {
	return json.Marshal(c)
}

func DecodeCurrent(data []byte) (*Current, error) {
	var c Current
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
