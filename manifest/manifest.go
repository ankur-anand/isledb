package manifest

import (
	"encoding/json"
	"time"
)

type Manifest struct {
	Version   int    `json:"version"`
	NextEpoch uint64 `json:"next_epoch"`

	WriterFence    *FenceToken `json:"writer_fence,omitempty"`
	CompactorFence *FenceToken `json:"compactor_fence,omitempty"`

	L0SSTs          []SSTMeta    `json:"l0_ssts,omitempty"`
	SortedRuns      []SortedRun  `json:"sorted_runs,omitempty"`
	NextSortedRunID uint32       `json:"next_sorted_run_id,omitempty"`
	TieredConfig    TieredConfig `json:"tiered_config,omitempty"`
}

type FenceToken struct {
	Epoch     uint64    `json:"epoch"`
	Owner     string    `json:"owner"`
	ClaimedAt time.Time `json:"claimed_at"`
}

type SortedRun struct {
	ID   uint32    `json:"id"`
	SSTs []SSTMeta `json:"ssts"`
}

type TieredConfig struct {
	L0CompactionThreshold   int  `json:"l0_compaction_threshold,omitempty"`
	TierCompactionThreshold int  `json:"tier_compaction_threshold,omitempty"`
	TierMaxRuns             int  `json:"tier_max_runs,omitempty"`
	MaxTiers                int  `json:"max_tiers,omitempty"`
	LazyLeveling            bool `json:"lazy_leveling,omitempty"`
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
}

type Current struct {
	Snapshot  string   `json:"snapshot"`
	Logs      []string `json:"logs,omitempty"`
	NextSeq   uint64   `json:"next_seq"`
	NextEpoch uint64   `json:"next_epoch"`

	WriterFence    *FenceToken `json:"writer_fence,omitempty"`
	CompactorFence *FenceToken `json:"compactor_fence,omitempty"`
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
