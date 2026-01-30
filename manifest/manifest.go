package manifest

import (
	"encoding/json"
	"time"
)

type CompactionConfig struct {
	L0CompactionThreshold int `json:"l0_compaction_threshold,omitempty"`
	MinSources            int `json:"min_sources,omitempty"`
	MaxSources            int `json:"max_sources,omitempty"`
	SizeThreshold         int `json:"size_threshold,omitempty"`
}

type Manifest struct {
	Version   int    `json:"version"`
	NextEpoch uint64 `json:"next_epoch"`

	WriterFence    *FenceToken `json:"writer_fence,omitempty"`
	CompactorFence *FenceToken `json:"compactor_fence,omitempty"`

	L0SSTs           []SSTMeta        `json:"l0_ssts,omitempty"`
	SortedRuns       []SortedRun      `json:"sorted_runs,omitempty"`
	NextSortedRunID  uint32           `json:"next_sorted_run_id,omitempty"`
	CompactionConfig CompactionConfig `json:"compaction_config,omitempty"`
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
	Snapshot    string `json:"snapshot"`
	LogSeqStart uint64 `json:"log_seq_start,omitempty"`
	NextSeq     uint64 `json:"next_seq"`
	NextEpoch   uint64 `json:"next_epoch"`

	WriterFence    *FenceToken `json:"writer_fence,omitempty"`
	CompactorFence *FenceToken `json:"compactor_fence,omitempty"`
}

func (c *Current) LogPaths(pathFn func(seq uint64) string) []string {
	if c == nil || pathFn == nil || c.NextSeq <= c.LogSeqStart {
		return nil
	}

	paths := make([]string, 0, c.NextSeq-c.LogSeqStart)
	for seq := c.LogSeqStart; seq < c.NextSeq; seq++ {
		paths = append(paths, pathFn(seq))
	}
	return paths
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
