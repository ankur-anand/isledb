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
	LogSeq    uint64 `json:"log_seq"`

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
	Offset     int64
	Length     int64
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

	Level       int
	HasBlobRefs bool
}

// ChangeBatchMeta describes one committed, seq-ordered mutation batch emitted
// alongside a memtable flush. The object is visible only after the manifest
// entry that references it is committed.
type ChangeBatchMeta struct {
	ID        string    `json:"id"`
	Path      string    `json:"path"`
	Epoch     uint64    `json:"epoch"`
	SeqLo     uint64    `json:"seq_lo"`
	SeqHi     uint64    `json:"seq_hi"`
	Count     uint32    `json:"count"`
	Size      int64     `json:"size"`
	Checksum  string    `json:"checksum"`
	CreatedAt time.Time `json:"created_at"`
	Version   int       `json:"version,omitempty"`
}

type Current struct {
	LayoutVersion int    `json:"layout_version,omitempty"`
	Format        string `json:"format,omitempty"`
	Snapshot      string `json:"snapshot"`
	LogSeqStart   uint64 `json:"log_seq_start,omitempty"`
	NextSeq       uint64 `json:"next_seq"`
	NextEpoch     uint64 `json:"next_epoch"`

	ChangeFeedLogStart uint64 `json:"change_feed_log_start,omitempty"`

	ActiveEntries []ManifestLogEntry `json:"active_entries,omitempty"`
	IndexFrontier []PageRef          `json:"index_frontier,omitempty"`

	MaxCommittedPosition *uint64 `json:"max_committed_position,omitempty"`
	LowWatermarkPosition *uint64 `json:"low_watermark_position,omitempty"`

	WriterFence    *FenceToken `json:"writer_fence,omitempty"`
	CompactorFence *FenceToken `json:"compactor_fence,omitempty"`
}

type PageRef struct {
	Level     uint8     `json:"level"`
	SeqLo     uint64    `json:"seq_lo"`
	SeqHi     uint64    `json:"seq_hi"`
	Path      string    `json:"path"`
	Count     uint32    `json:"count"`
	Checksum  string    `json:"checksum"`
	CreatedAt time.Time `json:"created_at"`
}

type CommitPage struct {
	LayoutVersion int                `json:"layout_version"`
	PageType      string             `json:"page_type"`
	Level         uint8              `json:"level"`
	SeqLo         uint64             `json:"seq_lo"`
	SeqHi         uint64             `json:"seq_hi"`
	Count         uint32             `json:"count"`
	Entries       []ManifestLogEntry `json:"entries,omitempty"`
	Children      []PageRef          `json:"children,omitempty"`
	CreatedAt     time.Time          `json:"created_at"`
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
