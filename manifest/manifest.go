package manifest

import (
	"encoding/json"
	"time"

	"github.com/segmentio/ksuid"
)

type Manifest struct {
	Version   int    `json:"version"`
	NextEpoch uint64 `json:"next_epoch"`
	LogSeq    uint64 `json:"log_seq"`

	WriterFence    *FenceToken `json:"writer_fence,omitempty"`
	CompactorFence *FenceToken `json:"compactor_fence,omitempty"`

	L0SSTs []SSTMeta `json:"l0_ssts,omitempty"`
	Levels []Level   `json:"levels,omitempty"`
}

type FenceToken struct {
	Epoch     uint64    `json:"epoch"`
	Owner     string    `json:"owner"`
	ClaimedAt time.Time `json:"claimed_at"`
}

// Level is one non-overlapping, key-sorted level. Number starts at 1; L0 is
// represented separately because its SSTs may overlap.
type Level struct {
	Number uint32    `json:"number"`
	SSTs   []SSTMeta `json:"ssts,omitempty"`
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

	// Level records the logical placement committed with this metadata. L0 is
	// zero; compacted levels start at one.
	Level       uint32
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

// WriterCommit is one logical memtable publication. ID remains unchanged when
// SST upload or manifest publication is retried.
type WriterCommit struct {
	ID          string
	SSTable     SSTMeta
	ChangeBatch *ChangeBatchMeta
}

// WriterCommitMarker is the bounded idempotency receipt retained in CURRENT.
// Maintenance updates preserve it even when the committed SST is compacted.
type WriterCommitMarker struct {
	CommitID    string      `json:"commit_id"`
	Fingerprint string      `json:"fingerprint"`
	EntryID     ksuid.KSUID `json:"entry_id"`
	ManifestSeq uint64      `json:"manifest_seq"`
	WriterEpoch uint64      `json:"writer_epoch"`
	SeqLo       uint64      `json:"seq_lo"`
	SeqHi       uint64      `json:"seq_hi"`
	CommittedAt time.Time   `json:"committed_at"`
}

type Current struct {
	LayoutVersion int    `json:"layout_version,omitempty"`
	Format        string `json:"format,omitempty"`
	Snapshot      string `json:"snapshot"`
	LogSeqStart   uint64 `json:"log_seq_start,omitempty"`
	NextSeq       uint64 `json:"next_seq"`
	NextEpoch     uint64 `json:"next_epoch"`

	ChangeFeedLogStart uint64 `json:"change_feed_log_start,omitempty"`
	RetirementLogStart uint64 `json:"retirement_log_start"`
	StateReplayPages   uint64 `json:"state_replay_pages,omitempty"`
	StateReplayBytes   uint64 `json:"state_replay_bytes,omitempty"`

	ActiveEntries []ManifestLogEntry `json:"active_entries,omitempty"`
	IndexFrontier []PageRef          `json:"index_frontier,omitempty"`

	WriterFence        *FenceToken         `json:"writer_fence,omitempty"`
	CompactorFence     *FenceToken         `json:"compactor_fence,omitempty"`
	LastWriterCommit   *WriterCommitMarker `json:"last_writer_commit,omitempty"`
	MaintenanceReceipt *MaintenanceReceipt `json:"maintenance_receipt,omitempty"`
}

type PageRef struct {
	Level        uint8     `json:"level"`
	SeqLo        uint64    `json:"seq_lo"`
	SeqHi        uint64    `json:"seq_hi"`
	Path         string    `json:"path"`
	Count        uint32    `json:"count"`
	EncodedBytes uint64    `json:"encoded_bytes"`
	Checksum     string    `json:"checksum"`
	CreatedAt    time.Time `json:"created_at"`
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
