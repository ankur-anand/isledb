package isledb

import (
	"github.com/ankur-anand/isledb/manifest"
	"github.com/segmentio/ksuid"
)

// DirectWriteThreshold is the value size above which values are written
// directly to VLog, bypassing memtable buffering.
// This prevents the memtable from bloat for large values.
const DirectWriteThreshold = 1 * 1024 * 1024

// MaxKeySize is the maximum allowed key size.
const MaxKeySize = 8 * 1024

type Manifest = manifest.Manifest
type LevelConfig = manifest.LevelConfig
type SSTMeta = manifest.SSTMeta
type VLogMeta = manifest.VLogMeta
type VLogRef = manifest.VLogRef
type BloomMeta = manifest.BloomMeta
type SSTSignature = manifest.SSTSignature
type CompactionLogPayload = manifest.CompactionLogPayload

func DefaultLevelConfig() LevelConfig {
	return manifest.DefaultLevelConfig()
}

type CompactionEntry struct {
	Key  []byte
	Seq  uint64
	Kind OpKind

	Inline bool
	Value  []byte

	VLogID    ksuid.KSUID
	VOffset   uint64
	VLength   uint32
	VChecksum uint32
}
