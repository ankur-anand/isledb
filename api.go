package isledb

import "github.com/segmentio/ksuid"

// VLogMeta describes a VLog file in the manifest.
type VLogMeta struct {
	ID   ksuid.KSUID `json:"id"`
	Size int64       `json:"size"`
	// ReferencedBy tracks which SST IDs reference this VLog.
	// each VLog is only referenced by one SST.
	ReferencedBy []string `json:"referenced_by"`
}

// DirectWriteThreshold is the value size above which values are written
// directly to VLog, bypassing memtable buffering.
// This prevents the memtable from bloat for large values.
const DirectWriteThreshold = 1 * 1024 * 1024

// MaxKeySize is the maximum allowed key size.
const MaxKeySize = 8 * 1024
