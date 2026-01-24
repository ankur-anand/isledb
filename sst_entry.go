package isledb

import (
	"encoding/binary"
	"errors"

	"github.com/segmentio/ksuid"
)

// OpKind represents the type of operation.
type OpKind byte

const (
	OpPut    OpKind = 1
	OpDelete OpKind = 2
)

const (
	MarkerInline  byte = 0x00
	MarkerPointer byte = 0x01
	MarkerDelete  byte = 0x02
)

const (
	// DefaultInlineThreshold is the maximum value size stored directly in SST.
	DefaultInlineThreshold = 16 * 1024
	KSUIDSize              = 20
)

// VLogEntryHeaderSize is the size of the VLog entry header.
// length:4 + checksum:4
const VLogEntryHeaderSize = 8

// KeyEntry represents a single key in the LSM tree. This is what gets stored in as the value for the key in SST.
// If Inline is true Value is Stored Inline in SST File.
// If Inline is false Check the Vlog Pointer which is where the Value is.
// Vlog pointer is basically a KSUID File name.
type KeyEntry struct {
	Key  []byte
	Seq  uint64
	Kind OpKind

	// Inline value
	Inline bool
	Value  []byte

	// VLog pointer
	VLogID    ksuid.KSUID
	VOffset   uint64
	VLength   uint32
	VChecksum uint32
}

// PointerSize is the size of an encoded VLog pointer (without marker).
// ksuid:20 + offset:8 + length:4 + checksum:4 = 36 bytes
const PointerSize = KSUIDSize + 8 + 4 + 4

// EncodeKeyEntry encodes a KeyEntry to bytes for storage in SST.
//
// format:
//
//	if Inline:  [0x00][value bytes...]
//	if Pointer: [0x01][ksuid:20][offset:8][length:4][checksum:4]
//	if Delete:  [0x02]
func EncodeKeyEntry(e KeyEntry) []byte {
	if e.Kind == OpDelete {
		return []byte{MarkerDelete}
	}

	if e.Inline {
		buf := make([]byte, 1+len(e.Value))
		buf[0] = MarkerInline
		copy(buf[1:], e.Value)
		return buf
	}

	buf := make([]byte, 1+PointerSize)
	buf[0] = MarkerPointer
	copy(buf[1:21], e.VLogID[:])
	binary.LittleEndian.PutUint64(buf[21:29], e.VOffset)
	binary.LittleEndian.PutUint32(buf[29:33], e.VLength)
	binary.LittleEndian.PutUint32(buf[33:37], e.VChecksum)
	return buf
}

// DecodeKeyEntry decodes a KeyEntry from SST value bytes.
func DecodeKeyEntry(key, encoded []byte) (KeyEntry, error) {
	if len(encoded) == 0 {
		return KeyEntry{}, errors.New("empty encoded entry")
	}

	e := KeyEntry{
		Key: key,
	}

	switch encoded[0] {
	case MarkerDelete:
		e.Kind = OpDelete
	case MarkerInline:
		e.Kind = OpPut
		e.Inline = true
		e.Value = encoded[1:]
	case MarkerPointer:
		if len(encoded) < 1+PointerSize {
			return KeyEntry{}, errors.New("pointer entry too short")
		}
		e.Kind = OpPut
		e.Inline = false

		var id ksuid.KSUID
		copy(id[:], encoded[1:21])
		e.VLogID = id

		e.VOffset = binary.LittleEndian.Uint64(encoded[21:29])
		e.VLength = binary.LittleEndian.Uint32(encoded[29:33])
		e.VChecksum = binary.LittleEndian.Uint32(encoded[33:37])
	default:
		return KeyEntry{}, errors.New("unknown marker byte")
	}

	return e, nil
}

// VLogPointer references a value in a VLog file.
type VLogPointer struct {
	VLogID ksuid.KSUID
	// Byte offset within file
	Offset   uint64
	Length   uint32
	Checksum uint32
}

type MemEntry struct {
	Key    []byte
	Seq    uint64
	Kind   OpKind
	Inline bool
	Value  []byte

	// 256B-1MB: buffered until flush
	PendingValue []byte
	// >1MB: Individual *VLogPointer
	VLogPtr *VLogPointer
}
