package internal

import (
	"encoding/binary"
	"errors"
)

type OpKind byte

const (
	OpPut    OpKind = 1
	OpDelete OpKind = 2
)

const (
	MarkerInline byte = 0x00
	MarkerDelete byte = 0x02
	MarkerBlob   byte = 0x03

	MarkerTTLFlag byte = 0x80
)

type KeyEntry struct {
	Key  []byte
	Seq  uint64
	Kind OpKind

	Inline bool
	Value  []byte

	BlobID [32]byte

	ExpireAt int64
}

func EncodeKeyEntry(e KeyEntry) []byte {
	hasTTL := e.ExpireAt > 0

	if e.Kind == OpDelete {
		if hasTTL {
			buf := make([]byte, 1+8)
			buf[0] = MarkerDelete | MarkerTTLFlag
			binary.BigEndian.PutUint64(buf[1:], uint64(e.ExpireAt))
			return buf
		}
		return []byte{MarkerDelete}
	}

	if e.Inline {
		if hasTTL {
			buf := make([]byte, 1+8+len(e.Value))
			buf[0] = MarkerInline | MarkerTTLFlag
			binary.BigEndian.PutUint64(buf[1:], uint64(e.ExpireAt))
			copy(buf[9:], e.Value)
			return buf
		}
		buf := make([]byte, 1+len(e.Value))
		buf[0] = MarkerInline
		copy(buf[1:], e.Value)
		return buf
	}

	if hasTTL {
		buf := make([]byte, 1+8+32)
		buf[0] = MarkerBlob | MarkerTTLFlag
		binary.BigEndian.PutUint64(buf[1:], uint64(e.ExpireAt))
		copy(buf[9:], e.BlobID[:])
		return buf
	}
	buf := make([]byte, 1+32)
	buf[0] = MarkerBlob
	copy(buf[1:], e.BlobID[:])
	return buf
}

func DecodeKeyEntry(key, encoded []byte) (KeyEntry, error) {
	if len(encoded) == 0 {
		return KeyEntry{}, errors.New("empty encoded entry")
	}

	e := KeyEntry{
		Key: key,
	}

	marker := encoded[0]
	hasTTL := (marker & MarkerTTLFlag) != 0
	baseMarker := marker & ^MarkerTTLFlag

	offset := 1
	if hasTTL {
		if len(encoded) < 9 {
			return KeyEntry{}, errors.New("ttl entry too short")
		}
		e.ExpireAt = int64(binary.BigEndian.Uint64(encoded[1:9]))
		offset = 9
	}

	switch baseMarker {
	case MarkerDelete:
		e.Kind = OpDelete
	case MarkerInline:
		e.Kind = OpPut
		e.Inline = true
		e.Value = encoded[offset:]
	case MarkerBlob:
		if len(encoded) < offset+32 {
			return KeyEntry{}, errors.New("blob entry too short")
		}
		e.Kind = OpPut
		e.Inline = false
		copy(e.BlobID[:], encoded[offset:offset+32])
	default:
		return KeyEntry{}, errors.New("unknown marker byte")
	}

	return e, nil
}

func (e *KeyEntry) HasBlobID() bool {
	var zeroBlobID [32]byte
	return e.BlobID != zeroBlobID
}

type MemEntry struct {
	Key    []byte
	Seq    uint64
	Kind   OpKind
	Inline bool
	Value  []byte

	BlobID [32]byte

	ExpireAt int64
}

type CompactionEntry struct {
	Key  []byte
	Seq  uint64
	Kind OpKind

	Inline bool
	Value  []byte

	BlobID [32]byte

	ExpireAt int64
}

func (e *KeyEntry) IsExpired(nowMs int64) bool {
	return e.ExpireAt > 0 && e.ExpireAt <= nowMs
}

func (e *MemEntry) IsExpired(nowMs int64) bool {
	return e.ExpireAt > 0 && e.ExpireAt <= nowMs
}

func (e *CompactionEntry) IsExpired(nowMs int64) bool {
	return e.ExpireAt > 0 && e.ExpireAt <= nowMs
}

func (e *CompactionEntry) HasBlobID() bool {
	var zeroBlobID [32]byte
	return e.BlobID != zeroBlobID
}
