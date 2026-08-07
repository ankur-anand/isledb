package isledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ankur-anand/isledb/internal"
)

const (
	changeBatchMagic       = "ISLC"
	changeBatchVersion     = 1
	changeBatchHeaderSize  = 40
	changeRecordHeaderSize = 32
)

const (
	changeFlagInline byte = 1 << iota
	changeFlagBlob
)

type changeKind byte

const (
	changePut    changeKind = changeKind(internal.OpPut)
	changeDelete changeKind = changeKind(internal.OpDelete)
)

type changeRecord struct {
	Seq      uint64
	Kind     changeKind
	Key      []byte
	Inline   bool
	Value    []byte
	BlobID   [32]byte
	ExpireAt int64
}

type changeBatch struct {
	Version int
	Epoch   uint64
	SeqLo   uint64
	SeqHi   uint64
	Changes []changeRecord
}

func buildChangeBatchIDWithTimestamp(epoch, seqLo, seqHi uint64, ts time.Time) string {
	return fmt.Sprintf("%d-%d-%d-%d.chg", epoch, seqLo, seqHi, ts.UnixNano())
}

func encodeChangeBatch(batch *changeBatch) ([]byte, error) {
	if batch == nil {
		return nil, errors.New("nil change batch")
	}
	version := batch.Version
	if version == 0 {
		version = changeBatchVersion
	}
	if version != changeBatchVersion {
		return nil, fmt.Errorf("unsupported change batch version %d", batch.Version)
	}
	if len(batch.Changes) > math.MaxUint32 {
		return nil, fmt.Errorf("change batch too large: count=%d", len(batch.Changes))
	}

	buf := bytes.NewBuffer(make([]byte, 0, changeBatchHeaderSize+len(batch.Changes)*changeRecordHeaderSize))
	buf.WriteString(changeBatchMagic)
	writeU16(buf, uint16(version))
	writeU16(buf, 0)
	writeU64(buf, batch.Epoch)
	writeU64(buf, batch.SeqLo)
	writeU64(buf, batch.SeqHi)
	writeU32(buf, uint32(len(batch.Changes)))
	writeU32(buf, 0)

	var prev uint64
	for i, change := range batch.Changes {
		if i == 0 {
			if change.Seq != batch.SeqLo {
				return nil, fmt.Errorf("first change seq=%d does not match seq_lo=%d", change.Seq, batch.SeqLo)
			}
		} else if change.Seq <= prev {
			return nil, fmt.Errorf("change batch out of order: previous=%d current=%d", prev, change.Seq)
		}
		prev = change.Seq
		if i == len(batch.Changes)-1 && change.Seq != batch.SeqHi {
			return nil, fmt.Errorf("last change seq=%d does not match seq_hi=%d", change.Seq, batch.SeqHi)
		}
		if err := encodeChange(buf, change); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func decodeChangeBatch(data []byte) (*changeBatch, error) {
	return decodeChangeBatchWithRawSize(data, 0)
}

func decodeChangeBatchWithRawSize(data []byte, rawSize int64) (*changeBatch, error) {
	decoded, err := decompressChangeBatchSized(data, rawSize)
	if err != nil {
		return nil, fmt.Errorf("decompress change batch: %w", err)
	}
	if rawSize > 0 && int64(len(decoded)) != rawSize {
		return nil, fmt.Errorf("change batch raw size=%d want=%d", len(decoded), rawSize)
	}
	data = decoded
	if len(data) < changeBatchHeaderSize {
		return nil, errors.New("change batch too small")
	}
	if string(data[:4]) != changeBatchMagic {
		return nil, errors.New("invalid change batch magic")
	}
	version := int(binary.BigEndian.Uint16(data[4:6]))
	if version != changeBatchVersion {
		return nil, fmt.Errorf("unsupported change batch version %d", version)
	}

	batch := &changeBatch{
		Version: version,
		Epoch:   binary.BigEndian.Uint64(data[8:16]),
		SeqLo:   binary.BigEndian.Uint64(data[16:24]),
		SeqHi:   binary.BigEndian.Uint64(data[24:32]),
	}
	count := binary.BigEndian.Uint32(data[32:36])

	off := changeBatchHeaderSize
	batch.Changes = make([]changeRecord, 0, int(count))
	for i := uint32(0); i < count; i++ {
		change, next, err := decodeChange(data, off)
		if err != nil {
			return nil, err
		}
		if i > 0 && change.Seq <= batch.Changes[len(batch.Changes)-1].Seq {
			return nil, fmt.Errorf("change batch out of order: previous=%d current=%d",
				batch.Changes[len(batch.Changes)-1].Seq, change.Seq)
		}
		batch.Changes = append(batch.Changes, change)
		off = next
	}
	if off != len(data) {
		return nil, fmt.Errorf("trailing change batch bytes: %d", len(data)-off)
	}
	if len(batch.Changes) > 0 {
		if batch.Changes[0].Seq != batch.SeqLo || batch.Changes[len(batch.Changes)-1].Seq != batch.SeqHi {
			return nil, fmt.Errorf("change batch seq range mismatch")
		}
	}
	return batch, nil
}

func encodeChange(buf *bytes.Buffer, change changeRecord) error {
	if len(change.Key) > math.MaxUint32 {
		return fmt.Errorf("change key too large: %d", len(change.Key))
	}
	var flags byte
	valueLen := 0
	switch change.Kind {
	case changeDelete:
	case changePut:
		if change.Inline {
			flags |= changeFlagInline
			valueLen = len(change.Value)
		} else {
			flags |= changeFlagBlob
			valueLen = len(change.BlobID)
		}
	default:
		return fmt.Errorf("unsupported change kind %d", change.Kind)
	}
	if valueLen > math.MaxUint32 {
		return fmt.Errorf("change value too large: %d", valueLen)
	}

	buf.WriteByte(byte(change.Kind))
	buf.WriteByte(flags)
	writeU16(buf, 0)
	writeU32(buf, uint32(len(change.Key)))
	writeU32(buf, uint32(valueLen))
	writeU32(buf, 0)
	writeU64(buf, change.Seq)
	writeI64(buf, change.ExpireAt)
	buf.Write(change.Key)
	if flags&changeFlagInline != 0 {
		buf.Write(change.Value)
	} else if flags&changeFlagBlob != 0 {
		buf.Write(change.BlobID[:])
	}
	return nil
}

func decodeChange(data []byte, off int) (changeRecord, int, error) {
	if off < 0 || len(data)-off < changeRecordHeaderSize {
		return changeRecord{}, 0, errors.New("truncated change record header")
	}
	header := data[off : off+changeRecordHeaderSize]
	kind := changeKind(header[0])
	flags := header[1]
	keyLen := binary.BigEndian.Uint32(header[4:8])
	valueLen := binary.BigEndian.Uint32(header[8:12])
	change := changeRecord{
		Kind:     kind,
		Seq:      binary.BigEndian.Uint64(header[16:24]),
		ExpireAt: int64(binary.BigEndian.Uint64(header[24:32])),
	}
	off += changeRecordHeaderSize

	need := uint64(keyLen) + uint64(valueLen)
	if need > uint64(len(data)-off) {
		return changeRecord{}, 0, errors.New("truncated change record body")
	}
	change.Key = data[off : off+int(keyLen)]
	off += int(keyLen)

	switch kind {
	case changeDelete:
		if flags != 0 || valueLen != 0 {
			return changeRecord{}, 0, errors.New("invalid delete change payload")
		}
	case changePut:
		switch flags {
		case changeFlagInline:
			change.Inline = true
			change.Value = data[off : off+int(valueLen)]
			off += int(valueLen)
		case changeFlagBlob:
			if valueLen != 32 {
				return changeRecord{}, 0, errors.New("invalid blob change payload")
			}
			copy(change.BlobID[:], data[off:off+32])
			off += 32
		default:
			return changeRecord{}, 0, errors.New("invalid put change flags")
		}
	default:
		return changeRecord{}, 0, fmt.Errorf("unsupported change kind %d", kind)
	}
	return change, off, nil
}

func writeU16(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

func writeU32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

func writeU64(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}

func writeI64(buf *bytes.Buffer, v int64) {
	writeU64(buf, uint64(v))
}
