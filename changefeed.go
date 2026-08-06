package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
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

type changeBatchBuildResult struct {
	Meta manifest.ChangeBatchMeta
	Data []byte
}

func buildChangeBatchIDWithTimestamp(epoch, seqLo, seqHi uint64, ts time.Time) string {
	return fmt.Sprintf("%d-%d-%d-%d.chg", epoch, seqLo, seqHi, ts.UnixNano())
}

func buildChangeBatch(ctx context.Context, it sstIterator, epoch, seqLo, seqHi uint64, createdAt time.Time) (changeBatchBuildResult, error) {
	defer it.Close()

	changes := make([]changeRecord, 0)
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return changeBatchBuildResult{}, err
		}
		change, err := changeFromMemEntry(it.Entry())
		if err != nil {
			return changeBatchBuildResult{}, err
		}
		changes = append(changes, change)
	}
	if err := it.Err(); err != nil {
		return changeBatchBuildResult{}, err
	}
	if len(changes) == 0 {
		return changeBatchBuildResult{}, errEmptyIterator
	}
	if len(changes) > math.MaxUint32 {
		return changeBatchBuildResult{}, fmt.Errorf("change batch too large: count=%d", len(changes))
	}

	sort.Slice(changes, func(i, j int) bool {
		return changes[i].Seq < changes[j].Seq
	})

	if changes[0].Seq != seqLo || changes[len(changes)-1].Seq != seqHi {
		return changeBatchBuildResult{}, fmt.Errorf("change batch seq range mismatch: got=%d-%d want=%d-%d",
			changes[0].Seq, changes[len(changes)-1].Seq, seqLo, seqHi)
	}

	batch := &changeBatch{
		Version: changeBatchVersion,
		Epoch:   epoch,
		SeqLo:   seqLo,
		SeqHi:   seqHi,
		Changes: changes,
	}
	data, err := encodeChangeBatch(batch)
	if err != nil {
		return changeBatchBuildResult{}, err
	}

	sum := sha256.Sum256(data)
	id := buildChangeBatchIDWithTimestamp(epoch, seqLo, seqHi, createdAt)
	return changeBatchBuildResult{
		Meta: manifest.ChangeBatchMeta{
			ID:        id,
			Epoch:     epoch,
			SeqLo:     seqLo,
			SeqHi:     seqHi,
			Count:     uint32(len(changes)),
			Size:      int64(len(data)),
			Checksum:  "sha256:" + hex.EncodeToString(sum[:]),
			CreatedAt: createdAt,
			Version:   changeBatchVersion,
		},
		Data: data,
	}, nil
}

func changeFromMemEntry(e internal.MemEntry) (changeRecord, error) {
	change := changeRecord{
		Seq:      e.Seq,
		Kind:     changeKind(e.Kind),
		Key:      append([]byte(nil), e.Key...),
		ExpireAt: e.ExpireAt,
	}
	switch e.Kind {
	case internal.OpDelete:
		return change, nil
	case internal.OpPut:
		if e.Inline {
			change.Inline = true
			change.Value = append([]byte(nil), e.Value...)
			return change, nil
		}
		var zero [32]byte
		if e.BlobID != zero {
			change.BlobID = e.BlobID
			return change, nil
		}
		return changeRecord{}, fmt.Errorf("corrupt change: non-inline, non-blob for key %q", e.Key)
	default:
		return changeRecord{}, fmt.Errorf("unsupported change kind %d", e.Kind)
	}
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
	change.Key = append([]byte(nil), data[off:off+int(keyLen)]...)
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
			change.Value = append([]byte(nil), data[off:off+int(valueLen)]...)
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
