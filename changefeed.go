package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/ankur-anand/isledb/internal"
)

const (
	changeBatchMagic          = "ISLC"
	changeBatchVersion        = 1
	changeBatchTrailerSize    = 96
	changeBatchIndexEntrySize = 64
	changeRecordHeaderSize    = 32
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

// changeBatchBlock describes one independently compressed frame in a change
// batch. Checksum covers the uncompressed record bytes so a range read verifies
// exactly the region it decoded.
type changeBatchBlock struct {
	FirstIndex     uint32
	Count          uint32
	SeqLo          uint64
	Offset         uint64
	CompressedSize uint32
	RawSize        uint32
	Checksum       [sha256.Size]byte
}

type changeBatchIndex struct {
	Version int
	Epoch   uint64
	SeqLo   uint64
	SeqHi   uint64
	Count   uint32
	RawSize uint64
	Blocks  []changeBatchBlock
}

func buildChangeBatchIDWithTimestamp(epoch, seqLo, seqHi uint64, ts time.Time) string {
	return fmt.Sprintf("%d-%d-%d-%d.chg", epoch, seqLo, seqHi, ts.UnixNano())
}

// encodeChangeBatch is the in-memory counterpart of the streaming writer. It
// exists for validation and tests; production writes use
// writeChangeBatchStreaming directly.
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
	if len(batch.Changes) == 0 {
		return nil, errEmptyIterator
	}
	if len(batch.Changes) > math.MaxUint32 {
		return nil, fmt.Errorf("change batch too large: count=%d", len(batch.Changes))
	}

	buffer := &changeBatchBuffer{}
	for _, change := range batch.Changes {
		if err := buffer.appendRecord(change); err != nil {
			return nil, err
		}
	}
	if buffer.seqLo != batch.SeqLo || buffer.seqHi != batch.SeqHi {
		return nil, fmt.Errorf("change batch seq range mismatch: got=%d-%d want=%d-%d",
			buffer.seqLo, buffer.seqHi, batch.SeqLo, batch.SeqHi)
	}

	var object bytes.Buffer
	_, err := writeChangeBatchStreamingWithOptions(
		context.Background(),
		buffer,
		batch.Epoch,
		time.Unix(0, 0).UTC(),
		defaultChangeBatchBlockOptions(),
		func(_ context.Context, _ string, reader io.Reader) error {
			_, copyErr := object.ReadFrom(reader)
			return copyErr
		},
	)
	if err != nil {
		return nil, err
	}
	return object.Bytes(), nil
}

func decodeChangeBatch(data []byte) (*changeBatch, error) {
	if len(data) < changeBatchTrailerSize {
		return nil, errors.New("change batch too small")
	}
	trailer := data[len(data)-changeBatchTrailerSize:]
	indexOffset, indexSize, err := changeBatchIndexLocation(trailer, int64(len(data)))
	if err != nil {
		return nil, err
	}
	indexEnd := indexOffset + indexSize
	index, err := decodeChangeBatchIndex(data[indexOffset:indexEnd], trailer, int64(len(data)))
	if err != nil {
		return nil, err
	}

	batch := &changeBatch{
		Version: index.Version,
		Epoch:   index.Epoch,
		SeqLo:   index.SeqLo,
		SeqHi:   index.SeqHi,
		Changes: make([]changeRecord, 0, index.Count),
	}
	for i := range index.Blocks {
		block := index.Blocks[i]
		end := block.Offset + uint64(block.CompressedSize)
		if end > uint64(len(data)) {
			return nil, errors.New("change block extends past object")
		}
		changes, err := decodeChangeBatchBlock(data[block.Offset:end], block)
		if err != nil {
			return nil, fmt.Errorf("decode change block %d: %w", i, err)
		}
		batch.Changes = append(batch.Changes, changes...)
	}
	if err := validateDecodedChangeBatch(batch, index); err != nil {
		return nil, err
	}
	return batch, nil
}

func changeBatchIndexLocation(trailer []byte, objectSize int64) (int, int, error) {
	if len(trailer) != changeBatchTrailerSize {
		return 0, 0, errors.New("invalid change batch trailer size")
	}
	if string(trailer[:4]) != changeBatchMagic {
		return 0, 0, errors.New("invalid change batch trailer magic")
	}
	version := int(binary.BigEndian.Uint16(trailer[4:6]))
	if version != changeBatchVersion {
		return 0, 0, fmt.Errorf("unsupported change batch version %d", version)
	}
	if objectSize < changeBatchTrailerSize {
		return 0, 0, errors.New("change batch too small")
	}
	indexOffset64 := binary.BigEndian.Uint64(trailer[40:48])
	indexSize64 := binary.BigEndian.Uint64(trailer[48:56])
	if indexOffset64 > uint64(maxInt()) || indexSize64 > uint64(maxInt()) {
		return 0, 0, errors.New("change batch index too large")
	}
	if indexSize64 == 0 || indexSize64%changeBatchIndexEntrySize != 0 {
		return 0, 0, fmt.Errorf("invalid change batch index size=%d", indexSize64)
	}
	wantEnd := uint64(objectSize - changeBatchTrailerSize)
	if indexOffset64 > wantEnd || indexSize64 > wantEnd-indexOffset64 || indexOffset64+indexSize64 != wantEnd {
		return 0, 0, errors.New("change batch index is not adjacent to trailer")
	}
	return int(indexOffset64), int(indexSize64), nil
}

func decodeChangeBatchIndex(indexData, trailer []byte, objectSize int64) (*changeBatchIndex, error) {
	indexOffset, indexSize, err := changeBatchIndexLocation(trailer, objectSize)
	if err != nil {
		return nil, err
	}
	if len(indexData) != indexSize {
		return nil, fmt.Errorf("change batch index bytes=%d want=%d", len(indexData), indexSize)
	}
	blockCount := binary.BigEndian.Uint32(trailer[36:40])
	if blockCount == 0 || uint64(blockCount)*changeBatchIndexEntrySize != uint64(indexSize) {
		return nil, fmt.Errorf("invalid change batch block count=%d", blockCount)
	}
	indexSum := sha256.Sum256(indexData)
	if !bytes.Equal(indexSum[:], trailer[64:96]) {
		return nil, errors.New("change batch index checksum mismatch")
	}

	result := &changeBatchIndex{
		Version: int(binary.BigEndian.Uint16(trailer[4:6])),
		Epoch:   binary.BigEndian.Uint64(trailer[8:16]),
		SeqLo:   binary.BigEndian.Uint64(trailer[16:24]),
		SeqHi:   binary.BigEndian.Uint64(trailer[24:32]),
		Count:   binary.BigEndian.Uint32(trailer[32:36]),
		RawSize: binary.BigEndian.Uint64(trailer[56:64]),
		Blocks:  make([]changeBatchBlock, 0, blockCount),
	}
	if result.Count == 0 || result.SeqHi < result.SeqLo {
		return nil, errors.New("invalid empty change batch")
	}

	var nextRecord uint64
	var nextOffset uint64
	var rawSize uint64
	var previousSeq uint64
	for i := uint32(0); i < blockCount; i++ {
		off := int(i) * changeBatchIndexEntrySize
		encoded := indexData[off : off+changeBatchIndexEntrySize]
		block := changeBatchBlock{
			FirstIndex:     binary.BigEndian.Uint32(encoded[0:4]),
			Count:          binary.BigEndian.Uint32(encoded[4:8]),
			SeqLo:          binary.BigEndian.Uint64(encoded[8:16]),
			Offset:         binary.BigEndian.Uint64(encoded[16:24]),
			CompressedSize: binary.BigEndian.Uint32(encoded[24:28]),
			RawSize:        binary.BigEndian.Uint32(encoded[28:32]),
		}
		copy(block.Checksum[:], encoded[32:64])
		if block.Count == 0 || block.CompressedSize == 0 || block.RawSize == 0 {
			return nil, fmt.Errorf("invalid empty change block %d", i)
		}
		if uint64(block.FirstIndex) != nextRecord || block.Offset != nextOffset {
			return nil, fmt.Errorf("non-contiguous change block %d", i)
		}
		if i == 0 {
			if block.SeqLo != result.SeqLo {
				return nil, fmt.Errorf("first change block sequence=%d want=%d", block.SeqLo, result.SeqLo)
			}
		} else if block.SeqLo <= previousSeq {
			return nil, fmt.Errorf("change block sequence did not advance: previous=%d current=%d", previousSeq, block.SeqLo)
		}
		blockEnd := block.Offset + uint64(block.CompressedSize)
		if blockEnd < block.Offset || blockEnd > uint64(indexOffset) {
			return nil, fmt.Errorf("change block %d extends into index", i)
		}
		nextRecord += uint64(block.Count)
		nextOffset = blockEnd
		rawSize += uint64(block.RawSize)
		previousSeq = block.SeqLo
		result.Blocks = append(result.Blocks, block)
	}
	if nextRecord != uint64(result.Count) {
		return nil, fmt.Errorf("indexed change count=%d want=%d", nextRecord, result.Count)
	}
	if nextOffset != uint64(indexOffset) {
		return nil, fmt.Errorf("indexed compressed bytes=%d want=%d", nextOffset, indexOffset)
	}
	if rawSize != result.RawSize {
		return nil, fmt.Errorf("indexed raw bytes=%d want=%d", rawSize, result.RawSize)
	}
	return result, nil
}

func decodeChangeBatchBlock(data []byte, block changeBatchBlock) ([]changeRecord, error) {
	if uint64(len(data)) != uint64(block.CompressedSize) {
		return nil, fmt.Errorf("compressed block bytes=%d want=%d", len(data), block.CompressedSize)
	}
	if uint64(block.RawSize) > uint64(maxInt()) || uint64(block.Count) > uint64(maxInt()) {
		return nil, errors.New("change block exceeds platform limits")
	}
	raw, err := decompressChangeBatchBlock(data, int(block.RawSize))
	if err != nil {
		return nil, err
	}
	if len(raw) != int(block.RawSize) {
		return nil, fmt.Errorf("raw block bytes=%d want=%d", len(raw), block.RawSize)
	}
	sum := sha256.Sum256(raw)
	if sum != block.Checksum {
		return nil, errors.New("change block checksum mismatch")
	}

	changes := make([]changeRecord, 0, block.Count)
	off := 0
	var previous uint64
	for i := uint32(0); i < block.Count; i++ {
		change, next, err := decodeChange(raw, off)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			if change.Seq != block.SeqLo {
				return nil, fmt.Errorf("change block first sequence=%d want=%d", change.Seq, block.SeqLo)
			}
		} else if change.Seq <= previous {
			return nil, fmt.Errorf("change block out of order: previous=%d current=%d", previous, change.Seq)
		}
		previous = change.Seq
		changes = append(changes, change)
		off = next
	}
	if off != len(raw) {
		return nil, fmt.Errorf("trailing change block bytes=%d", len(raw)-off)
	}
	return changes, nil
}

func validateDecodedChangeBatch(batch *changeBatch, index *changeBatchIndex) error {
	if batch == nil || index == nil || len(batch.Changes) != int(index.Count) {
		return errors.New("change batch count mismatch")
	}
	if batch.Changes[0].Seq != index.SeqLo || batch.Changes[len(batch.Changes)-1].Seq != index.SeqHi {
		return errors.New("change batch seq range mismatch")
	}
	var previous uint64
	for i := range batch.Changes {
		if i > 0 && batch.Changes[i].Seq <= previous {
			return fmt.Errorf("change batch out of order: previous=%d current=%d", previous, batch.Changes[i].Seq)
		}
		previous = batch.Changes[i].Seq
	}
	return nil
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
