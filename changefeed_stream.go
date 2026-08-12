package isledb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/klauspost/compress/zstd"
)

const changeBatchCompressionZstd = "zstd"

const (
	changeBatchChunkBytes           = 256 << 10
	changeBatchInitialChunkBytes    = 4 << 10
	defaultChangeBatchBlockRecords  = 512
	defaultChangeBatchBlockRawBytes = 1 << 20
)

var changeBatchEncoderPool = sync.Pool{New: func() any {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithZeroFrames(true),
	)
	if err != nil {
		panic(fmt.Errorf("change feed zstd encoder: %w", err))
	}
	return encoder
}}

var changeBatchDecoderPool = sync.Pool{New: func() any {
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecodeAllCapLimit(true),
	)
	if err != nil {
		panic(fmt.Errorf("change feed zstd decoder: %w", err))
	}
	return decoder
}}

type changeBatchBuffer struct {
	chunks        [][]byte
	recordOffsets []int64
	recordSeqs    []uint64
	bodySize      int64
	count         uint32
	seqLo         uint64
	seqHi         uint64
	payload       ChangeFeedPayload
}

func (b *changeBatchBuffer) appendPut(seq uint64, key, value []byte, expireAt int64) error {
	return b.appendPutForPayload(seq, key, value, expireAt, ChangeFeedFullValues)
}

func (b *changeBatchBuffer) appendPutForPayload(
	seq uint64,
	key, value []byte,
	expireAt int64,
	payload ChangeFeedPayload,
) error {
	return b.appendRecord(changeRecord{
		Seq:          seq,
		Kind:         changePut,
		Key:          key,
		Value:        value,
		ValueOmitted: payload == ChangeFeedKeysOnly,
		ExpireAt:     expireAt,
	})
}

func (b *changeBatchBuffer) appendDelete(seq uint64, key []byte) error {
	return b.appendRecord(changeRecord{Seq: seq, Kind: changeDelete, Key: key})
}

func (b *changeBatchBuffer) appendRecord(change changeRecord) error {
	if b.payload == 0 {
		if change.Kind == changePut && change.ValueOmitted {
			b.payload = ChangeFeedKeysOnly
		} else {
			b.payload = ChangeFeedFullValues
		}
	}
	if b.payload != ChangeFeedKeysOnly && b.payload != ChangeFeedFullValues {
		return fmt.Errorf("unsupported change feed payload %d", b.payload)
	}
	if b.count == math.MaxUint32 {
		return errorsChangeBatchTooLarge(b.count)
	}
	if b.count > 0 && change.Seq <= b.seqHi {
		return fmt.Errorf("change batch out of order: previous=%d current=%d", b.seqHi, change.Seq)
	}
	if len(change.Key) > math.MaxUint32 {
		return fmt.Errorf("change key too large: %d", len(change.Key))
	}

	valueLen := 0
	switch change.Kind {
	case changeDelete:
	case changePut:
		switch b.payload {
		case ChangeFeedKeysOnly:
			if !change.ValueOmitted {
				return errors.New("keys-only change put must omit its value")
			}
		case ChangeFeedFullValues:
			if change.ValueOmitted {
				return errors.New("full-value change put must contain a value")
			}
			valueLen = len(change.Value)
		}
	default:
		return fmt.Errorf("unsupported change kind %d", change.Kind)
	}
	if valueLen > math.MaxUint32 {
		return fmt.Errorf("change value too large: %d", valueLen)
	}

	b.recordOffsets = append(b.recordOffsets, b.bodySize)
	b.recordSeqs = append(b.recordSeqs, change.Seq)
	var header [changeRecordHeaderSize]byte
	header[0] = byte(change.Kind)
	if change.Kind == changePut {
		if change.ValueOmitted {
			header[1] = changeFlagValueOmitted
		}
	}
	binary.BigEndian.PutUint32(header[4:8], uint32(len(change.Key)))
	binary.BigEndian.PutUint32(header[8:12], uint32(valueLen))
	binary.BigEndian.PutUint64(header[16:24], change.Seq)
	binary.BigEndian.PutUint64(header[24:32], uint64(change.ExpireAt))
	b.appendBytes(header[:])
	b.appendBytes(change.Key)
	if change.Kind == changePut && !change.ValueOmitted {
		b.appendBytes(change.Value)
	}

	if b.count == 0 {
		b.seqLo = change.Seq
	}
	b.seqHi = change.Seq
	b.count++
	return nil
}

func (b *changeBatchBuffer) appendBytes(data []byte) {
	for len(data) > 0 {
		if len(b.chunks) == 0 || len(b.chunks[len(b.chunks)-1]) == cap(b.chunks[len(b.chunks)-1]) {
			capacity := changeBatchInitialChunkBytes
			if len(b.chunks) > 0 {
				capacity = cap(b.chunks[len(b.chunks)-1]) * 2
				if capacity > changeBatchChunkBytes {
					capacity = changeBatchChunkBytes
				}
			}
			b.chunks = append(b.chunks, make([]byte, 0, capacity))
		}
		last := len(b.chunks) - 1
		available := cap(b.chunks[last]) - len(b.chunks[last])
		if available > len(data) {
			available = len(data)
		}
		b.chunks[last] = append(b.chunks[last], data[:available]...)
		b.bodySize += int64(available)
		data = data[available:]
	}
}

func errorsChangeBatchTooLarge(count uint32) error {
	return fmt.Errorf("change batch too large: count=%d", count)
}

type changeBatchBlockOptions struct {
	MaxRecords     int
	TargetRawBytes int64
}

func defaultChangeBatchBlockOptions() changeBatchBlockOptions {
	return changeBatchBlockOptions{
		MaxRecords:     defaultChangeBatchBlockRecords,
		TargetRawBytes: defaultChangeBatchBlockRawBytes,
	}
}

func normalizeChangeBatchBlockOptions(opts changeBatchBlockOptions) changeBatchBlockOptions {
	defaults := defaultChangeBatchBlockOptions()
	if opts.MaxRecords <= 0 {
		opts.MaxRecords = defaults.MaxRecords
	}
	if opts.TargetRawBytes <= 0 {
		opts.TargetRawBytes = defaults.TargetRawBytes
	}
	return opts
}

type changeBatchBlockRange struct {
	first    int
	count    int
	rawStart int64
	rawEnd   int64
}

func (b *changeBatchBuffer) blockRanges(opts changeBatchBlockOptions) ([]changeBatchBlockRange, error) {
	opts = normalizeChangeBatchBlockOptions(opts)
	if b == nil || b.count == 0 || len(b.recordOffsets) != int(b.count) || len(b.recordSeqs) != int(b.count) {
		return nil, errors.New("invalid empty change batch buffer")
	}
	ranges := make([]changeBatchBlockRange, 0, (int(b.count)+opts.MaxRecords-1)/opts.MaxRecords)
	for first := 0; first < int(b.count); {
		rawStart := b.recordOffsets[first]
		end := first
		for end < int(b.count) {
			nextRawEnd := b.bodySize
			if end+1 < int(b.count) {
				nextRawEnd = b.recordOffsets[end+1]
			}
			if end > first && (end-first >= opts.MaxRecords || nextRawEnd-rawStart > opts.TargetRawBytes) {
				break
			}
			end++
			if end-first >= opts.MaxRecords {
				break
			}
		}
		rawEnd := b.bodySize
		if end < int(b.count) {
			rawEnd = b.recordOffsets[end]
		}
		if end == first || rawEnd <= rawStart {
			return nil, fmt.Errorf("invalid change block range first=%d end=%d", first, end)
		}
		ranges = append(ranges, changeBatchBlockRange{
			first: first, count: end - first, rawStart: rawStart, rawEnd: rawEnd,
		})
		first = end
	}
	return ranges, nil
}

func (b *changeBatchBuffer) bytesRange(start, end int64) ([]byte, error) {
	if b == nil || start < 0 || end <= start || end > b.bodySize || end-start > int64(maxInt()) {
		return nil, fmt.Errorf("invalid change batch byte range [%d,%d)", start, end)
	}
	result := make([]byte, 0, int(end-start))
	var chunkStart int64
	for _, chunk := range b.chunks {
		chunkEnd := chunkStart + int64(len(chunk))
		if chunkEnd <= start {
			chunkStart = chunkEnd
			continue
		}
		if chunkStart >= end {
			break
		}
		lo := max(start, chunkStart) - chunkStart
		hi := min(end, chunkEnd) - chunkStart
		result = append(result, chunk[lo:hi]...)
		chunkStart = chunkEnd
	}
	if int64(len(result)) != end-start {
		return nil, io.ErrUnexpectedEOF
	}
	return result, nil
}

type changeBatchStreamResult struct {
	Meta manifest.ChangeBatchMeta
}

func writeChangeBatchStreaming(
	ctx context.Context,
	buffer *changeBatchBuffer,
	epoch uint64,
	createdAt time.Time,
	uploadFn func(context.Context, string, io.Reader) error,
) (changeBatchStreamResult, error) {
	return writeChangeBatchStreamingWithOptions(
		ctx, buffer, epoch, createdAt, defaultChangeBatchBlockOptions(), uploadFn)
}

func writeChangeBatchStreamingWithOptions(
	ctx context.Context,
	buffer *changeBatchBuffer,
	epoch uint64,
	createdAt time.Time,
	blockOpts changeBatchBlockOptions,
	uploadFn func(context.Context, string, io.Reader) error,
) (changeBatchStreamResult, error) {
	if buffer == nil || buffer.count == 0 {
		return changeBatchStreamResult{}, errEmptyIterator
	}
	if buffer.payload != ChangeFeedKeysOnly && buffer.payload != ChangeFeedFullValues {
		return changeBatchStreamResult{}, fmt.Errorf("unsupported change feed payload %d", buffer.payload)
	}
	blockRanges, err := buffer.blockRanges(blockOpts)
	if err != nil {
		return changeBatchStreamResult{}, err
	}

	id := buildChangeBatchIDWithTimestamp(epoch, buffer.seqLo, buffer.seqHi, createdAt)
	reader, writer := io.Pipe()
	hasher := sha256.New()
	destination := &changeBatchHashWriter{writer: writer, hash: hasher}
	producerDone := make(chan error, 1)
	blocks := make([]changeBatchBlock, 0, len(blockRanges))
	var indexChecksum [sha256.Size]byte

	go func() {
		encoder := changeBatchEncoderPool.Get().(*zstd.Encoder)
		defer changeBatchEncoderPool.Put(encoder)

		var produceErr error
		for _, blockRange := range blockRanges {
			if err := ctx.Err(); err != nil {
				produceErr = err
				break
			}
			raw, err := buffer.bytesRange(blockRange.rawStart, blockRange.rawEnd)
			if err != nil {
				produceErr = err
				break
			}
			compressed := encoder.EncodeAll(raw, nil)
			if len(compressed) > math.MaxUint32 || len(raw) > math.MaxUint32 {
				produceErr = fmt.Errorf("change block too large: compressed=%d raw=%d", len(compressed), len(raw))
				break
			}
			block := changeBatchBlock{
				FirstIndex:     uint32(blockRange.first),
				Count:          uint32(blockRange.count),
				SeqLo:          buffer.recordSeqs[blockRange.first],
				Offset:         uint64(destination.size),
				CompressedSize: uint32(len(compressed)),
				RawSize:        uint32(len(raw)),
				Checksum:       sha256.Sum256(raw),
			}
			if err := writeChangeBatchBytes(destination, compressed); err != nil {
				produceErr = err
				break
			}
			blocks = append(blocks, block)
		}
		if produceErr == nil {
			indexData := encodeChangeBatchBlockIndex(blocks)
			indexChecksum = sha256.Sum256(indexData)
			indexOffset := uint64(destination.size)
			if err := writeChangeBatchBytes(destination, indexData); err != nil {
				produceErr = err
			} else {
				trailer := encodeChangeBatchTrailer(buffer, epoch, indexOffset, indexData, blocks)
				produceErr = writeChangeBatchBytes(destination, trailer)
			}
		}
		_ = writer.CloseWithError(produceErr)
		producerDone <- produceErr
	}()

	uploadErr := uploadFn(ctx, id, reader)
	_ = reader.CloseWithError(uploadErr)
	producerErr := <-producerDone
	if uploadErr != nil {
		return changeBatchStreamResult{}, uploadErr
	}
	if producerErr != nil {
		return changeBatchStreamResult{}, producerErr
	}

	return changeBatchStreamResult{Meta: manifest.ChangeBatchMeta{
		ID:            id,
		Epoch:         epoch,
		SeqLo:         buffer.seqLo,
		SeqHi:         buffer.seqHi,
		Count:         buffer.count,
		BlockCount:    uint32(len(blocks)),
		Size:          destination.size,
		RawSize:       buffer.bodySize,
		Checksum:      "sha256:" + hex.EncodeToString(hasher.Sum(nil)),
		IndexChecksum: "sha256:" + hex.EncodeToString(indexChecksum[:]),
		CreatedAt:     createdAt,
		Version:       changeBatchVersion,
		Compression:   changeBatchCompressionZstd,
		Payload:       manifestChangeFeedPayload(buffer.payload),
	}}, nil
}

func encodeChangeBatchBlockIndex(blocks []changeBatchBlock) []byte {
	data := make([]byte, len(blocks)*changeBatchIndexEntrySize)
	for i := range blocks {
		off := i * changeBatchIndexEntrySize
		block := blocks[i]
		binary.BigEndian.PutUint32(data[off:off+4], block.FirstIndex)
		binary.BigEndian.PutUint32(data[off+4:off+8], block.Count)
		binary.BigEndian.PutUint64(data[off+8:off+16], block.SeqLo)
		binary.BigEndian.PutUint64(data[off+16:off+24], block.Offset)
		binary.BigEndian.PutUint32(data[off+24:off+28], block.CompressedSize)
		binary.BigEndian.PutUint32(data[off+28:off+32], block.RawSize)
		copy(data[off+32:off+64], block.Checksum[:])
	}
	return data
}

func encodeChangeBatchTrailer(
	buffer *changeBatchBuffer,
	epoch uint64,
	indexOffset uint64,
	indexData []byte,
	blocks []changeBatchBlock,
) []byte {
	trailer := make([]byte, changeBatchTrailerSize)
	copy(trailer[:4], changeBatchMagic)
	binary.BigEndian.PutUint16(trailer[4:6], changeBatchVersion)
	trailer[6] = byte(buffer.payload)
	binary.BigEndian.PutUint64(trailer[8:16], epoch)
	binary.BigEndian.PutUint64(trailer[16:24], buffer.seqLo)
	binary.BigEndian.PutUint64(trailer[24:32], buffer.seqHi)
	binary.BigEndian.PutUint32(trailer[32:36], buffer.count)
	binary.BigEndian.PutUint32(trailer[36:40], uint32(len(blocks)))
	binary.BigEndian.PutUint64(trailer[40:48], indexOffset)
	binary.BigEndian.PutUint64(trailer[48:56], uint64(len(indexData)))
	binary.BigEndian.PutUint64(trailer[56:64], uint64(buffer.bodySize))
	indexSum := sha256.Sum256(indexData)
	copy(trailer[64:96], indexSum[:])
	return trailer
}

func manifestChangeFeedPayload(payload ChangeFeedPayload) manifest.ChangeFeedPayload {
	switch payload {
	case ChangeFeedKeysOnly:
		return manifest.ChangeFeedPayloadKeysOnly
	case ChangeFeedFullValues:
		return manifest.ChangeFeedPayloadFullValues
	default:
		return ""
	}
}

func writeChangeBatchBytes(writer io.Writer, data []byte) error {
	n, err := writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

type changeBatchHashWriter struct {
	writer io.Writer
	hash   hash.Hash
	size   int64
}

func (w *changeBatchHashWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n > 0 {
		_, _ = w.hash.Write(p[:n])
		w.size += int64(n)
	}
	return n, err
}

func decompressChangeBatchBlock(data []byte, rawSize int) ([]byte, error) {
	decoder := changeBatchDecoderPool.Get().(*zstd.Decoder)
	destination := make([]byte, 0, rawSize)
	decoded, err := decoder.DecodeAll(data, destination)
	changeBatchDecoderPool.Put(decoder)
	return decoded, err
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
