package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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

const changeBatchChunkBytes = 256 << 10

const changeBatchInitialChunkBytes = 4 << 10

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
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		panic(fmt.Errorf("change feed zstd decoder: %w", err))
	}
	return decoder
}}

type changeBatchBuffer struct {
	chunks   [][]byte
	bodySize int64
	count    uint32
	seqLo    uint64
	seqHi    uint64
}

func (b *changeBatchBuffer) appendPut(seq uint64, key, value []byte, expireAt int64) error {
	return b.appendRecord(changeRecord{
		Seq:      seq,
		Kind:     changePut,
		Key:      key,
		Inline:   true,
		Value:    value,
		ExpireAt: expireAt,
	})
}

func (b *changeBatchBuffer) appendDelete(seq uint64, key []byte) error {
	return b.appendRecord(changeRecord{Seq: seq, Kind: changeDelete, Key: key})
}

func (b *changeBatchBuffer) appendRecord(change changeRecord) error {
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
		if !change.Inline {
			return fmt.Errorf("change put must contain an inline value")
		}
		valueLen = len(change.Value)
	default:
		return fmt.Errorf("unsupported change kind %d", change.Kind)
	}
	if valueLen > math.MaxUint32 {
		return fmt.Errorf("change value too large: %d", valueLen)
	}

	var header [changeRecordHeaderSize]byte
	header[0] = byte(change.Kind)
	if change.Kind == changePut {
		header[1] = changeFlagInline
	}
	binary.BigEndian.PutUint32(header[4:8], uint32(len(change.Key)))
	binary.BigEndian.PutUint32(header[8:12], uint32(valueLen))
	binary.BigEndian.PutUint64(header[16:24], change.Seq)
	binary.BigEndian.PutUint64(header[24:32], uint64(change.ExpireAt))
	b.appendBytes(header[:])
	b.appendBytes(change.Key)
	if change.Kind == changePut {
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

func (b *changeBatchBuffer) writeBodyTo(writer io.Writer) error {
	for _, chunk := range b.chunks {
		n, err := writer.Write(chunk)
		if err != nil {
			return err
		}
		if n != len(chunk) {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (b *changeBatchBuffer) header(epoch uint64) []byte {
	header := make([]byte, changeBatchHeaderSize)
	copy(header[:4], changeBatchMagic)
	binary.BigEndian.PutUint16(header[4:6], changeBatchVersion)
	binary.BigEndian.PutUint64(header[8:16], epoch)
	binary.BigEndian.PutUint64(header[16:24], b.seqLo)
	binary.BigEndian.PutUint64(header[24:32], b.seqHi)
	binary.BigEndian.PutUint32(header[32:36], b.count)
	return header
}

func errorsChangeBatchTooLarge(count uint32) error {
	return fmt.Errorf("change batch too large: count=%d", count)
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
	if buffer == nil || buffer.count == 0 {
		return changeBatchStreamResult{}, errEmptyIterator
	}

	id := buildChangeBatchIDWithTimestamp(epoch, buffer.seqLo, buffer.seqHi, createdAt)
	reader, writer := io.Pipe()
	hasher := sha256.New()
	destination := &changeBatchHashWriter{writer: writer, hash: hasher}
	producerDone := make(chan error, 1)

	go func() {
		encoder := changeBatchEncoderPool.Get().(*zstd.Encoder)
		encoder.Reset(destination)
		_, err := encoder.Write(buffer.header(epoch))
		if err == nil {
			err = buffer.writeBodyTo(encoder)
		}
		if closeErr := encoder.Close(); err == nil {
			err = closeErr
		}
		encoder.Reset(io.Discard)
		changeBatchEncoderPool.Put(encoder)
		_ = writer.CloseWithError(err)
		producerDone <- err
	}()

	uploadErr := uploadFn(ctx, id, reader)
	if uploadErr != nil {
		_ = reader.CloseWithError(uploadErr)
	}
	producerErr := <-producerDone
	if uploadErr != nil {
		return changeBatchStreamResult{}, uploadErr
	}
	if producerErr != nil {
		return changeBatchStreamResult{}, producerErr
	}

	return changeBatchStreamResult{Meta: manifest.ChangeBatchMeta{
		ID:          id,
		Epoch:       epoch,
		SeqLo:       buffer.seqLo,
		SeqHi:       buffer.seqHi,
		Count:       buffer.count,
		Size:        destination.size,
		RawSize:     int64(changeBatchHeaderSize) + buffer.bodySize,
		Checksum:    "sha256:" + hex.EncodeToString(hasher.Sum(nil)),
		CreatedAt:   createdAt,
		Version:     changeBatchVersion,
		Compression: changeBatchCompressionZstd,
	}}, nil
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

func decompressChangeBatchSized(data []byte, rawSize int64) ([]byte, error) {
	if len(data) < 4 || !bytes.Equal(data[:4], []byte{0x28, 0xb5, 0x2f, 0xfd}) {
		return data, nil
	}
	decoder := changeBatchDecoderPool.Get().(*zstd.Decoder)
	var destination []byte
	if rawSize > 0 && rawSize <= int64(maxInt()) {
		destination = make([]byte, 0, int(rawSize))
	}
	decoded, err := decoder.DecodeAll(data, destination)
	changeBatchDecoderPool.Put(decoder)
	return decoded, err
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
