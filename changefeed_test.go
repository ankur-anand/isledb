package isledb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestChangeBatchBufferStreamsInAppendOrder(t *testing.T) {
	buffer := &changeBatchBuffer{}
	if err := buffer.appendPut(1, []byte("a"), []byte("va"), 0); err != nil {
		t.Fatalf("append first put: %v", err)
	}
	if err := buffer.appendPut(2, []byte("c"), []byte("external-value"), 1234); err != nil {
		t.Fatalf("append second put: %v", err)
	}
	if err := buffer.appendDelete(3, []byte("b")); err != nil {
		t.Fatalf("append delete: %v", err)
	}

	var object bytes.Buffer
	result, err := writeChangeBatchStreaming(context.Background(), buffer, 7, time.Unix(10, 0).UTC(),
		func(_ context.Context, _ string, reader io.Reader) error {
			_, copyErr := io.Copy(&object, reader)
			return copyErr
		})
	if err != nil {
		t.Fatalf("writeChangeBatchStreaming: %v", err)
	}
	if result.Meta.Epoch != 7 || result.Meta.SeqLo != 1 || result.Meta.SeqHi != 3 || result.Meta.Count != 3 {
		t.Fatalf("meta mismatch: %+v", result.Meta)
	}
	if result.Meta.Size != int64(object.Len()) {
		t.Fatalf("meta size=%d data=%d", result.Meta.Size, object.Len())
	}
	if got, want := result.Meta.RawSize, buffer.bodySize; got != want {
		t.Fatalf("meta raw size=%d want=%d", got, want)
	}
	if got, want := result.Meta.BlockCount, uint32(1); got != want {
		t.Fatalf("meta block count=%d want=%d", got, want)
	}
	if result.Meta.Checksum == "" || result.Meta.IndexChecksum == "" ||
		result.Meta.Compression != changeBatchCompressionZstd ||
		result.Meta.Payload != "full_values" {
		t.Fatalf("incomplete change batch metadata: %+v", result.Meta)
	}

	batch, err := decodeChangeBatch(object.Bytes())
	if err != nil {
		t.Fatalf("DecodeChangeBatch: %v", err)
	}
	gotSeqs := []uint64{batch.Changes[0].Seq, batch.Changes[1].Seq, batch.Changes[2].Seq}
	wantSeqs := []uint64{1, 2, 3}
	for i := range wantSeqs {
		if gotSeqs[i] != wantSeqs[i] {
			t.Fatalf("seq[%d]=%d want %d", i, gotSeqs[i], wantSeqs[i])
		}
	}
	if batch.Changes[0].Kind != changePut || !batch.Changes[0].Inline || string(batch.Changes[0].Value) != "va" {
		t.Fatalf("inline put mismatch: %+v", batch.Changes[0])
	}
	if batch.Changes[1].Kind != changePut || !batch.Changes[1].Inline || string(batch.Changes[1].Value) != "external-value" || batch.Changes[1].ExpireAt != 1234 {
		t.Fatalf("second put mismatch: %+v", batch.Changes[1])
	}
	if batch.Changes[2].Kind != changeDelete {
		t.Fatalf("delete mismatch: %+v", batch.Changes[2])
	}
}

func TestChangeBatchKeysOnlyOmitsPutValues(t *testing.T) {
	buffer := &changeBatchBuffer{payload: ChangeFeedKeysOnly}
	if err := buffer.appendPutForPayload(
		1, []byte("key"), []byte("value-that-must-not-be-encoded"), 1234, ChangeFeedKeysOnly,
	); err != nil {
		t.Fatalf("append keys-only put: %v", err)
	}
	if err := buffer.appendDelete(2, []byte("deleted")); err != nil {
		t.Fatalf("append delete: %v", err)
	}

	var object bytes.Buffer
	result, err := writeChangeBatchStreaming(
		context.Background(), buffer, 3, time.Unix(10, 0).UTC(),
		func(_ context.Context, _ string, reader io.Reader) error {
			_, copyErr := io.Copy(&object, reader)
			return copyErr
		},
	)
	if err != nil {
		t.Fatalf("write keys-only batch: %v", err)
	}
	if got, want := string(result.Meta.Payload), "keys_only"; got != want {
		t.Fatalf("metadata payload=%q want=%q", got, want)
	}
	if got, want := result.Meta.RawSize, int64(32+len("key")+32+len("deleted")); got != want {
		t.Fatalf("raw bytes=%d want=%d", got, want)
	}

	batch, err := decodeChangeBatch(object.Bytes())
	if err != nil {
		t.Fatalf("decode keys-only batch: %v", err)
	}
	if batch.Payload != ChangeFeedKeysOnly {
		t.Fatalf("batch payload=%s want=%s", batch.Payload, ChangeFeedKeysOnly)
	}
	put := batch.Changes[0]
	if put.Kind != changePut || !put.ValueOmitted || put.Inline || put.Value != nil ||
		string(put.Key) != "key" || put.ExpireAt != 1234 {
		t.Fatalf("keys-only put mismatch: %+v", put)
	}
}

func TestEncodeChangeBatchRejectsOutOfOrderChanges(t *testing.T) {
	_, err := encodeChangeBatch(&changeBatch{
		Version: changeBatchVersion,
		Epoch:   1,
		SeqLo:   1,
		SeqHi:   2,
		Changes: []changeRecord{
			{Seq: 2, Kind: changePut, Key: []byte("b"), Inline: true, Value: []byte("vb")},
			{Seq: 1, Kind: changePut, Key: []byte("a"), Inline: true, Value: []byte("va")},
		},
	})
	if err == nil {
		t.Fatal("expected out-of-order error")
	}
}

func TestDecodeChangeBatchRejectsCorruptBlock(t *testing.T) {
	data, err := encodeChangeBatch(&changeBatch{
		Version: changeBatchVersion,
		Epoch:   1,
		SeqLo:   1,
		SeqHi:   3,
		Changes: []changeRecord{
			{Seq: 1, Kind: changePut, Key: []byte("a"), Inline: true, Value: []byte("va")},
			{Seq: 2, Kind: changePut, Key: []byte("b"), Inline: true, Value: []byte("vb")},
			{Seq: 3, Kind: changePut, Key: []byte("c"), Inline: true, Value: []byte("vc")},
		},
	})
	if err != nil {
		t.Fatalf("EncodeChangeBatch: %v", err)
	}

	data[0] ^= 0xff

	if _, err := decodeChangeBatch(data); err == nil {
		t.Fatal("expected corrupt block decode error")
	}
}

func TestChangeBatchDecoderRejectsOutputBeyondDeclaredRawSize(t *testing.T) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatalf("new zstd encoder: %v", err)
	}
	defer encoder.Close()

	compressed := encoder.EncodeAll(make([]byte, 2<<20), nil)
	_, err = decompressChangeBatchBlock(compressed, 1<<10)
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("decode error=%v want=%v", err, zstd.ErrDecoderSizeExceeded)
	}
}

func TestDecodeChangeBatchRejectsOtherFormatVersions(t *testing.T) {
	data, err := encodeChangeBatch(&changeBatch{
		Version: changeBatchVersion,
		Epoch:   1,
		SeqLo:   1,
		SeqHi:   1,
		Changes: []changeRecord{
			{Seq: 1, Kind: changePut, Key: []byte("a"), Inline: true, Value: []byte("value")},
		},
	})
	if err != nil {
		t.Fatalf("encode batch: %v", err)
	}
	trailer := data[len(data)-changeBatchTrailerSize:]
	binary.BigEndian.PutUint16(trailer[4:6], changeBatchVersion-1)
	if _, err := decodeChangeBatch(data); err == nil {
		t.Fatal("expected unsupported format version error")
	}
}

func TestDecodeChangeBatchRejectsPayloadModeThatDoesNotMatchRecords(t *testing.T) {
	data, err := encodeChangeBatch(&changeBatch{
		Version: changeBatchVersion,
		Payload: ChangeFeedFullValues,
		Epoch:   1,
		SeqLo:   1,
		SeqHi:   1,
		Changes: []changeRecord{
			{Seq: 1, Kind: changePut, Key: []byte("a"), Inline: true, Value: []byte("value")},
		},
	})
	if err != nil {
		t.Fatalf("encode batch: %v", err)
	}
	data[len(data)-changeBatchTrailerSize+6] = byte(ChangeFeedKeysOnly)
	if _, err := decodeChangeBatch(data); err == nil {
		t.Fatal("expected payload/record mismatch")
	}
}

func TestChangeBatchUsesIndependentRecordBlocks(t *testing.T) {
	buffer := &changeBatchBuffer{}
	for i := 0; i < 5; i++ {
		if err := buffer.appendPut(uint64(i+1), []byte{byte('a' + i)}, []byte("value"), 0); err != nil {
			t.Fatalf("append change %d: %v", i, err)
		}
	}

	var object bytes.Buffer
	result, err := writeChangeBatchStreamingWithOptions(
		context.Background(), buffer, 9, time.Unix(20, 0).UTC(),
		changeBatchBlockOptions{MaxRecords: 2, TargetRawBytes: 1 << 20},
		func(_ context.Context, _ string, reader io.Reader) error {
			_, copyErr := io.Copy(&object, reader)
			return copyErr
		})
	if err != nil {
		t.Fatalf("write indexed batch: %v", err)
	}
	if got, want := result.Meta.BlockCount, uint32(3); got != want {
		t.Fatalf("block count=%d want=%d", got, want)
	}

	data := object.Bytes()
	trailer := data[len(data)-changeBatchTrailerSize:]
	indexOffset, indexSize, err := changeBatchIndexLocation(trailer, int64(len(data)))
	if err != nil {
		t.Fatalf("locate index: %v", err)
	}
	index, err := decodeChangeBatchIndex(data[indexOffset:indexOffset+indexSize], trailer, int64(len(data)))
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}
	for i, want := range []uint32{2, 2, 1} {
		if index.Blocks[i].Count != want {
			t.Fatalf("block[%d] count=%d want=%d", i, index.Blocks[i].Count, want)
		}
	}

	batch, err := decodeChangeBatch(data)
	if err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	if len(batch.Changes) != 5 || batch.Changes[4].Seq != 5 {
		t.Fatalf("decoded changes=%+v", batch.Changes)
	}
}
