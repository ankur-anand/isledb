package isledb

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"
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
	if got, want := result.Meta.RawSize, int64(changeBatchHeaderSize)+buffer.bodySize; got != want {
		t.Fatalf("meta raw size=%d want=%d", got, want)
	}
	if result.Meta.Checksum == "" || result.Meta.Compression != changeBatchCompressionZstd {
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

func TestDecodeChangeBatchRejectsOutOfOrderChanges(t *testing.T) {
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

	secondRecord := changeBatchHeaderSize + changeRecordHeaderSize + len("a") + len("va")
	binary.BigEndian.PutUint64(data[secondRecord+16:secondRecord+24], 1)

	if _, err := decodeChangeBatch(data); err == nil {
		t.Fatal("expected out-of-order decode error")
	}
}
