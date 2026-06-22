package isledb

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal"
)

func TestBuildChangeBatchOrdersBySeq(t *testing.T) {
	var blobID [32]byte
	copy(blobID[:], []byte("blob-id"))

	it := &sliceSSTIter{entries: []internal.MemEntry{
		{Key: []byte("b"), Seq: 3, Kind: internal.OpDelete},
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("va")},
		{Key: []byte("c"), Seq: 2, Kind: internal.OpPut, BlobID: blobID, ExpireAt: 1234},
	}}

	result, err := buildChangeBatch(context.Background(), it, 7, 1, 3, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatalf("buildChangeBatch: %v", err)
	}
	if result.Meta.Epoch != 7 || result.Meta.SeqLo != 1 || result.Meta.SeqHi != 3 || result.Meta.Count != 3 {
		t.Fatalf("meta mismatch: %+v", result.Meta)
	}
	if result.Meta.Size != int64(len(result.Data)) {
		t.Fatalf("meta size=%d data=%d", result.Meta.Size, len(result.Data))
	}
	if result.Meta.Checksum == "" {
		t.Fatal("expected checksum")
	}

	batch, err := DecodeChangeBatch(result.Data)
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
	if batch.Changes[0].Kind != ChangePut || !batch.Changes[0].Inline || string(batch.Changes[0].Value) != "va" {
		t.Fatalf("inline put mismatch: %+v", batch.Changes[0])
	}
	if batch.Changes[1].Kind != ChangePut || batch.Changes[1].Inline || batch.Changes[1].BlobID != blobID || batch.Changes[1].ExpireAt != 1234 {
		t.Fatalf("blob put mismatch: %+v", batch.Changes[1])
	}
	if batch.Changes[2].Kind != ChangeDelete {
		t.Fatalf("delete mismatch: %+v", batch.Changes[2])
	}
}

func TestEncodeChangeBatchRejectsOutOfOrderChanges(t *testing.T) {
	_, err := EncodeChangeBatch(&ChangeBatch{
		Version: changeBatchVersion,
		Epoch:   1,
		SeqLo:   1,
		SeqHi:   2,
		Changes: []Change{
			{Seq: 2, Kind: ChangePut, Key: []byte("b"), Inline: true, Value: []byte("vb")},
			{Seq: 1, Kind: ChangePut, Key: []byte("a"), Inline: true, Value: []byte("va")},
		},
	})
	if err == nil {
		t.Fatal("expected out-of-order error")
	}
}

func TestDecodeChangeBatchRejectsOutOfOrderChanges(t *testing.T) {
	data, err := EncodeChangeBatch(&ChangeBatch{
		Version: changeBatchVersion,
		Epoch:   1,
		SeqLo:   1,
		SeqHi:   3,
		Changes: []Change{
			{Seq: 1, Kind: ChangePut, Key: []byte("a"), Inline: true, Value: []byte("va")},
			{Seq: 2, Kind: ChangePut, Key: []byte("b"), Inline: true, Value: []byte("vb")},
			{Seq: 3, Kind: ChangePut, Key: []byte("c"), Inline: true, Value: []byte("vc")},
		},
	})
	if err != nil {
		t.Fatalf("EncodeChangeBatch: %v", err)
	}

	secondRecord := changeBatchHeaderSize + changeRecordHeaderSize + len("a") + len("va")
	binary.BigEndian.PutUint64(data[secondRecord+16:secondRecord+24], 1)

	if _, err := DecodeChangeBatch(data); err == nil {
		t.Fatal("expected out-of-order decode error")
	}
}
