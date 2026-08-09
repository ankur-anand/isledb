package manifest

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestSSTMetaJSONSchema(t *testing.T) {
	meta := SSTMeta{
		ID:       "sst-001",
		Epoch:    2,
		SeqLo:    10,
		SeqHi:    20,
		MinKey:   []byte("alpha"),
		MaxKey:   []byte("omega"),
		Size:     4096,
		Checksum: "sha256:abc",
		Signature: &SSTSignature{
			Algorithm: "ed25519",
			KeyID:     "key-1",
			Hash:      "sha256:def",
			Signature: []byte{1, 2, 3},
		},
		Bloom: BloomMeta{
			BitsPerKey: 10,
			K:          7,
			Offset:     2048,
			Length:     256,
		},
		CreatedAt: time.Date(2026, time.August, 8, 10, 30, 0, 0, time.UTC),
		Level:     1,
	}

	body, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"id":"sst-001","epoch":2,"seq_lo":10,"seq_hi":20,"min_key":"YWxwaGE=","max_key":"b21lZ2E=","size":4096,"checksum":"sha256:abc","signature":{"algorithm":"ed25519","key_id":"key-1","hash":"sha256:def","signature":"AQID"},"bloom":{"bits_per_key":10,"k":7,"offset":2048,"length":256},"created_at":"2026-08-08T10:30:00Z","level":1}`
	if string(body) != want {
		t.Fatalf("SSTMeta JSON schema changed\n got: %s\nwant: %s", body, want)
	}

	var got SSTMeta
	if err := json.Unmarshal([]byte(want), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, meta) {
		t.Fatalf("decode schema fixture\n got=%+v\nwant=%+v", got, meta)
	}
}

func TestSSTMetaJSONOmitsAbsentSignature(t *testing.T) {
	body, err := json.Marshal(SSTMeta{})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"id":"","epoch":0,"seq_lo":0,"seq_hi":0,"min_key":null,"max_key":null,"size":0,"checksum":"","bloom":{"bits_per_key":0,"k":0,"offset":0,"length":0},"created_at":"0001-01-01T00:00:00Z","level":0}`
	if string(body) != want {
		t.Fatalf("zero SSTMeta JSON schema changed\n got: %s\nwant: %s", body, want)
	}
}

func TestChangeBatchMetaJSONSchema(t *testing.T) {
	meta := ChangeBatchMeta{
		ID:            "change-001",
		Path:          "changes/abc/change-001",
		Epoch:         2,
		SeqLo:         10,
		SeqHi:         20,
		Count:         11,
		BlockCount:    2,
		Size:          4096,
		RawSize:       8192,
		Checksum:      "sha256:whole",
		IndexChecksum: "sha256:index",
		CreatedAt:     time.Date(2026, time.August, 8, 10, 30, 0, 0, time.UTC),
		Version:       1,
		Compression:   "zstd",
		Payload:       ChangeFeedPayloadFullValues,
	}
	body, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"id":"change-001","path":"changes/abc/change-001","epoch":2,"seq_lo":10,"seq_hi":20,"count":11,"block_count":2,"size":4096,"raw_size":8192,"checksum":"sha256:whole","index_checksum":"sha256:index","created_at":"2026-08-08T10:30:00Z","version":1,"compression":"zstd","payload":"full_values"}`
	if string(body) != want {
		t.Fatalf("ChangeBatchMeta JSON schema changed\n got: %s\nwant: %s", body, want)
	}
	var got ChangeBatchMeta
	if err := json.Unmarshal([]byte(want), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, meta) {
		t.Fatalf("decode schema fixture\n got=%+v\nwant=%+v", got, meta)
	}
}

func TestManifestSnapshotRoundTrip(t *testing.T) {
	m := &Manifest{
		Version:   2,
		NextEpoch: 4,
		LogSeq:    9,
		L0SSTs:    []SSTMeta{{ID: "l0", MinKey: []byte("a"), MaxKey: []byte("b")}},
		Levels:    []Level{{Number: 1, SSTs: []SSTMeta{{ID: "l1", MinKey: []byte("c"), MaxKey: []byte("z")}}}},
	}
	body, err := EncodeSnapshot(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSnapshot(body)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, m) {
		t.Fatalf("round trip\n got=%+v\nwant=%+v", got, m)
	}
}

func TestCurrentRoundTrip(t *testing.T) {
	c := &Current{LayoutVersion: 1, Format: "paged-v1", NextSeq: 8, NextEpoch: 2, RetirementLogStart: 3}
	body, err := EncodeCurrent(c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCurrent(body)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, c) {
		t.Fatalf("round trip\n got=%+v\nwant=%+v", got, c)
	}
}
