package manifest

import (
	"reflect"
	"testing"
)

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
