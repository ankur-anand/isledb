package manifest

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestImmutableManifestObjectRoundTrip(t *testing.T) {
	if _, err := EncodeSnapshot(nil); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("EncodeSnapshot(nil) error=%v, want %v", err, ErrInvalidManifest)
	}
	if _, err := EncodeCommitPage(nil); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("EncodeCommitPage(nil) error=%v, want %v", err, ErrInvalidManifest)
	}
	snapshot := &Manifest{
		Version:   2,
		NextEpoch: 3,
		LogSeq:    9,
		L0SSTs:    []SSTMeta{{ID: "l0.sst", MinKey: []byte("a"), MaxKey: []byte("z")}},
	}
	snapshotData, err := EncodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	assertManifestEnvelope(t, snapshotData, manifestObjectKindSnapshot)
	decodedSnapshot, err := DecodeSnapshot(snapshotData)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodedSnapshot, snapshot) {
		t.Fatalf("snapshot round trip\n got=%+v\nwant=%+v", decodedSnapshot, snapshot)
	}

	page := &CommitPage{
		LayoutVersion: LayoutVersion,
		PageType:      CommitPageTypeLeaf,
		Level:         0,
		SeqLo:         7,
		SeqHi:         7,
		Count:         1,
		Entries:       []ManifestLogEntry{testManifestEntry(7)},
		CreatedAt:     time.Unix(1_700_000_000, 0).UTC(),
	}
	pageData, err := EncodeCommitPage(page)
	if err != nil {
		t.Fatal(err)
	}
	assertManifestEnvelope(t, pageData, manifestObjectKindPage)
	decodedPage, err := DecodeCommitPage(pageData)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodedPage, page) {
		t.Fatalf("page round trip\n got=%+v\nwant=%+v", decodedPage, page)
	}

	if _, err := DecodeCommitPage(snapshotData); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("decode snapshot as page error=%v, want %v", err, ErrInvalidManifest)
	}
	if _, err := DecodeSnapshot([]byte(`{"version":2}`)); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("decode raw JSON error=%v, want %v", err, ErrInvalidManifest)
	}
	oldPageData, err := EncodeCommitPage(&CommitPage{LayoutVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCommitPage(oldPageData); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("decode old page layout error=%v, want %v", err, ErrInvalidManifest)
	}
}

func TestImmutableManifestObjectRejectsCorruption(t *testing.T) {
	encoded, err := EncodeSnapshot(&Manifest{Version: 2, NextEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "truncated-header", mutate: func(data []byte) []byte { return data[:manifestObjectHeaderBytes] }},
		{name: "magic", mutate: func(data []byte) []byte { data[0] ^= 0xff; return data }},
		{name: "version", mutate: func(data []byte) []byte { data[4]++; return data }},
		{name: "kind", mutate: func(data []byte) []byte { data[5] = manifestObjectKindPage; return data }},
		{name: "codec", mutate: func(data []byte) []byte { data[6]++; return data }},
		{name: "flags", mutate: func(data []byte) []byte { data[7] = 1; return data }},
		{name: "zero-raw-size", mutate: func(data []byte) []byte { binary.BigEndian.PutUint64(data[8:16], 0); return data }},
		{name: "oversized-raw-size", mutate: func(data []byte) []byte {
			binary.BigEndian.PutUint64(data[8:16], maxManifestSnapshotRawBytes+1)
			return data
		}},
		{name: "declared-too-small", mutate: func(data []byte) []byte {
			rawBytes := binary.BigEndian.Uint64(data[8:16])
			binary.BigEndian.PutUint64(data[8:16], rawBytes-1)
			return data
		}},
		{name: "declared-too-large", mutate: func(data []byte) []byte {
			rawBytes := binary.BigEndian.Uint64(data[8:16])
			binary.BigEndian.PutUint64(data[8:16], rawBytes+1)
			return data
		}},
		{name: "truncated-frame", mutate: func(data []byte) []byte { return data[:len(data)-1] }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := append([]byte(nil), encoded...)
			corrupt = tc.mutate(corrupt)
			if _, err := DecodeSnapshot(corrupt); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("DecodeSnapshot error=%v, want %v", err, ErrInvalidManifest)
			}
		})
	}
}

func TestImmutableManifestObjectRefVerification(t *testing.T) {
	encoded, err := EncodeSnapshot(&Manifest{Version: 2, NextEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := newManifestObjectRef("manifest/snapshots/test.manifest.zst", encoded, manifestObjectKindSnapshot, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyManifestObjectRef(encoded, ref, manifestObjectKindSnapshot); err != nil {
		t.Fatalf("verify valid ref: %v", err)
	}

	t.Run("stored-bytes", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		corrupt[len(corrupt)-1] ^= 0xff
		if err := verifyManifestObjectRef(corrupt, ref, manifestObjectKindSnapshot); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("verify error=%v, want %v", err, ErrInvalidManifest)
		}
	})
	t.Run("encoded-size", func(t *testing.T) {
		wrong := ref
		wrong.EncodedBytes++
		if err := verifyManifestObjectRef(encoded, wrong, manifestObjectKindSnapshot); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("verify error=%v, want %v", err, ErrInvalidManifest)
		}
	})
}

func assertManifestEnvelope(t *testing.T, encoded []byte, kind byte) {
	t.Helper()
	if len(encoded) <= manifestObjectHeaderBytes {
		t.Fatalf("encoded bytes=%d, want envelope and payload", len(encoded))
	}
	if string(encoded[:4]) != string(manifestObjectMagic[:]) {
		t.Fatalf("magic=%q, want=%q", encoded[:4], manifestObjectMagic)
	}
	if encoded[4] != manifestObjectVersion || encoded[5] != kind || encoded[6] != manifestObjectCodecZstd || encoded[7] != 0 {
		t.Fatalf("header version=%d kind=%d codec=%d flags=%d", encoded[4], encoded[5], encoded[6], encoded[7])
	}
}
