package isledb

import (
	"bytes"
	"testing"
)

func assertKeyEntry(t *testing.T, got, want KeyEntry) {
	t.Helper()

	if got.Kind != want.Kind {
		t.Errorf("kind mismatch: got %v, want %v", got.Kind, want.Kind)
	}
	if !bytes.Equal(got.Key, want.Key) {
		t.Errorf("key mismatch: got %v, want %v", got.Key, want.Key)
	}
	if want.Kind == OpDelete {
		return
	}

	if want.Inline {
		if !got.Inline {
			t.Errorf("expected inline=true")
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("value mismatch: got %v, want %v", got.Value, want.Value)
		}
		return
	}

	if got.Inline {
		t.Errorf("expected inline=false")
	}
	if got.BlobID != want.BlobID {
		t.Errorf("BlobID mismatch: got %v, want %v", got.BlobID, want.BlobID)
	}
}

func patternValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(i % 256)
	}
	return value
}

func TestEncodeDecodeKeyEntry(t *testing.T) {
	key := []byte("testkey")

	var blobID [32]byte
	copy(blobID[:], []byte("0123456789abcdef0123456789abcdef"))

	cases := []struct {
		name    string
		entry   KeyEntry
		marker  byte
		wantLen int
		check   func(t *testing.T, encoded []byte, entry KeyEntry)
	}{
		{
			name:    "delete",
			entry:   KeyEntry{Key: key, Kind: OpDelete},
			marker:  MarkerDelete,
			wantLen: 1,
		},
		{
			name: "inline",
			entry: KeyEntry{
				Key:    key,
				Kind:   OpPut,
				Inline: true,
				Value:  []byte("small value"),
			},
			marker:  MarkerInline,
			wantLen: 1 + len("small value"),
			check: func(t *testing.T, encoded []byte, entry KeyEntry) {
				t.Helper()
				if !bytes.Equal(encoded[1:], entry.Value) {
					t.Fatalf("value mismatch in encoded bytes")
				}
			},
		},
		{
			name: "blob_reference",
			entry: KeyEntry{
				Key:    key,
				Kind:   OpPut,
				Inline: false,
				BlobID: blobID,
			},
			marker:  MarkerBlob,
			wantLen: 1 + 32,
			check: func(t *testing.T, encoded []byte, entry KeyEntry) {
				t.Helper()
				if !bytes.Equal(encoded[1:33], entry.BlobID[:]) {
					t.Fatalf("blob ID mismatch in encoded bytes")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeKeyEntry(tc.entry)
			if len(encoded) != tc.wantLen {
				t.Fatalf("expected length %d, got %d", tc.wantLen, len(encoded))
			}
			if encoded[0] != tc.marker {
				t.Fatalf("expected marker %v, got %v", tc.marker, encoded[0])
			}
			if tc.check != nil {
				tc.check(t, encoded, tc.entry)
			}

			decoded, err := DecodeKeyEntry(tc.entry.Key, encoded)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertKeyEntry(t, decoded, tc.entry)
		})
	}
}

func TestDecodeKeyEntry_Errors(t *testing.T) {
	cases := []struct {
		name    string
		encoded []byte
	}{
		{name: "empty", encoded: []byte{}},
		{name: "unknown_marker", encoded: []byte{0xFF}},
		{name: "blob_too_short", encoded: []byte{MarkerBlob, 0x01, 0x02}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeKeyEntry([]byte("key"), tc.encoded)
			if err == nil {
				t.Errorf("expected error for %s", tc.name)
			}
		})
	}
}

func TestKeyEntry_HasBlobID(t *testing.T) {

	entry1 := KeyEntry{
		Key:    []byte("key1"),
		Kind:   OpPut,
		Inline: true,
		Value:  []byte("value"),
	}
	if entry1.HasBlobID() {
		t.Error("expected HasBlobID=false for inline entry")
	}

	var blobID [32]byte
	copy(blobID[:], []byte("0123456789abcdef0123456789abcdef"))
	entry2 := KeyEntry{
		Key:    []byte("key2"),
		Kind:   OpPut,
		BlobID: blobID,
	}
	if !entry2.HasBlobID() {
		t.Error("expected HasBlobID=true for blob reference entry")
	}
}

func TestCompactionEntry_HasBlobID(t *testing.T) {

	entry1 := CompactionEntry{
		Key:    []byte("key1"),
		Kind:   OpPut,
		Inline: true,
		Value:  []byte("value"),
	}
	if entry1.HasBlobID() {
		t.Error("expected HasBlobID=false for inline entry")
	}

	var blobID [32]byte
	copy(blobID[:], []byte("0123456789abcdef0123456789abcdef"))
	entry2 := CompactionEntry{
		Key:    []byte("key2"),
		Kind:   OpPut,
		BlobID: blobID,
	}
	if !entry2.HasBlobID() {
		t.Error("expected HasBlobID=true for blob reference entry")
	}
}
