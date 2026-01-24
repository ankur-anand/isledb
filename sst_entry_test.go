package isledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/segmentio/ksuid"
)

type VLogEntry struct {
	Length   uint32
	Checksum uint32
	Value    []byte
}

func EncodeVLogEntry(value []byte) []byte {
	buf := make([]byte, VLogEntryHeaderSize+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(value)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(value, crcTable))
	copy(buf[8:], value)
	return buf
}

func DecodeVLogEntry(data []byte) (VLogEntry, error) {
	if len(data) < VLogEntryHeaderSize {
		return VLogEntry{}, errors.New("vlog entry too short")
	}

	length := binary.LittleEndian.Uint32(data[0:4])
	checksum := binary.LittleEndian.Uint32(data[4:8])

	if len(data) < int(VLogEntryHeaderSize+length) {
		return VLogEntry{}, errors.New("vlog entry truncated")
	}

	value := data[8 : 8+length]

	if crc32.Checksum(value, crcTable) != checksum {
		return VLogEntry{}, errors.New("vlog checksum mismatch")
	}

	return VLogEntry{
		Length:   length,
		Checksum: checksum,
		Value:    value,
	}, nil
}

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
	if got.VLogID != want.VLogID {
		t.Errorf("VLogID mismatch: got %v, want %v", got.VLogID, want.VLogID)
	}
	if got.VOffset != want.VOffset {
		t.Errorf("VOffset mismatch: got %d, want %d", got.VOffset, want.VOffset)
	}
	if got.VLength != want.VLength {
		t.Errorf("VLength mismatch: got %d, want %d", got.VLength, want.VLength)
	}
	if got.VChecksum != want.VChecksum {
		t.Errorf("VChecksum mismatch: got %d, want %d", got.VChecksum, want.VChecksum)
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
	vlogID := ksuid.New()

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
			name: "pointer",
			entry: KeyEntry{
				Key:       key,
				Kind:      OpPut,
				Inline:    false,
				VLogID:    vlogID,
				VOffset:   123456789,
				VLength:   1024,
				VChecksum: 0xDEADBEEF,
			},
			marker:  MarkerPointer,
			wantLen: 1 + PointerSize,
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
		{name: "pointer_too_short", encoded: []byte{MarkerPointer, 0x01, 0x02}},
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

func TestEncodeDecodeVLogEntry(t *testing.T) {
	cases := []struct {
		name  string
		value []byte
	}{
		{name: "small", value: []byte("this is a test value for vlog")},
		{name: "empty", value: []byte{}},
		{name: "large", value: patternValue(1024 * 1024)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeVLogEntry(tc.value)
			if len(encoded) != VLogEntryHeaderSize+len(tc.value) {
				t.Fatalf("expected length %d, got %d", VLogEntryHeaderSize+len(tc.value), len(encoded))
			}

			decoded, err := DecodeVLogEntry(encoded)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if decoded.Length != uint32(len(tc.value)) {
				t.Errorf("length mismatch: got %d, want %d", decoded.Length, len(tc.value))
			}
			if decoded.Checksum != crc32.Checksum(tc.value, crcTable) {
				t.Errorf("checksum mismatch")
			}
			if !bytes.Equal(decoded.Value, tc.value) {
				t.Errorf("value mismatch: got %v, want %v", decoded.Value, tc.value)
			}
		})
	}
}

func TestDecodeVLogEntry_Errors(t *testing.T) {
	cases := []struct {
		name string
		data func() []byte
	}{
		{name: "too_short", data: func() []byte { return []byte{0x01, 0x02, 0x03} }},
		{name: "truncated", data: func() []byte {
			encoded := EncodeVLogEntry([]byte("ab"))
			return encoded[:VLogEntryHeaderSize+1]
		}},
		{name: "checksum_mismatch", data: func() []byte {
			encoded := EncodeVLogEntry([]byte("test value"))
			encoded[len(encoded)-1] ^= 0xFF
			return encoded
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeVLogEntry(tc.data())
			if err == nil {
				t.Errorf("expected error for %s", tc.name)
			}
		})
	}
}
