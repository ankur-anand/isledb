package isledb

import (
	"bytes"
	"context"
	"hash/crc32"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestVLogWriter_AppendAndReadValue(t *testing.T) {
	w := NewVLogWriter()
	valueA := []byte("alpha")
	valueB := []byte("bravo-bravo")

	ptrA, err := w.Append(valueA)
	if err != nil {
		t.Fatalf("append error: %v", err)
	}
	ptrB, err := w.Append(valueB)
	if err != nil {
		t.Fatalf("append error: %v", err)
	}

	expectedOffsetB := uint64(VLogEntryHeaderSize + len(valueA))
	if ptrA.Offset != 0 {
		t.Fatalf("offset mismatch: got %d, want 0", ptrA.Offset)
	}
	if ptrB.Offset != expectedOffsetB {
		t.Fatalf("offset mismatch: got %d, want %d", ptrB.Offset, expectedOffsetB)
	}
	if ptrA.Length != uint32(len(valueA)) || ptrB.Length != uint32(len(valueB)) {
		t.Fatalf("length mismatch: got %d/%d", ptrA.Length, ptrB.Length)
	}
	if ptrA.Checksum != crc32.Checksum(valueA, crcTable) || ptrB.Checksum != crc32.Checksum(valueB, crcTable) {
		t.Fatalf("checksum mismatch")
	}

	data := w.Bytes()
	if w.Size() != int64(len(data)) {
		t.Fatalf("size mismatch: got %d, want %d", w.Size(), len(data))
	}

	reader := NewVLogReader(data)
	readA, err := reader.ReadValue(ptrA)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !bytes.Equal(readA, valueA) {
		t.Fatalf("value mismatch")
	}
	readB, err := reader.ReadValue(ptrB)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !bytes.Equal(readB, valueB) {
		t.Fatalf("value mismatch")
	}
}

func TestVLogReader_Errors(t *testing.T) {
	value := []byte("test-value")
	encoded := EncodeVLogEntry(value)

	cases := []struct {
		name string
		data []byte
		ptr  VLogPointer
	}{
		{
			name: "offset_out_of_bounds",
			data: []byte{},
			ptr:  VLogPointer{Offset: 0},
		},
		{
			name: "length_mismatch",
			data: encoded,
			ptr: VLogPointer{
				Offset:   0,
				Length:   uint32(len(value) - 1),
				Checksum: crc32.Checksum(value, crcTable),
			},
		},
		{
			name: "value_truncated",
			data: encoded[:VLogEntryHeaderSize+1],
			ptr: VLogPointer{
				Offset:   0,
				Length:   uint32(len(value)),
				Checksum: crc32.Checksum(value, crcTable),
			},
		},
		{
			name: "checksum_mismatch",
			data: append([]byte(nil), encoded...),
			ptr: VLogPointer{
				Offset:   0,
				Length:   uint32(len(value)),
				Checksum: crc32.Checksum(value, crcTable),
			},
		},
		{
			name: "pointer_checksum_mismatch",
			data: encoded,
			ptr: VLogPointer{
				Offset:   0,
				Length:   uint32(len(value)),
				Checksum: 0xdeadbeef,
			},
		},
	}

	cases[3].data[len(cases[3].data)-1] ^= 0xFF

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := NewVLogReader(tc.data)
			_, err := reader.ReadValue(tc.ptr)
			if err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}

func TestVLogWriter_Upload(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	w := NewVLogWriter()
	if _, err := w.Append([]byte("first")); err != nil {
		t.Fatalf("append error: %v", err)
	}
	if _, err := w.Append([]byte("second")); err != nil {
		t.Fatalf("append error: %v", err)
	}

	if err := w.Upload(ctx, store); err != nil {
		t.Fatalf("upload error: %v", err)
	}

	data, _, err := store.Read(ctx, store.VLogPath(w.ID().String()))
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !bytes.Equal(data, w.Bytes()) {
		t.Fatalf("uploaded data mismatch")
	}
}
