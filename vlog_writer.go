package isledb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/segmentio/ksuid"
)

// VLogWriter buffers large values and writes them to a VLog file.
type VLogWriter struct {
	id     ksuid.KSUID
	buf    *bytes.Buffer
	offset uint64
	mu     sync.Mutex
	used   bool
}

// NewVLogWriter creates a new VLog writer with a fresh KSUID.
func NewVLogWriter() *VLogWriter {
	return &VLogWriter{
		id:  ksuid.New(),
		buf: new(bytes.Buffer),
	}
}

// Append adds a value to the VLog and returns a pointer to it.
func (w *VLogWriter) Append(value []byte) (VLogPointer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.used {
		return VLogPointer{}, errors.New("vlog bytes already used")
	}

	offset := w.offset
	checksum := crc32.Checksum(value, crcTable)

	var header [VLogEntryHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(value)))
	binary.LittleEndian.PutUint32(header[4:8], checksum)

	if _, err := w.buf.Write(header[:]); err != nil {
		return VLogPointer{}, fmt.Errorf("vlog: buffer write failed: %w", err)
	}
	if _, err := w.buf.Write(value); err != nil {
		return VLogPointer{}, fmt.Errorf("vlog: buffer write failed: %w", err)
	}
	w.offset += uint64(VLogEntryHeaderSize + len(value))

	return VLogPointer{
		VLogID:   w.id,
		Offset:   offset,
		Length:   uint32(len(value)),
		Checksum: checksum,
	}, nil
}

func (w *VLogWriter) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return int64(w.buf.Len())
}

func (w *VLogWriter) ID() ksuid.KSUID {
	return w.id
}

// Bytes returns the buffered VLog data for upload.
// After calling Bytes, the writer should not be used again.
func (w *VLogWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.used = true
	return w.buf.Bytes()
}

// Upload writes the VLog to object storage.
func (w *VLogWriter) Upload(ctx context.Context, store *blobstore.Store) error {
	data := w.Bytes()
	if len(data) == 0 {
		return nil
	}
	path := store.VLogPath(w.id.String())
	_, err := store.Write(ctx, path, data)
	return err
}

// VLogReader reads values from a VLog file.
type VLogReader struct {
	data []byte
}

// NewVLogReader creates a reader from VLog data.
func NewVLogReader(data []byte) *VLogReader {
	return &VLogReader{data: data}
}

// ReadValue reads a value at the given offset with validation.
func (r *VLogReader) ReadValue(ptr VLogPointer) ([]byte, error) {
	dataLen := uint64(len(r.data))

	if ptr.Offset > dataLen || uint64(VLogEntryHeaderSize) > dataLen-ptr.Offset {
		return nil, errors.New("vlog offset out of bounds")
	}

	headerStart := ptr.Offset
	length := binary.LittleEndian.Uint32(r.data[headerStart : headerStart+4])
	checksum := binary.LittleEndian.Uint32(r.data[headerStart+4 : headerStart+8])

	if length != ptr.Length {
		return nil, fmt.Errorf("vlog length mismatch: got %d, expected %d", length, ptr.Length)
	}

	valueStart := headerStart + VLogEntryHeaderSize

	if uint64(length) > dataLen-valueStart {
		return nil, errors.New("vlog value truncated")
	}
	valueEnd := valueStart + uint64(length)

	value := r.data[valueStart:valueEnd]

	if crc32.Checksum(value, crcTable) != checksum {
		return nil, errors.New("vlog checksum mismatch")
	}

	if checksum != ptr.Checksum {
		return nil, errors.New("vlog pointer checksum mismatch")
	}

	return value, nil
}
