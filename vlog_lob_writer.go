package isledb

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/segmentio/ksuid"
)

// StreamingVLogWriter handles direct writes of large values to VLog.
// Each large value is written immediately to its own VLog file with a KSUID.
type StreamingVLogWriter struct {
	store *blobstore.Store

	mu      sync.Mutex
	written []VLogMeta
}

// NewStreamingVLogWriter creates a streaming writer for large values.
func NewStreamingVLogWriter(store *blobstore.Store) *StreamingVLogWriter {
	return &StreamingVLogWriter{
		store: store,
	}
}

// WriteValue writes a large value directly to VLog and returns a pointer.
func (w *StreamingVLogWriter) WriteValue(ctx context.Context, value []byte) (VLogPointer, error) {
	vlogID := ksuid.New()
	checksum := crc32.Checksum(value, crcTable)

	data := make([]byte, VLogEntryHeaderSize+len(value))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(value)))
	binary.LittleEndian.PutUint32(data[4:8], checksum)
	copy(data[VLogEntryHeaderSize:], value)

	path := w.store.VLogPath(vlogID.String())
	_, err := w.store.Write(ctx, path, data)
	if err != nil {
		return VLogPointer{}, fmt.Errorf("upload streaming vlog %s: %w", vlogID.String(), err)
	}

	w.mu.Lock()
	w.written = append(w.written, VLogMeta{
		ID:   vlogID,
		Size: int64(len(data)),
	})
	w.mu.Unlock()

	return VLogPointer{
		VLogID:   vlogID,
		Offset:   0,
		Length:   uint32(len(value)),
		Checksum: checksum,
	}, nil
}

// WrittenVLogs returns VLogs written since last call and clears the list.
func (w *StreamingVLogWriter) WrittenVLogs() []VLogMeta {
	w.mu.Lock()
	defer w.mu.Unlock()

	vlogs := w.written
	w.written = nil
	return vlogs
}

// Reset clears the list of written VLogs without returning them.
func (w *StreamingVLogWriter) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written = nil
}
