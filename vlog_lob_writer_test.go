package isledb

import (
	"bytes"
	"context"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestStreamingVLogWriter_WriteValue(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	writer := NewStreamingVLogWriter(store)
	value := []byte("large-value")

	ptr, err := writer.WriteValue(ctx, value)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if ptr.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", ptr.Offset)
	}
	if int(ptr.Length) != len(value) {
		t.Fatalf("length mismatch: got %d, want %d", ptr.Length, len(value))
	}

	data, _, err := store.Read(ctx, store.VLogPath(ptr.VLogID.String()))
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	readValue, err := NewVLogReader(data).ReadValue(ptr)
	if err != nil {
		t.Fatalf("read value error: %v", err)
	}
	if !bytes.Equal(readValue, value) {
		t.Fatalf("value mismatch")
	}
}

func TestStreamingVLogWriter_WrittenVLogs(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	writer := NewStreamingVLogWriter(store)
	if _, err := writer.WriteValue(ctx, []byte("v1")); err != nil {
		t.Fatalf("write error: %v", err)
	}
	if _, err := writer.WriteValue(ctx, []byte("v2")); err != nil {
		t.Fatalf("write error: %v", err)
	}

	vlogs := writer.WrittenVLogs()
	if len(vlogs) != 2 {
		t.Fatalf("expected 2 vlogs, got %d", len(vlogs))
	}

	vlogs = writer.WrittenVLogs()
	if len(vlogs) != 0 {
		t.Fatalf("expected empty after drain, got %d", len(vlogs))
	}
}
