package internal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
)

func TestBlobStorage_WriteAndRead(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	bs := NewBlobStorage(store, config.DefaultValueStorageConfig())

	value := []byte("hello world this is a test value")
	blobID, err := bs.Write(ctx, value)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	expectedID := sha256.Sum256(value)
	if blobID != expectedID {
		t.Errorf("BlobID mismatch: got %x, want %x", blobID, expectedID)
	}

	readValue, err := bs.Read(ctx, blobID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(readValue) != string(value) {
		t.Errorf("value mismatch: got %q, want %q", readValue, value)
	}
}

func TestBlobStorage_Deduplication(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	bs := NewBlobStorage(store, config.DefaultValueStorageConfig())

	value := []byte("duplicate value")

	blobID1, err := bs.Write(ctx, value)
	if err != nil {
		t.Fatalf("First write failed: %v", err)
	}

	blobID2, err := bs.Write(ctx, value)
	if err != nil {
		t.Fatalf("Second write failed: %v", err)
	}

	if blobID1 != blobID2 {
		t.Errorf("Blob IDs should match for same content: %x != %x", blobID1, blobID2)
	}

	files, err := store.ListBlobFiles(ctx)
	if err != nil {
		t.Fatalf("ListBlobFiles failed: %v", err)
	}

	if len(files) != 1 {
		t.Errorf("Expected 1 blob file, got %d", len(files))
	}
}

func TestBlobStorage_LargeBlob(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	bs := NewBlobStorage(store, config.DefaultValueStorageConfig())

	value := make([]byte, 1024*1024)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}

	blobID, err := bs.Write(ctx, value)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	readValue, err := bs.Read(ctx, blobID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(readValue) != len(value) {
		t.Errorf("Length mismatch: got %d, want %d", len(readValue), len(value))
	}

	expectedID := sha256.Sum256(value)
	gotID := sha256.Sum256(readValue)
	if gotID != expectedID {
		t.Errorf("Content hash mismatch")
	}
}

func TestBlobStorage_VerifyOnRead(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	config := config.DefaultValueStorageConfig()
	config.VerifyBlobsOnRead = true
	bs := NewBlobStorage(store, config)

	value := []byte("verify this blob")
	blobID, err := bs.Write(ctx, value)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	readValue, err := bs.Read(ctx, blobID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(readValue) != string(value) {
		t.Errorf("value mismatch")
	}
}

func TestBlobStorage_Exists(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	bs := NewBlobStorage(store, config.DefaultValueStorageConfig())

	value := []byte("check if exists")
	blobID, err := bs.Write(ctx, value)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	exists, err := bs.Exists(ctx, blobID)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("Expected blob to exist")
	}

	fakeBlobID := sha256.Sum256([]byte("non-existent"))
	exists, err = bs.Exists(ctx, fakeBlobID)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("Expected blob not to exist")
	}
}

func TestBlobStorage_Delete(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	bs := NewBlobStorage(store, config.DefaultValueStorageConfig())

	value := []byte("delete this blob")
	blobID, err := bs.Write(ctx, value)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	exists, _ := bs.Exists(ctx, blobID)
	if !exists {
		t.Fatal("Blob should exist before delete")
	}

	if err := bs.Delete(ctx, blobID); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	exists, _ = bs.Exists(ctx, blobID)
	if exists {
		t.Error("Blob should not exist after delete")
	}
}

func TestBlobIDConversion(t *testing.T) {
	original := sha256.Sum256([]byte("test conversion"))

	hexStr := BlobIDToHex(original)
	if len(hexStr) != 64 {
		t.Errorf("Expected 64 char hex string, got %d", len(hexStr))
	}

	converted, err := HexToBlobID(hexStr)
	if err != nil {
		t.Fatalf("HexToBlobID failed: %v", err)
	}

	if converted != original {
		t.Errorf("Conversion round-trip failed: %x != %x", converted, original)
	}
}

func TestHexToBlobID_InvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"too short", "abc123"},
		{"invalid hex", "xyz" + string(make([]byte, 61))},
		{"too long", string(make([]byte, 128))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := HexToBlobID(tt.input)
			if err == nil {
				t.Error("Expected error for invalid input")
			}
		})
	}
}

func TestComputeBlobID(t *testing.T) {
	value := []byte("compute blob id")
	blobID := ComputeBlobID(value)
	expected := sha256.Sum256(value)

	if blobID != expected {
		t.Errorf("ComputeBlobID mismatch: %x != %x", blobID, expected)
	}
}
