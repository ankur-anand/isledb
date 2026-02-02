package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func TestWriteSSTStreaming_Basic(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("x")},
		{Key: []byte("b"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("y")},
	}
	it := &sliceSSTIter{entries: entries}

	var uploadedData []byte
	var uploadedID string
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		uploadedID = sstID
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		uploadedData = data
		return nil
	}

	result, err := writeSSTStreaming(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1, 1, 2, uploadFn)
	if err != nil {
		t.Fatalf("writeSSTStreaming error: %v", err)
	}

	if len(uploadedData) == 0 {
		t.Fatalf("expected uploaded data")
	}
	if uploadedID == "" {
		t.Fatalf("expected uploaded ID")
	}
	if !strings.HasSuffix(uploadedID, ".sst") {
		t.Errorf("expected .sst suffix, got %s", uploadedID)
	}

	if !strings.HasPrefix(uploadedID, "1-1-2-") {
		t.Errorf("expected ID to start with 1-1-2-, got %s", uploadedID)
	}

	if result.Meta.SeqLo != 1 || result.Meta.SeqHi != 2 {
		t.Errorf("seq range mismatch: got %d-%d", result.Meta.SeqLo, result.Meta.SeqHi)
	}
	if !bytes.Equal(result.Meta.MinKey, []byte("a")) || !bytes.Equal(result.Meta.MaxKey, []byte("b")) {
		t.Errorf("key range mismatch: %s-%s", result.Meta.MinKey, result.Meta.MaxKey)
	}
	if result.Meta.Epoch != 1 {
		t.Errorf("epoch mismatch: got %d", result.Meta.Epoch)
	}
	if result.Meta.Size != int64(len(uploadedData)) {
		t.Errorf("size mismatch: meta=%d, actual=%d", result.Meta.Size, len(uploadedData))
	}

	h := sha256.Sum256(uploadedData)
	expectedChecksum := "sha256:" + hex.EncodeToString(h[:])
	if result.Meta.Checksum != expectedChecksum {
		t.Errorf("checksum mismatch: got %s, want %s", result.Meta.Checksum, expectedChecksum)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(uploadedData), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		t.Fatalf("iter error: %v", err)
	}
	defer iter.Close()

	var count int
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 entries, got %d", count)
	}
}

func TestWriteSSTStreaming_UploadError(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("x")},
	}
	it := &sliceSSTIter{entries: entries}

	uploadErr := errors.New("upload failed")
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		buf := make([]byte, 100)
		r.Read(buf)
		return uploadErr
	}

	_, err := writeSSTStreaming(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1, 1, 1, uploadFn)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "upload failed") && !strings.Contains(err.Error(), "closed pipe") {
		t.Errorf("expected upload or pipe error, got %v", err)
	}
}

func TestWriteSSTStreaming_ProducerError(t *testing.T) {
	iterErr := errors.New("iterator failed")
	it := &sliceSSTIter{
		entries: []internal.MemEntry{
			{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("x")},
		},
		err: iterErr,
	}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		_, err := io.ReadAll(r)
		return err
	}

	_, err := writeSSTStreaming(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1, 1, 1, uploadFn)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "iterator failed") {
		t.Errorf("expected iterator error, got %v", err)
	}
}

func TestWriteSSTStreaming_ContextCancellation(t *testing.T) {
	entries := make([]internal.MemEntry, 1000)
	for i := range entries {
		entries[i] = internal.MemEntry{
			Key:    []byte{byte(i / 256), byte(i % 256)},
			Seq:    uint64(1000 - i),
			Kind:   internal.OpPut,
			Inline: true,
			Value:  bytes.Repeat([]byte("x"), 100),
		}
	}
	it := &sliceSSTIter{entries: entries}

	ctx, cancel := context.WithCancel(context.Background())

	var readCount atomic.Int32
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		buf := make([]byte, 1024)
		for {
			_, err := r.Read(buf)
			if err != nil {
				return err
			}
			if readCount.Add(1) == 5 {
				cancel()
			}
		}
	}

	_, err := writeSSTStreaming(ctx, it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1, 1, 1000, uploadFn)
	if err == nil {
		t.Fatalf("expected error due to context cancellation")
	}
}

func TestWriteSSTStreaming_EmptyIterator(t *testing.T) {
	it := &sliceSSTIter{}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		_, err := io.ReadAll(r)
		return err
	}

	_, err := writeSSTStreaming(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1, 0, 0, uploadFn)
	if !errors.Is(err, ErrEmptyIterator) {
		t.Fatalf("expected ErrEmptyIterator, got %v", err)
	}
}

func TestWriteSSTStreaming_HashVerification(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("key1"), Seq: 3, Kind: internal.OpPut, Inline: true, Value: []byte("value1")},
		{Key: []byte("key2"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("value2")},
		{Key: []byte("key3"), Seq: 1, Kind: internal.OpDelete},
	}
	it := &sliceSSTIter{entries: entries}

	var uploadedData []byte
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		uploadedData = data
		return nil
	}

	result, err := writeSSTStreaming(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1, 1, 3, uploadFn)
	if err != nil {
		t.Fatalf("writeSSTStreaming error: %v", err)
	}

	h := sha256.Sum256(uploadedData)
	computedHash := hex.EncodeToString(h[:])

	if !strings.HasPrefix(result.Meta.Checksum, "sha256:") {
		t.Fatalf("expected sha256: prefix in checksum")
	}
	metaHash := strings.TrimPrefix(result.Meta.Checksum, "sha256:")

	if metaHash != computedHash {
		t.Errorf("hash mismatch: meta=%s, computed=%s", metaHash, computedHash)
	}
}

func TestWriteSSTStreaming_BlobReference(t *testing.T) {
	blobID := internal.ComputeBlobID([]byte("large-value-content"))

	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: false, BlobID: blobID},
	}
	it := &sliceSSTIter{entries: entries}

	var uploadedData []byte
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		uploadedData = data
		return nil
	}

	result, err := writeSSTStreaming(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1, 1, 1, uploadFn)
	if err != nil {
		t.Fatalf("writeSSTStreaming error: %v", err)
	}
	if len(uploadedData) == 0 {
		t.Fatalf("expected uploaded data")
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(uploadedData), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		t.Fatalf("iter error: %v", err)
	}
	defer iter.Close()

	kv := iter.First()
	if kv == nil {
		t.Fatalf("expected entry")
	}
	v, _, err := kv.V.Value(nil)
	if err != nil {
		t.Fatalf("value error: %v", err)
	}
	decoded, err := internal.DecodeKeyEntry(kv.K.UserKey, v)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if decoded.Inline {
		t.Fatalf("expected blob reference, got inline")
	}
	if decoded.BlobID != blobID {
		t.Fatalf("blob id mismatch")
	}

	if result.Meta.SeqLo != 1 || result.Meta.SeqHi != 1 {
		t.Errorf("seq range mismatch: got %d-%d", result.Meta.SeqLo, result.Meta.SeqHi)
	}
}

func TestBuildSSTIDWithTimestamp(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	id := buildSSTIDWithTimestamp(5, 10, 20, ts)
	if !strings.HasPrefix(id, "5-10-20-") {
		t.Errorf("expected prefix 5-10-20-, got %s", id)
	}
	if !strings.HasSuffix(id, ".sst") {
		t.Errorf("expected .sst suffix, got %s", id)
	}
	expectedNanos := ts.UnixNano()
	expectedID := fmt.Sprintf("5-10-20-%d.sst", expectedNanos)
	if id != expectedID {
		t.Errorf("ID mismatch: got %s, want %s", id, expectedID)
	}
}
