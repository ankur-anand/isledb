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

func testSSTStreamIdentity(epoch, seqLo, seqHi uint64) sstStreamIdentity {
	return newSSTStreamIdentity(epoch, seqLo, seqHi,
		time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
}

func testSSTStreamSetIdentity(epoch uint64) sstStreamSetIdentity {
	return sstStreamSetIdentity{
		OutputKey: "test-compaction",
		Epoch:     epoch,
		CreatedAt: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
	}
}

func TestWriteSSTStreaming_Basic(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 2, Kind: internal.OpPut, Value: []byte("x")},
		{Key: []byte("b"), Seq: 1, Kind: internal.OpPut, Value: []byte("y")},
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

	result, err := writeSSTStreaming(context.Background(), it, sstWriterOptions{
		BlockSize: 4096, Compression: "none", BloomBitsPerKey: 10,
	}, testSSTStreamIdentity(1, 1, 2), uploadFn)
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
	expectedSize := result.Meta.Size + result.Meta.Bloom.Length
	if result.Meta.Bloom.Length > 0 {
		expectedSize += bloomTrailerLen
	}
	if expectedSize != int64(len(uploadedData)) {
		t.Errorf("size mismatch: meta=%d bloom=%d actual=%d", result.Meta.Size, result.Meta.Bloom.Length, len(uploadedData))
	}

	payload := sstPayload(t, result.Meta, uploadedData)
	h := sha256.Sum256(payload)
	expectedChecksum := "sha256:" + hex.EncodeToString(h[:])
	if result.Meta.Checksum != expectedChecksum {
		t.Errorf("checksum mismatch: got %s, want %s", result.Meta.Checksum, expectedChecksum)
	}
	requireBloomChecksum(t, result.Meta, uploadedData)

	reader, err := sstable.NewReader(context.Background(), newMemReadable(payload), sstable.ReaderOptions{})
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

func TestWriteSSTStreaming_ReturnsWhenUploaderStopsReadingSuccessfully(t *testing.T) {
	done := make(chan error, 1)
	go func() {
		_, err := writeSSTStreaming(context.Background(),
			&sliceSSTIter{entries: []internal.MemEntry{{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: bytes.Repeat([]byte("x"), 8192)}}},
			sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamIdentity(1, 1, 1),
			func(context.Context, string, io.Reader) error { return nil })
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("uploader returned without consuming the SST, but the incomplete upload was accepted")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("writeSSTStreaming remained blocked after the uploader returned")
	}
}

func TestWriteMultipleSSTsStreaming_ReturnsWhenUploaderStopsReadingSuccessfully(t *testing.T) {
	done := make(chan error, 1)
	go func() {
		_, err := writeMultipleSSTsStreaming(context.Background(),
			&sliceSSTIter{entries: []internal.MemEntry{{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: bytes.Repeat([]byte("x"), 8192)}}},
			sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamSetIdentity(1), 1<<20,
			func(context.Context, string, io.Reader) error { return nil })
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("uploader returned without consuming the SST, but the incomplete upload was accepted")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("writeMultipleSSTsStreaming remained blocked after the uploader returned")
	}
}

func TestWriteSSTStreaming_UploadError(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("x")},
	}
	it := &sliceSSTIter{entries: entries}

	uploadErr := errors.New("upload failed")
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		buf := make([]byte, 100)
		r.Read(buf)
		return uploadErr
	}

	_, err := writeSSTStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamIdentity(1, 1, 1), uploadFn)
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
			{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("x")},
		},
		err: iterErr,
	}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		_, err := io.ReadAll(r)
		return err
	}

	_, err := writeSSTStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamIdentity(1, 1, 1), uploadFn)
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
			Key:   []byte{byte(i / 256), byte(i % 256)},
			Seq:   uint64(1000 - i),
			Kind:  internal.OpPut,
			Value: bytes.Repeat([]byte("x"), 100),
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

	_, err := writeSSTStreaming(ctx, it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamIdentity(1, 1, 1000), uploadFn)
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

	_, err := writeSSTStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamIdentity(1, 0, 0), uploadFn)
	if !errors.Is(err, errEmptyIterator) {
		t.Fatalf("expected ErrEmptyIterator, got %v", err)
	}
}

func TestWriteSSTStreaming_HashVerification(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("key1"), Seq: 3, Kind: internal.OpPut, Value: []byte("value1")},
		{Key: []byte("key2"), Seq: 2, Kind: internal.OpPut, Value: []byte("value2")},
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

	result, err := writeSSTStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamIdentity(1, 1, 3), uploadFn)
	if err != nil {
		t.Fatalf("writeSSTStreaming error: %v", err)
	}

	payload := sstPayload(t, result.Meta, uploadedData)
	h := sha256.Sum256(payload)
	computedHash := hex.EncodeToString(h[:])

	if !strings.HasPrefix(result.Meta.Checksum, "sha256:") {
		t.Fatalf("expected sha256: prefix in checksum")
	}
	metaHash := strings.TrimPrefix(result.Meta.Checksum, "sha256:")

	if metaHash != computedHash {
		t.Errorf("hash mismatch: meta=%s, computed=%s", metaHash, computedHash)
	}
}

func TestWriteSSTStreaming_LargeValue(t *testing.T) {
	value := bytes.Repeat([]byte("large-value-content"), 16<<10)

	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: value},
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

	result, err := writeSSTStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamIdentity(1, 1, 1), uploadFn)
	if err != nil {
		t.Fatalf("writeSSTStreaming error: %v", err)
	}
	if len(uploadedData) == 0 {
		t.Fatalf("expected uploaded data")
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(t, result.Meta, uploadedData)), sstable.ReaderOptions{})
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
	if !bytes.Equal(decoded.Value, value) {
		t.Fatalf("large value mismatch: got %d bytes, want %d", len(decoded.Value), len(value))
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

func TestWriteMultipleSSTsStreaming_Basic(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 3, Kind: internal.OpPut, Value: []byte("value-a")},
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Value: []byte("value-b")},
		{Key: []byte("c"), Seq: 1, Kind: internal.OpPut, Value: []byte("value-c")},
	}
	it := &sliceSSTIter{entries: entries}

	var uploadedSSTs []struct {
		id   string
		data []byte
	}
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		uploadedSSTs = append(uploadedSSTs, struct {
			id   string
			data []byte
		}{id: sstID, data: data})
		return nil
	}

	results, err := writeMultipleSSTsStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamSetIdentity(1), 1, uploadFn)
	if err != nil {
		t.Fatalf("writeMultipleSSTsStreaming error: %v", err)
	}

	if len(results) == 0 {
		t.Fatalf("expected at least one result")
	}
	if len(uploadedSSTs) != len(results) {
		t.Errorf("upload count mismatch: uploaded=%d, results=%d", len(uploadedSSTs), len(results))
	}

	for i, result := range results {
		if result.Meta.ID == "" {
			t.Errorf("result %d: empty ID", i)
		}
		if !strings.HasSuffix(result.Meta.ID, ".sst") {
			t.Errorf("result %d: expected .sst suffix, got %s", i, result.Meta.ID)
		}
		if result.Meta.Checksum == "" {
			t.Errorf("result %d: empty checksum", i)
		}
		if result.Meta.Size == 0 {
			t.Errorf("result %d: zero size", i)
		}
	}
}

func TestWriteMultipleSSTsStreaming_SingleSST(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 2, Kind: internal.OpPut, Value: []byte("x")},
		{Key: []byte("b"), Seq: 1, Kind: internal.OpPut, Value: []byte("y")},
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

	results, err := writeMultipleSSTsStreaming(context.Background(), it, sstWriterOptions{
		BlockSize: 4096, Compression: "none", BloomBitsPerKey: 10,
	}, testSSTStreamSetIdentity(1), 1<<20, uploadFn)
	if err != nil {
		t.Fatalf("writeMultipleSSTsStreaming error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	result := results[0]
	payload := sstPayload(t, result.Meta, uploadedData)
	h := sha256.Sum256(payload)
	expectedChecksum := "sha256:" + hex.EncodeToString(h[:])
	if result.Meta.Checksum != expectedChecksum {
		t.Errorf("checksum mismatch: got %s, want %s", result.Meta.Checksum, expectedChecksum)
	}
	requireBloomChecksum(t, result.Meta, uploadedData)

	reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(t, result.Meta, uploadedData)), sstable.ReaderOptions{})
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

func TestWriteMultipleSSTsStreaming_EmptyIterator(t *testing.T) {
	it := &sliceSSTIter{}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		io.ReadAll(r)
		return nil
	}

	_, err := writeMultipleSSTsStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamSetIdentity(1), 1<<20, uploadFn)
	if !errors.Is(err, errEmptyIterator) {
		t.Fatalf("expected ErrEmptyIterator, got %v", err)
	}
}

func TestWriteMultipleSSTsStreaming_UploadError(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("x")},
	}
	it := &sliceSSTIter{entries: entries}

	uploadErr := errors.New("upload failed")
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		buf := make([]byte, 100)
		r.Read(buf)
		return uploadErr
	}

	_, err := writeMultipleSSTsStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamSetIdentity(1), 1<<20, uploadFn)
	if err == nil {
		t.Fatalf("expected error")
	}

	if !strings.Contains(err.Error(), "upload failed") && !strings.Contains(err.Error(), "closed pipe") {
		t.Errorf("expected upload or pipe error, got %v", err)
	}
}

func TestWriteMultipleSSTsStreaming_RetryReusesObjectIdentity(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 2, Kind: internal.OpPut, Value: []byte("value-a")},
		{Key: []byte("b"), Seq: 1, Kind: internal.OpPut, Value: []byte("value-b")},
	}

	type upload struct {
		id   string
		data []byte
	}
	run := func(createdAt time.Time, uploadErr error) ([]upload, error) {
		var uploads []upload
		_, err := writeMultipleSSTsStreaming(
			context.Background(),
			&sliceSSTIter{entries: entries},
			sstWriterOptions{BlockSize: 4096, Compression: "none"},
			sstStreamSetIdentity{
				OutputKey: "same-compaction-plan",
				Epoch:     7,
				CreatedAt: createdAt,
			},
			1<<20,
			func(_ context.Context, sstID string, r io.Reader) error {
				data, err := io.ReadAll(r)
				if err != nil {
					return err
				}
				uploads = append(uploads, upload{id: sstID, data: data})
				return uploadErr
			},
		)
		return uploads, err
	}

	ambiguousErr := errors.New("upload response lost")
	firstCreatedAt := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	first, err := run(firstCreatedAt, ambiguousErr)
	if !errors.Is(err, ambiguousErr) {
		t.Fatalf("first write error=%v, want %v", err, ambiguousErr)
	}
	if len(first) != 1 {
		t.Fatalf("first upload count=%d, want 1", len(first))
	}

	// A retry of one immutable compaction plan must address the same object and
	// reproduce the same bytes even when the first response was ambiguous.
	second, err := run(firstCreatedAt.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("retry write: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("retry upload count=%d, want 1", len(second))
	}
	if first[0].id != second[0].id {
		t.Fatalf("retry changed SST ID: first=%q second=%q", first[0].id, second[0].id)
	}
	if !bytes.Equal(first[0].data, second[0].data) {
		t.Fatal("retry changed SST bytes")
	}
}

func TestWriteMultipleSSTsStreaming_ContextCancellation(t *testing.T) {
	entries := make([]internal.MemEntry, 100)
	for i := range entries {
		entries[i] = internal.MemEntry{
			Key:   []byte{byte(i)},
			Seq:   uint64(100 - i),
			Kind:  internal.OpPut,
			Value: bytes.Repeat([]byte("x"), 100),
		}
	}
	it := &sliceSSTIter{entries: entries}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		io.ReadAll(r)
		return nil
	}

	_, err := writeMultipleSSTsStreaming(ctx, it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamSetIdentity(1), 1<<20, uploadFn)
	if err == nil {
		t.Fatalf("expected error due to context cancellation")
	}
}

func TestWriteMultipleSSTsStreaming_HashVerification(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("key1"), Seq: 2, Kind: internal.OpPut, Value: []byte("value1")},
		{Key: []byte("key2"), Seq: 1, Kind: internal.OpPut, Value: []byte("value2")},
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

	results, err := writeMultipleSSTsStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamSetIdentity(1), 1<<20, uploadFn)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	result := results[0]
	payload := sstPayload(t, result.Meta, uploadedData)
	h := sha256.Sum256(payload)
	computedHash := hex.EncodeToString(h[:])
	metaHash := strings.TrimPrefix(result.Meta.Checksum, "sha256:")

	if metaHash != computedHash {
		t.Errorf("hash mismatch: meta=%s, computed=%s", metaHash, computedHash)
	}
}

func TestWriteMultipleSSTsStreaming_ProducerError(t *testing.T) {
	iterErr := errors.New("iterator failed")
	it := &sliceSSTIter{
		entries: []internal.MemEntry{
			{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("x")},
		},
		err: iterErr,
	}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		io.ReadAll(r)
		return nil
	}

	_, err := writeMultipleSSTsStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamSetIdentity(1), 1<<20, uploadFn)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "iterator failed") {
		t.Errorf("expected iterator error, got %v", err)
	}
}

func TestWriteMultipleSSTsStreaming_CancelsUploaderAfterProducerError(t *testing.T) {
	iterErr := errors.New("iterator failed")
	it := &sliceSSTIter{
		entries: []internal.MemEntry{
			{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("x")},
		},
		err: iterErr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := writeMultipleSSTsStreaming(ctx, it,
			sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamSetIdentity(1), 1<<20,
			func(uploadCtx context.Context, _ string, r io.Reader) error {
				_, _ = io.Copy(io.Discard, r)
				<-uploadCtx.Done()
				return uploadCtx.Err()
			})
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, iterErr) {
			t.Fatalf("writeMultipleSSTsStreaming error=%v, want %v", err, iterErr)
		}
	case <-time.After(500 * time.Millisecond):
		// Release the currently stuck uploader before failing the test.
		cancel()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("writeMultipleSSTsStreaming did not stop after parent cancellation")
		}
		t.Fatal("writeMultipleSSTsStreaming did not cancel the uploader after the producer failed")
	}
}

func TestWriteMultipleSSTsStreaming_LargeValue(t *testing.T) {
	value := bytes.Repeat([]byte("large-value-content"), 16<<10)
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: value},
	}
	it := &sliceSSTIter{entries: entries}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		_, err := io.ReadAll(r)
		return err
	}

	results, err := writeMultipleSSTsStreaming(context.Background(), it, sstWriterOptions{BlockSize: 4096, Compression: "none"}, testSSTStreamSetIdentity(1), 1<<20, uploadFn)
	if err != nil {
		t.Fatalf("writeMultipleSSTsStreaming error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}
