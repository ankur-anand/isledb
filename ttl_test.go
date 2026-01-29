package isledb

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestTTL_EntryEncodeDecode(t *testing.T) {

	expireAt := time.Now().Add(time.Hour).UnixMilli()

	entry := KeyEntry{
		Key:      []byte("key1"),
		Seq:      1,
		Kind:     OpPut,
		Inline:   true,
		Value:    []byte("value1"),
		ExpireAt: expireAt,
	}

	encoded := EncodeKeyEntry(entry)
	decoded, err := DecodeKeyEntry(entry.Key, encoded)
	if err != nil {
		t.Fatalf("DecodeKeyEntry failed: %v", err)
	}

	if decoded.ExpireAt != expireAt {
		t.Errorf("ExpireAt mismatch: got %d, want %d", decoded.ExpireAt, expireAt)
	}
	if decoded.Kind != OpPut {
		t.Errorf("Kind mismatch: got %d, want %d", decoded.Kind, OpPut)
	}
	if !decoded.Inline {
		t.Error("Expected Inline to be true")
	}
	if string(decoded.Value) != "value1" {
		t.Errorf("Value mismatch: got %q, want %q", decoded.Value, "value1")
	}
}

func TestTTL_EntryEncodeDecodeNoTTL(t *testing.T) {

	entry := KeyEntry{
		Key:      []byte("key1"),
		Seq:      1,
		Kind:     OpPut,
		Inline:   true,
		Value:    []byte("value1"),
		ExpireAt: 0,
	}

	encoded := EncodeKeyEntry(entry)
	decoded, err := DecodeKeyEntry(entry.Key, encoded)
	if err != nil {
		t.Fatalf("DecodeKeyEntry failed: %v", err)
	}

	if decoded.ExpireAt != 0 {
		t.Errorf("ExpireAt should be 0, got %d", decoded.ExpireAt)
	}
	if string(decoded.Value) != "value1" {
		t.Errorf("Value mismatch: got %q, want %q", decoded.Value, "value1")
	}
}

func TestTTL_DeleteWithTTL(t *testing.T) {

	expireAt := time.Now().Add(time.Hour).UnixMilli()

	entry := KeyEntry{
		Key:      []byte("key1"),
		Seq:      1,
		Kind:     OpDelete,
		ExpireAt: expireAt,
	}

	encoded := EncodeKeyEntry(entry)
	decoded, err := DecodeKeyEntry(entry.Key, encoded)
	if err != nil {
		t.Fatalf("DecodeKeyEntry failed: %v", err)
	}

	if decoded.Kind != OpDelete {
		t.Errorf("Kind mismatch: got %d, want %d", decoded.Kind, OpDelete)
	}
	if decoded.ExpireAt != expireAt {
		t.Errorf("ExpireAt mismatch: got %d, want %d", decoded.ExpireAt, expireAt)
	}
}

func TestTTL_BlobWithTTL(t *testing.T) {

	expireAt := time.Now().Add(time.Hour).UnixMilli()
	var blobID [32]byte
	copy(blobID[:], []byte("0123456789abcdef0123456789abcdef"))

	entry := KeyEntry{
		Key:      []byte("key1"),
		Seq:      1,
		Kind:     OpPut,
		Inline:   false,
		BlobID:   blobID,
		ExpireAt: expireAt,
	}

	encoded := EncodeKeyEntry(entry)
	decoded, err := DecodeKeyEntry(entry.Key, encoded)
	if err != nil {
		t.Fatalf("DecodeKeyEntry failed: %v", err)
	}

	if decoded.ExpireAt != expireAt {
		t.Errorf("ExpireAt mismatch: got %d, want %d", decoded.ExpireAt, expireAt)
	}
	if decoded.BlobID != blobID {
		t.Errorf("BlobID mismatch")
	}
	if decoded.Inline {
		t.Error("Expected Inline to be false")
	}
}

func TestTTL_IsExpired(t *testing.T) {
	now := time.Now().UnixMilli()

	tests := []struct {
		name     string
		expireAt int64
		want     bool
	}{
		{"no expiration", 0, false},
		{"not expired", now + 10000, false},
		{"expired", now - 10000, true},
		{"exactly now", now, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := KeyEntry{ExpireAt: tt.expireAt}
			if got := entry.IsExpired(now); got != tt.want {
				t.Errorf("IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTTL_MemtablePutWithTTL(t *testing.T) {
	m := NewMemtable(1024*1024, 4096)
	expireAt := time.Now().Add(time.Hour).UnixMilli()

	m.PutWithTTL([]byte("key1"), []byte("value1"), 1, expireAt)

	it := m.Iterator()
	defer it.Close()

	if !it.Next() {
		t.Fatal("Expected at least one entry")
	}

	entry := it.Entry()
	if entry.ExpireAt != expireAt {
		t.Errorf("ExpireAt mismatch: got %d, want %d", entry.ExpireAt, expireAt)
	}
	if string(entry.Value) != "value1" {
		t.Errorf("Value mismatch: got %q, want %q", entry.Value, "value1")
	}
}

func TestTTL_WriterPutWithTTL(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.MemtableSize = 1024 * 1024
	opts.FlushInterval = 0

	w, err := NewWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer w.Close()

	err = w.PutWithTTL([]byte("key1"), []byte("value1"), time.Hour)
	if err != nil {
		t.Fatalf("PutWithTTL failed: %v", err)
	}

	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	r, err := NewReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	val, found, err := r.Get(ctx, []byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Error("Expected to find key1")
	}
	if string(val) != "value1" {
		t.Errorf("Value mismatch: got %q, want %q", val, "value1")
	}
}

func TestTTL_ReaderFiltersExpired(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.MemtableSize = 1024 * 1024
	opts.FlushInterval = 0

	w, err := NewWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer w.Close()

	w.mu.Lock()
	w.seq++
	seq := w.seq
	expireAt := time.Now().Add(-time.Millisecond).UnixMilli()
	w.memtable.PutWithTTL([]byte("expired_key"), []byte("expired_value"), seq, expireAt)
	w.mu.Unlock()

	err = w.PutWithTTL([]byte("valid_key"), []byte("valid_value"), time.Hour)
	if err != nil {
		t.Fatalf("PutWithTTL failed: %v", err)
	}

	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	r, err := NewReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	_, found, err := r.Get(ctx, []byte("expired_key"))
	if err != nil {
		t.Fatalf("Get expired_key failed: %v", err)
	}
	if found {
		t.Error("Expected expired_key to NOT be found (expired)")
	}

	val, found, err := r.Get(ctx, []byte("valid_key"))
	if err != nil {
		t.Fatalf("Get valid_key failed: %v", err)
	}
	if !found {
		t.Error("Expected valid_key to be found")
	}
	if string(val) != "valid_value" {
		t.Errorf("Value mismatch: got %q, want %q", val, "valid_value")
	}
}

func TestTTL_ScanFiltersExpired(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.MemtableSize = 1024 * 1024
	opts.FlushInterval = 0

	w, err := NewWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer w.Close()

	w.mu.Lock()
	w.seq++
	expireAt := time.Now().Add(-time.Millisecond).UnixMilli()
	w.memtable.PutWithTTL([]byte("aaa"), []byte("expired1"), w.seq, expireAt)
	w.seq++
	w.memtable.PutWithTTL([]byte("ccc"), []byte("expired2"), w.seq, expireAt)
	w.mu.Unlock()

	err = w.PutWithTTL([]byte("bbb"), []byte("valid1"), time.Hour)
	if err != nil {
		t.Fatalf("PutWithTTL failed: %v", err)
	}
	err = w.PutWithTTL([]byte("ddd"), []byte("valid2"), time.Hour)
	if err != nil {
		t.Fatalf("PutWithTTL failed: %v", err)
	}

	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	r, err := NewReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	results, err := r.Scan(ctx, []byte("aaa"), []byte("zzz"))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	expectedKeys := map[string]bool{"bbb": true, "ddd": true}
	for _, kv := range results {
		if !expectedKeys[string(kv.Key)] {
			t.Errorf("Unexpected key in scan: %s", kv.Key)
		}
	}
}

func TestTTL_ExpiredEntryDoesNotShadowOlder(t *testing.T) {

	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.MemtableSize = 1024 * 1024
	opts.FlushInterval = 0

	w, err := NewWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer w.Close()

	if err := w.Put([]byte("key1"), []byte("old_value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	w.mu.Lock()
	w.seq++
	seq := w.seq
	expireAt := time.Now().Add(-time.Millisecond).UnixMilli()
	w.memtable.PutWithTTL([]byte("key1"), []byte("new_value"), seq, expireAt)
	w.mu.Unlock()

	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	r, err := NewReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer r.Close()

	val, found, err := r.Get(ctx, []byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if found {
		t.Errorf("Expected key1 to NOT be found (expired TTL should shadow old value), but got: %s", val)
	}
}
