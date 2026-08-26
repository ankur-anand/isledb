package isledb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
)

func TestTTL_EntryEncodeDecode(t *testing.T) {

	expireAt := time.Now().Add(time.Hour).UnixMilli()

	entry := internal.KeyEntry{
		Key:      []byte("key1"),
		Seq:      1,
		Kind:     internal.OpPut,
		Value:    []byte("value1"),
		ExpireAt: expireAt,
	}

	encoded := internal.EncodeKeyEntry(entry)
	decoded, err := internal.DecodeKeyEntry(entry.Key, encoded)
	if err != nil {
		t.Fatalf("DecodeKeyEntry failed: %v", err)
	}

	if decoded.ExpireAt != expireAt {
		t.Errorf("ExpireAt mismatch: got %d, want %d", decoded.ExpireAt, expireAt)
	}
	if decoded.Kind != internal.OpPut {
		t.Errorf("kind mismatch: got %d, want %d", decoded.Kind, internal.OpPut)
	}
	if string(decoded.Value) != "value1" {
		t.Errorf("value mismatch: got %q, want %q", decoded.Value, "value1")
	}
}

func TestTTL_EntryEncodeDecodeNoTTL(t *testing.T) {

	entry := internal.KeyEntry{
		Key:      []byte("key1"),
		Seq:      1,
		Kind:     internal.OpPut,
		Value:    []byte("value1"),
		ExpireAt: 0,
	}

	encoded := internal.EncodeKeyEntry(entry)
	decoded, err := internal.DecodeKeyEntry(entry.Key, encoded)
	if err != nil {
		t.Fatalf("DecodeKeyEntry failed: %v", err)
	}

	if decoded.ExpireAt != 0 {
		t.Errorf("ExpireAt should be 0, got %d", decoded.ExpireAt)
	}
	if string(decoded.Value) != "value1" {
		t.Errorf("value mismatch: got %q, want %q", decoded.Value, "value1")
	}
}

func TestTTL_DeleteWithTTL(t *testing.T) {

	expireAt := time.Now().Add(time.Hour).UnixMilli()

	entry := internal.KeyEntry{
		Key:      []byte("key1"),
		Seq:      1,
		Kind:     internal.OpDelete,
		ExpireAt: expireAt,
	}

	encoded := internal.EncodeKeyEntry(entry)
	decoded, err := internal.DecodeKeyEntry(entry.Key, encoded)
	if err != nil {
		t.Fatalf("DecodeKeyEntry failed: %v", err)
	}

	if decoded.Kind != internal.OpDelete {
		t.Errorf("kind mismatch: got %d, want %d", decoded.Kind, internal.OpDelete)
	}
	if decoded.ExpireAt != expireAt {
		t.Errorf("ExpireAt mismatch: got %d, want %d", decoded.ExpireAt, expireAt)
	}
}

func TestTTL_LargeValueWithTTL(t *testing.T) {

	expireAt := time.Now().Add(time.Hour).UnixMilli()
	value := make([]byte, 256<<10)
	for i := range value {
		value[i] = byte(i)
	}

	entry := internal.KeyEntry{
		Key:      []byte("key1"),
		Seq:      1,
		Kind:     internal.OpPut,
		Value:    value,
		ExpireAt: expireAt,
	}

	encoded := internal.EncodeKeyEntry(entry)
	decoded, err := internal.DecodeKeyEntry(entry.Key, encoded)
	if err != nil {
		t.Fatalf("DecodeKeyEntry failed: %v", err)
	}

	if decoded.ExpireAt != expireAt {
		t.Errorf("ExpireAt mismatch: got %d, want %d", decoded.ExpireAt, expireAt)
	}
	if len(decoded.Value) != len(value) || decoded.Value[12345] != value[12345] {
		t.Error("large value mismatch")
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
			entry := internal.KeyEntry{ExpireAt: tt.expireAt}
			if got := entry.IsExpired(now); got != tt.want {
				t.Errorf("IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTTL_MemtablePutWithTTL(t *testing.T) {
	m := internal.NewMemtable(1024 * 1024)
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
		t.Errorf("value mismatch: got %q, want %q", entry.Value, "value1")
	}
}

func TestTTL_WriterPutWithTTL(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	opts := DefaultWriterOptions()
	opts.Memtable.TargetBytes = 1024 * 1024
	opts.Flush.Interval = 0

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close(ctx)

	err = w.putWithTTL(ctx, []byte("key1"), []byte("value1"), time.Hour)
	if err != nil {
		t.Fatalf("putWithTTL failed: %v", err)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	rOpts := defaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	r, err := newReader(ctx, store, rOpts)
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
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
		t.Errorf("value mismatch: got %q, want %q", val, "value1")
	}
}

func TestTTL_WriterRejectsNegativeTTL(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("ttl-negative")
	defer store.Close()

	w, err := newWriter(ctx, store, newManifestStore(store, nil), testWriterOptions(1<<20, 0))
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer w.close(ctx)

	if err := w.putWithTTL(ctx, []byte("negative"), []byte("value"), -time.Nanosecond); !errors.Is(err, ErrInvalidMutation) {
		t.Fatalf("negative TTL error=%v, want %v", err, ErrInvalidMutation)
	}
	if !w.memtable.Empty() {
		t.Fatal("negative-TTL put mutated the memtable")
	}
	if err := w.putWithTTL(ctx, []byte("permanent"), []byte("value"), 0); err != nil {
		t.Fatalf("zero TTL: %v", err)
	}
}

func TestTTL_ReaderFiltersExpired(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	opts := DefaultWriterOptions()
	opts.Memtable.TargetBytes = 1024 * 1024
	opts.Flush.Interval = 0

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close(ctx)

	w.mu.Lock()
	w.seq++
	seq := w.seq
	expireAt := time.Now().Add(-time.Millisecond).UnixMilli()
	w.memtable.PutWithTTL([]byte("expired_key"), []byte("expired_value"), seq, expireAt)
	w.mu.Unlock()

	err = w.putWithTTL(ctx, []byte("valid_key"), []byte("valid_value"), time.Hour)
	if err != nil {
		t.Fatalf("putWithTTL failed: %v", err)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	rOpts := defaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	r, err := newReader(ctx, store, rOpts)
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
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
		t.Errorf("value mismatch: got %q, want %q", val, "valid_value")
	}
}

func TestTTL_ScanFiltersExpired(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	opts := DefaultWriterOptions()
	opts.Memtable.TargetBytes = 1024 * 1024
	opts.Flush.Interval = 0

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close(ctx)

	w.mu.Lock()
	w.seq++
	expireAt := time.Now().Add(-time.Millisecond).UnixMilli()
	w.memtable.PutWithTTL([]byte("aaa"), []byte("expired1"), w.seq, expireAt)
	w.seq++
	w.memtable.PutWithTTL([]byte("ccc"), []byte("expired2"), w.seq, expireAt)
	w.mu.Unlock()

	err = w.putWithTTL(ctx, []byte("bbb"), []byte("valid1"), time.Hour)
	if err != nil {
		t.Fatalf("putWithTTL failed: %v", err)
	}
	err = w.putWithTTL(ctx, []byte("ddd"), []byte("valid2"), time.Hour)
	if err != nil {
		t.Fatalf("putWithTTL failed: %v", err)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	rOpts := defaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	r, err := newReader(ctx, store, rOpts)
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
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

func TestTTL_ExpiredEntryShadowsOlder(t *testing.T) {

	ctx := context.Background()
	store := blobstore.NewMemory("")

	manifestStore := newManifestStore(store, nil)

	opts := DefaultWriterOptions()
	opts.Memtable.TargetBytes = 1024 * 1024
	opts.Flush.Interval = 0

	w, err := newWriter(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close(ctx)

	if err := w.put(ctx, []byte("key1"), []byte("old_value")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	w.mu.Lock()
	w.seq++
	seq := w.seq
	expireAt := time.Now().Add(-time.Millisecond).UnixMilli()
	w.memtable.PutWithTTL([]byte("key1"), []byte("new_value"), seq, expireAt)
	w.mu.Unlock()

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	rOpts := defaultReaderOptions()
	rOpts.CacheDir = t.TempDir()
	r, err := newReader(ctx, store, rOpts)
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
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
