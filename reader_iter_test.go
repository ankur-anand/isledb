package isledb

import (
	"context"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestReader_ScanLimit(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.MemtableSize = 1024 * 1024
	opts.FlushInterval = 0

	w, err := newWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%03d", i)
		value := fmt.Sprintf("value:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	r, err := newReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	results, err := r.ScanLimit(ctx, []byte("key:"), []byte("key:~"), 10)
	if err != nil {
		t.Fatalf("ScanLimit failed: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("Expected 10 results, got %d", len(results))
	}
	if string(results[0].Key) != "key:000" {
		t.Errorf("First key should be key:000, got %s", results[0].Key)
	}
	if string(results[9].Key) != "key:009" {
		t.Errorf("Last key should be key:009, got %s", results[9].Key)
	}

	results, err = r.ScanLimit(ctx, []byte("key:"), []byte("key:~"), 0)
	if err != nil {
		t.Fatalf("ScanLimit unlimited failed: %v", err)
	}
	if len(results) != 100 {
		t.Errorf("Expected 100 results (unlimited), got %d", len(results))
	}

	results, err = r.ScanLimit(ctx, []byte("key:050"), []byte("key:059"), 100)
	if err != nil {
		t.Fatalf("ScanLimit failed: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("Expected 10 results, got %d", len(results))
	}

	results, err = r.ScanLimit(ctx, []byte("key:050"), []byte("key:~"), 5)
	if err != nil {
		t.Fatalf("ScanLimit failed: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}
	if string(results[0].Key) != "key:050" {
		t.Errorf("First key should be key:050, got %s", results[0].Key)
	}
}

func TestReader_Iterator(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.MemtableSize = 1024 * 1024
	opts.FlushInterval = 0

	w, err := newWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key:%03d", i)
		value := fmt.Sprintf("value:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	r, err := newReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	iter, err := r.NewIterator(ctx, IteratorOptions{
		MinKey: []byte("key:"),
		MaxKey: []byte("key:~"),
	})
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
		if !iter.Valid() {
			t.Error("Expected Valid() to be true")
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}
	if count != 50 {
		t.Errorf("Expected 50 entries, got %d", count)
	}

	iter2, err := r.NewIterator(ctx, IteratorOptions{
		MinKey: []byte("key:"),
		MaxKey: []byte("key:~"),
	})
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	defer iter2.Close()

	count = 0
	for iter2.Next() {
		count++
		if count >= 10 {
			break
		}
	}
	if count != 10 {
		t.Errorf("Expected 10 entries, got %d", count)
	}
}

func TestReader_Iterator_Empty(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.FlushInterval = 0

	w, err := newWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	if err := w.put([]byte("other:key"), []byte("value")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	r, err := newReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	iter, err := r.NewIterator(ctx, IteratorOptions{
		MinKey: []byte("nonexistent:"),
		MaxKey: []byte("nonexistent:~"),
	})
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	defer iter.Close()

	if iter.Next() {
		t.Error("Expected no results for empty range")
	}
	if iter.Err() != nil {
		t.Errorf("Unexpected error: %v", iter.Err())
	}
}

func TestReader_Iterator_SeekGE(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.MemtableSize = 1024 * 1024
	opts.FlushInterval = 0

	w, err := newWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("key:%03d", i*10)
		value := fmt.Sprintf("value:%03d", i*10)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	r, err := newReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	iter, err := r.NewIterator(ctx, IteratorOptions{
		MinKey: []byte("key:"),
		MaxKey: []byte("key:~"),
	})
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	defer iter.Close()

	if !iter.SeekGE([]byte("key:050")) {
		t.Fatal("SeekGE should find key:050")
	}
	if string(iter.Key()) != "key:050" {
		t.Errorf("Expected key:050, got %s", iter.Key())
	}

	iter2, err := r.NewIterator(ctx, IteratorOptions{
		MinKey: []byte("key:"),
		MaxKey: []byte("key:~"),
	})
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	defer iter2.Close()

	if !iter2.SeekGE([]byte("key:025")) {
		t.Fatal("SeekGE should find next key after key:025")
	}

	if string(iter2.Key()) != "key:030" {
		t.Errorf("Expected key:030, got %s", iter2.Key())
	}

	iter3, err := r.NewIterator(ctx, IteratorOptions{
		MinKey: []byte("key:"),
		MaxKey: []byte("key:~"),
	})
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	defer iter3.Close()

	if iter3.SeekGE([]byte("key:999")) {
		t.Error("SeekGE should return false for key beyond range")
	}
}

func TestReader_Iterator_WithDeletes(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.MemtableSize = 1024 * 1024
	opts.FlushInterval = 0

	w, err := newWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key:%03d", i)
		value := fmt.Sprintf("value:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	for i := 0; i < 10; i += 2 {
		key := fmt.Sprintf("key:%03d", i)
		if err := w.delete([]byte(key)); err != nil {
			t.Fatalf("delete failed: %v", err)
		}
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	r, err := newReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	iter, err := r.NewIterator(ctx, IteratorOptions{
		MinKey: []byte("key:"),
		MaxKey: []byte("key:~"),
	})
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}
	if count != 5 {
		t.Errorf("Expected 5 entries (odd keys only), got %d", count)
	}
}

func TestReader_ScanLimit_WithDeletes(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")

	opts := DefaultWriterOptions()
	opts.MemtableSize = 1024 * 1024
	opts.FlushInterval = 0

	w, err := newWriter(ctx, store, opts)
	if err != nil {
		t.Fatalf("newWriter failed: %v", err)
	}
	defer w.close()

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key:%03d", i)
		value := fmt.Sprintf("value:%03d", i)
		if err := w.put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key:%03d", i)
		if err := w.delete([]byte(key)); err != nil {
			t.Fatalf("delete failed: %v", err)
		}
	}

	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	r, err := newReader(ctx, store, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("newReader failed: %v", err)
	}
	defer r.Close()

	results, err := r.ScanLimit(ctx, []byte("key:"), []byte("key:~"), 5)
	if err != nil {
		t.Fatalf("ScanLimit failed: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	if string(results[0].Key) != "key:010" {
		t.Errorf("First key should be key:010, got %s", results[0].Key)
	}
}
