package internal

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func readEntries(t *testing.T, mt *Memtable) []MemEntry {
	t.Helper()

	it := mt.Iterator()
	defer it.Close()

	var entries []MemEntry
	for it.Next() {
		entries = append(entries, it.Entry())
	}
	if it.Err() != nil {
		t.Fatalf("unexpected error: %v", it.Err())
	}
	return entries
}

func singleEntry(t *testing.T, mt *Memtable) MemEntry {
	t.Helper()

	entries := readEntries(t, mt)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	return entries[0]
}

func TestMemtable_PutAndIterator(t *testing.T) {
	key := []byte("key1")
	value := []byte("test value")

	mt := NewMemtable(1<<20, 0)
	mt.Put(key, value, 1)

	entry := singleEntry(t, mt)
	if !bytes.Equal(entry.Key, key) {
		t.Errorf("key mismatch: got %s, want %s", entry.Key, key)
	}
	if entry.Seq != 1 {
		t.Errorf("seq mismatch: got %d, want 1", entry.Seq)
	}
	if entry.Kind != OpPut {
		t.Errorf("kind mismatch: got %v, want OpPut", entry.Kind)
	}
	if !entry.Inline {
		t.Errorf("expected inline")
	}
	if !bytes.Equal(entry.Value, value) {
		t.Errorf("value mismatch: got %s, want %s", entry.Value, value)
	}
}

func TestMemtable_PutBlobRef(t *testing.T) {
	key := []byte("blobkey")
	blobID := ComputeBlobID([]byte("large-blob-content"))

	mt := NewMemtable(1<<20, 0)
	mt.PutBlobRef(key, blobID, 1)

	entry := singleEntry(t, mt)
	if !bytes.Equal(entry.Key, key) {
		t.Errorf("key mismatch: got %s, want %s", entry.Key, key)
	}
	if entry.Seq != 1 {
		t.Errorf("seq mismatch: got %d, want 1", entry.Seq)
	}
	if entry.Kind != OpPut {
		t.Errorf("kind mismatch: got %v, want OpPut", entry.Kind)
	}
	if entry.Inline {
		t.Errorf("expected non-inline for blob ref")
	}
	if entry.BlobID != blobID {
		t.Errorf("blob id mismatch: got %x, want %x", entry.BlobID, blobID)
	}
}

func TestMemtable_Delete(t *testing.T) {
	mt := NewMemtable(1<<20, 0)
	key := []byte("deletekey")
	mt.Delete(key, 5)

	entry := singleEntry(t, mt)
	if !bytes.Equal(entry.Key, key) {
		t.Errorf("key mismatch: got %s, want %s", entry.Key, key)
	}
	if entry.Kind != OpDelete {
		t.Errorf("kind mismatch: got %v, want OpDelete", entry.Kind)
	}
	if entry.Seq != 5 {
		t.Errorf("seq mismatch: got %d, want 5", entry.Seq)
	}
}

func TestMemtable_MultipleEntries_SortedOrder(t *testing.T) {
	mt := NewMemtable(1<<20, 0)
	inputs := []struct {
		key   string
		value string
		seq   uint64
	}{
		{key: "charlie", value: "c", seq: 1},
		{key: "alpha", value: "a", seq: 2},
		{key: "bravo", value: "b", seq: 3},
	}

	for _, in := range inputs {
		mt.Put([]byte(in.key), []byte(in.value), in.seq)
	}

	entries := readEntries(t, mt)
	expectedKeys := []string{"alpha", "bravo", "charlie"}
	if len(entries) != len(expectedKeys) {
		t.Fatalf("got %d entries, want %d", len(entries), len(expectedKeys))
	}
	for i, entry := range entries {
		if string(entry.Key) != expectedKeys[i] {
			t.Errorf("entry %d: got key %s, want %s", i, entry.Key, expectedKeys[i])
		}
	}
}

func TestMemtable_SameKeyDifferentSeq(t *testing.T) {
	mt := NewMemtable(1<<20, 0)

	key := []byte("mykey")
	values := []struct {
		value string
		seq   uint64
	}{
		{value: "v1", seq: 1},
		{value: "v2", seq: 2},
		{value: "v3", seq: 3},
	}

	for _, v := range values {
		mt.Put(key, []byte(v.value), v.seq)
	}

	entries := readEntries(t, mt)
	if len(entries) != len(values) {
		t.Errorf("expected %d entries for same key with different seqs, got %d", len(values), len(entries))
	}
	for _, entry := range entries {
		if !bytes.Equal(entry.Key, key) {
			t.Errorf("unexpected key: %s", entry.Key)
		}
	}
}

func TestMemtable_ApproxSize(t *testing.T) {
	mt := NewMemtable(1<<20, 0)

	initialSize := mt.ApproxSize()
	puts := []struct {
		key   string
		value string
		seq   uint64
	}{
		{key: "key1", value: "value1", seq: 1},
		{key: "key2", value: "value2", seq: 2},
	}
	for _, p := range puts {
		mt.Put([]byte(p.key), []byte(p.value), p.seq)
	}

	if mt.ApproxSize() <= initialSize {
		t.Error("expected size to increase after puts")
	}
}

func TestMemtable_TotalSize(t *testing.T) {
	mt := NewMemtable(1<<20, 0)

	mt.Put([]byte("small"), []byte("tiny"), 1)
	smallTotal := mt.TotalSize()

	largeValue := make([]byte, 10000)
	mt.Put([]byte("large"), largeValue, 2)

	if mt.TotalSize() <= smallTotal {
		t.Error("expected total size to increase with large value")
	}
}

func TestMemtable_EmptyIterator(t *testing.T) {
	mt := NewMemtable(1<<20, 0)

	entries := readEntries(t, mt)
	if len(entries) != 0 {
		t.Errorf("expected no entries in empty memtable")
	}
}

func TestMemtable_ConcurrentPuts(t *testing.T) {
	mt := NewMemtable(1<<20, 0)

	var wg sync.WaitGroup
	numGoroutines := 10
	entriesPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("g%d-k%d", gid, i))
				value := []byte(fmt.Sprintf("v%d-%d", gid, i))
				mt.Put(key, value, uint64(gid*1000+i))
			}
		}(g)
	}

	wg.Wait()

	entries := readEntries(t, mt)
	expected := numGoroutines * entriesPerGoroutine
	if len(entries) != expected {
		t.Errorf("expected %d entries, got %d", expected, len(entries))
	}
}

func TestMemtable_ConcurrentBlobRefs(t *testing.T) {
	mt := NewMemtable(10<<20, 0)

	var wg sync.WaitGroup
	numGoroutines := 5
	entriesPerGoroutine := 10

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("g%d-k%d", gid, i))
				blobID := ComputeBlobID([]byte(fmt.Sprintf("blob-content-%d-%d", gid, i)))
				mt.PutBlobRef(key, blobID, uint64(gid*1000+i))
			}
		}(g)
	}

	wg.Wait()

	entries := readEntries(t, mt)
	expected := numGoroutines * entriesPerGoroutine
	if len(entries) != expected {
		t.Errorf("expected %d entries, got %d", expected, len(entries))
	}
}

func TestMemtable_Iterator_MultipleRewinds(t *testing.T) {
	mt := NewMemtable(1<<20, 0)

	mt.Put([]byte("a"), []byte("1"), 1)
	mt.Put([]byte("b"), []byte("2"), 2)

	it := mt.Iterator()
	defer it.Close()

	count := 0
	for it.Next() {
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 entries, got %d", count)
	}
	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}
}

func TestMemtable_EmptyKeyAndValue(t *testing.T) {
	mt := NewMemtable(1<<20, 0)

	mt.Put([]byte{}, []byte("value"), 1)
	mt.Put([]byte("key"), []byte{}, 2)

	entries := readEntries(t, mt)
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
}

func BenchmarkMemtable_Put_Small(b *testing.B) {
	mt := NewMemtable(64<<20, 0)
	key := []byte("benchmark-key")
	value := []byte("small value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Put(key, value, uint64(i))
	}
}

func BenchmarkMemtable_Put_Large(b *testing.B) {
	mt := NewMemtable(64<<20, 0)
	key := []byte("benchmark-key")
	value := make([]byte, 10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Put(key, value, uint64(i))
	}
}

func BenchmarkMemtable_Iterator(b *testing.B) {
	mt := NewMemtable(64<<20, 0)

	for i := 0; i < 10000; i++ {
		mt.Put([]byte(fmt.Sprintf("key%05d", i)), []byte("value"), uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := mt.Iterator()
		for it.Next() {
			_ = it.Entry()
		}
		it.Close()
	}
}

func TestMemtable_SeqBounds(t *testing.T) {
	mt := NewMemtable(1<<20, 0)

	if mt.SeqLo() != 0 {
		t.Errorf("expected SeqLo=0 for empty memtable, got %d", mt.SeqLo())
	}
	if mt.SeqHi() != 0 {
		t.Errorf("expected SeqHi=0 for empty memtable, got %d", mt.SeqHi())
	}

	mt.Put([]byte("a"), []byte("v1"), 5)
	if mt.SeqLo() != 5 || mt.SeqHi() != 5 {
		t.Errorf("after first put: expected 5-5, got %d-%d", mt.SeqLo(), mt.SeqHi())
	}

	mt.Put([]byte("b"), []byte("v2"), 10)
	if mt.SeqLo() != 5 || mt.SeqHi() != 10 {
		t.Errorf("after second put: expected 5-10, got %d-%d", mt.SeqLo(), mt.SeqHi())
	}

	mt.Put([]byte("c"), []byte("v3"), 3)
	if mt.SeqLo() != 3 || mt.SeqHi() != 10 {
		t.Errorf("after third put: expected 3-10, got %d-%d", mt.SeqLo(), mt.SeqHi())
	}

	mt.Delete([]byte("d"), 15)
	if mt.SeqLo() != 3 || mt.SeqHi() != 15 {
		t.Errorf("after delete: expected 3-15, got %d-%d", mt.SeqLo(), mt.SeqHi())
	}

	var blobID [32]byte
	mt.PutBlobRef([]byte("e"), blobID, 1)
	if mt.SeqLo() != 1 || mt.SeqHi() != 15 {
		t.Errorf("after blob ref: expected 1-15, got %d-%d", mt.SeqLo(), mt.SeqHi())
	}
}
