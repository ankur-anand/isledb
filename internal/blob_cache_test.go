package internal

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestLRUBlobCache_BasicOperations(t *testing.T) {
	cache := NewLRUBlobCache(1024*1024, 256*1024)

	blobID := hex.EncodeToString(sha256.New().Sum([]byte("test"))[:])
	data := []byte("hello world")

	got, ok := cache.Get(blobID)
	if ok {
		t.Error("Expected cache miss on empty cache")
	}
	if got != nil {
		t.Error("Expected nil data on cache miss")
	}

	cache.Set(blobID, data)

	got, ok = cache.Get(blobID)
	if !ok {
		t.Error("Expected cache hit after set")
	}
	if string(got) != string(data) {
		t.Errorf("Data mismatch: got %q, want %q", got, data)
	}

	stats := cache.Stats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
	if stats.EntryCount != 1 {
		t.Errorf("Expected 1 entry, got %d", stats.EntryCount)
	}
}

func TestLRUBlobCache_Eviction(t *testing.T) {

	cache := NewLRUBlobCache(200, 1024*1024)

	data1 := make([]byte, 100)
	data2 := make([]byte, 100)
	data3 := make([]byte, 100)

	id1 := "blob1"
	id2 := "blob2"
	id3 := "blob3"

	cache.Set(id1, data1)
	cache.Set(id2, data2)

	_, ok1 := cache.Get(id1)
	_, ok2 := cache.Get(id2)
	if !ok1 || !ok2 {
		t.Error("Expected both items to be in cache")
	}

	cache.Set(id3, data3)

	_, ok1 = cache.Get(id1)
	_, ok2 = cache.Get(id2)
	_, ok3 := cache.Get(id3)

	if ok1 {
		t.Error("Expected id1 to be evicted")
	}
	if !ok2 {
		t.Error("Expected id2 to still be in cache")
	}
	if !ok3 {
		t.Error("Expected id3 to be in cache")
	}
}

func TestLRUBlobCache_LRUOrder(t *testing.T) {

	cache := NewLRUBlobCache(200, 1024*1024)

	data := make([]byte, 100)
	id1 := "blob1"
	id2 := "blob2"
	id3 := "blob3"

	cache.Set(id1, data)
	cache.Set(id2, data)

	cache.Get(id1)

	cache.Set(id3, data)

	_, ok1 := cache.Get(id1)
	_, ok2 := cache.Get(id2)
	_, ok3 := cache.Get(id3)

	if !ok1 {
		t.Error("Expected id1 to be in cache (was recently accessed)")
	}
	if ok2 {
		t.Error("Expected id2 to be evicted (LRU)")
	}
	if !ok3 {
		t.Error("Expected id3 to be in cache")
	}
}

func TestLRUBlobCache_MaxItemSize(t *testing.T) {

	cache := NewLRUBlobCache(1024*1024, 100)

	smallData := make([]byte, 50)
	largeData := make([]byte, 200)

	cache.Set("small", smallData)
	cache.Set("large", largeData)

	_, ok := cache.Get("small")
	if !ok {
		t.Error("Expected small item to be cached")
	}

	_, ok = cache.Get("large")
	if ok {
		t.Error("Expected large item NOT to be cached (exceeds max item size)")
	}
}

func TestLRUBlobCache_Remove(t *testing.T) {
	cache := NewLRUBlobCache(1024*1024, 256*1024)

	data := []byte("test data")
	id := "testblob"

	cache.Set(id, data)

	_, ok := cache.Get(id)
	if !ok {
		t.Fatal("Expected item to be in cache")
	}

	cache.Remove(id)

	_, ok = cache.Get(id)
	if ok {
		t.Error("Expected item to be removed from cache")
	}

	stats := cache.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("Expected 0 entries, got %d", stats.EntryCount)
	}
}

func TestLRUBlobCache_Clear(t *testing.T) {
	cache := NewLRUBlobCache(1024*1024, 256*1024)

	for i := 0; i < 10; i++ {
		data := make([]byte, 100)
		rand.Read(data)
		id := hex.EncodeToString(data[:8])
		cache.Set(id, data)
	}

	stats := cache.Stats()
	if stats.EntryCount != 10 {
		t.Fatalf("Expected 10 entries, got %d", stats.EntryCount)
	}

	cache.Clear()

	stats = cache.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("Expected 0 entries after clear, got %d", stats.EntryCount)
	}
	if stats.TotalBytes != 0 {
		t.Errorf("Expected 0 bytes after clear, got %d", stats.TotalBytes)
	}
}

func TestLRUBlobCache_Update(t *testing.T) {
	cache := NewLRUBlobCache(1024*1024, 256*1024)

	id := "updatetest"
	data1 := []byte("original value")
	data2 := []byte("updated value which is longer")

	cache.Set(id, data1)

	got, _ := cache.Get(id)
	if string(got) != string(data1) {
		t.Error("Expected original value")
	}

	cache.Set(id, data2)

	got, _ = cache.Get(id)
	if string(got) != string(data2) {
		t.Error("Expected updated value")
	}

	stats := cache.Stats()
	if stats.EntryCount != 1 {
		t.Errorf("Expected 1 entry after update, got %d", stats.EntryCount)
	}
}

func TestLRUBlobCache_ImmutableData(t *testing.T) {
	cache := NewLRUBlobCache(1024*1024, 256*1024)

	id := "immutable"
	data := []byte("original")

	cache.Set(id, data)

	data[0] = 'X'

	got, _ := cache.Get(id)
	if string(got) != "original" {
		t.Error("ManifestCache data was mutated when original was changed")
	}

	got[0] = 'Y'

	got2, _ := cache.Get(id)
	if string(got2) != "original" {
		t.Error("ManifestCache data was mutated when retrieved copy was changed")
	}
}

func TestNoopBlobCache(t *testing.T) {
	cache := NewNoopBlobCache()

	data := []byte("test")
	id := "test"

	cache.Set(id, data)

	got, ok := cache.Get(id)
	if ok {
		t.Error("NoopCache should always miss")
	}
	if got != nil {
		t.Error("NoopCache should return nil")
	}

	cache.Remove(id)
	cache.Clear()

	stats := cache.Stats()
	if stats.EntryCount != 0 || stats.TotalBytes != 0 {
		t.Error("NoopCache stats should be zero")
	}
}

func TestLRUBlobCache_ConcurrentAccess(t *testing.T) {
	cache := NewLRUBlobCache(1024*1024, 256*1024)

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()

			idStr := hex.EncodeToString([]byte{byte(id)})
			data := make([]byte, 100)
			rand.Read(data)

			for j := 0; j < 100; j++ {
				cache.Set(idStr, data)
				cache.Get(idStr)
				cache.Remove(idStr)
				cache.Set(idStr, data)
			}
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	stats := cache.Stats()
	if stats.EntryCount < 0 || stats.TotalBytes < 0 {
		t.Error("Stats should be non-negative")
	}
}
