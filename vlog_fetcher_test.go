package isledb

import (
	"bytes"
	"context"
	"hash/crc32"
	"sync"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/segmentio/ksuid"
)

type stubVLogCache struct {
	mu     sync.Mutex
	data   map[ksuid.KSUID][]byte
	gets   int
	sets   int
	clears int
}

func (c *stubVLogCache) Get(id ksuid.KSUID) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	data, ok := c.data[id]
	return data, ok
}

func (c *stubVLogCache) Set(id ksuid.KSUID, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sets++
	if c.data == nil {
		c.data = make(map[ksuid.KSUID][]byte)
	}
	c.data[id] = data
}

func (c *stubVLogCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clears++
	c.data = make(map[ksuid.KSUID][]byte)
}

func TestVLogFetcher_CachesValue(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	writer := NewVLogWriter()
	value := []byte("cached-value")
	ptr, err := writer.Append(value)
	if err != nil {
		t.Fatalf("append error: %v", err)
	}
	data := writer.Bytes()

	if _, err := store.Write(ctx, store.VLogPath(writer.ID().String()), data); err != nil {
		t.Fatalf("write error: %v", err)
	}

	fetcher := NewVLogFetcherWithCache(store, NewLRUVLogCache(1))
	got, err := fetcher.GetValue(ctx, ptr)
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch")
	}

	if err := store.Delete(ctx, store.VLogPath(writer.ID().String())); err != nil {
		t.Fatalf("delete error: %v", err)
	}

	got, err = fetcher.GetValue(ctx, ptr)
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch")
	}
}

func TestVLogFetcher_UsesInjectedCache(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("test")
	defer store.Close()

	value := []byte("preloaded")
	encoded := EncodeVLogEntry(value)
	id := ksuid.New()
	cache := &stubVLogCache{
		data: map[ksuid.KSUID][]byte{id: encoded},
	}
	ptr := VLogPointer{
		VLogID:   id,
		Offset:   0,
		Length:   uint32(len(value)),
		Checksum: crc32.Checksum(value, crcTable),
	}

	fetcher := NewVLogFetcherWithCache(store, cache)
	got, err := fetcher.GetValue(ctx, ptr)
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch")
	}

	cache.mu.Lock()
	gets := cache.gets
	sets := cache.sets
	cache.mu.Unlock()

	if gets != 1 {
		t.Fatalf("expected 1 cache get, got %d", gets)
	}
	if sets != 0 {
		t.Fatalf("expected 0 cache sets, got %d", sets)
	}
}

func TestVLogFetcher_ClearCache(t *testing.T) {
	store := blobstore.NewMemory("test")
	defer store.Close()

	cache := &stubVLogCache{data: make(map[ksuid.KSUID][]byte)}
	fetcher := NewVLogFetcherWithCache(store, cache)
	fetcher.ClearCache()

	cache.mu.Lock()
	clears := cache.clears
	cache.mu.Unlock()

	if clears != 1 {
		t.Fatalf("expected 1 clear, got %d", clears)
	}
}
