package cachestore

import (
	"testing"
)

func TestLRUManifestLogCache_BasicOps(t *testing.T) {
	cache := NewLRUManifestLogCache(10)

	data, ok := cache.Get("path1")
	if ok {
		t.Error("expected miss on empty manifestCache")
	}
	if data != nil {
		t.Error("expected nil data on miss")
	}

	cache.Set("path1", []byte("data1"))

	data, ok = cache.Get("path1")
	if !ok {
		t.Error("expected hit after set")
	}
	if string(data) != "data1" {
		t.Errorf("expected data1, got %s", string(data))
	}

	cache.Set("path1", []byte("updated"))
	data, ok = cache.Get("path1")
	if !ok {
		t.Error("expected hit after update")
	}
	if string(data) != "updated" {
		t.Errorf("expected updated, got %s", string(data))
	}

	cache.Remove("path1")
	_, ok = cache.Get("path1")
	if ok {
		t.Error("expected miss after remove")
	}
}

func TestLRUManifestLogCache_Eviction(t *testing.T) {
	cache := NewLRUManifestLogCache(3)

	cache.Set("path1", []byte("data1"))
	cache.Set("path2", []byte("data2"))
	cache.Set("path3", []byte("data3"))

	if _, ok := cache.Get("path1"); !ok {
		t.Error("path1 should still be in manifestCache")
	}
	if _, ok := cache.Get("path2"); !ok {
		t.Error("path2 should still be in manifestCache")
	}
	if _, ok := cache.Get("path3"); !ok {
		t.Error("path3 should still be in manifestCache")
	}

	cache.Set("path4", []byte("data4"))

	if _, ok := cache.Get("path1"); ok {
		t.Error("path1 should have been evicted")
	}

	if _, ok := cache.Get("path2"); !ok {
		t.Error("path2 should still be in manifestCache")
	}
	if _, ok := cache.Get("path3"); !ok {
		t.Error("path3 should still be in manifestCache")
	}
	if _, ok := cache.Get("path4"); !ok {
		t.Error("path4 should be in manifestCache")
	}
}

func TestLRUManifestLogCache_LRUOrdering(t *testing.T) {
	cache := NewLRUManifestLogCache(3)

	cache.Set("path1", []byte("data1"))
	cache.Set("path2", []byte("data2"))
	cache.Set("path3", []byte("data3"))

	cache.Get("path1")

	cache.Set("path4", []byte("data4"))

	if _, ok := cache.Get("path1"); !ok {
		t.Error("path1 should still be in manifestCache (was accessed)")
	}
	if _, ok := cache.Get("path2"); ok {
		t.Error("path2 should have been evicted (LRU)")
	}
	if _, ok := cache.Get("path3"); !ok {
		t.Error("path3 should still be in manifestCache")
	}
	if _, ok := cache.Get("path4"); !ok {
		t.Error("path4 should be in manifestCache")
	}
}

func TestLRUManifestLogCache_DataImmutability(t *testing.T) {
	cache := NewLRUManifestLogCache(10)

	original := []byte("original")
	cache.Set("path1", original)

	original[0] = 'X'

	data, ok := cache.Get("path1")
	if !ok {
		t.Error("expected hit")
	}
	if string(data) != "original" {
		t.Errorf("manifestCache data should not have changed, got %s", string(data))
	}

	data[0] = 'Y'

	data2, _ := cache.Get("path1")
	if string(data2) != "original" {
		t.Errorf("manifestCache data should not have changed from Get modification, got %s", string(data2))
	}
}

func TestLRUManifestLogCache_Stats(t *testing.T) {
	cache := NewLRUManifestLogCache(10)

	cache.Set("path1", []byte("data1"))
	cache.Set("path2", []byte("data2"))

	cache.Get("path1")
	cache.Get("path2")
	cache.Get("path3")

	stats := cache.Stats()

	if stats.Hits != 2 {
		t.Errorf("expected 2 hits, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
	if stats.EntryCount != 2 {
		t.Errorf("expected 2 entries, got %d", stats.EntryCount)
	}
	if stats.MaxEntries != 10 {
		t.Errorf("expected max entries 10, got %d", stats.MaxEntries)
	}
}

func TestLRUManifestLogCache_Clear(t *testing.T) {
	cache := NewLRUManifestLogCache(10)

	cache.Set("path1", []byte("data1"))
	cache.Set("path2", []byte("data2"))

	cache.Clear()

	if _, ok := cache.Get("path1"); ok {
		t.Error("manifestCache should be empty after clear")
	}

	stats := cache.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("expected 0 entries after clear, got %d", stats.EntryCount)
	}
}

func TestLRUManifestLogCache_DefaultSize(t *testing.T) {
	cache := NewLRUManifestLogCache(0)
	stats := cache.Stats()
	if stats.MaxEntries != DefaultManifestLogCacheSize {
		t.Errorf("expected default size %d, got %d", DefaultManifestLogCacheSize, stats.MaxEntries)
	}

	cache = NewLRUManifestLogCache(-1)
	stats = cache.Stats()
	if stats.MaxEntries != DefaultManifestLogCacheSize {
		t.Errorf("expected default size %d for negative input, got %d", DefaultManifestLogCacheSize, stats.MaxEntries)
	}
}

func TestNoopManifestLogCache(t *testing.T) {
	cache := NewNoopManifestLogCache()

	cache.Set("path1", []byte("data1"))

	if data, ok := cache.Get("path1"); ok || data != nil {
		t.Error("noop manifestCache should always miss")
	}

	cache.Remove("path1")
	cache.Clear()

	stats := cache.Stats()
	if stats.Hits != 0 || stats.Misses != 0 || stats.EntryCount != 0 {
		t.Error("noop manifestCache stats should be zero")
	}
}
