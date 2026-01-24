package isledb

import (
	"testing"

	"github.com/segmentio/ksuid"
)

func TestLRUVLogCache_Eviction(t *testing.T) {
	cache := NewLRUVLogCache(2)
	id1 := ksuid.New()
	id2 := ksuid.New()
	id3 := ksuid.New()

	cache.Set(id1, []byte("one"))
	cache.Set(id2, []byte("two"))

	if _, ok := cache.Get(id1); !ok {
		t.Fatalf("expected id1 to be present")
	}

	cache.Set(id1, []byte("one-updated"))
	cache.Set(id3, []byte("three"))

	if _, ok := cache.Get(id2); ok {
		t.Fatalf("expected id2 to be evicted")
	}
	if v, ok := cache.Get(id1); !ok || string(v) != "one-updated" {
		t.Fatalf("expected id1 to be updated")
	}
	if v, ok := cache.Get(id3); !ok || string(v) != "three" {
		t.Fatalf("expected id3 to be present")
	}
}

func TestLRUVLogCache_Clear(t *testing.T) {
	cache := NewLRUVLogCache(2)
	id1 := ksuid.New()

	cache.Set(id1, []byte("one"))
	cache.Clear()

	if _, ok := cache.Get(id1); ok {
		t.Fatalf("expected cache to be cleared")
	}
}
