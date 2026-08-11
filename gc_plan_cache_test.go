package isledb

import "testing"

func TestBoundedPlanCacheEnforcesEntryAndByteLimits(t *testing.T) {
	cache := newBoundedPlanCache[string](2, 5)
	cache.put("a", "one", 2)
	cache.put("b", "two", 2)
	if value, ok := cache.get("a"); !ok || value != "one" {
		t.Fatalf("get a=(%q, %v), want (one, true)", value, ok)
	}

	// Reading a makes b the least-recently-used entry.
	cache.put("c", "three", 2)
	if _, ok := cache.get("b"); ok {
		t.Fatal("entry limit did not evict least-recently-used plan")
	}
	if len(cache.entries) != 2 || cache.encodedBytes != 4 {
		t.Fatalf("cache after entry eviction: entries=%d bytes=%d", len(cache.entries), cache.encodedBytes)
	}

	// Replacing a with a larger payload evicts c to stay within the byte cap.
	cache.put("a", "larger", 5)
	if _, ok := cache.get("c"); ok {
		t.Fatal("byte limit did not evict least-recently-used plan")
	}
	if value, ok := cache.get("a"); !ok || value != "larger" {
		t.Fatalf("updated a=(%q, %v), want (larger, true)", value, ok)
	}
	if len(cache.entries) != 1 || cache.encodedBytes != 5 {
		t.Fatalf("cache after byte eviction: entries=%d bytes=%d", len(cache.entries), cache.encodedBytes)
	}

	cache.put("oversized", "ignored", 6)
	if _, ok := cache.get("oversized"); ok {
		t.Fatal("cache retained an individually oversized plan")
	}
	cache.remove("a")
	if len(cache.entries) != 0 || cache.encodedBytes != 0 {
		t.Fatalf("cache after remove: entries=%d bytes=%d", len(cache.entries), cache.encodedBytes)
	}
}
