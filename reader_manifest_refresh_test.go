package isledb

import (
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestPublishManifestViewDoesNotConsultBloomCache(t *testing.T) {
	oldManifest := &manifestState{L0SSTs: []manifest.SSTMeta{{ID: "retired-sst"}}}
	newManifest := &manifestState{}
	reader := &Reader{
		manifest:   oldManifest,
		bloomCache: newBloomFilterCache(1 << 20),
		viewPolicy: ReaderViewPolicy{RefreshAfter: time.Hour},
	}
	defer reader.stopViewTimer()

	// Manifest publication must not scan or invalidate the independently bounded
	// decoded-Bloom cache.
	reader.bloomCache.entries.mu.Lock()
	defer reader.bloomCache.entries.mu.Unlock()

	done := make(chan struct{})
	go func() {
		reader.publishManifestView(newManifest, &manifest.Current{
			MaxPinnedViewAge: time.Hour,
		}, time.Now())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("manifest publication consulted the locked Bloom cache")
	}
	reader.mu.RLock()
	published := reader.manifest == newManifest
	reader.mu.RUnlock()
	if !published {
		t.Fatal("new manifest was not published")
	}
}

func TestPublishManifestViewRetainsRetiredDecodedBloom(t *testing.T) {
	const retiredID = "retired-sst"
	filter := bloomFilterForCacheTest(t, []byte("key"))
	cache := newBloomFilterCache(bloomFilterCacheCost(retiredID, filter))
	cache.put(retiredID, filter)
	reader := &Reader{
		manifest:   &manifestState{L0SSTs: []manifest.SSTMeta{{ID: retiredID}}},
		bloomCache: cache,
		viewPolicy: ReaderViewPolicy{RefreshAfter: time.Hour},
	}
	defer reader.stopViewTimer()

	reader.publishManifestView(&manifestState{}, &manifest.Current{
		MaxPinnedViewAge: time.Hour,
	}, time.Now())
	if _, ok := cache.peek(retiredID); !ok {
		t.Fatal("retired decoded Bloom was removed instead of remaining LRU-managed")
	}
}
