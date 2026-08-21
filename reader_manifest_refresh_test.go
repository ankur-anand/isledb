package isledb

import (
	"runtime"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestPublishManifestViewDoesNotHoldReaderLockDuringInvalidation(t *testing.T) {
	oldManifest := &manifestState{L0SSTs: []manifest.SSTMeta{{ID: "retired-sst"}}}
	newManifest := &manifestState{}
	reader := &Reader{
		manifest:   oldManifest,
		bloomCache: newBloomFilterCache(1 << 20),
		viewPolicy: ReaderViewPolicy{RefreshAfter: time.Hour},
	}
	defer reader.stopManifestExpiry()

	// Force invalidation to stop at the decoded-Bloom cache. Publication must
	// still release Reader.mu before it reaches this independently locked cache.
	reader.bloomCache.mu.Lock()
	bloomLocked := true
	defer func() {
		if bloomLocked {
			reader.bloomCache.mu.Unlock()
		}
	}()

	done := make(chan struct{})
	go func() {
		reader.publishManifestView(newManifest, &manifest.Current{
			MaxPinnedViewAge: time.Hour,
		}, time.Now())
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	published := false
	for time.Now().Before(deadline) {
		if reader.mu.TryRLock() {
			published = reader.manifest == newManifest
			reader.mu.RUnlock()
			if published {
				break
			}
		}
		runtime.Gosched()
	}
	if !published {
		t.Fatal("new manifest was not readable while Bloom invalidation was blocked")
	}
	select {
	case <-done:
		t.Fatal("manifest publication completed without reaching blocked invalidation")
	default:
	}

	reader.bloomCache.mu.Unlock()
	bloomLocked = false
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("manifest invalidation did not finish after Bloom cache was released")
	}
}
