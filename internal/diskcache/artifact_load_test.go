package diskcache

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func TestArtifactCacheConcurrentMixedLoadMaintainsInvariants(t *testing.T) {
	const (
		entryBytes      = 16 << 10
		entryCount      = 48
		hotEntries      = 6
		residentEntries = 12
		workers         = 32
		operations      = 200
	)
	payloads, descriptors := benchmarkArtifactDataset(entryCount, entryBytes)
	cache := openTestArtifactCache(
		t, t.TempDir(), residentEntries*entryBytes, residentEntries*entryBytes)
	defer cache.Close()

	for index := range hotEntries {
		handle, admission, err := cache.AdmitBytes(descriptors[index], payloads[index])
		if err != nil || admission != ArtifactAdmitted {
			t.Fatalf("prime hot entry %d: admission=%d err=%v", index, admission, err)
		}
		if err := handle.Close(); err != nil {
			t.Fatal(err)
		}
	}

	start := make(chan struct{})
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	var sequence atomic.Uint64
	var maxResident atomic.Int64
	var maxPinned atomic.Int64
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			<-start
			for operation := range operations {
				sequenceNumber := sequence.Add(1) - 1
				index := int(sequenceNumber % hotEntries)
				if sequenceNumber%20 == 0 {
					index = hotEntries + int((sequenceNumber/20)%(entryCount-hotEntries))
				}

				handle, ok, err := cache.Acquire(descriptors[index])
				if err != nil {
					errors <- fmt.Errorf("acquire %d: %w", index, err)
					return
				}
				if !ok {
					handle, _, err = cache.AdmitBytes(descriptors[index], payloads[index])
					if err != nil {
						errors <- fmt.Errorf("admit %d: %w", index, err)
						return
					}
				}
				if err := validateBenchmarkArtifactData(handle.Bytes(), payloads[index]); err != nil {
					errors <- fmt.Errorf("operation %d: %w", operation, err)
					_ = handle.Close()
					return
				}
				if err := handle.Close(); err != nil {
					errors <- fmt.Errorf("close %d: %w", index, err)
					return
				}

				if operation%8 == 0 {
					stats := cache.Stats(ArtifactSST)
					updateAtomicMaximum(&maxResident, stats.ResidentBytes)
					updateAtomicMaximum(&maxPinned, stats.PinnedBytes)
					if stats.ResidentBytes > stats.MaxBytes {
						errors <- fmt.Errorf(
							"resident bytes=%d exceed max=%d", stats.ResidentBytes, stats.MaxBytes)
						return
					}
				}
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}

	stats := cache.Stats(ArtifactSST)
	if stats.ResidentBytes > stats.MaxBytes || maxResident.Load() > stats.MaxBytes {
		t.Fatalf("capacity exceeded: stats=%+v sampled_max=%d", stats, maxResident.Load())
	}
	if stats.PinnedBytes != 0 {
		t.Fatalf("pinned bytes after load=%d sampled_max=%d", stats.PinnedBytes, maxPinned.Load())
	}
	if stats.Hits == 0 || stats.Misses == 0 || stats.CapacityEvictions == 0 {
		t.Fatalf("load did not exercise hit/miss/eviction paths: %+v", stats)
	}

	cache.mu.Lock()
	activeOps := cache.activeOps
	activeFills := cache.activeFills
	activeHandles := cache.activeHandles
	openEntries := cache.openEntries
	maxOpenEntries := cache.maxOpenEntries
	cache.mu.Unlock()
	if activeOps != 0 || activeFills != 0 || activeHandles != 0 {
		t.Fatalf(
			"activity leaked: operations=%d fills=%d handles=%d",
			activeOps, activeFills, activeHandles)
	}
	if openEntries > maxOpenEntries {
		t.Fatalf("open mappings=%d exceed max=%d", openEntries, maxOpenEntries)
	}
	incoming, err := os.ReadDir(cache.incomingDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(incoming) != 0 {
		t.Fatalf("incoming files leaked after load: %v", incoming)
	}
}

func updateAtomicMaximum(maximum *atomic.Int64, value int64) {
	for current := maximum.Load(); value > current; current = maximum.Load() {
		if maximum.CompareAndSwap(current, value) {
			return
		}
	}
}
