package diskcache

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkArtifactCacheAcquireWarm(b *testing.B) {
	for _, benchmarkCase := range []struct {
		name string
		kind ArtifactKind
		size int
	}{
		{name: "bloom-64KiB", kind: ArtifactBloom, size: 64 << 10},
		{name: "sst-4MiB", kind: ArtifactSST, size: 4 << 20},
	} {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			data := benchmarkArtifactData(benchmarkCase.size, 1)
			desc := contentFillDescriptor(benchmarkCase.kind, "warm", data)
			cache := openBenchmarkArtifactCache(
				b, b.TempDir(), int64(benchmarkCase.size*2), int64(benchmarkCase.size*2))
			defer cache.Close()

			handle, admission, err := cache.AdmitBytes(desc, data)
			if err != nil || admission != ArtifactAdmitted {
				b.Fatalf("prime cache: admission=%d err=%v", admission, err)
			}
			if err := handle.Close(); err != nil {
				b.Fatalf("close priming handle: %v", err)
			}

			var firstErr error
			var errOnce sync.Once
			b.ReportAllocs()
			b.ReportMetric(float64(benchmarkCase.size), "artifact_B")
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var observed byte
				for pb.Next() {
					handle, ok, err := cache.Acquire(desc)
					if err != nil || !ok {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("warm acquire ok=%t: %w", ok, err)
						})
						return
					}
					observed ^= handle.Bytes()[0]
					if err := handle.Close(); err != nil {
						errOnce.Do(func() { firstErr = fmt.Errorf("close warm handle: %w", err) })
						return
					}
				}
				benchmarkArtifactByteSink.Store(uint32(observed))
			})
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			stats := cache.Stats(benchmarkCase.kind)
			b.ReportMetric(float64(stats.Hits)/float64(max(1, b.N)), "hits/op")
			b.ReportMetric(float64(stats.ResidentBytes), "resident_B")
		})
	}
}

func BenchmarkArtifactCacheAcquireRecovered(b *testing.B) {
	for _, benchmarkCase := range []struct {
		name string
		kind ArtifactKind
		size int
	}{
		{name: "bloom-64KiB", kind: ArtifactBloom, size: 64 << 10},
		{name: "sst-4MiB", kind: ArtifactSST, size: 4 << 20},
	} {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			data := benchmarkArtifactData(benchmarkCase.size, 2)
			desc := contentFillDescriptor(benchmarkCase.kind, "recovered", data)
			dir := b.TempDir()
			cache := openBenchmarkArtifactCache(
				b, dir, int64(benchmarkCase.size*2), int64(benchmarkCase.size*2))
			handle, _, err := cache.AdmitBytes(desc, data)
			if err != nil {
				b.Fatalf("prime recovered artifact: %v", err)
			}
			if err := handle.Close(); err != nil {
				b.Fatal(err)
			}
			if err := cache.Close(); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(benchmarkCase.size))
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				cache = openBenchmarkArtifactCache(
					b, dir, int64(benchmarkCase.size*2), int64(benchmarkCase.size*2))
				b.StartTimer()
				handle, ok, err := cache.Acquire(desc)
				if err != nil || !ok {
					b.Fatalf("acquire recovered artifact ok=%t err=%v", ok, err)
				}
				benchmarkArtifactByteSink.Store(uint32(handle.Bytes()[0]))
				if err := handle.Close(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := cache.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkArtifactCacheAdmission(b *testing.B) {
	for _, benchmarkCase := range []struct {
		name string
		kind ArtifactKind
		size int
	}{
		{name: "bloom-64KiB", kind: ArtifactBloom, size: 64 << 10},
		{name: "sst-1MiB", kind: ArtifactSST, size: 1 << 20},
	} {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			data := benchmarkArtifactData(benchmarkCase.size, 3)
			desc := contentFillDescriptor(benchmarkCase.kind, "admission", data)
			cache := openBenchmarkArtifactCache(
				b, b.TempDir(), int64(benchmarkCase.size*2), int64(benchmarkCase.size*2))
			defer cache.Close()

			b.ReportAllocs()
			b.SetBytes(int64(benchmarkCase.size))
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				if err := cache.Remove(desc, ArtifactRemovalPurge); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				handle, admission, err := cache.AdmitBytes(desc, data)
				if err != nil || admission != ArtifactAdmitted {
					b.Fatalf("admit artifact: admission=%d err=%v", admission, err)
				}
				benchmarkArtifactByteSink.Store(uint32(handle.Bytes()[0]))
				if err := handle.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkArtifactCacheOpenRecovery(b *testing.B) {
	for _, entryCount := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", entryCount), func(b *testing.B) {
			dir := b.TempDir()
			seedRecoveredArtifactDirectory(b, dir, entryCount)

			b.ReportAllocs()
			b.ReportMetric(float64(entryCount), "entries/op")
			b.ResetTimer()
			for range b.N {
				cache, err := OpenArtifactCache(ArtifactCacheOptions{
					Dir: dir, SSTMaxBytes: int64(entryCount * 2), BloomMaxBytes: 1,
				})
				if err != nil {
					b.Fatal(err)
				}
				if got := cache.Stats(ArtifactSST).ResidentEntries; got != entryCount {
					b.Fatalf("recovered entries=%d want=%d", got, entryCount)
				}
				b.StopTimer()
				if err := cache.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

// BenchmarkArtifactCacheLoad exercises the persistent cache under a 95%-hot
// workload. The coalesced case models Reader's same-SST load coalescing. The
// uncoalesced-stress case exposes duplicate fill and capacity pressure inside
// the cache itself. Misses include durable checksum/fsync work.
func BenchmarkArtifactCacheLoad(b *testing.B) {
	for _, mode := range []string{"coalesced", "uncoalesced-stress"} {
		b.Run(mode, func(b *testing.B) {
			const (
				entryBytes = 32 << 10
				entryCount = 64
				hotEntries = 8
				capacity   = 16 * entryBytes
			)
			payloads, descriptors := benchmarkArtifactDataset(entryCount, entryBytes)
			accessCache := &benchmarkArtifactAccessCache{
				cache:    openBenchmarkArtifactCache(b, b.TempDir(), capacity, 1),
				payloads: payloads, descriptors: descriptors,
			}
			defer accessCache.close()
			coalesced := &benchmarkCoalescedAccessCache{
				cache: accessCache, locks: make([]sync.Mutex, entryCount),
			}
			for index := range hotEntries {
				if err := accessCache.admit(index); err != nil {
					b.Fatalf("prime hot entry %d: %v", index, err)
				}
			}

			var sequence atomic.Uint64
			var hits atomic.Int64
			var misses atomic.Int64
			var firstErr error
			var errOnce sync.Once
			b.ReportAllocs()
			b.SetBytes(entryBytes)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					operation := sequence.Add(1) - 1
					index := int(operation % hotEntries)
					if operation%20 == 0 {
						index = hotEntries + int((operation/20)%(entryCount-hotEntries))
					}
					var hit bool
					var err error
					if mode == "coalesced" {
						hit, err = coalesced.access(index)
					} else {
						hit, err = benchmarkUncoalescedAccess(accessCache, index)
					}
					if err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
					if hit {
						hits.Add(1)
					} else {
						misses.Add(1)
					}
				}
			})
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			stats := accessCache.stats()
			if stats.residentBytes > stats.maxBytes {
				b.Fatalf("resident bytes=%d exceed max=%d", stats.residentBytes, stats.maxBytes)
			}
			operations := float64(max(1, hits.Load()+misses.Load()))
			b.ReportMetric(float64(hits.Load())/operations, "hits/op")
			b.ReportMetric(float64(misses.Load())/operations, "misses/op")
			b.ReportMetric(float64(stats.residentBytes), "resident_B")
			b.ReportMetric(float64(stats.admissionBypasses)/operations, "admission_bypasses/op")
		})
	}
}

var benchmarkArtifactByteSink atomic.Uint32

func benchmarkArtifactData(size int, seed byte) []byte {
	data := bytes.Repeat([]byte{seed}, size)
	if size >= 8 {
		data[0] = seed
		data[size-1] = seed ^ 0xff
	}
	return data
}

func benchmarkArtifactDataset(count, size int) ([][]byte, []ArtifactDescriptor) {
	payloads := make([][]byte, count)
	descriptors := make([]ArtifactDescriptor, count)
	for index := range count {
		payloads[index] = benchmarkArtifactData(size, byte(index+1))
		descriptors[index] = contentFillDescriptor(
			ArtifactSST, fmt.Sprintf("load-%04d", index), payloads[index])
	}
	return payloads, descriptors
}

func openBenchmarkArtifactCache(
	b testing.TB,
	dir string,
	sstBytes, bloomBytes int64,
) *ArtifactCache {
	b.Helper()
	cache, err := OpenArtifactCache(ArtifactCacheOptions{
		Dir: dir, SSTMaxBytes: sstBytes, BloomMaxBytes: bloomBytes,
	})
	if err != nil {
		b.Fatalf("open artifact cache: %v", err)
	}
	return cache
}

func seedRecoveredArtifactDirectory(b *testing.B, dir string, entryCount int) {
	b.Helper()
	cache := openBenchmarkArtifactCache(b, dir, int64(entryCount*2), 1)
	if err := cache.Close(); err != nil {
		b.Fatal(err)
	}

	createdShards := make(map[string]struct{})
	for index := range entryCount {
		checksum := sha256.Sum256([]byte(fmt.Sprintf("recovery-%08d", index)))
		path := cache.inner.path(artifactContentAddress{kind: ArtifactSST, checksum: checksum})
		shard := filepath.Dir(path)
		if _, ok := createdShards[shard]; !ok {
			if err := os.MkdirAll(shard, 0o700); err != nil {
				b.Fatal(err)
			}
			createdShards[shard] = struct{}{}
		}
		if err := os.WriteFile(path, []byte{byte(index)}, 0o600); err != nil {
			b.Fatal(err)
		}
	}
}

type benchmarkAccessStats struct {
	residentBytes     int64
	maxBytes          int64
	admissionBypasses int64
}

type benchmarkCoalescedAccessCache struct {
	cache *benchmarkArtifactAccessCache
	locks []sync.Mutex
}

func (c *benchmarkCoalescedAccessCache) access(index int) (bool, error) {
	hit, err := c.cache.acquire(index)
	if hit || err != nil {
		return hit, err
	}
	c.locks[index].Lock()
	defer c.locks[index].Unlock()
	if hit, err = c.cache.acquire(index); hit || err != nil {
		// This operation still observed a miss before joining the in-flight
		// load, so report it as a miss even though the recheck now hits.
		return false, err
	}
	return false, c.cache.admit(index)
}

func benchmarkUncoalescedAccess(cache *benchmarkArtifactAccessCache, index int) (bool, error) {
	hit, err := cache.acquire(index)
	if hit || err != nil {
		return hit, err
	}
	return false, cache.admit(index)
}

type benchmarkArtifactAccessCache struct {
	cache       *ArtifactCache
	payloads    [][]byte
	descriptors []ArtifactDescriptor
}

func (c *benchmarkArtifactAccessCache) acquire(index int) (bool, error) {
	desc := c.descriptors[index]
	if handle, ok, err := c.cache.Acquire(desc); err != nil {
		return false, err
	} else if ok {
		err := validateBenchmarkArtifactData(handle.Bytes(), c.payloads[index])
		closeErr := handle.Close()
		return true, errorsJoinBenchmark(err, closeErr)
	}
	return false, nil
}

func (c *benchmarkArtifactAccessCache) admit(index int) error {
	desc := c.descriptors[index]
	handle, _, err := c.cache.AdmitBytes(desc, c.payloads[index])
	if err != nil {
		return err
	}
	validateErr := validateBenchmarkArtifactData(handle.Bytes(), c.payloads[index])
	return errorsJoinBenchmark(validateErr, handle.Close())
}

func (c *benchmarkArtifactAccessCache) stats() benchmarkAccessStats {
	stats := c.cache.Stats(ArtifactSST)
	return benchmarkAccessStats{
		residentBytes: stats.ResidentBytes, maxBytes: stats.MaxBytes,
		admissionBypasses: stats.AdmissionBypasses,
	}
}

func (c *benchmarkArtifactAccessCache) close() error { return c.cache.Close() }

func validateBenchmarkArtifactData(got, want []byte) error {
	if len(got) != len(want) || len(got) == 0 || got[0] != want[0] || got[len(got)-1] != want[len(want)-1] {
		return fmt.Errorf("artifact data mismatch: got=%d bytes want=%d", len(got), len(want))
	}
	benchmarkArtifactByteSink.Store(uint32(got[0]))
	return nil
}

func errorsJoinBenchmark(first, second error) error {
	if first != nil {
		return first
	}
	return second
}
