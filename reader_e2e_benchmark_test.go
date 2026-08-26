package isledb

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type kvS3ReadCounts struct {
	current    atomic.Int64
	ssts       atomic.Int64
	lists      atomic.Int64
	rangeBytes atomic.Int64

	sstInFlight    atomic.Int64
	maxSSTInFlight atomic.Int64

	// sstDelay models a small amount of object-store latency in the
	// synchronized cold-miss benchmark. It is configured before readers start.
	sstDelay time.Duration

	recordRanges atomic.Bool
	rangesMu     sync.Mutex
	ranges       []kvS3ByteRange
}

type kvS3ByteRange struct {
	offset int64
	length int64
}

func (c *kvS3ReadCounts) observe(request *http.Request) {
	if request == nil || request.Method != http.MethodGet {
		return
	}
	if request.URL.Query().Get("list-type") != "" {
		c.lists.Add(1)
		return
	}
	switch path := request.URL.Path; {
	case strings.HasSuffix(path, "/manifest/CURRENT"):
		c.current.Add(1)
	case strings.Contains(path, "/sstable/"):
		c.ssts.Add(1)
		if byteRange, ok := parseKVReaderBenchmarkRange(request.Header.Get("Range")); ok {
			c.rangeBytes.Add(byteRange.length)
			if c.recordRanges.Load() {
				c.rangesMu.Lock()
				c.ranges = append(c.ranges, byteRange)
				c.rangesMu.Unlock()
			}
		}
		if c.sstDelay > 0 {
			inFlight := c.sstInFlight.Add(1)
			for maximum := c.maxSSTInFlight.Load(); inFlight > maximum; maximum = c.maxSSTInFlight.Load() {
				if c.maxSSTInFlight.CompareAndSwap(maximum, inFlight) {
					break
				}
			}
			defer c.sstInFlight.Add(-1)
			time.Sleep(c.sstDelay)
		}
	}
}

func (c *kvS3ReadCounts) reset() {
	c.current.Store(0)
	c.ssts.Store(0)
	c.lists.Store(0)
	c.rangeBytes.Store(0)
	c.maxSSTInFlight.Store(0)
	c.rangesMu.Lock()
	c.ranges = c.ranges[:0]
	c.rangesMu.Unlock()
}

func (c *kvS3ReadCounts) rangeSnapshot() []kvS3ByteRange {
	c.rangesMu.Lock()
	defer c.rangesMu.Unlock()
	return append([]kvS3ByteRange(nil), c.ranges...)
}

func parseKVReaderBenchmarkRange(value string) (kvS3ByteRange, bool) {
	value = strings.TrimPrefix(value, "bytes=")
	startValue, endValue, ok := strings.Cut(value, "-")
	if !ok || startValue == "" || endValue == "" {
		return kvS3ByteRange{}, false
	}
	start, err := strconv.ParseInt(startValue, 10, 64)
	if err != nil || start < 0 {
		return kvS3ByteRange{}, false
	}
	end, err := strconv.ParseInt(endValue, 10, 64)
	if err != nil || end < start {
		return kvS3ByteRange{}, false
	}
	return kvS3ByteRange{offset: start, length: end - start + 1}, true
}

func BenchmarkFakeS3_KVReaderGet_16384x256B(b *testing.B) {
	const (
		records   = 16_384
		valueSize = 256
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	reader, counts := prepareFakeS3KVReaderBenchmark(b, ctx, records, valueSize)
	key := []byte("key-00008192")

	for _, cache := range []string{"cold", "warm"} {
		b.Run(cache, func(b *testing.B) {
			if err := reader.clearSSTCache(); err != nil {
				b.Fatalf("clear SST cache: %v", err)
			}
			if cache == "warm" {
				assertKVReaderBenchmarkGet(b, ctx, reader, key, valueSize)
			}
			counts.reset()
			b.ReportAllocs()
			b.SetBytes(valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if cache == "cold" {
					b.StopTimer()
					if err := reader.clearSSTCache(); err != nil {
						b.Fatalf("clear SST cache: %v", err)
					}
					b.StartTimer()
				}
				assertKVReaderBenchmarkGet(b, ctx, reader, key, valueSize)
			}
			b.StopTimer()
			iterations := float64(b.N)
			b.ReportMetric(float64(counts.current.Load())/iterations, "current_GETs/op")
			b.ReportMetric(float64(counts.ssts.Load())/iterations, "sst_GETs/op")
			b.ReportMetric(float64(counts.lists.Load())/iterations, "LISTs/op")
		})
	}
}

func BenchmarkFakeS3_KVReaderScan_16384x256B(b *testing.B) {
	const (
		records   = 16_384
		valueSize = 256
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	reader, counts := prepareFakeS3KVReaderBenchmark(b, ctx, records, valueSize)
	for _, cache := range []string{"cold", "warm"} {
		b.Run(cache, func(b *testing.B) {
			if err := reader.clearSSTCache(); err != nil {
				b.Fatalf("clear SST cache: %v", err)
			}
			if cache == "warm" {
				assertKVReaderBenchmarkScan(b, ctx, reader, records)
			}
			counts.reset()
			b.ReportAllocs()
			b.SetBytes(int64(records * valueSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if cache == "cold" {
					b.StopTimer()
					if err := reader.clearSSTCache(); err != nil {
						b.Fatalf("clear SST cache: %v", err)
					}
					b.StartTimer()
				}
				assertKVReaderBenchmarkScan(b, ctx, reader, records)
			}
			b.StopTimer()
			iterations := float64(b.N)
			b.ReportMetric(float64(counts.current.Load())/iterations, "current_GETs/op")
			b.ReportMetric(float64(counts.ssts.Load())/iterations, "sst_GETs/op")
			b.ReportMetric(float64(counts.lists.Load())/iterations, "LISTs/op")
			b.ReportMetric(float64(b.N*records)/b.Elapsed().Seconds(), "records/s")
		})
	}
}

// BenchmarkFakeS3_KVReaderGet_WholeSSTVsRange compares the two current reader
// paths over the same SST. Besides latency and allocations, it reports the
// object-store request and byte cost of one point lookup.
func BenchmarkFakeS3_KVReaderGet_WholeSSTVsRange(b *testing.B) {
	const (
		records   = 16_384
		valueSize = 256
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db, counts := prepareFakeS3KVBenchmarkDB(
		b, ctx, records, valueSize, kvReaderComparisonSSTOutput())
	key := []byte("key-00008192")

	for _, mode := range []string{"whole-sst", "range-read"} {
		for _, temperature := range []string{"cold", "warm"} {
			b.Run(mode+"/"+temperature, func(b *testing.B) {
				reader, metrics := openFakeS3KVBenchmarkReader(b, ctx, db, mode)
				defer func() { _ = reader.Close() }()

				// Warm the bloom cache in both cases so this benchmark isolates
				// the whole-SST and range-read data paths. Cold cases then clear
				// only the corresponding SST-data cache.
				assertKVReaderBenchmarkGet(b, ctx, reader, key, valueSize)
				waitKVReaderBenchmarkCache(reader)
				if temperature == "cold" {
					clearKVReaderBenchmarkCache(b, reader)
				}

				counts.reset()
				bytesBefore := kvReaderRemoteBytes(metrics)
				b.ReportAllocs()
				b.SetBytes(valueSize)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if temperature == "cold" {
						b.StopTimer()
						clearKVReaderBenchmarkCache(b, reader)
						b.StartTimer()
					}
					assertKVReaderBenchmarkGet(b, ctx, reader, key, valueSize)
				}
				b.StopTimer()

				iterations := float64(b.N)
				b.ReportMetric(float64(counts.ssts.Load())/iterations, "sst_GETs/op")
				b.ReportMetric((kvReaderRemoteBytes(metrics)-bytesBefore)/iterations, "remote_B/op")
			})
		}
	}
}

// BenchmarkFakeS3_KVReaderGet_ConcurrentColdMiss measures request amplification
// when many callers ask for the same uncached key at once. A small fixed delay
// keeps the fake provider close enough to real object-store latency for the
// misses to overlap reliably.
func BenchmarkFakeS3_KVReaderGet_ConcurrentColdMiss(b *testing.B) {
	const (
		records     = 16_384
		valueSize   = 256
		concurrency = 32
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db, counts := prepareFakeS3KVBenchmarkDB(
		b, ctx, records, valueSize, kvReaderComparisonSSTOutput())
	counts.sstDelay = 2 * time.Millisecond
	key := []byte("key-00008192")

	for _, mode := range []string{"whole-sst", "range-read"} {
		b.Run(mode, func(b *testing.B) {
			reader, metrics := openFakeS3KVBenchmarkReader(b, ctx, db, mode)
			defer func() { _ = reader.Close() }()

			// Keep the bloom sidecar out of the synchronized miss wave. It has
			// independent miss coalescing and is not the cache path this
			// benchmark compares.
			assertKVReaderBenchmarkGet(b, ctx, reader, key, valueSize)
			waitKVReaderBenchmarkCache(reader)
			clearKVReaderBenchmarkCache(b, reader)
			counts.reset()
			bytesBefore := kvReaderRemoteBytes(metrics)
			b.ReportAllocs()
			b.SetBytes(concurrency * valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				clearKVReaderBenchmarkCache(b, reader)
				start := make(chan struct{})
				errs := make(chan error, concurrency)
				var workers sync.WaitGroup
				workers.Add(concurrency)
				for range concurrency {
					go func() {
						defer workers.Done()
						<-start
						value, found, err := reader.Get(ctx, key)
						if err == nil && (!found || len(value) != valueSize) {
							err = fmt.Errorf("Get found=%v value_bytes=%d want=%d", found, len(value), valueSize)
						}
						errs <- err
					}()
				}
				b.StartTimer()
				close(start)
				workers.Wait()
				b.StopTimer()
				for range concurrency {
					if err := <-errs; err != nil {
						b.Fatalf("concurrent Get: %v", err)
					}
				}
			}

			waves := float64(b.N)
			b.ReportMetric(float64(counts.ssts.Load())/waves, "sst_GETs/wave")
			b.ReportMetric((kvReaderRemoteBytes(metrics)-bytesBefore)/waves, "remote_B/wave")
		})
	}
}

// BenchmarkFakeS3_KVReaderGet_SortedLevelDepth establishes the baseline for
// point lookups through an increasingly deep LSM. Every level contains one
// multi-block SST whose range covers each probe key. Bloom filters reject the
// key in preceding levels; a deep hit therefore still pays one sequential
// object-store request per level before reading the SST that owns the value.
//
// A fixed provider delay makes the latency shape visible without changing the
// request count. The concurrent case also shows whether the existing load
// coalescing bounds request amplification when many callers miss together.
func BenchmarkFakeS3_KVReaderGet_SortedLevelDepth(b *testing.B) {
	const (
		maxLevels   = 16
		valueSize   = 256
		concurrency = 100
	)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	fixture := prepareFakeS3KVBenchmarkPointLevels(b, ctx, maxLevels, valueSize)
	defer func() { _ = fixture.reader.Close() }()
	fixture.counts.sstDelay = 2 * time.Millisecond

	for _, depth := range []int{1, 4, 8, 16} {
		state := &manifestState{Levels: fixture.state.Levels[:depth]}
		cases := []struct {
			name       string
			key        []byte
			wantFound  bool
			candidates int
		}{
			{name: "l1-hit", key: fixture.hitKeys[1], wantFound: true, candidates: 1},
			{name: "deepest-hit", key: fixture.hitKeys[depth], wantFound: true, candidates: depth},
			{name: "missing", key: fixture.missingKey, candidates: depth},
		}

		for _, benchmarkCase := range cases {
			for _, temperature := range []string{"cold", "warm"} {
				name := fmt.Sprintf(
					"levels=%d/%s/%s", depth, benchmarkCase.name, temperature)
				b.Run(name, func(b *testing.B) {
					clearKVReaderPointBenchmarkCaches(b, fixture.reader)
					if temperature == "warm" {
						assertKVReaderBenchmarkManifestGet(
							b, ctx, fixture.reader, state, benchmarkCase.key,
							benchmarkCase.wantFound, valueSize)
						waitKVReaderBenchmarkCache(fixture.reader)
					}

					fixture.counts.reset()
					b.ReportAllocs()
					b.SetBytes(valueSize)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if temperature == "cold" {
							b.StopTimer()
							clearKVReaderPointBenchmarkCaches(b, fixture.reader)
							b.StartTimer()
						}
						assertKVReaderBenchmarkManifestGet(
							b, ctx, fixture.reader, state, benchmarkCase.key,
							benchmarkCase.wantFound, valueSize)
					}
					b.StopTimer()

					iterations := float64(b.N)
					b.ReportMetric(float64(depth), "sorted_levels")
					b.ReportMetric(float64(benchmarkCase.candidates), "candidate_ssts")
					b.ReportMetric(float64(fixture.counts.ssts.Load())/iterations, "sst_GETs/op")
					b.ReportMetric(float64(fixture.counts.rangeBytes.Load())/iterations, "range_B/op")
					b.ReportMetric(float64(fixture.counts.maxSSTInFlight.Load()), "max_sst_GET_concurrency")
				})
			}
		}
	}

	for _, temperature := range []string{"cold", "warm"} {
		b.Run("levels=16/l0-tombstone/"+temperature, func(b *testing.B) {
			state := &manifestState{
				L0SSTs: fixture.state.L0SSTs,
				Levels: fixture.state.Levels[:maxLevels],
			}
			clearKVReaderPointBenchmarkCaches(b, fixture.reader)
			if temperature == "warm" {
				assertKVReaderBenchmarkManifestGet(
					b, ctx, fixture.reader, state, fixture.tombstoneKey, false, valueSize)
				waitKVReaderBenchmarkCache(fixture.reader)
			}

			fixture.counts.reset()
			b.ReportAllocs()
			b.SetBytes(valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if temperature == "cold" {
					b.StopTimer()
					clearKVReaderPointBenchmarkCaches(b, fixture.reader)
					b.StartTimer()
				}
				assertKVReaderBenchmarkManifestGet(
					b, ctx, fixture.reader, state, fixture.tombstoneKey, false, valueSize)
			}
			b.StopTimer()

			iterations := float64(b.N)
			b.ReportMetric(1, "candidate_ssts")
			b.ReportMetric(float64(fixture.counts.ssts.Load())/iterations, "sst_GETs/op")
			b.ReportMetric(float64(fixture.counts.rangeBytes.Load())/iterations, "range_B/op")
			b.ReportMetric(float64(fixture.counts.maxSSTInFlight.Load()), "max_sst_GET_concurrency")
		})
	}

	windowState := &manifestState{Levels: fixture.state.Levels[:maxLevels]}
	windowCases := []struct {
		name       string
		key        []byte
		wantFound  bool
		candidates int
	}{
		{name: "l1-hit", key: fixture.hitKeys[1], wantFound: true, candidates: 1},
		{name: "deepest-hit", key: fixture.hitKeys[maxLevels], wantFound: true, candidates: maxLevels},
		{name: "missing", key: fixture.missingKey, candidates: maxLevels},
	}
	for _, window := range []int{1, 2, 4, 8} {
		for _, benchmarkCase := range windowCases {
			for _, temperature := range []string{"cold", "warm"} {
				name := fmt.Sprintf(
					"levels=16/window=%d/%s/%s",
					window, benchmarkCase.name, temperature)
				b.Run(name, func(b *testing.B) {
					clearKVReaderPointBenchmarkCaches(b, fixture.reader)
					if temperature == "warm" {
						assertKVReaderBenchmarkWindowedGet(
							b, ctx, fixture.reader, windowState, benchmarkCase.key,
							benchmarkCase.wantFound, valueSize, window)
						waitKVReaderBenchmarkCache(fixture.reader)
					}

					fixture.counts.reset()
					b.ReportAllocs()
					b.SetBytes(valueSize)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if temperature == "cold" {
							b.StopTimer()
							clearKVReaderPointBenchmarkCaches(b, fixture.reader)
							b.StartTimer()
						}
						assertKVReaderBenchmarkWindowedGet(
							b, ctx, fixture.reader, windowState, benchmarkCase.key,
							benchmarkCase.wantFound, valueSize, window)
					}
					b.StopTimer()

					iterations := float64(b.N)
					b.ReportMetric(float64(window), "level_window")
					b.ReportMetric(float64(benchmarkCase.candidates), "candidate_ssts")
					b.ReportMetric(float64(fixture.counts.ssts.Load())/iterations, "sst_GETs/op")
					b.ReportMetric(float64(fixture.counts.rangeBytes.Load())/iterations, "range_B/op")
					b.ReportMetric(float64(fixture.counts.maxSSTInFlight.Load()), "max_sst_GET_concurrency")
				})
			}
		}
	}

	// Use the deepest hit for the synchronized wave: all callers traverse the
	// same 16 candidates, so singleflight should keep the provider request count
	// close to one ordinary deep lookup rather than multiplying it by 100.
	state := &manifestState{Levels: fixture.state.Levels[:maxLevels]}
	for _, temperature := range []string{"cold", "warm"} {
		b.Run("levels=16/deepest-hit/concurrent-100/"+temperature, func(b *testing.B) {
			clearKVReaderPointBenchmarkCaches(b, fixture.reader)
			if temperature == "warm" {
				assertKVReaderBenchmarkManifestGet(
					b, ctx, fixture.reader, state, fixture.hitKeys[maxLevels], true, valueSize)
				waitKVReaderBenchmarkCache(fixture.reader)
			}

			fixture.counts.reset()
			b.ReportAllocs()
			b.SetBytes(concurrency * valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if temperature == "cold" {
					clearKVReaderPointBenchmarkCaches(b, fixture.reader)
				}
				start := make(chan struct{})
				errs := make(chan error, concurrency)
				var workers sync.WaitGroup
				workers.Add(concurrency)
				for range concurrency {
					go func() {
						defer workers.Done()
						<-start
						errs <- checkKVReaderBenchmarkManifestGet(
							ctx, fixture.reader, state, fixture.hitKeys[maxLevels], true, valueSize)
					}()
				}
				b.StartTimer()
				close(start)
				workers.Wait()
				b.StopTimer()
				for range concurrency {
					if err := <-errs; err != nil {
						b.Fatalf("concurrent Get: %v", err)
					}
				}
			}

			waves := float64(b.N)
			b.ReportMetric(float64(fixture.counts.ssts.Load())/waves, "sst_GETs/wave")
			b.ReportMetric(float64(fixture.counts.rangeBytes.Load())/waves, "range_B/wave")
			b.ReportMetric(float64(fixture.counts.maxSSTInFlight.Load()), "max_sst_GET_concurrency")
		})
	}
}

// BenchmarkFakeS3_KVReaderGet_RangeReadRequestShape reports the exact ordered
// range requests used by one cold point lookup. The rN_gap_B metrics are the
// distance between the end of request N and the logical end of the Pebble SST;
// they make it visible which reads a tail-oriented read-before window covers.
func BenchmarkFakeS3_KVReaderGet_RangeReadRequestShape(b *testing.B) {
	const valueSize = 256
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cases := []struct {
		records    int
		blockBytes int
	}{
		{records: 16_384, blockBytes: 4 << 10},
		{records: 50_000, blockBytes: 4 << 10},
		{records: 16_384, blockBytes: 16 << 10},
	}
	for _, benchmarkCase := range cases {
		name := fmt.Sprintf(
			"records=%d/block=%dKiB", benchmarkCase.records, benchmarkCase.blockBytes>>10)
		b.Run(name, func(b *testing.B) {
			db, counts := prepareFakeS3KVBenchmarkDB(
				b, ctx, benchmarkCase.records, valueSize,
				kvReaderBenchmarkSSTOutput(benchmarkCase.blockBytes))
			reader, _ := openFakeS3KVBenchmarkReader(b, ctx, db, "range-read")
			defer func() { _ = reader.Close() }()
			key := []byte(fmt.Sprintf("key-%08d", benchmarkCase.records/2))
			manifestState := reader.currentManifest()
			if manifestState == nil {
				b.Fatal("benchmark manifest is not loaded")
			}
			if len(manifestState.L0SSTs) != 1 {
				b.Fatalf("benchmark manifest has %d L0 SSTs, want 1", len(manifestState.L0SSTs))
			}
			logicalSSTSize := manifestState.L0SSTs[0].Size

			// Keep the container Bloom warm so the trace contains only Pebble SST
			// navigation. Each measured iteration starts with an empty range cache.
			assertKVReaderBenchmarkGet(b, ctx, reader, key, valueSize)
			waitKVReaderBenchmarkCache(reader)
			clearKVReaderBenchmarkCache(b, reader)
			counts.recordRanges.Store(true)

			var ranges []kvS3ByteRange
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				clearKVReaderBenchmarkCache(b, reader)
				counts.reset()
				b.StartTimer()
				assertKVReaderBenchmarkGet(b, ctx, reader, key, valueSize)
				b.StopTimer()
				ranges = counts.rangeSnapshot()
			}

			b.ReportMetric(float64(logicalSSTSize), "logical_sst_B")
			b.ReportMetric(float64(len(ranges)), "range_GETs/op")
			for i, byteRange := range ranges {
				request := i + 1
				b.ReportMetric(float64(byteRange.length), fmt.Sprintf("r%d_B", request))
				gap := logicalSSTSize - (byteRange.offset + byteRange.length)
				b.ReportMetric(float64(gap), fmt.Sprintf("r%d_gap_B", request))
			}
			for _, window := range []int64{32 << 10, 64 << 10, 512 << 10} {
				covered := 0
				windowStart := max(int64(0), logicalSSTSize-window)
				for _, byteRange := range ranges {
					if byteRange.offset >= windowStart &&
						byteRange.offset+byteRange.length <= logicalSSTSize {
						covered++
					}
				}
				b.ReportMetric(float64(covered), fmt.Sprintf("tail_%dKiB_ranges", window>>10))
			}
		})
	}
}

// BenchmarkFakeS3_KVReaderScanLimit_ManyL1SSTs guards the lazy per-level scan
// path. L1 SSTs are sorted and non-overlapping, so the cost of returning one
// row should remain roughly constant as the number of SSTs in the level grows.
//
// The cold case exposes the resulting object-store request amplification. The
// warm case keeps all range bytes in memory and isolates the CPU/allocation
// cost of rebuilding every SST reader and iterator.
func BenchmarkFakeS3_KVReaderScanLimit_ManyL1SSTs(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for _, sstCount := range []int{16, 128, 512} {
		b.Run(fmt.Sprintf("ssts=%d", sstCount), func(b *testing.B) {
			reader, state, counts, metrics := prepareFakeS3KVBenchmarkL1(b, ctx, sstCount)
			defer func() { _ = reader.Close() }()

			for _, temperature := range []string{"cold", "warm"} {
				b.Run(temperature, func(b *testing.B) {
					if temperature == "warm" {
						assertKVReaderBenchmarkScanLimit(b, ctx, reader, state, 1)
						waitKVReaderBenchmarkCache(reader)
					} else {
						waitKVReaderBenchmarkCache(reader)
						reader.blockCache.Clear()
					}

					counts.reset()
					bytesBefore := kvReaderRemoteBytes(metrics)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if temperature == "cold" {
							b.StopTimer()
							waitKVReaderBenchmarkCache(reader)
							reader.blockCache.Clear()
							b.StartTimer()
						}
						assertKVReaderBenchmarkScanLimit(b, ctx, reader, state, 1)
					}
					b.StopTimer()

					iterations := float64(b.N)
					b.ReportMetric(float64(sstCount), "l1_ssts")
					b.ReportMetric(float64(counts.ssts.Load())/iterations, "sst_GETs/op")
					b.ReportMetric((kvReaderRemoteBytes(metrics)-bytesBefore)/iterations, "remote_B/op")
				})
			}
		})
	}
}

// BenchmarkFakeS3_KVReaderScanLimit_LeveledDataset exercises lazy sorted-level
// scanning through a more production-shaped manifest: multi-block SSTs,
// overlapping L0 updates and tombstones, and two sorted levels containing
// newer and older versions of the same keyspace.
func BenchmarkFakeS3_KVReaderScanLimit_LeveledDataset(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	fixture := prepareFakeS3KVBenchmarkLeveled(b, ctx)
	defer func() { _ = fixture.reader.Close() }()

	cases := []struct {
		name   string
		minKey []byte
		maxKey []byte
		limit  int
	}{
		{name: "first-page-100", limit: 100},
		{name: "first-page-1000", limit: 1000},
		{name: "middle-page-100", minKey: kvLeveledBenchmarkKey(fixture.totalKeys / 2), limit: 100},
		{
			name:   "bounded-middle-window-100",
			minKey: kvLeveledBenchmarkKey(fixture.totalKeys / 4),
			maxKey: kvLeveledBenchmarkKey(3 * fixture.totalKeys / 4),
			limit:  100,
		},
	}

	for _, benchmarkCase := range cases {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			candidateSSTs := kvBenchmarkOverlappingSSTCount(
				fixture.state, benchmarkCase.minKey, benchmarkCase.maxKey)

			for _, temperature := range []string{"cold", "warm"} {
				b.Run(temperature, func(b *testing.B) {
					if temperature == "warm" {
						assertKVReaderBenchmarkScanLimitRange(
							b, ctx, fixture.reader, fixture.state,
							benchmarkCase.minKey, benchmarkCase.maxKey, benchmarkCase.limit)
						waitKVReaderBenchmarkCache(fixture.reader)
					} else {
						waitKVReaderBenchmarkCache(fixture.reader)
						fixture.reader.blockCache.Clear()
					}

					fixture.counts.reset()
					bytesBefore := kvReaderRemoteBytes(fixture.metrics)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if temperature == "cold" {
							b.StopTimer()
							waitKVReaderBenchmarkCache(fixture.reader)
							fixture.reader.blockCache.Clear()
							b.StartTimer()
						}
						assertKVReaderBenchmarkScanLimitRange(
							b, ctx, fixture.reader, fixture.state,
							benchmarkCase.minKey, benchmarkCase.maxKey, benchmarkCase.limit)
					}
					b.StopTimer()

					iterations := float64(b.N)
					b.ReportMetric(float64(candidateSSTs), "candidate_ssts")
					b.ReportMetric(float64(fixture.totalSSTs), "dataset_ssts")
					b.ReportMetric(float64(fixture.totalKeys), "keyspace_keys")
					b.ReportMetric(float64(fixture.sstBytes)/(1<<20), "sst_MiB")
					b.ReportMetric(float64(fixture.counts.ssts.Load())/iterations, "sst_GETs/op")
					b.ReportMetric(
						(kvReaderRemoteBytes(fixture.metrics)-bytesBefore)/iterations, "remote_B/op")
				})
			}
		})
	}
}

type kvLeveledBenchmarkFixture struct {
	reader    *Reader
	state     *manifestState
	counts    *kvS3ReadCounts
	metrics   *ReaderMetrics
	totalKeys int
	totalSSTs int
	sstBytes  int64
}

type kvPointLevelBenchmarkFixture struct {
	reader       *Reader
	state        *manifestState
	counts       *kvS3ReadCounts
	hitKeys      map[int][]byte
	missingKey   []byte
	tombstoneKey []byte
}

func prepareFakeS3KVBenchmarkPointLevels(
	b *testing.B,
	ctx context.Context,
	levelCount, valueSize int,
) kvPointLevelBenchmarkFixture {
	b.Helper()

	const rowsPerLevel = 4096
	counts := &kvS3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(b, counts.observe)
	store, err := blobstore.Open(
		ctx, bucketURL, fmt.Sprintf("bench/kv-point-levels-%d", time.Now().UnixNano()))
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })

	values := benchmarkChangeFeedValues(rowsPerLevel, valueSize, true)
	hitKeys := make(map[int][]byte, levelCount)
	for level := 1; level <= levelCount; level++ {
		hitKeys[level] = []byte(fmt.Sprintf("key-00002048/probe-level-%02d", level))
	}
	missingKey := []byte("key-00002048/probe-missing")
	tombstoneKey := []byte("key-00002048/probe-tombstone")

	state := &manifestState{Levels: make([]manifest.Level, 0, levelCount)}
	for level := 1; level <= levelCount; level++ {
		entries := make([]internal.MemEntry, 0, rowsPerLevel+1)
		seqBase := uint64((levelCount - level + 1) * (rowsPerLevel + 1))
		for row := 0; row < rowsPerLevel; row++ {
			entries = append(entries, internal.MemEntry{
				Key:   kvLeveledBenchmarkKey(row),
				Seq:   seqBase + uint64(row+1),
				Kind:  internal.OpPut,
				Value: values[row],
			})
		}
		entries = append(entries, internal.MemEntry{
			Key:   hitKeys[level],
			Seq:   seqBase + uint64(rowsPerLevel+1),
			Kind:  internal.OpPut,
			Value: bytes.Repeat([]byte{byte(level)}, valueSize),
		})
		if level == 1 {
			entries = append(entries, internal.MemEntry{
				Key:   tombstoneKey,
				Seq:   seqBase + uint64(rowsPerLevel+2),
				Kind:  internal.OpPut,
				Value: bytes.Repeat([]byte("v"), valueSize),
			})
		}
		sort.Slice(entries, func(i, j int) bool {
			return bytes.Compare(entries[i].Key, entries[j].Key) < 0
		})

		meta, _ := writeFakeS3KVBenchmarkSST(
			b, ctx, store, entries, uint32(level), uint64(level))
		state.Levels = append(state.Levels, manifest.Level{
			Number: uint32(level), SSTs: []manifest.SSTMeta{meta},
		})
	}
	l0Meta, _ := writeFakeS3KVBenchmarkSST(b, ctx, store, []internal.MemEntry{{
		Key: tombstoneKey, Seq: uint64((levelCount + 1) * (rowsPerLevel + 2)),
		Kind: internal.OpDelete,
	}}, 0, uint64(levelCount+1))
	state.L0SSTs = []manifest.SSTMeta{l0Meta}
	if err := state.ValidateLevels(); err != nil {
		b.Fatalf("validate point-level benchmark manifest: %v", err)
	}

	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir:                 b.TempDir(),
		BlockCacheSize:           64 << 20,
		AllowUnverifiedRangeRead: true,
		RangeReadMinSSTSize:      1,
		Metrics:                  metrics,
	})
	if err != nil {
		b.Fatalf("open reader: %v", err)
	}

	return kvPointLevelBenchmarkFixture{
		reader: reader, state: state, counts: counts,
		hitKeys: hitKeys, missingKey: missingKey, tombstoneKey: tombstoneKey,
	}
}

func prepareFakeS3KVBenchmarkLeveled(
	b *testing.B,
	ctx context.Context,
) kvLeveledBenchmarkFixture {
	b.Helper()

	const (
		totalKeys      = 65_536
		valueBytes     = 256
		l0SSTs         = 8
		l0RowsPerSST   = 256
		l1SSTs         = 256
		l1RowsPerSST   = 256
		l2SSTs         = 128
		l2RowsPerSST   = 512
		readerCacheMax = 128 << 20
	)

	counts := &kvS3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(b, counts.observe)
	store, err := blobstore.Open(ctx, bucketURL, fmt.Sprintf("bench/kv-leveled-%d", time.Now().UnixNano()))
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })

	values := benchmarkChangeFeedValues(totalKeys, valueBytes, true)
	state := &manifestState{}
	var sstBytes int64

	l1, bytesWritten := writeFakeS3KVBenchmarkLevel(
		b, ctx, store, 1, l1SSTs, l1RowsPerSST, uint64(totalKeys), 2, values)
	state.Levels = append(state.Levels, l1)
	sstBytes += bytesWritten

	l2, bytesWritten := writeFakeS3KVBenchmarkLevel(
		b, ctx, store, 2, l2SSTs, l2RowsPerSST, 0, 1, values)
	state.Levels = append(state.Levels, l2)
	sstBytes += bytesWritten

	for i := 0; i < l0SSTs; i++ {
		entries := make([]internal.MemEntry, 0, l0RowsPerSST)
		for j := 0; j < l0RowsPerSST; j++ {
			keyIndex := j*(totalKeys/l0RowsPerSST) + i
			entry := internal.MemEntry{
				Key:   kvLeveledBenchmarkKey(keyIndex),
				Seq:   uint64(2*totalKeys + (l0SSTs-i)*l0RowsPerSST + j),
				Kind:  internal.OpPut,
				Value: values[keyIndex],
			}
			if (i+j)%11 == 0 {
				entry.Kind = internal.OpDelete
				entry.Value = nil
			}
			entries = append(entries, entry)
		}

		meta, written := writeFakeS3KVBenchmarkSST(
			b, ctx, store, entries, 0, uint64(10-i))
		state.L0SSTs = append(state.L0SSTs, meta)
		sstBytes += written
	}

	if err := state.ValidateLevels(); err != nil {
		b.Fatalf("validate benchmark manifest: %v", err)
	}

	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir:                 b.TempDir(),
		BlockCacheSize:           readerCacheMax,
		AllowUnverifiedRangeRead: true,
		RangeReadMinSSTSize:      1,
		Metrics:                  metrics,
	})
	if err != nil {
		b.Fatalf("open reader: %v", err)
	}

	return kvLeveledBenchmarkFixture{
		reader: reader, state: state, counts: counts, metrics: metrics,
		totalKeys: totalKeys, totalSSTs: l0SSTs + l1SSTs + l2SSTs, sstBytes: sstBytes,
	}
}

func writeFakeS3KVBenchmarkLevel(
	b *testing.B,
	ctx context.Context,
	store *blobstore.Store,
	levelNumber, sstCount, rowsPerSST int,
	seqBase, epoch uint64,
	values [][]byte,
) (manifest.Level, int64) {
	b.Helper()

	level := manifest.Level{
		Number: uint32(levelNumber),
		SSTs:   make([]manifest.SSTMeta, 0, sstCount),
	}
	var bytesWritten int64
	for sstIndex := 0; sstIndex < sstCount; sstIndex++ {
		entries := make([]internal.MemEntry, rowsPerSST)
		for row := 0; row < rowsPerSST; row++ {
			keyIndex := sstIndex*rowsPerSST + row
			entries[row] = internal.MemEntry{
				Key: kvLeveledBenchmarkKey(keyIndex), Seq: seqBase + uint64(keyIndex+1),
				Kind: internal.OpPut, Value: values[keyIndex],
			}
		}
		meta, written := writeFakeS3KVBenchmarkSST(
			b, ctx, store, entries, uint32(levelNumber), epoch)
		level.SSTs = append(level.SSTs, meta)
		bytesWritten += written
	}
	return level, bytesWritten
}

func writeFakeS3KVBenchmarkSST(
	b *testing.B,
	ctx context.Context,
	store *blobstore.Store,
	entries []internal.MemEntry,
	level uint32,
	epoch uint64,
) (manifest.SSTMeta, int64) {
	b.Helper()

	result, err := writeSST(ctx, &sliceSSTIter{entries: entries}, sstWriterOptions{
		BlockSize: 4096, BloomBitsPerKey: 10, Compression: "snappy",
	}, epoch)
	if err != nil {
		b.Fatalf("write L%d SST: %v", level, err)
	}
	result.Meta.Level = level
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
		b.Fatalf("store L%d SST: %v", level, err)
	}
	return result.Meta, int64(len(result.SSTData))
}

func kvLeveledBenchmarkKey(index int) []byte {
	return []byte(fmt.Sprintf("key-%08d", index))
}

func kvBenchmarkOverlappingSSTCount(state *manifestState, minKey, maxKey []byte) int {
	if state == nil {
		return 0
	}
	count := 0
	for _, sst := range state.L0SSTs {
		if sstOverlapsHalfOpenRange(sst, KeyRange{Min: minKey, Max: maxKey}) {
			count++
		}
	}
	for i := range state.Levels {
		count += len(state.Levels[i].OverlappingSSTsHalfOpen(minKey, maxKey))
	}
	return count
}

func prepareFakeS3KVBenchmarkL1(
	b *testing.B,
	ctx context.Context,
	sstCount int,
) (*Reader, *manifestState, *kvS3ReadCounts, *ReaderMetrics) {
	b.Helper()

	counts := &kvS3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(b, counts.observe)
	store, err := blobstore.Open(ctx, bucketURL, fmt.Sprintf("bench/kv-l1-%d", time.Now().UnixNano()))
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })

	level := manifest.Level{Number: 1, SSTs: make([]manifest.SSTMeta, 0, sstCount)}
	value := []byte("value")
	for i := 0; i < sstCount; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		result, err := writeSST(ctx, &sliceSSTIter{entries: []internal.MemEntry{{
			Key: key, Seq: uint64(i + 1), Kind: internal.OpPut, Value: value,
		}}}, sstWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
		if err != nil {
			b.Fatalf("write SST %d: %v", i, err)
		}
		result.Meta.Level = 1
		if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
			b.Fatalf("store SST %d: %v", i, err)
		}
		level.SSTs = append(level.SSTs, result.Meta)
	}

	state := &manifestState{Levels: []manifest.Level{level}}
	if err := state.ValidateLevels(); err != nil {
		b.Fatalf("validate benchmark manifest: %v", err)
	}

	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir:                 b.TempDir(),
		BlockCacheSize:           64 << 20,
		AllowUnverifiedRangeRead: true,
		RangeReadMinSSTSize:      1,
		Metrics:                  metrics,
	})
	if err != nil {
		b.Fatalf("open reader: %v", err)
	}
	return reader, state, counts, metrics
}

func prepareFakeS3KVReaderBenchmark(
	b *testing.B,
	ctx context.Context,
	records int,
	valueSize int,
) (*Reader, *kvS3ReadCounts) {
	b.Helper()
	db, counts := prepareFakeS3KVBenchmarkDB(b, ctx, records, valueSize, SSTOutputOptions{})
	reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(b.TempDir()))
	if err != nil {
		b.Fatalf("open reader: %v", err)
	}
	b.Cleanup(func() { _ = reader.Close() })
	return reader, counts
}

func prepareFakeS3KVBenchmarkDB(
	b *testing.B,
	ctx context.Context,
	records int,
	valueSize int,
	sstOutput SSTOutputOptions,
) (*DB, *kvS3ReadCounts) {
	b.Helper()
	counts := &kvS3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(b, counts.observe)
	db, err := Open(ctx, bucketURL, DBOptions{
		Prefix:    fmt.Sprintf("bench/kv-reader-%d", time.Now().UnixNano()),
		SSTOutput: sstOutput,
	})
	if err != nil {
		b.Fatalf("open DB: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "kv-reader-benchmark-writer"
	writerOpts.Flush.Interval = 0
	writerOpts.Memtable.TargetBytes = 16 << 20
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		b.Fatalf("open writer: %v", err)
	}
	values := benchmarkChangeFeedValues(records, valueSize, true)
	for i := 0; i < records; i++ {
		if err := writer.Put(ctx, []byte(fmt.Sprintf("key-%08d", i)), values[i]); err != nil {
			b.Fatalf("put %d: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("flush: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		b.Fatalf("close writer: %v", err)
	}
	return db, counts
}

func kvReaderComparisonSSTOutput() SSTOutputOptions {
	return kvReaderBenchmarkSSTOutput(16 << 10)
}

func kvReaderBenchmarkSSTOutput(blockBytes int) SSTOutputOptions {
	encoding := SSTEncodingOptions{
		Compression:     "none",
		BlockBytes:      blockBytes,
		BloomBitsPerKey: 10,
	}
	return SSTOutputOptions{L0: encoding, Compacted: encoding}
}

func openFakeS3KVBenchmarkReader(
	b *testing.B,
	ctx context.Context,
	db *DB,
	mode string,
) (*Reader, *ReaderMetrics) {
	b.Helper()
	metrics := DefaultReaderMetrics(nil)
	opts := DefaultReaderOpenOptions(b.TempDir())
	opts.Metrics = metrics
	switch mode {
	case "whole-sst":
	case "range-read":
		opts.BlockCacheSize = 16 << 20
		opts.RangeReadMinSSTSize = 1
		opts.AllowUnverifiedRangeRead = true
	default:
		b.Fatalf("unknown reader benchmark mode %q", mode)
	}
	reader, err := db.OpenReader(ctx, opts)
	if err != nil {
		b.Fatalf("open reader: %v", err)
	}
	return reader, metrics
}

func clearKVReaderBenchmarkCache(b *testing.B, reader *Reader) {
	b.Helper()
	if reader.blockCache != nil {
		reader.blockCache.Clear()
		return
	}
	if err := reader.clearSSTCache(); err != nil {
		b.Fatalf("clear SST cache: %v", err)
	}
}

func clearKVReaderPointBenchmarkCaches(b *testing.B, reader *Reader) {
	b.Helper()
	waitKVReaderBenchmarkCache(reader)
	if reader.blockCache != nil {
		reader.blockCache.Clear()
	}
	if reader.bloomCache != nil {
		reader.bloomCache.clear()
	}
	if err := reader.clearSSTCache(); err != nil {
		b.Fatalf("clear SST cache: %v", err)
	}
	if err := reader.clearBloomDiskCache(); err != nil {
		b.Fatalf("clear Bloom disk cache: %v", err)
	}
}

func waitKVReaderBenchmarkCache(reader *Reader) {
	if reader != nil && reader.blockCache != nil {
		reader.blockCache.Wait()
	}
}

func kvReaderRemoteBytes(metrics *ReaderMetrics) float64 {
	if metrics == nil {
		return 0
	}
	return testutil.ToFloat64(metrics.SSTDownloadBytes) +
		testutil.ToFloat64(metrics.SSTRangeReadBytes)
}

func assertKVReaderBenchmarkGet(b *testing.B, ctx context.Context, reader *Reader, key []byte, valueSize int) {
	b.Helper()
	value, found, err := reader.Get(ctx, key)
	if err != nil {
		b.Fatalf("Get: %v", err)
	}
	if !found || len(value) != valueSize {
		b.Fatalf("Get found=%v value_bytes=%d want=%d", found, len(value), valueSize)
	}
}

func assertKVReaderBenchmarkManifestGet(
	b *testing.B,
	ctx context.Context,
	reader *Reader,
	state *manifestState,
	key []byte,
	wantFound bool,
	valueSize int,
) {
	b.Helper()
	if err := checkKVReaderBenchmarkManifestGet(
		ctx, reader, state, key, wantFound, valueSize); err != nil {
		b.Fatal(err)
	}
}

func checkKVReaderBenchmarkManifestGet(
	ctx context.Context,
	reader *Reader,
	state *manifestState,
	key []byte,
	wantFound bool,
	valueSize int,
) error {
	value, found, err := reader.getWithManifest(ctx, state, key)
	if err != nil {
		return fmt.Errorf("Get(%q): %w", key, err)
	}
	if found != wantFound {
		return fmt.Errorf("Get(%q) found=%t want=%t", key, found, wantFound)
	}
	if found && len(value) != valueSize {
		return fmt.Errorf("Get(%q) value_bytes=%d want=%d", key, len(value), valueSize)
	}
	return nil
}

type kvWindowedGetResult struct {
	value   []byte
	got     bool
	deleted bool
	err     error
}

// getKVReaderBenchmarkWindowed is an experimental point-lookup path used only
// by the benchmark. It preserves the production ordering rules while allowing
// a fixed number of sorted-level candidates to perform their object reads in
// parallel. A window of one calls the production implementation directly.
func getKVReaderBenchmarkWindowed(
	ctx context.Context,
	reader *Reader,
	state *manifestState,
	key []byte,
	window int,
) ([]byte, bool, error) {
	if window <= 1 {
		return reader.getWithManifest(ctx, state, key)
	}

	for _, sst := range state.L0SSTs {
		if !keyInSSTRange(key, sst.MinKey, sst.MaxKey) {
			continue
		}
		value, got, deleted, err := reader.getFromSST(ctx, sst, key)
		if err != nil {
			return nil, false, err
		}
		if got {
			if deleted {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	candidates := make([]sstMetadata, 0, len(state.Levels))
	for i := range state.Levels {
		if sst := state.Levels[i].FindSST(key); sst != nil {
			candidates = append(candidates, *sst)
		}
	}

	for start := 0; start < len(candidates); start += window {
		end := min(start+window, len(candidates))
		results := make([]kvWindowedGetResult, end-start)
		var workers sync.WaitGroup
		workers.Add(len(results))
		for i := range results {
			go func(result *kvWindowedGetResult, sst sstMetadata) {
				defer workers.Done()
				result.value, result.got, result.deleted, result.err =
					reader.getFromSST(ctx, sst, key)
			}(&results[i], candidates[start+i])
		}
		workers.Wait()

		// Examine results in level order. An error or tombstone in a shallower
		// level has the same precedence it has in the sequential implementation;
		// speculative failures after an earlier hit remain unobservable.
		for i := range results {
			result := &results[i]
			if result.err != nil {
				return nil, false, result.err
			}
			if result.got {
				if result.deleted {
					return nil, false, nil
				}
				return result.value, true, nil
			}
		}
	}

	return nil, false, nil
}

func assertKVReaderBenchmarkWindowedGet(
	b *testing.B,
	ctx context.Context,
	reader *Reader,
	state *manifestState,
	key []byte,
	wantFound bool,
	valueSize int,
	window int,
) {
	b.Helper()
	value, found, err := getKVReaderBenchmarkWindowed(ctx, reader, state, key, window)
	if err != nil {
		b.Fatalf("windowed Get(%q): %v", key, err)
	}
	if found != wantFound {
		b.Fatalf("windowed Get(%q) found=%t want=%t", key, found, wantFound)
	}
	if found && len(value) != valueSize {
		b.Fatalf("windowed Get(%q) value_bytes=%d want=%d", key, len(value), valueSize)
	}
}

func assertKVReaderBenchmarkScan(b *testing.B, ctx context.Context, reader *Reader, records int) {
	b.Helper()
	values, err := reader.Scan(ctx, nil, nil)
	if err != nil {
		b.Fatalf("Scan: %v", err)
	}
	if len(values) != records {
		b.Fatalf("Scan records=%d want=%d", len(values), records)
	}
}

func assertKVReaderBenchmarkScanLimit(
	b *testing.B,
	ctx context.Context,
	reader *Reader,
	state *manifestState,
	limit int,
) {
	b.Helper()
	assertKVReaderBenchmarkScanLimitRange(b, ctx, reader, state, nil, nil, limit)
}

func assertKVReaderBenchmarkScanLimitRange(
	b *testing.B,
	ctx context.Context,
	reader *Reader,
	state *manifestState,
	minKey, maxKey []byte,
	limit int,
) {
	b.Helper()
	values, err := reader.scanInternalWithManifest(ctx, state, minKey, maxKey, limit)
	if err != nil {
		b.Fatalf("ScanLimit: %v", err)
	}
	if len(values) != limit {
		b.Fatalf("ScanLimit records=%d want=%d", len(values), limit)
	}
}
