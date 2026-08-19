package isledb

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

type kvS3ReadCounts struct {
	current atomic.Int64
	ssts    atomic.Int64
	lists   atomic.Int64

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
		if c.recordRanges.Load() {
			if byteRange, ok := parseKVReaderBenchmarkRange(request.Header.Get("Range")); ok {
				c.rangesMu.Lock()
				c.ranges = append(c.ranges, byteRange)
				c.rangesMu.Unlock()
			}
		}
		if c.sstDelay > 0 {
			time.Sleep(c.sstDelay)
		}
	}
}

func (c *kvS3ReadCounts) reset() {
	c.current.Store(0)
	c.ssts.Store(0)
	c.lists.Store(0)
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
			if err := reader.sstCache.Clear(); err != nil {
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
					if err := reader.sstCache.Clear(); err != nil {
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
			if err := reader.sstCache.Clear(); err != nil {
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
					if err := reader.sstCache.Clear(); err != nil {
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
	if err := reader.sstCache.Clear(); err != nil {
		b.Fatalf("clear SST cache: %v", err)
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
