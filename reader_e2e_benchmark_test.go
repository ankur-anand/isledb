package isledb

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type kvS3ReadCounts struct {
	current atomic.Int64
	ssts    atomic.Int64
	lists   atomic.Int64
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
	}
}

func (c *kvS3ReadCounts) reset() {
	c.current.Store(0)
	c.ssts.Store(0)
	c.lists.Store(0)
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

func prepareFakeS3KVReaderBenchmark(
	b *testing.B,
	ctx context.Context,
	records int,
	valueSize int,
) (*Reader, *kvS3ReadCounts) {
	b.Helper()
	counts := &kvS3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(b, counts.observe)
	db, err := Open(ctx, bucketURL, DBOptions{
		Prefix: fmt.Sprintf("bench/kv-reader-%d", time.Now().UnixNano()),
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

	reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(b.TempDir()))
	if err != nil {
		b.Fatalf("open reader: %v", err)
	}
	b.Cleanup(func() { _ = reader.Close() })
	return reader, counts
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
