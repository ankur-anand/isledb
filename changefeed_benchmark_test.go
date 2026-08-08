package isledb

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func BenchmarkChangeBatchAppend(b *testing.B) {
	for _, records := range []int{1_024, 16_384} {
		for _, feed := range benchmarkReadableChangeFeedPayloads() {
			b.Run(fmt.Sprintf("records=%d/value=256B/changefeed=%s", records, feed.name), func(b *testing.B) {
				keys := make([][]byte, records)
				for i := 0; i < records; i++ {
					keys[i] = []byte(fmt.Sprintf("key-%08d", i))
				}
				values := benchmarkChangeFeedValues(records, 256, true)

				b.SetBytes(int64(records * (len("key-00000000") + 256)))
				b.ReportAllocs()
				b.ResetTimer()

				var buffer *changeBatchBuffer
				for i := 0; i < b.N; i++ {
					buffer = &changeBatchBuffer{payload: feed.payload}
					for j := 0; j < records; j++ {
						if err := buffer.appendPutForPayload(
							uint64(j+1), keys[j], values[j], 0, feed.payload,
						); err != nil {
							b.Fatalf("append change: %v", err)
						}
					}
				}
				b.ReportMetric(float64(buffer.bodySize), "raw_B")
			})
		}
	}
}

func BenchmarkChangeBatchStream(b *testing.B) {
	const records = 16_384
	for _, values := range []struct {
		name   string
		unique bool
	}{
		{name: "compressible"},
		{name: "unique", unique: true},
	} {
		for _, feed := range benchmarkReadableChangeFeedPayloads() {
			b.Run("values="+values.name+"/changefeed="+feed.name, func(b *testing.B) {
				feedValues := benchmarkChangeFeedValues(records, 256, values.unique)
				buffer := &changeBatchBuffer{payload: feed.payload}
				for i := 0; i < records; i++ {
					key := []byte(fmt.Sprintf("key-%08d", i))
					if err := buffer.appendPutForPayload(
						uint64(i+1), key, feedValues[i], 0, feed.payload,
					); err != nil {
						b.Fatalf("append change: %v", err)
					}
				}

				b.SetBytes(int64(records * (len("key-00000000") + 256)))
				b.ReportAllocs()
				b.ResetTimer()

				var result changeBatchStreamResult
				for i := 0; i < b.N; i++ {
					var err error
					result, err = writeChangeBatchStreaming(context.Background(), buffer, 1, time.Unix(int64(i+1), 0),
						func(_ context.Context, _ string, reader io.Reader) error {
							_, copyErr := io.Copy(io.Discard, reader)
							return copyErr
						})
					if err != nil {
						b.Fatalf("stream change batch: %v", err)
					}
				}
				b.ReportMetric(float64(buffer.bodySize), "raw_B")
				b.ReportMetric(float64(result.Meta.Size), "compressed_B")
			})
		}
	}
}

func BenchmarkWriterFlushChangeFeed(b *testing.B) {
	for _, values := range []struct {
		name   string
		unique bool
	}{
		{name: "compressible"},
		{name: "unique", unique: true},
	} {
		for _, feed := range benchmarkChangeFeedPayloads() {
			b.Run("payload="+values.name+"/changefeed="+feed.name, func(b *testing.B) {
				benchmarkWriterChangeFeed(b, feed.payload, values.unique, false, 16_384, 256)
			})
		}
	}
}

func BenchmarkWriterWriteFlushChangeFeed(b *testing.B) {
	for _, feed := range benchmarkChangeFeedPayloads() {
		b.Run(feed.name, func(b *testing.B) {
			benchmarkWriterChangeFeed(b, feed.payload, true, true, 16_384, 256)
		})
	}
}

func benchmarkChangeFeedPayloads() []struct {
	name    string
	payload ChangeFeedPayload
} {
	return []struct {
		name    string
		payload ChangeFeedPayload
	}{
		{name: "disabled"},
		{name: "keys_only", payload: ChangeFeedKeysOnly},
		{name: "full_values", payload: ChangeFeedFullValues},
	}
}

func benchmarkWriterChangeFeed(b *testing.B, payload ChangeFeedPayload, uniqueValues, timePuts bool, records, valueBytes int) {
	b.Helper()
	b.StopTimer()

	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-bench")
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	opts.Memtable.TargetBytes = 64 << 20

	db, err := openDB(ctx, store, dbOpenOptions{changeFeedPayload: manifestChangeFeedPayload(payload)})
	if err != nil {
		b.Fatalf("open DB: %v", err)
	}
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		_ = db.Close()
		b.Fatalf("open writer: %v", err)
	}
	values := benchmarkChangeFeedValues(records, valueBytes, uniqueValues)

	b.SetBytes(int64(records * (len("key-00000000-00000000") + valueBytes)))
	b.ReportAllocs()
	b.ResetTimer()
	if timePuts {
		b.StartTimer()
	}

	for i := 0; i < b.N; i++ {
		if !timePuts {
			b.StopTimer()
		}
		for j := 0; j < records; j++ {
			key := []byte(fmt.Sprintf("key-%08d-%08d", i, j))
			if err := writer.Put(ctx, key, values[j]); err != nil {
				b.Fatalf("put: %v", err)
			}
		}
		if !timePuts {
			b.StartTimer()
		}
		if err := writer.Flush(ctx); err != nil {
			b.Fatalf("flush: %v", err)
		}
	}
	b.StopTimer()

	changes, err := store.List(ctx, blobstore.ListOptions{Prefix: "changes/"})
	if err != nil {
		b.Fatalf("list change batches: %v", err)
	}
	var changeBytes int64
	for _, object := range changes.Objects {
		changeBytes += object.Size
	}
	ssts, err := store.List(ctx, blobstore.ListOptions{Prefix: "sstable/"})
	if err != nil {
		b.Fatalf("list SSTs: %v", err)
	}
	var sstBytes int64
	for _, object := range ssts.Objects {
		sstBytes += object.Size
	}
	if b.N > 0 {
		b.ReportMetric(float64(len(changes.Objects))/float64(b.N), "change_objects/op")
		b.ReportMetric(float64(changeBytes)/float64(b.N), "change_B/op")
		b.ReportMetric(float64(sstBytes)/float64(b.N), "sst_B/op")
	}

	closeBenchResources(b, writer, db, store)
}

func benchmarkChangeFeedValues(records, valueBytes int, unique bool) [][]byte {
	values := make([][]byte, records)
	if !unique {
		value := make([]byte, valueBytes)
		for i := range values {
			values[i] = value
		}
		return values
	}

	state := uint64(0x9e3779b97f4a7c15)
	for i := range values {
		value := make([]byte, valueBytes)
		for j := range value {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			value[j] = byte(state)
		}
		values[i] = value
	}
	return values
}
