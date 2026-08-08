package isledb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/klauspost/compress/zstd"
)

func BenchmarkChangeReaderRead(b *testing.B) {
	const records = 16_384
	ctx := context.Background()
	store := blobstore.NewMemory("change-reader-benchmark")
	db, err := openDB(ctx, store, dbOpenOptions{changeFeedEnabled: true})
	if err != nil {
		b.Fatalf("open DB: %v", err)
	}
	writer, err := db.OpenWriter(ctx, testChangeWriterOptions())
	if err != nil {
		b.Fatalf("open writer: %v", err)
	}
	values := benchmarkChangeFeedValues(records, 256, true)
	for i := 0; i < records; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		if err := writer.Put(ctx, key, values[i]); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("flush: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		b.Fatalf("close writer: %v", err)
	}
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		b.Fatalf("open change reader: %v", err)
	}
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		b.Fatalf("bounds: %v", err)
	}
	b.Cleanup(func() {
		_ = reader.Close()
		_ = db.Close()
		_ = store.Close()
	})

	for _, maxChanges := range []int{1, 64, 1024, records} {
		for _, cache := range []string{"cold", "warm"} {
			b.Run(fmt.Sprintf("max_changes=%d/cache=%s", maxChanges, cache), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(maxChanges * (len("key-00000000") + 256)))
				opts := ChangeReadOptions{
					MaxChanges: maxChanges,
					MaxBytes:   64 << 20,
				}
				if cache == "warm" {
					if _, err := reader.Read(ctx, bounds.Oldest, opts); err != nil {
						b.Fatalf("warm cache: %v", err)
					}
				}
				reader.work.reset()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if cache == "cold" {
						b.StopTimer()
						clearChangeReaderBatchCache(reader)
						b.StartTimer()
					}
					page, err := reader.Read(ctx, bounds.Oldest, opts)
					if err != nil {
						b.Fatalf("read: %v", err)
					}
					if len(page.Changes) != maxChanges {
						b.Fatalf("changes=%d want=%d", len(page.Changes), maxChanges)
					}
				}
				b.StopTimer()
				work := reader.work.snapshot()
				b.ReportMetric(float64(work.RangeGETs)/float64(b.N), "range_GETs/op")
				b.ReportMetric(float64(work.DownloadedBytes)/float64(b.N), "downloaded_B/op")
				b.ReportMetric(float64(work.DecompressedBytes)/float64(b.N), "decompressed_B/op")
			})
		}
	}
}

func BenchmarkChangeReaderSequentialPaging(b *testing.B) {
	const (
		records  = 16_384
		pageSize = 1024
	)
	ctx := context.Background()
	store := blobstore.NewMemory("change-reader-paging-benchmark")
	db, err := openDB(ctx, store, dbOpenOptions{changeFeedEnabled: true})
	if err != nil {
		b.Fatalf("open DB: %v", err)
	}
	writer, err := db.OpenWriter(ctx, testChangeWriterOptions())
	if err != nil {
		b.Fatalf("open writer: %v", err)
	}
	values := benchmarkChangeFeedValues(records, 256, true)
	for i := 0; i < records; i++ {
		if err := writer.Put(ctx, []byte(fmt.Sprintf("key-%08d", i)), values[i]); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("flush: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		b.Fatalf("close writer: %v", err)
	}
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		b.Fatalf("open change reader: %v", err)
	}
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		b.Fatalf("bounds: %v", err)
	}
	b.Cleanup(func() {
		_ = reader.Close()
		_ = db.Close()
		_ = store.Close()
	})

	opts := ChangeReadOptions{MaxChanges: pageSize, MaxBytes: 64 << 20}
	b.ReportAllocs()
	b.SetBytes(int64(records * (len("key-00000000") + 256)))
	reader.work.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor := bounds.Oldest
		read := 0
		for !cursor.IsZero() {
			page, err := reader.Read(ctx, cursor, opts)
			if err != nil {
				b.Fatalf("read: %v", err)
			}
			read += len(page.Changes)
			cursor = page.Next
			if page.CaughtUp() {
				break
			}
		}
		if read != records {
			b.Fatalf("changes=%d want=%d", read, records)
		}
	}
	b.StopTimer()
	work := reader.work.snapshot()
	b.ReportMetric(float64(work.RangeGETs)/float64(b.N), "range_GETs/op")
	b.ReportMetric(float64(work.DownloadedBytes)/float64(b.N), "downloaded_B/op")
	b.ReportMetric(float64(work.DecompressedBytes)/float64(b.N), "decompressed_B/op")
	b.ReportMetric(float64(b.N*records)/b.Elapsed().Seconds(), "records/s")
}

func BenchmarkChangeBatchReadStrategies(b *testing.B) {
	const records = 16_384
	object, meta, preparedIndex := prepareIndexedChangeBatchBenchmark(
		b, records, 256, defaultChangeBatchBlockOptions())
	indexOffset := int(preparedIndex.Blocks[len(preparedIndex.Blocks)-1].Offset) +
		int(preparedIndex.Blocks[len(preparedIndex.Blocks)-1].CompressedSize)

	tests := []struct {
		name  string
		start int
		count int
	}{
		{name: "request=1", count: 1},
		{name: "request=64", count: 64},
		{name: "request=1024", count: 1024},
		{name: "request=16384", count: records},
		{name: "restart=8192/request=1024", start: 8192, count: 1024},
	}
	for _, test := range tests {
		for _, strategy := range []string{"whole", "stream", "indexed"} {
			b.Run(strategy+"/"+test.name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(test.count * (len("key-00000000") + 256)))
				var downloaded, decompressed, gets uint64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var (
						changes []Change
						work    changeReaderWorkSnapshot
						err     error
					)
					switch strategy {
					case "whole":
						changes, err = benchmarkWholeBatchRead(object, test.start, test.count)
						work = changeReaderWorkSnapshot{
							RangeGETs: 1, DownloadedBytes: uint64(len(object)), DecompressedBytes: uint64(meta.RawSize),
						}
					case "stream":
						changes, work, err = benchmarkStreamingBatchRead(
							object[:indexOffset], test.start, test.count)
					case "indexed":
						changes, work, err = benchmarkIndexedBatchRead(
							object, meta, test.start, test.count)
					}
					if err != nil {
						b.Fatalf("%s read: %v", strategy, err)
					}
					if len(changes) != test.count {
						b.Fatalf("changes=%d want=%d", len(changes), test.count)
					}
					downloaded += work.DownloadedBytes
					decompressed += work.DecompressedBytes
					gets += work.RangeGETs
				}
				b.StopTimer()
				b.ReportMetric(float64(gets)/float64(b.N), "GETs/op")
				b.ReportMetric(float64(downloaded)/float64(b.N), "downloaded_B/op")
				b.ReportMetric(float64(decompressed)/float64(b.N), "decompressed_B/op")
				if test.start == 0 && test.count == records {
					b.ReportMetric(float64(b.N*records)/b.Elapsed().Seconds(), "records/s")
				}
			})
		}
	}
}

func BenchmarkChangeBatchIndexedBlockSizes(b *testing.B) {
	const records = 16_384
	for _, maxRecords := range []int{512, 1024} {
		for _, rawBytes := range []int64{256 << 10, 1 << 20} {
			opts := changeBatchBlockOptions{MaxRecords: maxRecords, TargetRawBytes: rawBytes}
			object, meta, index := prepareIndexedChangeBatchBenchmark(b, records, 256, opts)
			for _, request := range []int{1, 64, 1024, records} {
				name := fmt.Sprintf("records_per_block=%d/raw_block=%dKiB/request=%d", maxRecords, rawBytes>>10, request)
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(request * (len("key-00000000") + 256)))
					var downloaded, decompressed, gets uint64
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						changes, work, err := benchmarkIndexedBatchRead(object, meta, 0, request)
						if err != nil {
							b.Fatalf("indexed read: %v", err)
						}
						if len(changes) != request {
							b.Fatalf("changes=%d want=%d", len(changes), request)
						}
						downloaded += work.DownloadedBytes
						decompressed += work.DecompressedBytes
						gets += work.RangeGETs
					}
					b.StopTimer()
					b.ReportMetric(float64(len(index.Blocks)), "blocks")
					b.ReportMetric(float64(gets)/float64(b.N), "GETs/op")
					b.ReportMetric(float64(downloaded)/float64(b.N), "downloaded_B/op")
					b.ReportMetric(float64(decompressed)/float64(b.N), "decompressed_B/op")
					if request == records {
						b.ReportMetric(float64(b.N*records)/b.Elapsed().Seconds(), "records/s")
					}
				})
			}
		}
	}
}

func prepareIndexedChangeBatchBenchmark(
	b *testing.B,
	records int,
	valueBytes int,
	opts changeBatchBlockOptions,
) ([]byte, *manifest.ChangeBatchMeta, *changeBatchIndex) {
	b.Helper()
	values := benchmarkChangeFeedValues(records, valueBytes, true)
	buffer := &changeBatchBuffer{}
	for i := 0; i < records; i++ {
		if err := buffer.appendPut(
			uint64(i+1), []byte(fmt.Sprintf("key-%08d", i)), values[i], 0); err != nil {
			b.Fatalf("append change: %v", err)
		}
	}
	var object bytes.Buffer
	result, err := writeChangeBatchStreamingWithOptions(
		context.Background(), buffer, 1, time.Unix(1, 0).UTC(), opts,
		func(_ context.Context, _ string, reader io.Reader) error {
			_, copyErr := io.Copy(&object, reader)
			return copyErr
		})
	if err != nil {
		b.Fatalf("write indexed batch: %v", err)
	}
	result.Meta.Path = "benchmark/change-batch"
	data := object.Bytes()
	trailer := data[len(data)-changeBatchTrailerSize:]
	indexOffset, indexSize, err := changeBatchIndexLocation(trailer, int64(len(data)))
	if err != nil {
		b.Fatalf("locate index: %v", err)
	}
	index, err := decodeChangeBatchIndex(data[indexOffset:indexOffset+indexSize], trailer, int64(len(data)))
	if err != nil {
		b.Fatalf("decode index: %v", err)
	}
	return data, &result.Meta, index
}

func benchmarkWholeBatchRead(data []byte, start, count int) ([]Change, error) {
	batch, err := decodeChangeBatch(data)
	if err != nil {
		return nil, err
	}
	if start < 0 || count < 0 || start+count > len(batch.Changes) {
		return nil, io.ErrUnexpectedEOF
	}
	return benchmarkPublicChanges(batch.Changes[start : start+count]), nil
}

func benchmarkIndexedBatchRead(
	data []byte,
	meta *manifest.ChangeBatchMeta,
	start int,
	count int,
) ([]Change, changeReaderWorkSnapshot, error) {
	work := changeReaderWorkSnapshot{RangeGETs: 1}
	suffixSize := int(meta.BlockCount)*changeBatchIndexEntrySize + changeBatchTrailerSize
	work.DownloadedBytes += uint64(suffixSize)
	suffix := data[len(data)-suffixSize:]
	index, err := decodeChangeBatchIndex(
		suffix[:len(suffix)-changeBatchTrailerSize], suffix[len(suffix)-changeBatchTrailerSize:], int64(len(data)))
	if err != nil {
		return nil, work, err
	}
	ordinal, _, ok := changeBatchBlockForRecord(index, uint64(start))
	if !ok {
		return nil, work, io.ErrUnexpectedEOF
	}
	endOrdinal := changeBatchBlockSpan(index, ordinal, uint64(start), count, 64<<20)
	first := index.Blocks[ordinal]
	last := index.Blocks[endOrdinal-1]
	endOffset := last.Offset + uint64(last.CompressedSize)
	work.RangeGETs++
	work.DownloadedBytes += endOffset - first.Offset

	records := make([]changeRecord, 0, count)
	for blockOrdinal := ordinal; blockOrdinal < endOrdinal; blockOrdinal++ {
		block := index.Blocks[blockOrdinal]
		changes, err := decodeChangeBatchBlock(
			data[block.Offset:block.Offset+uint64(block.CompressedSize)], block)
		if err != nil {
			return nil, work, err
		}
		work.DecompressedBytes += uint64(block.RawSize)
		localStart := 0
		if blockOrdinal == ordinal {
			localStart = start - int(block.FirstIndex)
		}
		remaining := count - len(records)
		end := min(len(changes), localStart+remaining)
		records = append(records, changes[localStart:end]...)
		if len(records) == count {
			break
		}
	}
	if len(records) != count {
		return nil, work, io.ErrUnexpectedEOF
	}
	return benchmarkPublicChanges(records), work, nil
}

func benchmarkStreamingBatchRead(
	compressedBlocks []byte,
	start int,
	count int,
) ([]Change, changeReaderWorkSnapshot, error) {
	source := &benchmarkCountingReader{reader: bytes.NewReader(compressedBlocks)}
	decoder, err := zstd.NewReader(source, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, changeReaderWorkSnapshot{}, err
	}
	decompressed := &benchmarkCountingReader{reader: decoder}
	changes := make([]Change, 0, count)
	for i := 0; i < start+count; i++ {
		change, err := benchmarkReadStreamingChange(decompressed, i >= start)
		if err != nil {
			decoder.Close()
			return nil, changeReaderWorkSnapshot{}, err
		}
		if i >= start {
			changes = append(changes, change)
		}
	}
	decoder.Close()
	return changes, changeReaderWorkSnapshot{
		RangeGETs: 1, DownloadedBytes: source.count, DecompressedBytes: decompressed.count,
	}, nil
}

func benchmarkReadStreamingChange(reader io.Reader, retain bool) (Change, error) {
	var header [changeRecordHeaderSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return Change{}, err
	}
	keyLen := binary.BigEndian.Uint32(header[4:8])
	valueLen := binary.BigEndian.Uint32(header[8:12])
	if !retain {
		_, err := io.CopyN(io.Discard, reader, int64(keyLen)+int64(valueLen))
		return Change{}, err
	}
	key := make([]byte, int(keyLen))
	if _, err := io.ReadFull(reader, key); err != nil {
		return Change{}, err
	}
	value := make([]byte, int(valueLen))
	if _, err := io.ReadFull(reader, value); err != nil {
		return Change{}, err
	}
	change := Change{Sequence: binary.BigEndian.Uint64(header[16:24]), Key: key, Value: value}
	switch changeKind(header[0]) {
	case changePut:
		change.Operation = ChangePut
	case changeDelete:
		change.Operation = ChangeDelete
	default:
		return Change{}, fmt.Errorf("unknown operation=%d", header[0])
	}
	if expireAt := int64(binary.BigEndian.Uint64(header[24:32])); expireAt != 0 {
		change.ExpiresAt = time.UnixMilli(expireAt)
	}
	return change, nil
}

func benchmarkPublicChanges(records []changeRecord) []Change {
	dataBytes := 0
	for i := range records {
		dataBytes += len(records[i].Key) + len(records[i].Value)
	}
	pageData := make([]byte, 0, dataBytes)
	changes := make([]Change, 0, len(records))
	for i := range records {
		changes = append(changes, publicChange(records[i], &pageData))
	}
	return changes
}

type benchmarkCountingReader struct {
	reader io.Reader
	count  uint64
}

func (r *benchmarkCountingReader) Read(data []byte) (int, error) {
	n, err := r.reader.Read(data)
	r.count += uint64(n)
	return n, err
}
