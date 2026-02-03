package isledb

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func openBenchWriter(b *testing.B, ctx context.Context, store *blobstore.Store, opts WriterOptions) (*DB, *Writer) {
	b.Helper()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		b.Fatalf("OpenDB: %v", err)
	}

	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		_ = db.Close()
		b.Fatalf("OpenWriter: %v", err)
	}

	return db, writer
}

func openBenchReader(b *testing.B, ctx context.Context, store *blobstore.Store) *Reader {
	b.Helper()

	opts := DefaultReaderOptions()
	opts.CacheDir = b.TempDir()

	reader, err := newReader(ctx, store, opts)
	if err != nil {
		b.Fatalf("newReader: %v", err)
	}

	return reader
}

func closeBenchResources(b *testing.B, writer *Writer, db *DB, store *blobstore.Store) {
	b.Helper()
	if writer != nil {
		if err := writer.Close(); err != nil {
			b.Fatalf("Writer close: %v", err)
		}
	}
	if db != nil {
		if err := db.Close(); err != nil {
			b.Fatalf("DB close: %v", err)
		}
	}
	if store != nil {
		if err := store.Close(); err != nil {
			b.Fatalf("Store close: %v", err)
		}
	}
}

func BenchmarkDB_Put_Sequential(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	opts := DefaultWriterOptions()
	opts.FlushInterval = -1
	opts.MemtableSize = 64 * 1024 * 1024

	db, writer := openBenchWriter(b, ctx, store, opts)
	defer closeBenchResources(b, writer, db, store)

	value := make([]byte, 100)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(key), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}

	b.StopTimer()
}

func BenchmarkDB_Put_Random(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	opts := DefaultWriterOptions()
	opts.FlushInterval = -1
	opts.MemtableSize = 64 * 1024 * 1024

	db, writer := openBenchWriter(b, ctx, store, opts)
	defer closeBenchResources(b, writer, db, store)

	value := make([]byte, 100)
	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%016d", rng.Int63())
		if err := writer.Put([]byte(key), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}

	b.StopTimer()
}

func BenchmarkDB_Put_ValueSizes(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096, 16384}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("value_%d", size), func(b *testing.B) {
			store := blobstore.NewMemory("bench")
			ctx := context.Background()

			opts := DefaultWriterOptions()
			opts.FlushInterval = -1
			opts.MemtableSize = 64 * 1024 * 1024

			db, writer := openBenchWriter(b, ctx, store, opts)
			defer closeBenchResources(b, writer, db, store)

			value := make([]byte, size)
			for i := range value {
				value[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(size))

			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key-%016d", i)
				if err := writer.Put([]byte(key), value); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}

func BenchmarkDB_Put_WithFlush(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	opts := DefaultWriterOptions()
	opts.FlushInterval = -1
	opts.MemtableSize = 1 * 1024 * 1024

	db, writer := openBenchWriter(b, ctx, store, opts)
	defer closeBenchResources(b, writer, db, store)

	value := make([]byte, 100)
	flushEvery := int(opts.MemtableSize / int64(len(value)))
	if flushEvery < 1 {
		flushEvery = 1
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(key), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
		if (i+1)%flushEvery == 0 {
			if err := writer.Flush(ctx); err != nil {
				b.Fatalf("Flush: %v", err)
			}
		}
	}
}

func BenchmarkDB_Get_Sequential(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = -1

	db, writer := openBenchWriter(b, ctx, store, wOpts)
	defer closeBenchResources(b, writer, db, store)

	numKeys := 10000
	value := make([]byte, 100)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(key), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("Flush: %v", err)
	}

	reader := openBenchReader(b, ctx, store)
	defer reader.Close()

	if err := reader.Refresh(ctx); err != nil {
		b.Fatalf("Refresh: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%016d", i%numKeys)
		_, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		if !found {
			b.Fatalf("key not found: %s", key)
		}
	}
}

func BenchmarkDB_Get_Random(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = -1

	db, writer := openBenchWriter(b, ctx, store, wOpts)
	defer closeBenchResources(b, writer, db, store)

	numKeys := 10000
	value := make([]byte, 100)
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(keys[i]), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("Flush: %v", err)
	}

	reader := openBenchReader(b, ctx, store)
	defer reader.Close()

	if err := reader.Refresh(ctx); err != nil {
		b.Fatalf("Refresh: %v", err)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[rng.Intn(numKeys)]
		_, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		if !found {
			b.Fatalf("key not found: %s", key)
		}
	}
}

func BenchmarkDB_Get_NotFound(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = -1

	db, writer := openBenchWriter(b, ctx, store, wOpts)
	defer closeBenchResources(b, writer, db, store)

	numKeys := 10000
	value := make([]byte, 100)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(key), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("Flush: %v", err)
	}

	reader := openBenchReader(b, ctx, store)
	defer reader.Close()

	if err := reader.Refresh(ctx); err != nil {
		b.Fatalf("Refresh: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("notfound-%016d", i)
		_, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		if found {
			b.Fatalf("key should not be found: %s", key)
		}
	}
}

func BenchmarkDB_Get_MultipleSSTs(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = -1
	wOpts.MemtableSize = 32 * 1024

	db, writer := openBenchWriter(b, ctx, store, wOpts)
	defer closeBenchResources(b, writer, db, store)

	numKeys := 1000
	value := make([]byte, 100)
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(keys[i]), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
		if i%100 == 99 {
			if err := writer.Flush(ctx); err != nil {
				b.Fatalf("Flush: %v", err)
			}
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("Flush: %v", err)
	}

	reader := openBenchReader(b, ctx, store)
	defer reader.Close()

	if err := reader.Refresh(ctx); err != nil {
		b.Fatalf("Refresh: %v", err)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[rng.Intn(numKeys)]
		_, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		if !found {
			b.Fatalf("key not found: %s", key)
		}
	}
}

func BenchmarkDB_Get_AfterCompaction(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = -1
	wOpts.MemtableSize = 32 * 1024

	db, writer := openBenchWriter(b, ctx, store, wOpts)
	defer closeBenchResources(b, writer, db, store)

	numKeys := 1000
	value := make([]byte, 100)
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(keys[i]), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
		if i%100 == 99 {
			if err := writer.Flush(ctx); err != nil {
				b.Fatalf("Flush: %v", err)
			}
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("Flush: %v", err)
	}

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.L0CompactionThreshold = 4

	compactor, err := db.OpenCompactor(ctx, compactorOpts)
	if err != nil {
		b.Fatalf("OpenCompactor: %v", err)
	}
	if err := compactor.RunCompaction(ctx); err != nil {
		_ = compactor.Close()
		b.Fatalf("RunCompaction: %v", err)
	}
	if err := compactor.Close(); err != nil {
		b.Fatalf("Compactor close: %v", err)
	}

	reader := openBenchReader(b, ctx, store)
	defer reader.Close()

	if err := reader.Refresh(ctx); err != nil {
		b.Fatalf("Refresh: %v", err)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[rng.Intn(numKeys)]
		_, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		if !found {
			b.Fatalf("key not found: %s", key)
		}
	}
}

func BenchmarkDB_Scan_Full(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = -1

	db, writer := openBenchWriter(b, ctx, store, wOpts)
	defer closeBenchResources(b, writer, db, store)

	numKeys := 1000
	value := make([]byte, 100)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(key), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("Flush: %v", err)
	}

	reader := openBenchReader(b, ctx, store)
	defer reader.Close()

	if err := reader.Refresh(ctx); err != nil {
		b.Fatalf("Refresh: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		kvs, err := reader.Scan(ctx, nil, nil)
		if err != nil {
			b.Fatalf("Scan: %v", err)
		}
		if len(kvs) != numKeys {
			b.Fatalf("expected %d keys, got %d", numKeys, len(kvs))
		}
	}

	b.ReportMetric(float64(numKeys), "keys/scan")
}

func BenchmarkDB_Scan_Range(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = -1

	db, writer := openBenchWriter(b, ctx, store, wOpts)
	defer closeBenchResources(b, writer, db, store)

	numKeys := 10000
	value := make([]byte, 100)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(key), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("Flush: %v", err)
	}

	reader := openBenchReader(b, ctx, store)
	defer reader.Close()

	if err := reader.Refresh(ctx); err != nil {
		b.Fatalf("Refresh: %v", err)
	}

	start := fmt.Sprintf("key-%016d", 4500)
	end := fmt.Sprintf("key-%016d", 5500)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		kvs, err := reader.Scan(ctx, []byte(start), []byte(end))
		if err != nil {
			b.Fatalf("Scan: %v", err)
		}
		if len(kvs) == 0 {
			b.Fatal("expected some keys")
		}
	}
}

func BenchmarkDB_MixedWorkload_ReadHeavy(b *testing.B) {
	benchmarkMixedWorkload(b, 90)
}

func BenchmarkDB_MixedWorkload_Balanced(b *testing.B) {
	benchmarkMixedWorkload(b, 50)
}

func BenchmarkDB_MixedWorkload_WriteHeavy(b *testing.B) {
	benchmarkMixedWorkload(b, 10)
}

func benchmarkMixedWorkload(b *testing.B, readPercent int) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	wOpts := DefaultWriterOptions()
	wOpts.FlushInterval = -1
	wOpts.MemtableSize = 4 * 1024 * 1024

	db, writer := openBenchWriter(b, ctx, store, wOpts)
	defer closeBenchResources(b, writer, db, store)

	numKeys := 10000
	value := make([]byte, 100)
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(keys[i]), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		b.Fatalf("Flush: %v", err)
	}

	reader := openBenchReader(b, ctx, store)
	defer reader.Close()

	if err := reader.Refresh(ctx); err != nil {
		b.Fatalf("Refresh: %v", err)
	}

	rng := rand.New(rand.NewSource(42))
	writeIdx := numKeys
	pendingKeys := make([]string, 0, 128)
	pendingWrites := 0
	flushEvery := 128

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if rng.Intn(100) < readPercent {
			key := keys[rng.Intn(len(keys))]
			_, _, err := reader.Get(ctx, []byte(key))
			if err != nil {
				b.Fatalf("Get: %v", err)
			}
		} else {
			key := fmt.Sprintf("key-%016d", writeIdx)
			writeIdx++
			if err := writer.Put([]byte(key), value); err != nil {
				b.Fatalf("Put: %v", err)
			}
			pendingKeys = append(pendingKeys, key)
			pendingWrites++
			if pendingWrites >= flushEvery {
				if err := writer.Flush(ctx); err != nil {
					b.Fatalf("Flush: %v", err)
				}
				if err := reader.Refresh(ctx); err != nil {
					b.Fatalf("Refresh: %v", err)
				}
				keys = append(keys, pendingKeys...)
				pendingKeys = pendingKeys[:0]
				pendingWrites = 0
			}
		}
	}

	if pendingWrites > 0 {
		if err := writer.Flush(ctx); err != nil {
			b.Fatalf("Flush: %v", err)
		}
		if err := reader.Refresh(ctx); err != nil {
			b.Fatalf("Refresh: %v", err)
		}
		keys = append(keys, pendingKeys...)
	}
}

func BenchmarkWriter_Put(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	opts := DefaultWriterOptions()
	opts.FlushInterval = -1
	opts.MemtableSize = 64 * 1024 * 1024

	db, writer := openBenchWriter(b, ctx, store, opts)
	defer closeBenchResources(b, writer, db, store)

	value := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(key), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

func BenchmarkWriter_Flush(b *testing.B) {
	store := blobstore.NewMemory("bench")
	ctx := context.Background()

	opts := DefaultWriterOptions()
	opts.FlushInterval = -1
	opts.MemtableSize = 64 * 1024 * 1024

	db, writer := openBenchWriter(b, ctx, store, opts)
	defer closeBenchResources(b, writer, db, store)

	value := make([]byte, 100)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%016d", i)
		if err := writer.Put([]byte(key), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("key-%016d-%d", i, j)
			if err := writer.Put([]byte(key), value); err != nil {
				b.Fatalf("Put: %v", err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			b.Fatalf("Flush: %v", err)
		}
	}
}

func BenchmarkCompactor_L0Compaction(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		store := blobstore.NewMemory("bench")
		ctx := context.Background()

		db, err := OpenDB(ctx, store, DBOptions{})
		if err != nil {
			b.Fatalf("OpenDB: %v", err)
		}

		writerOpts := DefaultWriterOptions()
		writerOpts.FlushInterval = -1
		writerOpts.MemtableSize = 32 * 1024

		writer, err := db.OpenWriter(ctx, writerOpts)
		if err != nil {
			_ = db.Close()
			b.Fatalf("OpenWriter: %v", err)
		}

		value := make([]byte, 100)
		for j := 0; j < 8; j++ {
			for k := 0; k < 100; k++ {
				key := fmt.Sprintf("key-%016d-%d", j, k)
				if err := writer.Put([]byte(key), value); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
			if err := writer.Flush(ctx); err != nil {
				b.Fatalf("Flush: %v", err)
			}
		}
		if err := writer.Close(); err != nil {
			_ = db.Close()
			_ = store.Close()
			b.Fatalf("Writer close: %v", err)
		}

		compactorOpts := DefaultCompactorOptions()
		compactorOpts.L0CompactionThreshold = 4

		compactor, err := db.OpenCompactor(ctx, compactorOpts)
		if err != nil {
			_ = db.Close()
			b.Fatalf("OpenCompactor: %v", err)
		}

		b.StartTimer()

		if err := compactor.RunCompaction(ctx); err != nil {
			_ = compactor.Close()
			_ = db.Close()
			_ = store.Close()
			b.Fatalf("RunCompaction: %v", err)
		}

		b.StopTimer()

		if err := compactor.Close(); err != nil {
			_ = db.Close()
			_ = store.Close()
			b.Fatalf("Compactor close: %v", err)
		}
		if err := db.Close(); err != nil {
			_ = store.Close()
			b.Fatalf("DB close: %v", err)
		}
		if err := store.Close(); err != nil {
			b.Fatalf("Store close: %v", err)
		}
	}
}
