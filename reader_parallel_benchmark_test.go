package isledb

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

// BenchmarkReaderWarmGetParallel measures warm point lookups from many
// goroutines at once. Every key is read once before timing, so lookups touch
// only in-memory caches: the Bloom filter cache, the open-SST cache and the
// block cache. Run with -cpu 1,8 to see how lookups scale with cores; a gap
// between the two is time spent waiting on shared state, not doing work.
func BenchmarkReaderWarmGetParallel(b *testing.B) {
	ctx := context.Background()
	const keys = 200_000
	store := blobstore.NewMemory("reader-parallel-get")
	b.Cleanup(func() { _ = store.Close() })
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	opts.Memtable.TargetBytes = 1 << 20 // several SSTs: each lookup checks Bloom filters
	opts.Memtable.MaxPendingMemtables = 64
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		b.Fatal(err)
	}
	for i := range keys {
		if _, err := writer.Put(ctx, benchKey(i), []byte(fmt.Sprintf("value-%08d", i))); err != nil {
			b.Fatal(err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		b.Fatal(err)
	}

	reader := openReaderFromDBForTest(b, ctx, store, ReaderOpenOptions{CacheDir: b.TempDir()})
	b.Cleanup(func() { _ = reader.Close() })
	for i := range keys {
		if _, found, err := reader.Get(ctx, benchKey(i)); err != nil || !found {
			b.Fatalf("warm %d: found=%v err=%v", i, found, err)
		}
	}

	// Keys are made before timing, so allocations counted are the lookup's.
	lookupKeys := make([][]byte, keys)
	for i := range lookupKeys {
		lookupKeys[i] = benchKey(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			if _, _, err := reader.Get(ctx, lookupKeys[rng.Intn(keys)]); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func benchKey(i int) []byte { return []byte(fmt.Sprintf("key-%08d", i)) }
