package isledb

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// BenchmarkReaderWarmGet_BlockCache measures warm point lookups on one 16 MiB
// SST, read from the local disk cache or by range, through the decoded block
// cache. Every key in the working set is read once before timing, so no
// lookup touches object storage or decodes a block.
//
// get is a whole lookup; open is only opening and closing the SST iterator,
// the part of each lookup the block cache cannot remove.
func BenchmarkReaderWarmGet_BlockCache(b *testing.B) {
	ctx := context.Background()
	for _, blockKiB := range []int{4, 16} {
		records := (16 << 20) / kvBlockSizeEntryBytes
		store := blobstore.NewMemory(fmt.Sprintf("block-cache-bench-%d", blockKiB))
		b.Cleanup(func() { _ = store.Close() })

		modes := []struct {
			name string
			opts readerOptions
		}{
			{"local", readerOptions{}},
			{"remote", readerOptions{RangeRead: true, RangeReadMinSSTSize: 1}},
		}
		// Open before writing: a reader refuses a prefix that already holds
		// SSTs but no manifest.
		readers := make([]*Reader, len(modes))
		for i, mode := range modes {
			opts := mode.opts
			opts.CacheDir = b.TempDir()
			reader, err := newReader(ctx, store, opts)
			if err != nil {
				b.Fatalf("open %s reader: %v", mode.name, err)
			}
			b.Cleanup(func() { _ = reader.Close() })
			readers[i] = reader
		}

		entries := make([]internal.MemEntry, records)
		values := kvBlockSizeValues(records)
		for i := range entries {
			entries[i] = internal.MemEntry{
				Key: kvLeveledBenchmarkKey(i), Seq: uint64(i + 1), Kind: internal.OpPut, Value: values[i],
			}
		}
		result, err := writeSST(ctx, &sliceSSTIter{entries: entries}, sstWriterOptions{
			BlockSize: blockKiB << 10, BloomBitsPerKey: 12, Compression: "snappy",
		}, 1)
		if err != nil {
			b.Fatalf("write SST: %v", err)
		}
		if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
			b.Fatalf("store SST: %v", err)
		}
		meta := result.Meta
		meta.Level = 1
		state := &manifestState{Levels: []manifest.Level{{Number: 1, SSTs: []manifest.SSTMeta{meta}}}}

		rng := rand.New(rand.NewSource(1))
		keys := make([][]byte, 1000)
		for i := range keys {
			keys[i] = kvLeveledBenchmarkKey(rng.Intn(records))
		}

		for i, mode := range modes {
			reader := readers[i]
			for _, key := range keys {
				assertKVReaderBenchmarkManifestGet(b, ctx, reader, state, key, true, kvBlockSizeValueBytes)
			}

			b.Run(fmt.Sprintf("block=%dKiB/%s/get", blockKiB, mode.name), func(b *testing.B) {
				b.ReportAllocs()
				for n := 0; b.Loop(); n++ {
					if _, found, err := reader.getWithManifest(ctx, state, keys[n%len(keys)]); err != nil || !found {
						b.Fatalf("Get: found=%v err=%v", found, err)
					}
				}
			})
			b.Run(fmt.Sprintf("block=%dKiB/%s/open", blockKiB, mode.name), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_, iter, err := reader.openSSTIterBounded(ctx, meta, nil, nil, false)
					if err != nil {
						b.Fatalf("open: %v", err)
					}
					_ = iter.Close()
				}
			})
		}
	}
}
