package isledb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// BenchmarkFakeS3_KVReaderBlockSize measures how SST data block size shapes
// range-read cost on one SST. Every workload runs against the production
// range-read path with an empty block cache; the recorded request trace is
// also replayed through an aligned-chunk model so the same run shows what
// fixed chunk fetching would issue instead.
//
// The layout leaf reports the SST's physical shape: data blocks, index size and
// partitions, the two filters (Pebble's block and the isledb sidecar), and the
// metadata region. Reads run twice: through Pebble's read-before hints, as for
// SSTs without a recorded MetaOffset, and through the MetaOffset region.
func BenchmarkFakeS3_KVReaderBlockSize(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	for _, sstMiB := range []int{16, 64} {
		for _, blockKiB := range []int{4, 16, 32, 64} {
			name := fmt.Sprintf("sst=%dMiB/block=%dKiB", sstMiB, blockKiB)
			b.Run(name, func(b *testing.B) {
				fixture := prepareKVBlockSizeFixture(b, ctx, sstMiB<<20, sstWriterOptions{
					BlockSize:       blockKiB << 10,
					BloomBitsPerKey: 10,
					Compression:     "snappy",
				}, 0)
				runKVBlockSizeWorkloads(b, ctx, fixture)
			})
		}
	}
}

type kvBlockSizeFixture struct {
	reader *Reader
	// whole reads the same SST by downloading it whole into the local file
	// cache, as a reader without a block cache does.
	whole   *Reader
	state   *manifestState
	counts  *kvS3ReadCounts
	meta    manifest.SSTMeta
	props   sstable.Properties
	records int
}

const kvBlockSizeValueBytes = 256

// kvBlockSizeEntryBytes approximates one stored entry after snappy so the
// record count lands near the requested SST size. The layout leaf reports the
// size actually produced.
const kvBlockSizeEntryBytes = 224

func prepareKVBlockSizeFixture(
	b *testing.B,
	ctx context.Context,
	targetBytes int,
	opts sstWriterOptions,
	chunkSize int64,
) kvBlockSizeFixture {
	b.Helper()

	records := targetBytes / kvBlockSizeEntryBytes
	counts := &kvS3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(b, counts.observe)
	store, err := blobstore.Open(
		ctx, bucketURL, fmt.Sprintf("bench/kv-block-size-%d", time.Now().UnixNano()))
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })

	// Open before writing: a reader refuses a prefix that already holds SSTs
	// but no manifest. The benchmark passes its own manifest to each read.
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir:            b.TempDir(),
		RangeRead:           true,
		BlockCacheSize:      256 << 20,
		RangeReadMinSSTSize: 1,
		RangeReadChunkSize:  chunkSize,
		Metrics:             DefaultReaderMetrics(nil),
	})
	if err != nil {
		b.Fatalf("open reader: %v", err)
	}
	b.Cleanup(func() { _ = reader.Close() })
	whole, err := newReader(ctx, store, readerOptions{
		CacheDir: b.TempDir(),
		Metrics:  DefaultReaderMetrics(nil),
	})
	if err != nil {
		b.Fatalf("open whole-SST reader: %v", err)
	}
	b.Cleanup(func() { _ = whole.Close() })

	entries := make([]internal.MemEntry, records)
	values := kvBlockSizeValues(records)
	for i := range entries {
		entries[i] = internal.MemEntry{
			Key:   kvLeveledBenchmarkKey(i),
			Seq:   uint64(i + 1),
			Kind:  internal.OpPut,
			Value: values[i],
		}
	}
	result, err := writeSST(ctx, &sliceSSTIter{entries: entries}, opts, 1)
	if err != nil {
		b.Fatalf("write SST: %v", err)
	}
	result.Meta.Level = 1
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
		b.Fatalf("store SST: %v", err)
	}

	props := kvBlockSizeProperties(b, ctx, result.SSTData[:result.Meta.Size])

	state := &manifestState{Levels: []manifest.Level{{
		Number: 1, SSTs: []manifest.SSTMeta{result.Meta},
	}}}
	if err := state.ValidateLevels(); err != nil {
		b.Fatalf("validate manifest: %v", err)
	}

	return kvBlockSizeFixture{
		reader: reader, whole: whole, state: state, counts: counts,
		meta: result.Meta, props: props, records: records,
	}
}

// kvBlockSizeValues makes values that snappy compresses to roughly 3/4 of
// their size: a random prefix followed by a repeated tail. Fully random values
// would hide the compression effect of larger blocks.
func kvBlockSizeValues(records int) [][]byte {
	random := benchmarkChangeFeedValues(records, kvBlockSizeValueBytes*3/4, true)
	values := make([][]byte, records)
	for i := range values {
		value := make([]byte, kvBlockSizeValueBytes)
		n := copy(value, random[i])
		for j := n; j < len(value); j++ {
			value[j] = 'v'
		}
		values[i] = value
	}
	return values
}

func kvBlockSizeProperties(b *testing.B, ctx context.Context, data []byte) sstable.Properties {
	b.Helper()
	reader, err := sstable.NewReader(ctx, newMemReadable(data), sstable.ReaderOptions{})
	if err != nil {
		b.Fatalf("open SST for properties: %v", err)
	}
	defer func() { _ = reader.Close() }()
	props, err := reader.ReadPropertiesBlock(ctx, nil)
	if err != nil {
		b.Fatalf("read SST properties: %v", err)
	}
	return props
}

func runKVBlockSizeWorkloads(b *testing.B, ctx context.Context, f kvBlockSizeFixture) {
	b.Run("layout", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
		}
		b.ReportMetric(float64(f.meta.Size)/(1<<20), "logical_sst_MiB")
		b.ReportMetric(float64(f.records), "records")
		b.ReportMetric(float64(f.props.NumDataBlocks), "data_blocks")
		b.ReportMetric(float64(f.props.DataSize)/(1<<10), "data_KiB")
		b.ReportMetric(float64(f.props.IndexSize)/(1<<10), "index_KiB")
		b.ReportMetric(float64(f.props.IndexPartitions), "index_partitions")
		b.ReportMetric(float64(f.props.TopLevelIndexSize)/(1<<10), "top_index_KiB")
		b.ReportMetric(float64(f.props.FilterSize)/(1<<10), "pebble_filter_KiB")
		b.ReportMetric(float64(f.meta.Bloom.Length)/(1<<10), "sidecar_bloom_KiB")
		b.ReportMetric(float64(f.meta.Size-f.meta.MetaOffset)/(1<<10), "meta_region_KiB")
	})

	b.Run("meta=read-before", func(b *testing.B) {
		runKVBlockSizeReads(b, ctx, f.withMetaOffset(0))
	})
	b.Run("meta=offset", func(b *testing.B) {
		runKVBlockSizeReads(b, ctx, f.withMetaOffset(f.meta.MetaOffset))
	})
}

// withMetaOffset returns the fixture with a manifest whose SST records the
// given metadata offset. Zero reproduces SSTs written before MetaOffset
// existed, which open through Pebble's read-before hints.
func (f kvBlockSizeFixture) withMetaOffset(offset int64) kvBlockSizeFixture {
	meta := f.meta
	meta.MetaOffset = offset
	f.state = &manifestState{Levels: []manifest.Level{{
		Number: 1, SSTs: []manifest.SSTMeta{meta},
	}}}
	return f
}

func runKVBlockSizeReads(b *testing.B, ctx context.Context, f kvBlockSizeFixture) {
	middle := f.records / 2
	key := kvLeveledBenchmarkKey(middle)

	// Load the sidecar Bloom once so cold Gets measure only Pebble navigation
	// and data reads.
	assertKVBlockSizeGet(b, ctx, f, key)
	waitKVReaderBenchmarkCache(f.reader)

	b.Run("get/cold", func(b *testing.B) {
		runKVBlockSizeCold(b, f, func() { assertKVBlockSizeGet(b, ctx, f, key) })
	})
	b.Run("get/warm", func(b *testing.B) {
		assertKVBlockSizeGet(b, ctx, f, key)
		waitKVReaderBenchmarkCache(f.reader)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			assertKVBlockSizeGet(b, ctx, f, key)
		}
	})
	for _, limit := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("scan-%d/cold", limit), func(b *testing.B) {
			runKVBlockSizeCold(b, f, func() {
				assertKVReaderBenchmarkScanLimitRange(b, ctx, f.reader, f.state, key, nil, limit)
			})
		})
	}
}

func assertKVBlockSizeGet(b *testing.B, ctx context.Context, f kvBlockSizeFixture, key []byte) {
	b.Helper()
	assertKVReaderBenchmarkManifestGet(b, ctx, f.reader, f.state, key, true, kvBlockSizeValueBytes)
}

var kvBlockSizeChunkSizes = []int64{64 << 10, 128 << 10, 256 << 10}

// runKVBlockSizeCold runs op against an empty block cache each iteration and
// reports the production request cost next to the chunk-model cost of the
// same trace.
func runKVBlockSizeCold(b *testing.B, f kvBlockSizeFixture, op func()) {
	b.Helper()
	f.counts.recordRanges.Store(true)
	defer f.counts.recordRanges.Store(false)

	var gets, rangeBytes int64
	chunkGets := make([]int64, len(kvBlockSizeChunkSizes))
	chunkBytes := make([]int64, len(kvBlockSizeChunkSizes))
	var doublingGets, doublingBytes int64

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clearKVReaderBenchmarkCache(b, f.reader)
		f.counts.reset()
		b.StartTimer()
		op()
		b.StopTimer()

		ranges := f.counts.rangeSnapshot()
		gets += f.counts.ssts.Load()
		for _, r := range ranges {
			rangeBytes += r.length
		}
		for j, chunk := range kvBlockSizeChunkSizes {
			g, n := simulateChunkedRangeReads(ranges, f.meta.Size, chunk, 1)
			chunkGets[j] += g
			chunkBytes[j] += n
		}
		g, n := simulateChunkedRangeReads(ranges, f.meta.Size, 128<<10, 8)
		doublingGets += g
		doublingBytes += n
	}

	iterations := float64(b.N)
	b.ReportMetric(float64(gets)/iterations, "sst_GETs/op")
	b.ReportMetric(float64(rangeBytes)/iterations/(1<<10), "range_KiB/op")
	for j, chunk := range kvBlockSizeChunkSizes {
		label := fmt.Sprintf("chunk%dK", chunk>>10)
		b.ReportMetric(float64(chunkGets[j])/iterations, label+"_GETs/op")
		b.ReportMetric(float64(chunkBytes[j])/iterations/(1<<10), label+"_KiB/op")
	}
	b.ReportMetric(float64(doublingGets)/iterations, "chunk128K_x2_GETs/op")
	b.ReportMetric(float64(doublingBytes)/iterations/(1<<10), "chunk128K_x2_KiB/op")
}

// simulateChunkedRangeReads replays a request trace through aligned chunk
// fetching with a cache that starts empty. Each request fetches its missing
// chunks as contiguous runs, one GET per run. With maxRun > 1, a miss on the
// chunk right after the previous fetch doubles the run length up to maxRun
// chunks; any other miss resets it to one.
//
// The trace comes from today's reader, so it already includes read-before
// expansions; the model treats those bytes as requested.
func simulateChunkedRangeReads(
	ranges []kvS3ByteRange,
	objectSize, chunk int64,
	maxRun int64,
) (gets, bytes int64) {
	if chunk <= 0 || objectSize <= 0 {
		return 0, 0
	}
	chunks := (objectSize + chunk - 1) / chunk
	cached := make(map[int64]bool)
	lastFetched, run := int64(-2), int64(1)

	fetch := func(first, last int64) {
		if maxRun > 1 {
			if first == lastFetched+1 {
				run = min(run*2, maxRun)
			} else {
				run = 1
			}
			last = min(max(last, first+run-1), chunks-1)
		}
		for k := first; k <= last; k++ {
			cached[k] = true
		}
		gets++
		bytes += min((last+1)*chunk, objectSize) - first*chunk
		lastFetched = last
	}

	for _, r := range ranges {
		if r.length <= 0 {
			continue
		}
		first := r.offset / chunk
		last := min((r.offset+r.length-1)/chunk, chunks-1)
		runStart := int64(-1)
		for k := first; k <= last; k++ {
			if cached[k] {
				if runStart >= 0 {
					fetch(runStart, k-1)
					runStart = -1
				}
				continue
			}
			if runStart < 0 {
				runStart = k
			}
		}
		if runStart >= 0 {
			fetch(runStart, last)
		}
	}
	return gets, bytes
}

func TestSimulateChunkedRangeReads(t *testing.T) {
	const chunk = 100
	cases := []struct {
		name      string
		ranges    []kvS3ByteRange
		size      int64
		maxRun    int64
		wantGets  int64
		wantBytes int64
	}{
		{
			name:     "reads inside one chunk share one GET",
			ranges:   []kvS3ByteRange{{0, 10}, {10, 10}, {90, 10}},
			size:     1000,
			maxRun:   1,
			wantGets: 1, wantBytes: 100,
		},
		{
			name:     "read across a boundary fetches both chunks in one GET",
			ranges:   []kvS3ByteRange{{95, 10}},
			size:     1000,
			maxRun:   1,
			wantGets: 1, wantBytes: 200,
		},
		{
			name:     "last chunk is clamped to the object size",
			ranges:   []kvS3ByteRange{{950, 30}},
			size:     980,
			maxRun:   1,
			wantGets: 1, wantBytes: 80,
		},
		{
			name: "sequential misses double the run up to the cap",
			// chunks 0, 1, 3, 7 miss; runs of 1, 2, 4, 4 cover chunks 0..10.
			ranges:   []kvS3ByteRange{{0, 1}, {100, 1}, {300, 1}, {700, 1}, {1000, 1}},
			size:     2000,
			maxRun:   4,
			wantGets: 4, wantBytes: 1100,
		},
		{
			name:     "a jump resets the run",
			ranges:   []kvS3ByteRange{{0, 1}, {100, 1}, {1500, 1}},
			size:     2000,
			maxRun:   4,
			wantGets: 3, wantBytes: 400,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gets, bytes := simulateChunkedRangeReads(tc.ranges, tc.size, chunk, tc.maxRun)
			if gets != tc.wantGets || bytes != tc.wantBytes {
				t.Fatalf("gets=%d bytes=%d, want gets=%d bytes=%d",
					gets, bytes, tc.wantGets, tc.wantBytes)
			}
		})
	}
}

// BenchmarkFakeS3_KVReaderChunkedRangeRead compares exact block range reads
// (chunk=0KiB) with chunked range reads on one 64 MiB SST. Each ranged GET is
// delayed to model object storage: 20 ms plus transfer at 100 MB/s. Every
// workload starts with an empty block cache, so ns/op is cold-read latency.
func BenchmarkFakeS3_KVReaderChunkedRangeRead(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	for _, blockKiB := range []int{4, 16} {
		for _, chunkKiB := range []int64{0, 64, 128, 256} {
			b.Run(fmt.Sprintf("block=%dKiB/chunk=%dKiB", blockKiB, chunkKiB), func(b *testing.B) {
				f := prepareKVBlockSizeFixture(b, ctx, 64<<20, sstWriterOptions{
					BlockSize:       blockKiB << 10,
					BloomBitsPerKey: 12,
					Compression:     "snappy",
				}, chunkKiB<<10)
				key := kvLeveledBenchmarkKey(f.records / 2)
				// Load the sidecar Bloom before adding latency; cold Gets
				// then measure only SST reads.
				assertKVBlockSizeGet(b, ctx, f, key)
				waitKVReaderBenchmarkCache(f.reader)
				f.counts.sstDelay = 20 * time.Millisecond
				f.counts.sstBytesPerSecond = 100e6

				b.Run("get/cold", func(b *testing.B) {
					runKVBlockSizeCold(b, f, func() { assertKVBlockSizeGet(b, ctx, f, key) })
				})
				for _, limit := range []int{100, 1_000, 10_000} {
					b.Run(fmt.Sprintf("scan-%d/cold", limit), func(b *testing.B) {
						runKVBlockSizeCold(b, f, func() {
							assertKVReaderBenchmarkScanLimitRange(b, ctx, f.reader, f.state, key, nil, limit)
						})
					})
				}
			})
		}
	}
}

// BenchmarkFakeS3_KVReaderRangeVsWholeBySSTSize measures, for SSTs of several
// sizes, whether range reads or whole-SST downloads serve reads better from a
// cold start. Each ranged GET is delayed to model object storage: 20 ms plus
// transfer at 100 MB/s. Before every iteration the range reader's block and
// metadata caches and the whole reader's file cache are emptied, so each
// workload includes first access to the SST; Bloom filters stay loaded in
// both modes.
func BenchmarkFakeS3_KVReaderRangeVsWholeBySSTSize(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	sizes := []struct {
		bytes    int
		blockKiB int
	}{
		{256 << 10, 4}, {1 << 20, 4}, {4 << 20, 4}, {16 << 20, 4}, {64 << 20, 16},
	}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("sst=%dKiB", size.bytes>>10), func(b *testing.B) {
			f := prepareKVBlockSizeFixture(b, ctx, size.bytes, sstWriterOptions{
				BlockSize:       size.blockKiB << 10,
				BloomBitsPerKey: 12,
				Compression:     "snappy",
			}, 128<<10)
			f = f.withMetaOffset(f.meta.MetaOffset)
			for _, reader := range []*Reader{f.reader, f.whole} {
				assertKVReaderBenchmarkManifestGet(
					b, ctx, reader, f.state, kvLeveledBenchmarkKey(0), true, kvBlockSizeValueBytes)
				waitKVReaderBenchmarkCache(reader)
			}
			f.counts.sstDelay = 20 * time.Millisecond
			f.counts.sstBytesPerSecond = 100e6

			modes := []struct {
				name   string
				reader *Reader
				reset  func()
			}{
				{"range", f.reader, func() {
					waitKVReaderBenchmarkCache(f.reader)
					f.reader.blockCache.Clear()
					f.reader.metaCache.clear()
				}},
				{"whole", f.whole, f.whole.clearSSTCache},
			}
			for _, mode := range modes {
				for _, gets := range []int{1, 10, 100} {
					b.Run(fmt.Sprintf("%s/gets=%d", mode.name, gets), func(b *testing.B) {
						runKVRangeVsWholeCold(b, f, mode.reset, func() {
							for i := 0; i < gets; i++ {
								key := kvLeveledBenchmarkKey((i*f.records/gets + f.records/(2*gets)) % f.records)
								assertKVReaderBenchmarkManifestGet(
									b, ctx, mode.reader, f.state, key, true, kvBlockSizeValueBytes)
							}
						})
					})
				}
				b.Run(mode.name+"/scan-all", func(b *testing.B) {
					runKVRangeVsWholeCold(b, f, mode.reset, func() {
						assertKVReaderBenchmarkScanLimitRange(
							b, ctx, mode.reader, f.state, nil, nil, f.records)
					})
				})
			}
		})
	}
}

// runKVRangeVsWholeCold runs op after reset each iteration and reports its
// latency, requests and downloaded bytes.
func runKVRangeVsWholeCold(b *testing.B, f kvBlockSizeFixture, reset, op func()) {
	b.Helper()
	var gets, bytes int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		reset()
		f.counts.reset()
		b.StartTimer()
		op()
		b.StopTimer()
		gets += f.counts.ssts.Load()
		bytes += f.counts.rangeBytes.Load()
	}
	iterations := float64(b.N)
	b.ReportMetric(float64(b.Elapsed().Milliseconds())/iterations, "ms/op")
	b.ReportMetric(float64(gets)/iterations, "GETs/op")
	b.ReportMetric(float64(bytes)/iterations/(1<<10), "KiB/op")
}
