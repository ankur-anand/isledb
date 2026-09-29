package isledb

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/dgraph-io/ristretto/v2"
)

// chunkTestStore serves one object from fake S3 and records each ranged GET.
type chunkTestStore struct {
	store  *blobstore.Store
	path   string
	data   []byte
	gets   atomic.Int64
	mu     sync.Mutex
	ranges []string
	delay  time.Duration
	// loads is shared by every readable, as the Reader shares one group.
	loads coalescedLoadGroup
}

func newChunkTestStore(t *testing.T, size int) *chunkTestStore {
	t.Helper()
	s := &chunkTestStore{data: make([]byte, size)}
	for i := range s.data {
		s.data[i] = byte(i*31 + i/251)
	}
	bucketURL := setupFakeS3BucketURLWithObserver(t, func(request *http.Request) {
		if request.Method != http.MethodGet || request.Header.Get("Range") == "" {
			return
		}
		s.gets.Add(1)
		s.mu.Lock()
		s.ranges = append(s.ranges, request.Header.Get("Range"))
		s.mu.Unlock()
		if s.delay > 0 {
			time.Sleep(s.delay)
		}
	})
	store, err := blobstore.Open(context.Background(), bucketURL, fmt.Sprintf("chunks-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	s.store = store
	s.path = store.SSTPath("chunked-sst")
	if _, err := store.Write(context.Background(), s.path, s.data); err != nil {
		t.Fatalf("write object: %v", err)
	}
	return s
}

func (s *chunkTestStore) reset() {
	s.gets.Store(0)
	s.mu.Lock()
	s.ranges = nil
	s.mu.Unlock()
}

// readable opens the object with a read-ahead fixed at chunk bytes.
func (s *chunkTestStore) readable(t *testing.T, cache *ristretto.Cache[string, []byte], chunk int64) *sstRangeReadable {
	t.Helper()
	return s.growingReadable(t, cache, chunk, chunk)
}

// growingReadable opens the object with a read-ahead that grows from aheadMin
// to aheadMax bytes.
func (s *chunkTestStore) growingReadable(
	t *testing.T, cache *ristretto.Cache[string, []byte], aheadMin, aheadMax int64,
) *sstRangeReadable {
	t.Helper()
	r := newSSTRangeReadable(s.store, s.path, "chunked-sst", int64(len(s.data)),
		cache, &s.loads, DefaultReaderMetrics(nil))
	r.useReadAhead(aheadMin, aheadMax)
	return r
}

func newChunkTestCache(t *testing.T) *ristretto.Cache[string, []byte] {
	t.Helper()
	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: 1 << 12, MaxCost: 64 << 20, BufferItems: 64, IgnoreInternalCost: true,
	})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(cache.Close)
	return cache
}

func readChunked(t *testing.T, r *sstRangeReadable, off, length int64) []byte {
	t.Helper()
	p := make([]byte, length)
	if err := r.ReadAt(context.Background(), p, off); err != nil {
		t.Fatalf("ReadAt(%d, %d): %v", off, length, err)
	}
	return p
}

// handleRead reads through one iterator's read handle, as Pebble does for
// data blocks.
func handleRead(t *testing.T, h objstorage.ReadHandle, s *chunkTestStore, off, length int64) {
	t.Helper()
	p := make([]byte, length)
	if err := h.ReadAt(context.Background(), p, off); err != nil {
		t.Fatalf("ReadAt(%d, %d): %v", off, length, err)
	}
	if !bytes.Equal(p, s.data[off:off+length]) {
		t.Fatalf("bytes at %d+%d differ", off, length)
	}
}

func (s *chunkTestStore) wantRanges(t *testing.T, want ...string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ranges) != len(want) {
		t.Fatalf("ranges = %v, want %v", s.ranges, want)
	}
	for i := range want {
		if s.ranges[i] != want[i] {
			t.Fatalf("ranges = %v, want %v", s.ranges, want)
		}
	}
}

// TestSSTRangeHandle_PointReadUsesSharedCache checks that a lone read, as a
// point lookup makes, fetches exactly its block and leaves it in the shared
// cache for the next lookup.
func TestSSTRangeHandle_PointReadUsesSharedCache(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	cache := newChunkTestCache(t)
	handleRead(t, s.readable(t, cache, 1000).NewReadHandle(objstorage.NoReadBefore), s, 500, 100)
	s.wantRanges(t, "bytes=500-599")

	cache.Wait()
	s.reset()
	handleRead(t, s.readable(t, cache, 1000).NewReadHandle(objstorage.NoReadBefore), s, 500, 100)
	s.wantRanges(t)
}

// TestSSTRangeHandle_ScanReadsAheadInChunks reads consecutive blocks of
// varying length, as a scan does: the first block is fetched exactly, the
// scan then reads ahead one chunk at a time, and a block crossing into the
// next chunk fetches only that chunk.
func TestSSTRangeHandle_ScanReadsAheadInChunks(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	h := s.readable(t, newChunkTestCache(t), 1000).NewReadHandle(objstorage.NoReadBefore)
	off := int64(0)
	for _, length := range []int64{120, 95, 130, 110, 101, 99, 125, 105, 90, 150} {
		handleRead(t, h, s, off, length)
		off += length
	}
	s.wantRanges(t, "bytes=0-119", "bytes=0-999", "bytes=1000-1999")
}

// TestSSTRangeHandle_ReadAheadDoubles scans 100-byte blocks: after the
// first exact read, each read-ahead doubles from 1000 bytes up to the 4000
// byte cap, starting where the previous one ended.
func TestSSTRangeHandle_ReadAheadDoubles(t *testing.T) {
	s := newChunkTestStore(t, 20_000)
	h := s.growingReadable(t, newChunkTestCache(t), 1000, 4000).NewReadHandle(objstorage.NoReadBefore)
	for off := int64(0); off < 15_000; off += 100 {
		handleRead(t, h, s, off, 100)
	}
	s.wantRanges(t, "bytes=0-99", "bytes=0-999", "bytes=1000-2999",
		"bytes=3000-6999", "bytes=7000-10999", "bytes=11000-14999")
}

// TestSSTRangeHandle_JumpResetsReadAhead seeks elsewhere after the read-ahead
// has grown: the new scan starts again from the smallest read-ahead.
func TestSSTRangeHandle_JumpResetsReadAhead(t *testing.T) {
	s := newChunkTestStore(t, 20_000)
	h := s.growingReadable(t, newChunkTestCache(t), 1000, 4000).NewReadHandle(objstorage.NoReadBefore)
	for off := int64(0); off < 3000; off += 100 {
		handleRead(t, h, s, off, 100)
	}
	s.reset()
	handleRead(t, h, s, 17_000, 100)
	handleRead(t, h, s, 17_100, 100)
	s.wantRanges(t, "bytes=17000-17099", "bytes=17000-17999")
}

// TestSSTRangeHandle_ReadAheadStopsAtDataEndWhileGrowing lets a growing read-ahead reach
// the metadata region: the fetch is cut where the data ends.
func TestSSTRangeHandle_ReadAheadStopsAtDataEndWhileGrowing(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	r := s.growingReadable(t, newChunkTestCache(t), 1000, 4000)
	r.useMetaRegion(4500)
	h := r.NewReadHandle(objstorage.NoReadBefore)
	for off := int64(0); off < 4500; off += 100 {
		handleRead(t, h, s, off, 100)
	}
	s.wantRanges(t, "bytes=0-99", "bytes=0-999", "bytes=1000-2999", "bytes=3000-4499")
}

// TestSSTRangeHandle_ReadAheadMaxRoundsToMin checks that a cap which is not a
// multiple of the minimum is rounded down to one, so fetches stay aligned.
func TestSSTRangeHandle_ReadAheadMaxRoundsToMin(t *testing.T) {
	s := newChunkTestStore(t, 20_000)
	h := s.growingReadable(t, newChunkTestCache(t), 1000, 2500).NewReadHandle(objstorage.NoReadBefore)
	for off := int64(0); off < 7000; off += 100 {
		handleRead(t, h, s, off, 100)
	}
	s.wantRanges(t, "bytes=0-99", "bytes=0-999", "bytes=1000-2999",
		"bytes=3000-4999", "bytes=5000-6999")
}

// TestSSTRangeHandle_ScanChunksStayPrivate checks that chunks a scan read
// ahead serve neither a point lookup nor another scan.
func TestSSTRangeHandle_ScanChunksStayPrivate(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	cache := newChunkTestCache(t)
	scan := s.readable(t, cache, 1000).NewReadHandle(objstorage.NoReadBefore)
	handleRead(t, scan, s, 0, 100)
	handleRead(t, scan, s, 100, 100)
	cache.Wait()

	s.reset()
	handleRead(t, s.readable(t, cache, 1000).NewReadHandle(objstorage.NoReadBefore), s, 500, 10)
	other := s.readable(t, cache, 1000).NewReadHandle(objstorage.NoReadBefore)
	handleRead(t, other, s, 600, 100)
	handleRead(t, other, s, 700, 100)
	s.wantRanges(t, "bytes=500-509", "bytes=600-699", "bytes=0-999")
}

// TestSSTRangeHandle_ScanUsesCachedBlockWithoutFillingCache has a scan reach
// a block a point lookup cached: the scan uses it, and reads ahead after it.
func TestSSTRangeHandle_ScanUsesCachedBlockWithoutFillingCache(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	cache := newChunkTestCache(t)
	handleRead(t, s.readable(t, cache, 1000).NewReadHandle(objstorage.NoReadBefore), s, 500, 100)
	cache.Wait()

	s.reset()
	scan := s.readable(t, cache, 1000).NewReadHandle(objstorage.NoReadBefore)
	handleRead(t, scan, s, 400, 100)
	handleRead(t, scan, s, 500, 100) // cached block
	handleRead(t, scan, s, 600, 100)
	s.wantRanges(t, "bytes=400-499", "bytes=0-999")
}

// TestSSTRangeHandle_JumpReturnsToExactReads seeks elsewhere mid-scan.
func TestSSTRangeHandle_JumpReturnsToExactReads(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	h := s.readable(t, newChunkTestCache(t), 1000).NewReadHandle(objstorage.NoReadBefore)
	handleRead(t, h, s, 0, 100)
	handleRead(t, h, s, 100, 100)
	handleRead(t, h, s, 3000, 100)
	handleRead(t, h, s, 3100, 100)
	s.wantRanges(t, "bytes=0-99", "bytes=0-999", "bytes=3000-3099", "bytes=3000-3999")
}

// TestSSTRangeHandle_ReadAheadAcrossBoundaryFetchesBothChunks reads a block
// that crosses a chunk boundary right after the scan starts.
func TestSSTRangeHandle_ReadAheadAcrossBoundaryFetchesBothChunks(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	h := s.readable(t, newChunkTestCache(t), 1000).NewReadHandle(objstorage.NoReadBefore)
	handleRead(t, h, s, 900, 50)
	handleRead(t, h, s, 950, 100)
	s.wantRanges(t, "bytes=900-949", "bytes=0-1999")
}

// TestSSTRangeHandle_ReadAheadStopsAtMetaOffset checks that read-ahead never
// reaches into the metadata region, which has its own single request, and
// that metadata reads do not interrupt a scan.
func TestSSTRangeHandle_ReadAheadStopsAtMetaOffset(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	r := s.readable(t, newChunkTestCache(t), 1000)
	r.useMetaRegion(4500)
	h := r.NewReadHandle(objstorage.NoReadBefore)
	handleRead(t, h, s, 4200, 100)
	handleRead(t, h, s, 4600, 50) // metadata
	handleRead(t, h, s, 4300, 100)
	handleRead(t, h, s, 4400, 100)
	s.wantRanges(t, "bytes=4200-4299", "bytes=4500-4999", "bytes=4000-4499")
}

// TestSSTRangeHandle_CacheHitAdvancesScan checks that a block Pebble served
// from its own cache counts toward the scan.
func TestSSTRangeHandle_CacheHitAdvancesScan(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	h := s.readable(t, newChunkTestCache(t), 1000).NewReadHandle(objstorage.NoReadBefore)
	h.RecordCacheHit(context.Background(), 0, 100)
	handleRead(t, h, s, 100, 100)
	s.wantRanges(t, "bytes=0-999")
}

// TestSSTRangeHandle_ReadAheadOffReadsExactBlocks keeps exact reads for
// every read when read-ahead is off.
func TestSSTRangeHandle_ReadAheadOffReadsExactBlocks(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	h := s.readable(t, newChunkTestCache(t), 0).NewReadHandle(objstorage.NoReadBefore)
	handleRead(t, h, s, 0, 100)
	handleRead(t, h, s, 100, 100)
	s.wantRanges(t, "bytes=0-99", "bytes=100-199")
}

// TestSSTRangeHandle_ConcurrentPointMissesShareOneRequest has many point
// lookups miss the same block at once.
func TestSSTRangeHandle_ConcurrentPointMissesShareOneRequest(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	s.delay = 20 * time.Millisecond
	cache := newChunkTestCache(t)

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			h := s.readable(t, cache, 1000).NewReadHandle(objstorage.NoReadBefore)
			p := make([]byte, 100)
			<-start
			if err := h.ReadAt(context.Background(), p, 500); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(p, s.data[500:600]) {
				errs <- fmt.Errorf("bytes differ")
			}
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := s.gets.Load(); got != 1 {
		t.Fatalf("GETs = %d, want one shared request", got)
	}
}

// TestSSTRangeHandle_MixedReadsMatchData runs scans, jumps and metadata reads
// through handles across chunk sizes that do and do not divide the data
// region, comparing every read with the object.
func TestSSTRangeHandle_MixedReadsMatchData(t *testing.T) {
	s := newChunkTestStore(t, 10_007)
	rng := rand.New(rand.NewSource(1))
	for _, ahead := range [][2]int64{
		{1, 1}, {97, 97}, {1000, 1000}, {4096, 4096}, {20_000, 20_000},
		{1, 64}, {97, 1000}, {1000, 8000}, {1000, 2500},
	} {
		r := s.growingReadable(t, newChunkTestCache(t), ahead[0], ahead[1])
		r.useMetaRegion(9000)
		h := r.NewReadHandle(objstorage.NoReadBefore)
		off := int64(0)
		for range 400 {
			length := 1 + rng.Int63n(700)
			switch {
			case rng.Intn(10) == 0: // metadata read
				metaOff := 9000 + rng.Int63n(900)
				handleRead(t, h, s, metaOff, min(length, 10_007-metaOff))
				continue
			case rng.Intn(5) == 0: // jump
				off = rng.Int63n(8999)
			}
			if off+length > 9000 {
				off = rng.Int63n(8000)
			}
			handleRead(t, h, s, off, length)
			off += length
		}
	}
}

// TestReader_ChunkedRangeReadsServeGetsAndScans runs the reader end to end with
// chunked range reads.
func TestReader_ChunkedRangeReadsServeGetsAndScans(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("chunked-reader")
	defer store.Close()
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(), RangeRead: true, BlockCacheSize: 16 << 20,
		RangeReadMinSSTSize: 1, RangeReadAheadMin: 8 << 10, RangeReadAheadMax: 64 << 10,
	})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	const n = 5_000
	entries := make([]internal.MemEntry, n)
	for i := range entries {
		entries[i] = internal.MemEntry{
			Key: kvLeveledBenchmarkKey(i), Seq: uint64(i + 1), Kind: internal.OpPut,
			Value: []byte(fmt.Sprintf("value-%08d-%s", i, bytes.Repeat([]byte("x"), 100))),
		}
	}
	result, err := writeSST(ctx, &sliceSSTIter{entries: entries},
		sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 12, Compression: "snappy"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
		t.Fatalf("store SST: %v", err)
	}
	meta := result.Meta
	meta.Level = 1
	state := &manifestState{Levels: []manifest.Level{{Number: 1, SSTs: []manifest.SSTMeta{meta}}}}

	for _, i := range []int{0, 1, 2_500, n - 1} {
		value, found, err := reader.getWithManifest(ctx, state, kvLeveledBenchmarkKey(i))
		if err != nil || !found || !bytes.Equal(value, entries[i].Value) {
			t.Fatalf("Get(%d) found=%t err=%v", i, found, err)
		}
	}
	kvs, err := reader.scanInternalWithManifest(ctx, state, kvLeveledBenchmarkKey(100), nil, 2_000)
	if err != nil || len(kvs) != 2_000 {
		t.Fatalf("scan rows=%d err=%v", len(kvs), err)
	}
	for j, kv := range kvs {
		if !bytes.Equal(kv.Key, entries[100+j].Key) || !bytes.Equal(kv.Value, entries[100+j].Value) {
			t.Fatalf("scan row %d differs", j)
		}
	}
}
