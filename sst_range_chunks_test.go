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

func (s *chunkTestStore) readable(t *testing.T, cache *ristretto.Cache[string, []byte], chunk int64) *sstRangeReadable {
	t.Helper()
	r := newSSTRangeReadable(s.store, s.path, "chunked-sst", int64(len(s.data)),
		cache, &s.loads, DefaultReaderMetrics(nil))
	r.useChunks(chunk)
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

// TestSSTRangeChunks_SequentialBlocksShareOneRequest reads consecutive
// variable-length blocks, as a scan does, and counts requests.
func TestSSTRangeChunks_SequentialBlocksShareOneRequest(t *testing.T) {
	const chunk = 1000
	s := newChunkTestStore(t, 5000)
	r := s.readable(t, newChunkTestCache(t), chunk)

	// Blocks of varying length laid end to end, crossing a chunk boundary.
	off := int64(0)
	for _, length := range []int64{120, 95, 130, 110, 101, 99, 125, 105, 90, 150} {
		if got := readChunked(t, r, off, length); !bytes.Equal(got, s.data[off:off+length]) {
			t.Fatalf("bytes at %d differ", off)
		}
		off += length
	}
	// 1,125 bytes read span chunks 0 and 1: one request each.
	if got := s.gets.Load(); got != 2 {
		t.Fatalf("GETs = %d (%v), want 2", got, s.ranges)
	}
	if s.ranges[0] != "bytes=0-999" || s.ranges[1] != "bytes=1000-1999" {
		t.Fatalf("ranges = %v", s.ranges)
	}
}

// TestSSTRangeChunks_BoundaryReadFetchesOnlyMissingChunks reads a block that
// crosses a chunk boundary, with and without its first chunk cached.
func TestSSTRangeChunks_BoundaryReadFetchesOnlyMissingChunks(t *testing.T) {
	const chunk = 1000
	s := newChunkTestStore(t, 5000)

	t.Run("neither cached", func(t *testing.T) {
		s.reset()
		r := s.readable(t, newChunkTestCache(t), chunk)
		if got := readChunked(t, r, 950, 100); !bytes.Equal(got, s.data[950:1050]) {
			t.Fatal("bytes differ")
		}
		if s.gets.Load() != 1 || s.ranges[0] != "bytes=0-1999" {
			t.Fatalf("ranges = %v, want one request for both chunks", s.ranges)
		}
	})
	t.Run("first cached", func(t *testing.T) {
		s.reset()
		r := s.readable(t, newChunkTestCache(t), chunk)
		readChunked(t, r, 10, 10)
		if got := readChunked(t, r, 950, 100); !bytes.Equal(got, s.data[950:1050]) {
			t.Fatal("bytes differ")
		}
		if s.gets.Load() != 2 || s.ranges[1] != "bytes=1000-1999" {
			t.Fatalf("ranges = %v, want the second chunk only", s.ranges)
		}
	})
}

// TestSSTRangeChunks_LastChunkStopsAtMetaOffset checks that data chunks never
// reach into the metadata region, which has its own single request.
func TestSSTRangeChunks_LastChunkStopsAtMetaOffset(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	r := s.readable(t, newChunkTestCache(t), 1000)
	r.useMetaRegion(4500)

	if got := readChunked(t, r, 4400, 100); !bytes.Equal(got, s.data[4400:4500]) {
		t.Fatal("data bytes differ")
	}
	if got := readChunked(t, r, 4600, 50); !bytes.Equal(got, s.data[4600:4650]) {
		t.Fatal("metadata bytes differ")
	}
	if len(s.ranges) != 2 || s.ranges[0] != "bytes=4000-4499" || s.ranges[1] != "bytes=4500-4999" {
		t.Fatalf("ranges = %v, want the cut data chunk then the metadata region", s.ranges)
	}
}

// TestSSTRangeChunks_CachedChunksServeOtherReads opens the SST again, as
// another query would, and reads different offsets and lengths inside the
// chunks already cached.
func TestSSTRangeChunks_CachedChunksServeOtherReads(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	cache := newChunkTestCache(t)
	readChunked(t, s.readable(t, cache, 1000), 0, 1500)
	cache.Wait()

	s.reset()
	other := s.readable(t, cache, 1000)
	for _, read := range [][2]int64{{3, 7}, {400, 333}, {999, 2}, {1200, 700}} {
		if got := readChunked(t, other, read[0], read[1]); !bytes.Equal(got, s.data[read[0]:read[0]+read[1]]) {
			t.Fatalf("bytes at %d differ", read[0])
		}
	}
	if got := s.gets.Load(); got != 0 {
		t.Fatalf("GETs = %d (%v), want all reads from cached chunks", got, s.ranges)
	}
}

// TestSSTRangeChunks_ConcurrentMissesShareOneRequest has many callers miss the
// same chunk at once.
func TestSSTRangeChunks_ConcurrentMissesShareOneRequest(t *testing.T) {
	s := newChunkTestStore(t, 5000)
	s.delay = 20 * time.Millisecond
	cache := newChunkTestCache(t)

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 16)
	for i := range 16 {
		wg.Go(func() {
			r := s.readable(t, cache, 1000)
			p := make([]byte, 50)
			<-start
			off := int64(i * 50)
			if err := r.ReadAt(context.Background(), p, off); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(p, s.data[off:off+50]) {
				errs <- fmt.Errorf("bytes at %d differ", off)
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

// TestSSTRangeChunks_RandomReadsMatchData compares many random reads against
// the object, across chunk sizes that do and do not divide its size.
func TestSSTRangeChunks_RandomReadsMatchData(t *testing.T) {
	s := newChunkTestStore(t, 10_007)
	rng := rand.New(rand.NewSource(1))
	for _, chunk := range []int64{1, 97, 1000, 4096, 20_000} {
		r := s.readable(t, newChunkTestCache(t), chunk)
		r.useMetaRegion(9000)
		for range 300 {
			off := rng.Int63n(int64(len(s.data)))
			length := 1 + rng.Int63n(min(int64(len(s.data))-off, 3000))
			if got := readChunked(t, r, off, length); !bytes.Equal(got, s.data[off:off+length]) {
				t.Fatalf("chunk=%d: bytes at %d+%d differ", chunk, off, length)
			}
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
		CacheDir: t.TempDir(), BlockCacheSize: 16 << 20, AllowUnverifiedRangeRead: true,
		RangeReadMinSSTSize: 1, RangeReadChunkSize: 8 << 10,
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
