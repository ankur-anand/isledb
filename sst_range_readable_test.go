package isledb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSSTRangeReadable_ReadAt_CachesBlocks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := blobstore.NewMemory("range-cache")
	t.Cleanup(func() { _ = store.Close() })

	data := []byte("abcdefghijklmnopqrstuvwxyz")
	path := store.SSTPath("sst-1")
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("write sst: %v", err)
	}

	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters:        1024,
		MaxCost:            1 << 20,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(func() { cache.Close() })

	metrics := DefaultReaderMetrics(nil)
	rr := newSSTRangeReadable(store, path, "sst-1", int64(len(data)), cache, nil, metrics)

	buf := make([]byte, 5)
	if err := rr.ReadAt(ctx, buf, 2); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if got := string(buf); got != "cdefg" {
		t.Fatalf("unexpected data: %s", got)
	}

	cache.Wait()
	if err := store.Delete(ctx, path); err != nil {
		t.Fatalf("delete sst: %v", err)
	}

	buf2 := make([]byte, 5)
	if err := rr.ReadAt(ctx, buf2, 2); err != nil {
		t.Fatalf("ReadAt cached: %v", err)
	}
	if got := string(buf2); got != "cdefg" {
		t.Fatalf("unexpected cached data: %s", got)
	}

	if got := testutil.ToFloat64(metrics.SSTRangeBlockCacheMisses); got != 1 {
		t.Fatalf("sst_range_block_cache_misses_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeBlockCacheHits); got != 1 {
		t.Fatalf("sst_range_block_cache_hits_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadTotal); got != 1 {
		t.Fatalf("sst_range_read_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadErrors); got != 0 {
		t.Fatalf("sst_range_read_errors_total mismatch: got=%v want=0", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadBytes); got != 5 {
		t.Fatalf("sst_range_read_bytes_total mismatch: got=%v want=5", got)
	}
}

func TestSSTRangeReadable_ReadAt_CoalescesConcurrentCacheMisses(t *testing.T) {
	const callers = 32

	ctx := context.Background()
	var remoteReads atomic.Int64
	bucketURL := setupFakeS3BucketURLWithObserver(t, func(request *http.Request) {
		if request.Method != http.MethodGet {
			return
		}
		remoteReads.Add(1)
		// Keep the first range request in flight until every caller has had a
		// chance to miss the cache and join the same load.
		time.Sleep(20 * time.Millisecond)
	})
	store, err := blobstore.Open(ctx, bucketURL, "range-singleflight")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	data := []byte("abcdefghijklmnopqrstuvwxyz")
	path := store.SSTPath("sst-shared")
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("write sst: %v", err)
	}

	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters:        1024,
		MaxCost:            1 << 20,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(func() { cache.Close() })

	metrics := DefaultReaderMetrics(nil)
	loads := &coalescedLoadGroup{}
	rr := newSSTRangeReadable(
		store, path, "sst-shared", int64(len(data)), cache, loads, metrics)

	start := make(chan struct{})
	errs := make(chan error, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for range callers {
		go func() {
			defer group.Done()
			<-start
			buf := make([]byte, 5)
			if err := rr.ReadAt(ctx, buf, 2); err != nil {
				errs <- err
				return
			}
			if got := string(buf); got != "cdefg" {
				errs <- errors.New("unexpected range data: " + got)
				return
			}
			errs <- nil
		}()
	}
	close(start)
	group.Wait()
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
	}

	if got := remoteReads.Load(); got != 1 {
		t.Fatalf("remote range reads=%d want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadTotal); got != 1 {
		t.Fatalf("sst_range_read_total=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeBlockCacheMisses); got != callers {
		t.Fatalf("sst_range_block_cache_misses_total=%v want=%d", got, callers)
	}
}

func TestSSTRangeReadable_ReadHandle_ReadsBeforeLogicalSSTEnd(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("range-read-before")
	t.Cleanup(func() { _ = store.Close() })

	const logicalSize = 1 << 20
	data := bytes.Repeat([]byte("s"), logicalSize)
	data = append(data, []byte("BLOOM-CONTAINER")...)
	path := store.SSTPath("sst-read-before")
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("write sst: %v", err)
	}

	metrics := DefaultReaderMetrics(nil)
	rr := newSSTRangeReadable(store, path, "sst-read-before", logicalSize, nil, nil, metrics)
	handle := rr.NewReadHandle(objstorage.ReadBeforeForIndexAndFilter)
	t.Cleanup(func() { _ = handle.Close() })

	footer := make([]byte, 61)
	if err := handle.ReadAt(ctx, footer, logicalSize-int64(len(footer))); err != nil {
		t.Fatalf("read footer: %v", err)
	}
	if !bytes.Equal(footer, bytes.Repeat([]byte("s"), len(footer))) {
		t.Fatal("footer read crossed into the appended Bloom container")
	}

	// This second request lies inside the retained 32 KiB window and must not
	// issue another store read.
	metadata := make([]byte, 100)
	if err := handle.ReadAt(ctx, metadata, logicalSize-1024); err != nil {
		t.Fatalf("read buffered metadata: %v", err)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadTotal); got != 1 {
		t.Fatalf("range reads after buffered request=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadBytes); got != 32<<10 {
		t.Fatalf("range bytes after buffered request=%v want=%d", got, 32<<10)
	}

	// A request outside the retained tail falls back to an exact range read.
	prefix := make([]byte, 16)
	if err := handle.ReadAt(ctx, prefix, 0); err != nil {
		t.Fatalf("read outside buffer: %v", err)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadTotal); got != 2 {
		t.Fatalf("range reads after fallback=%v want=2", got)
	}
}

func TestSSTRangeReadHandle_DoesNotExposeAppendedSuffix(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("range-suffix-isolation")
	t.Cleanup(func() { _ = store.Close() })

	logical := []byte("logical-sst-payload")
	physical := append(append([]byte(nil), logical...), []byte("SECRET-BLOOM-SUFFIX")...)
	path := store.SSTPath("sst-suffix-isolation")
	if _, err := store.Write(ctx, path, physical); err != nil {
		t.Fatalf("write sst: %v", err)
	}

	rr := newSSTRangeReadable(
		store, path, "sst-suffix-isolation", int64(len(logical)), nil, nil, DefaultReaderMetrics(nil))
	handle := rr.NewReadHandle(objstorage.ReadBeforeForIndexAndFilter)
	t.Cleanup(func() { _ = handle.Close() })

	crossing := bytes.Repeat([]byte{'?'}, 8)
	before := append([]byte(nil), crossing...)
	if err := handle.ReadAt(ctx, crossing, int64(len(logical)-4)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("cross-boundary read error=%v want io.ErrUnexpectedEOF", err)
	}
	if !bytes.Equal(crossing, before) {
		t.Fatalf("cross-boundary read modified destination: got=%q want=%q", crossing, before)
	}

	suffix := bytes.Repeat([]byte{'?'}, 6)
	before = append(before[:0], suffix...)
	if err := handle.ReadAt(ctx, suffix, int64(len(logical))); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("physical-suffix read error=%v want io.ErrUnexpectedEOF", err)
	}
	if !bytes.Equal(suffix, before) {
		t.Fatalf("physical-suffix read modified destination: got=%q want=%q", suffix, before)
	}

	tail := make([]byte, 4)
	if err := handle.ReadAt(ctx, tail, int64(len(logical)-len(tail))); err != nil {
		t.Fatalf("valid logical tail read: %v", err)
	}
	if want := logical[len(logical)-len(tail):]; !bytes.Equal(tail, want) {
		t.Fatalf("logical tail=%q want=%q", tail, want)
	}
}

func TestSSTRangeReadHandle_CallerCannotMutateCachedBuffer(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("range-buffer-isolation")
	t.Cleanup(func() { _ = store.Close() })

	const logicalSize = 64 << 10
	logical := make([]byte, logicalSize)
	for i := range logical {
		logical[i] = byte(i)
	}
	path := store.SSTPath("sst-buffer-isolation")
	if _, err := store.Write(ctx, path, logical); err != nil {
		t.Fatalf("write sst: %v", err)
	}

	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters:        1024,
		MaxCost:            1 << 20,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(cache.Close)

	loads := &coalescedLoadGroup{}
	rr := newSSTRangeReadable(
		store, path, "sst-buffer-isolation", logicalSize, cache, loads, DefaultReaderMetrics(nil))
	const readBytes = 61
	offset := int64(logicalSize - readBytes)
	want := append([]byte(nil), logical[offset:]...)

	firstHandle := rr.NewReadHandle(objstorage.ReadBeforeForNewReader)
	first := make([]byte, readBytes)
	if err := firstHandle.ReadAt(ctx, first, offset); err != nil {
		t.Fatalf("first read: %v", err)
	}
	for i := range first {
		first[i] = 0xff
	}
	cache.Wait()
	if err := store.Delete(ctx, path); err != nil {
		t.Fatalf("delete origin SST: %v", err)
	}

	secondHandle := rr.NewReadHandle(objstorage.ReadBeforeForNewReader)
	t.Cleanup(func() { _ = secondHandle.Close() })
	second := make([]byte, readBytes)
	if err := secondHandle.ReadAt(ctx, second, offset); err != nil {
		t.Fatalf("cached read after deleting origin: %v", err)
	}
	if !bytes.Equal(second, want) {
		t.Fatalf("caller mutation reached cached buffer: got=%x want=%x", second, want)
	}

	concrete := firstHandle.(*sstRangeReadHandle)
	if len(concrete.buffer) == 0 {
		t.Fatal("first handle retained no read-before buffer")
	}
	if err := firstHandle.Close(); err != nil {
		t.Fatalf("close first handle: %v", err)
	}
	if concrete.buffer != nil || concrete.readable != nil {
		t.Fatal("Close retained the cached buffer or readable")
	}
	if err := firstHandle.ReadAt(ctx, make([]byte, 1), 0); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("read after Close error=%v want io.ErrClosedPipe", err)
	}
	loads.Close(ErrReaderClosed)
}

func TestRangeReadBeforeSize(t *testing.T) {
	tests := []struct {
		name      string
		sstSize   int64
		requested objstorage.ReadBeforeSize
		want      int64
	}{
		{name: "disabled", sstSize: 4 << 20, requested: objstorage.NoReadBefore, want: 0},
		{name: "tiny SST", sstSize: 1024, requested: objstorage.ReadBeforeForIndexAndFilter, want: 1024},
		{name: "4 MiB", sstSize: 4 << 20, requested: objstorage.ReadBeforeForIndexAndFilter, want: 32 << 10},
		{name: "8 MiB", sstSize: 8 << 20, requested: objstorage.ReadBeforeForIndexAndFilter, want: 64 << 10},
		{name: "16 MiB", sstSize: 16 << 20, requested: objstorage.ReadBeforeForIndexAndFilter, want: 128 << 10},
		{name: "32 MiB", sstSize: 32 << 20, requested: objstorage.ReadBeforeForIndexAndFilter, want: 256 << 10},
		{name: "64 MiB", sstSize: 64 << 20, requested: objstorage.ReadBeforeForIndexAndFilter, want: 512 << 10},
		{name: "Pebble hint caps table", sstSize: 64 << 20, requested: objstorage.ReadBeforeForNewReader, want: 32 << 10},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := rangeReadBeforeSize(test.sstSize, test.requested); got != test.want {
				t.Fatalf("rangeReadBeforeSize(%d, %d)=%d want=%d",
					test.sstSize, test.requested, got, test.want)
			}
		})
	}
}

func TestSSTRangeReadable_ReadAt_NoCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := blobstore.NewMemory("range-nocache")
	t.Cleanup(func() { _ = store.Close() })

	data := []byte("abcdefghijklmnopqrstuvwxyz")
	path := store.SSTPath("sst-2")
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("write sst: %v", err)
	}

	metrics := DefaultReaderMetrics(nil)
	rr := newSSTRangeReadable(store, path, "sst-2", int64(len(data)), nil, nil, metrics)

	buf := make([]byte, 3)
	if err := rr.ReadAt(ctx, buf, 1); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if got := string(buf); got != "bcd" {
		t.Fatalf("unexpected data: %s", got)
	}

	if got := testutil.ToFloat64(metrics.SSTRangeBlockCacheMisses); got != 0 {
		t.Fatalf("sst_range_block_cache_misses_total mismatch: got=%v want=0", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeBlockCacheHits); got != 0 {
		t.Fatalf("sst_range_block_cache_hits_total mismatch: got=%v want=0", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadTotal); got != 1 {
		t.Fatalf("sst_range_read_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadErrors); got != 0 {
		t.Fatalf("sst_range_read_errors_total mismatch: got=%v want=0", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadBytes); got != 3 {
		t.Fatalf("sst_range_read_bytes_total mismatch: got=%v want=3", got)
	}
}

func TestSSTRangeReadable_ReadAt_OutOfBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := blobstore.NewMemory("range-oob")
	t.Cleanup(func() { _ = store.Close() })

	data := []byte("abcdefghijklmnopqrstuvwxyz")
	path := store.SSTPath("sst-3")
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("write sst: %v", err)
	}

	metrics := DefaultReaderMetrics(nil)
	rr := newSSTRangeReadable(store, path, "sst-3", int64(len(data)), nil, nil, metrics)

	buf := make([]byte, 5)
	if err := rr.ReadAt(ctx, buf, int64(len(data))-2); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}

	if err := rr.ReadAt(ctx, buf, -1); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF for negative offset, got %v", err)
	}

	if got := testutil.ToFloat64(metrics.SSTRangeReadTotal); got != 0 {
		t.Fatalf("sst_range_read_total mismatch: got=%v want=0", got)
	}
}

func TestSSTRangeReadable_ReadAt_MetricsReadError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := blobstore.NewMemory("range-metrics-error")
	t.Cleanup(func() { _ = store.Close() })

	data := []byte("abcdefghijklmnopqrstuvwxyz")
	path := store.SSTPath("sst-4")
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("write sst: %v", err)
	}
	if err := store.Delete(ctx, path); err != nil {
		t.Fatalf("delete sst: %v", err)
	}

	metrics := DefaultReaderMetrics(nil)
	rr := newSSTRangeReadable(store, path, "sst-4", int64(len(data)), nil, nil, metrics)

	buf := make([]byte, 4)
	if err := rr.ReadAt(ctx, buf, 0); err == nil {
		t.Fatalf("expected range read error")
	}

	if got := testutil.ToFloat64(metrics.SSTRangeReadTotal); got != 1 {
		t.Fatalf("sst_range_read_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadErrors); got != 1 {
		t.Fatalf("sst_range_read_errors_total mismatch: got=%v want=1", got)
	}
	if got := testutil.ToFloat64(metrics.SSTRangeReadBytes); got != 0 {
		t.Fatalf("sst_range_read_bytes_total mismatch: got=%v want=0", got)
	}
}
