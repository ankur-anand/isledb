package isledb

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"golang.org/x/sync/singleflight"
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
	loads := &singleflight.Group{}
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
