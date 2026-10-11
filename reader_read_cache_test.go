package isledb

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/cacheclient"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type cacheServer struct {
	*httptest.Server
	objects        *readcache.Objects
	noCache, pages atomic.Int32
}

// readCacheServer serves store's database prefix through a read cache and
// counts the requests asking it not to fill, and those for manifest pages.
func readCacheServer(t *testing.T, store *blobstore.Store) *cacheServer {
	t.Helper()
	disk, err := diskcache.Open(diskcache.Options{Dir: t.TempDir(), MaxBytes: 256 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	objects, err := readcache.NewObjects(store, disk, readcache.ObjectsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := readcache.NewHandler(objects, map[string]*readcache.Relay{
		store.Prefix(): readcache.NewRelay(manifest.NewBlobStoreBackend(store), time.Hour),
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	s := &cacheServer{objects: objects}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(readcache.HeaderNoCache) != "" {
			s.noCache.Add(1)
		}
		if strings.Contains(r.URL.Path, "/manifest/pages/") {
			s.pages.Add(1)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func fallbacks(m *ReaderMetrics) float64 {
	var n float64
	for _, reason := range []string{cacheclient.ReasonTimeout, cacheclient.ReasonUnavailable, cacheclient.ReasonNotFound,
		cacheclient.ReasonSizeMismatch, cacheclient.ReasonRefused, cacheclient.ReasonInvalid, cacheclient.ReasonBypassed} {
		n += testutil.ToFloat64(m.ReadCacheFallbacks.WithLabelValues(reason))
	}
	return n
}

func TestReaderReadsThroughTheReadCacheAndFallsBack(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-read-cache")
	defer func() { _ = store.Close() }()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	values := map[string][]byte{}
	put := func(n, size int) {
		for i := range n {
			key := fmt.Sprintf("key-%06d", len(values)+i)
			value := make([]byte, size)
			_, _ = rand.Read(value)
			values[key] = value
			if _, err := writer.Put(ctx, []byte(key), value); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	put(2500, 4<<10) // about 10 MiB: a large SST, read by metadata, Bloom and chunks
	put(20, 100)     // a small SST, read whole
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	server := readCacheServer(t, store)
	read := func(url string) *ReaderMetrics {
		t.Helper()
		metrics := DefaultReaderMetrics(nil)
		opts := DefaultReaderOpenOptions(t.TempDir())
		opts.ReadCache = ReadCacheOptions{URL: url, Timeout: time.Second}
		opts.Metrics = metrics
		reader, err := db.OpenReader(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		// The scan first, while nothing is on the reader's disk: past its
		// first 64 KiB per SST it reads privately, through the cache.
		kvs, err := reader.Scan(ctx, nil, nil)
		if err != nil || len(kvs) != len(values) {
			t.Fatalf("Scan: %d entries, %v; want %d", len(kvs), err, len(values))
		}
		for key, want := range values {
			got, found, err := reader.Get(ctx, []byte(key))
			if err != nil || !found || !bytes.Equal(got, want) {
				t.Fatalf("Get(%s): found %t, %v", key, found, err)
			}
		}
		return metrics
	}

	metrics := read(server.URL)
	if n := testutil.ToFloat64(metrics.ReadCacheReads); n == 0 {
		t.Fatal("no SST read went through the read cache")
	}
	if n := fallbacks(metrics); n != 0 {
		t.Fatalf("%v reads fell back to storage with the cache up: the size sent for some SST is wrong", n)
	}
	if n := testutil.ToFloat64(metrics.SSTRangeReadTotal); n != 0 {
		t.Fatalf("%v SST reads went to storage with the cache up, want 0", n)
	}
	if server.objects.Stats().StorageReads == 0 {
		t.Fatal("the cache read nothing from storage")
	}
	if server.noCache.Load() == 0 {
		t.Fatal("the scan's private reads did not ask the cache not to fill")
	}

	server.Close()
	metrics = read(server.URL)
	if n := testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(cacheclient.ReasonUnavailable)); n == 0 {
		t.Fatal("no fallback counted with the cache down")
	}
	if n := testutil.ToFloat64(metrics.SSTRangeReadTotal); n == 0 {
		t.Fatal("no SST read reached storage with the cache down")
	}
}

func TestReaderRefusesABadReadCacheURL(t *testing.T) {
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.ReadCache.URL = "isledb-cache:8090"
	if _, err := readerOptionsFromPublic(opts); err == nil {
		t.Fatal("a read cache URL without a scheme was accepted")
	}
}
