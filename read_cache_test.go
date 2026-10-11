package isledb

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/cacheclient"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestReaderOpenReadsManifestPagesThroughTheReadCache(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-read-cache-pages")
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
	// Each flush is a manifest entry; past the 64 kept in CURRENT, entries
	// move to pages, which a reader's open replays.
	const commits = 80
	for i := range commits {
		if _, err := writer.Put(ctx, fmt.Appendf(nil, "key-%03d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	server := readCacheServer(t, store)
	open := func(url string) *ReaderMetrics {
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
		if got, found, err := reader.Get(ctx, []byte("key-000")); err != nil || !found || string(got) != "value" {
			t.Fatalf("Get(key-000): %q, %t, %v", got, found, err)
		}
		return metrics
	}

	metrics := open(server.URL)
	if server.pages.Load() == 0 {
		t.Fatal("opening replayed no manifest page through the cache")
	}
	if n := fallbacks(metrics); n != 0 {
		t.Fatalf("%v reads fell back with the cache up: a page's size is wrong", n)
	}

	server.Close()
	metrics = open(server.URL)
	if n := testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(cacheclient.ReasonUnavailable)); n == 0 {
		t.Fatal("no fallback counted with the cache down")
	}
}

func TestManifestSnapshotsThroughTheReadCache(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-read-cache-snapshot")
	defer func() { _ = store.Close() }()
	path := store.ManifestSnapshotPath("s1")
	snapshot := bytes.Repeat([]byte("snapshot"), 50_000)
	if _, err := store.Write(ctx, path, snapshot); err != nil {
		t.Fatal(err)
	}
	server := readCacheServer(t, store)
	read := func(url string, size uint64) (*ReaderMetrics, []byte) {
		t.Helper()
		client, err := cacheclient.New(cacheclient.Options{URL: url, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		metrics := DefaultReaderMetrics(nil)
		storage := newManifestThroughCache(manifest.NewBlobStoreBackend(store), &readThroughCache{client: client, m: metrics})
		data, err := storage.(manifest.SizedReader).ReadSnapshotSized(ctx, path, size)
		if err != nil {
			t.Fatal(err)
		}
		return metrics, data
	}

	metrics, data := read(server.URL, uint64(len(snapshot)))
	if !bytes.Equal(data, snapshot) || testutil.ToFloat64(metrics.ReadCacheReads) != 1 {
		t.Fatalf("through the cache: %d bytes, %v cache reads", len(data), testutil.ToFloat64(metrics.ReadCacheReads))
	}
	metrics, data = read(server.URL, uint64(len(snapshot))+1)
	if !bytes.Equal(data, snapshot) || testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(cacheclient.ReasonSizeMismatch)) != 1 {
		t.Fatal("a wrong size did not fall back to storage for the right bytes")
	}
	server.Close()
	if _, data := read(server.URL, uint64(len(snapshot))); !bytes.Equal(data, snapshot) {
		t.Fatal("cache down: wrong bytes")
	}
}
