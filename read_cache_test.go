package isledb

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"net/http"
	"testing"
	"time"

	"strings"

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
	sum := sha256.Sum256(snapshot)
	ref := manifest.ObjectRef{Path: path, EncodedBytes: uint64(len(snapshot)), Checksum: fmt.Sprintf("sha256:%x", sum[:])}
	read := func(url string, ref manifest.ObjectRef) (*ReaderMetrics, []byte) {
		t.Helper()
		client, err := cacheclient.New(cacheclient.Options{URL: url, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		metrics := DefaultReaderMetrics(nil)
		storage := newManifestThroughCache(manifest.NewBlobStoreBackend(store), &readThroughCache{client: client, m: metrics}, store.Prefix())
		data, err := storage.(manifest.RefReader).ReadSnapshotRef(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		return metrics, data
	}

	metrics, data := read(server.URL, ref)
	if !bytes.Equal(data, snapshot) || testutil.ToFloat64(metrics.ReadCacheReads) != 1 {
		t.Fatalf("through the cache: %d bytes, %v cache reads", len(data), testutil.ToFloat64(metrics.ReadCacheReads))
	}
	wrongSize := ref
	wrongSize.EncodedBytes++
	metrics, data = read(server.URL, wrongSize)
	if !bytes.Equal(data, snapshot) || testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(cacheclient.ReasonSizeMismatch)) != 1 {
		t.Fatal("a wrong size did not fall back to storage for the right bytes")
	}
	damaged := func(*http.Request) bool { return true }
	server.damage.Store(&damaged)
	metrics, data = read(server.URL, ref)
	if !bytes.Equal(data, snapshot) || testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(reasonChecksumMismatch)) != 1 {
		t.Fatal("damaged bytes from the cache did not fall back to storage for the right bytes")
	}
	server.Close()
	if _, data := read(server.URL, ref); !bytes.Equal(data, snapshot) {
		t.Fatal("cache down: wrong bytes")
	}
}

// writeLargeAndSmallSSTs writes a large SST, read by metadata, Bloom and
// chunks, and a small one, read whole, and returns what it wrote.
func writeLargeAndSmallSSTs(t *testing.T, ctx context.Context, db *DB) map[string][]byte {
	t.Helper()
	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	values := map[string][]byte{}
	for _, batch := range []struct{ n, size int }{{2500, 4 << 10}, {20, 100}} {
		for range batch.n {
			key := fmt.Sprintf("key-%06d", len(values))
			value := make([]byte, batch.size) // random, so it does not compress
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
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return values
}

func TestReaderRecoversFromDamagedBytesInTheReadCache(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-read-cache-damage")
	defer func() { _ = store.Close() }()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	values := writeLargeAndSmallSSTs(t, ctx, db)

	server := readCacheServer(t, store)
	damaged := func(r *http.Request) bool { return strings.Contains(r.URL.Path, "/sstable/") }
	server.damage.Store(&damaged)
	metrics := DefaultReaderMetrics(nil)
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.ReadCache = ReadCacheOptions{URL: server.URL, Timeout: time.Second}
	opts.Metrics = metrics
	reader, err := db.OpenReader(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	for key, want := range values {
		got, found, err := reader.Get(ctx, []byte(key))
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("Get(%s) with every SST answer from the cache damaged: found %t, %v", key, found, err)
		}
	}
	if n := testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(reasonAfterDamage)); n == 0 {
		t.Error("no read of a damaged SST went to storage")
	}
	if n := testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(reasonChecksumMismatch)); n == 0 {
		t.Error("no damaged whole SST or Bloom filter was read again from storage")
	}
}

func TestReaderOpenRecoversFromDamagedManifestPagesInTheReadCache(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("db-read-cache-damaged-pages")
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
	for i := range 80 {
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
	damaged := func(r *http.Request) bool { return strings.Contains(r.URL.Path, "/manifest/pages/") }
	server.damage.Store(&damaged)
	metrics := DefaultReaderMetrics(nil)
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.ReadCache = ReadCacheOptions{URL: server.URL, Timeout: time.Second}
	opts.Metrics = metrics
	reader, err := db.OpenReader(ctx, opts)
	if err != nil {
		t.Fatalf("open with every manifest page from the cache damaged: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if server.pages.Load() == 0 {
		t.Fatal("no manifest page was read through the cache")
	}
	if n := testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(reasonChecksumMismatch)); n == 0 {
		t.Fatal("no damaged page was read again from storage")
	}
}

func TestDamagedSSTsSkipTheReadCacheForAWhile(t *testing.T) {
	f := &sstFetcher{cache: &readThroughCache{}}
	a, b := sstObject{key: [32]byte{1}}, sstObject{key: [32]byte{2}}
	f.markDamaged(a)
	if !f.recentlyDamaged(a) || f.recentlyDamaged(b) {
		t.Fatal("want only the SST marked damaged to skip the cache")
	}
	f.damagedMu.Lock()
	f.damagedUntil[a.key] = time.Now().Add(-time.Second)
	f.damagedMu.Unlock()
	if f.recentlyDamaged(a) {
		t.Fatal("an SST still skips the cache after its mark expired")
	}
	for i := range damagedMax + 10 {
		f.markDamaged(sstObject{key: [32]byte{byte(i), byte(i >> 8), 9}})
	}
	if n := len(f.damagedUntil); n > damagedMax {
		t.Fatalf("%d SSTs remembered, want at most %d", n, damagedMax)
	}
	storageOnly := &sstFetcher{}
	storageOnly.markDamaged(a)
	if storageOnly.recentlyDamaged(a) {
		t.Fatal("a Reader without a read cache marked an SST")
	}
}
