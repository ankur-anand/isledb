package isledb

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// e2eCache runs the read cache as isledb cache does: a relay reading
// CURRENT from storage, and the chunk cache, behind one handler. down drops
// every connection, as a crashed cache would; the relay's storage can be
// made to fail.
type e2eCache struct {
	url                        string
	objects                    *readcache.Objects
	relayStorage               *failingCurrent
	down                       atomic.Bool
	longPolls, currents, reads atomic.Int64
}

// failingCurrent is the relay's storage, which fails while fail is set.
type failingCurrent struct {
	*manifest.BlobStoreBackend
	fail atomic.Bool
}

func (f *failingCurrent) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	if f.fail.Load() {
		return nil, "", errors.New("storage unreachable")
	}
	return f.BlobStoreBackend.ReadCurrent(ctx)
}

func startE2ECache(t *testing.T, store *blobstore.Store) *e2eCache {
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
	relayStorage := &failingCurrent{BlobStoreBackend: manifest.NewBlobStoreBackend(store)}
	relay := readcache.NewRelay(relayStorage, 20*time.Millisecond)
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { relay.Run(ctx, nil); close(done) }()
	t.Cleanup(func() { stop(); <-done })
	h, err := readcache.NewHandler(objects, map[string]*readcache.Relay{store.Prefix(): relay}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := &e2eCache{objects: objects, relayStorage: relayStorage}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c.down.Load() {
			if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
				_ = conn.Close()
			}
			return
		}
		switch {
		case r.URL.Query().Has("after"):
			c.longPolls.Add(1)
		case r.URL.Path == "/v1/current":
			c.currents.Add(1)
		case strings.HasPrefix(r.URL.Path, "/v1/object/"):
			c.reads.Add(1)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	c.url = server.URL
	return c
}

func within(t *testing.T, d time.Duration, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(d); !done(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, d)
		}
	}
}

func TestReadCacheEndToEnd(t *testing.T) {
	ctx := context.Background()
	store, db, writer := openChangeReaderTestDB(t, "read-cache-e2e")
	values := map[string][]byte{}
	put := func(key string, size int) {
		t.Helper()
		value := make([]byte, size)
		_, _ = rand.Read(value)
		values[key] = value
		if _, err := writer.Put(ctx, []byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 2000 {
		put(fmt.Sprintf("key-%05d", i), 4<<10) // about 8 MiB: a large SST
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	cache := startE2ECache(t, store)
	opts := func(metrics *ReaderMetrics) ReaderOpenOptions {
		o := DefaultReaderOpenOptions(t.TempDir())
		o.Views.RefreshAfter = time.Hour // only the doorbell brings new commits
		o.ReadCache = ReadCacheOptions{URL: cache.url, Timeout: 2 * time.Second}
		o.Metrics = metrics
		return o
	}
	readAll := func(reader *Reader) {
		t.Helper()
		for key, want := range values {
			got, found, err := reader.Get(ctx, []byte(key))
			if err != nil || !found || !bytes.Equal(got, want) {
				t.Fatalf("Get(%s): found %t, %v", key, found, err)
			}
		}
	}

	// 1. Reads go through the cache.
	first := DefaultReaderMetrics(nil)
	reader, err := db.OpenReader(ctx, opts(first))
	if err != nil {
		t.Fatal(err)
	}
	readAll(reader)
	if testutil.ToFloat64(first.ReadCacheReads) == 0 || fallbacks(first) != 0 {
		t.Fatalf("first reader: %v cache reads, %v fallbacks; want reads through the cache and no fallbacks",
			testutil.ToFloat64(first.ReadCacheReads), fallbacks(first))
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. A second reader, with an empty disk of its own, is served from the
	// cache without the cache reading storage again.
	storageReads := cache.objects.Stats().StorageReads
	second := DefaultReaderMetrics(nil)
	reader, err = db.OpenReader(ctx, opts(second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	readAll(reader)
	if n := cache.objects.Stats().StorageReads - storageReads; n != 0 {
		t.Fatalf("the cache read storage %d times for a second reader of the same data, want 0", n)
	}

	// 3. A commit reaches the reader through the relay and the doorbell,
	// with no refresh timer and no Refresh call.
	put("committed", 100)
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	within(t, 3*time.Second, "the commit reaching the reader", func() bool {
		_, found, err := reader.Get(ctx, []byte("committed"))
		return err == nil && found
	})
	if testutil.ToFloat64(second.ReadCacheDoorbells.WithLabelValues("rang")) == 0 ||
		testutil.ToFloat64(second.ReadCacheCurrent.WithLabelValues("relayed")) == 0 {
		t.Fatal("the commit did not come through the doorbell and the relay")
	}

	// 4. The change feed reads through the cache and wakes on a commit.
	changeMetrics := DefaultReaderMetrics(nil)
	feed, err := db.OpenChangeReader(ctx, ChangeReaderOptions{
		ReadCache: ReadCacheOptions{URL: cache.url, Timeout: 2 * time.Second},
		Metrics:   changeMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = feed.Close() }()
	bounds, err := feed.Bounds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changes := 0
	cursor := bounds.Oldest
	for cursor != bounds.Head {
		page, err := feed.Read(ctx, cursor, ChangeReadOptions{MaxChanges: 500})
		if err != nil {
			t.Fatal(err)
		}
		changes += len(page.Changes)
		cursor = page.Next
	}
	if changes != len(values) {
		t.Fatalf("change feed: %d changes, want %d", changes, len(values))
	}
	waited := make(chan error, 1)
	go func() { waited <- feed.WaitForChanges(ctx, cursor) }()
	time.Sleep(100 * time.Millisecond)
	put("later", 100)
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("WaitForChanges did not return within 3s of a commit")
	}
	page, err := feed.Read(ctx, cursor, ChangeReadOptions{})
	if err != nil || len(page.Changes) != 1 || string(page.Changes[0].Key) != "later" {
		t.Fatalf("after the wait: %d changes, %v; want the new one", len(page.Changes), err)
	}
	if testutil.ToFloat64(changeMetrics.ReadCacheReads) == 0 || fallbacks(changeMetrics) != 0 {
		t.Fatalf("change feed: %v cache reads, %v fallbacks; want reads through the cache and no fallbacks",
			testutil.ToFloat64(changeMetrics.ReadCacheReads), fallbacks(changeMetrics))
	}
	if cache.longPolls.Load() == 0 || cache.currents.Load() == 0 || cache.reads.Load() == 0 {
		t.Fatalf("cache saw %d long polls, %d CURRENT reads, %d object reads; want all three",
			cache.longPolls.Load(), cache.currents.Load(), cache.reads.Load())
	}
}

// e2eDB is a change-feed database with about 8 MiB of data in a large SST.
type e2eDB struct {
	ctx    context.Context
	store  *blobstore.Store
	db     *DB
	writer *Writer
	values map[string][]byte
}

func newE2EDB(t *testing.T, prefix string) *e2eDB {
	t.Helper()
	d := &e2eDB{ctx: context.Background(), values: map[string][]byte{}}
	d.store, d.db, d.writer = openChangeReaderTestDB(t, prefix)
	for i := range 2000 {
		d.put(t, fmt.Sprintf("key-%05d", i), 4<<10)
	}
	d.flush(t)
	return d
}

func (d *e2eDB) put(t *testing.T, key string, size int) {
	t.Helper()
	value := make([]byte, size)
	_, _ = rand.Read(value)
	d.values[key] = value
	if _, err := d.writer.Put(d.ctx, []byte(key), value); err != nil {
		t.Fatal(err)
	}
}

func (d *e2eDB) flush(t *testing.T) {
	t.Helper()
	if err := d.writer.Flush(d.ctx); err != nil {
		t.Fatal(err)
	}
}

// openReader opens a Reader through cache that hears of commits only from
// the doorbell, with its CURRENT age limit at maxAge, or the default.
func (d *e2eDB) openReader(t *testing.T, cache *e2eCache, maxAge time.Duration) (*Reader, *ReaderMetrics) {
	t.Helper()
	metrics := DefaultReaderMetrics(nil)
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.Views.RefreshAfter = time.Hour
	opts.ReadCache = ReadCacheOptions{URL: cache.url, Timeout: 2 * time.Second, MaxCurrentAge: maxAge}
	opts.Metrics = metrics
	reader, err := d.db.OpenReader(d.ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return reader, metrics
}

// readAllCold reads every key after emptying the reader's own caches, so
// every read leaves the process.
func (d *e2eDB) readAllCold(t *testing.T, reader *Reader) {
	t.Helper()
	reader.clearDiskCache()
	reader.openSSTs.clear()
	reader.blockCache.clear()
	for key, want := range d.values {
		got, found, err := reader.Get(d.ctx, []byte(key))
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("Get(%s): found %t, %v", key, found, err)
		}
	}
}

func fallbackReason(m *ReaderMetrics, reason string) float64 {
	return testutil.ToFloat64(m.ReadCacheFallbacks.WithLabelValues(reason))
}

func TestReadCacheEndToEndOutageAndRecovery(t *testing.T) {
	unit, ceiling := doorbellBackoffUnit, doorbellMaxBackoff
	doorbellBackoffUnit, doorbellMaxBackoff = 20*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { doorbellBackoffUnit, doorbellMaxBackoff = unit, ceiling })
	d := newE2EDB(t, "read-cache-e2e-outage")
	cache := startE2ECache(t, d.store)
	reader, metrics := d.openReader(t, cache, 0)
	d.readAllCold(t, reader)

	// The cache goes down: every read is still right, from storage, and
	// after a few failures the cache is skipped instead of waited on.
	cache.down.Store(true)
	d.readAllCold(t, reader)
	within(t, 5*time.Second, "the doorbell failing against the cache down", func() bool {
		return testutil.ToFloat64(metrics.ReadCacheDoorbells.WithLabelValues("error")) > 0
	})
	if fallbackReason(metrics, "unavailable") == 0 || fallbackReason(metrics, "bypassed") == 0 {
		t.Fatalf("cache down: unavailable %v, bypassed %v; want both",
			fallbackReason(metrics, "unavailable"), fallbackReason(metrics, "bypassed"))
	}

	// It comes back: once the breaker's cooldown has passed, reads go
	// through it again, and so do commits.
	cache.down.Store(false)
	time.Sleep(5500 * time.Millisecond)
	through := testutil.ToFloat64(metrics.ReadCacheReads)
	d.readAllCold(t, reader)
	if testutil.ToFloat64(metrics.ReadCacheReads) == through {
		t.Fatal("reads did not go through the cache again after it came back")
	}
	d.put(t, "after-outage", 100)
	d.flush(t)
	within(t, 3*time.Second, "the commit after the outage reaching the reader", func() bool {
		_, found, err := reader.Get(d.ctx, []byte("after-outage"))
		return err == nil && found
	})
}

func TestReadCacheEndToEndRelayLosesStorage(t *testing.T) {
	d := newE2EDB(t, "read-cache-e2e-relay-storage")
	cache := startE2ECache(t, d.store)
	reader, metrics := d.openReader(t, cache, 300*time.Millisecond)

	// The relay cannot read storage: its copy ages past the reader's limit,
	// and a refresh reads storage itself, so commits still arrive.
	cache.relayStorage.fail.Store(true)
	time.Sleep(500 * time.Millisecond)
	d.put(t, "while-stale", 100)
	d.flush(t)
	reader.RequestRefresh()
	within(t, 3*time.Second, "the commit reaching the reader from storage", func() bool {
		_, found, err := reader.Get(d.ctx, []byte("while-stale"))
		return err == nil && found
	})
	if testutil.ToFloat64(metrics.ReadCacheCurrent.WithLabelValues("too_old")) == 0 {
		t.Fatal("the relay's aged copy was not refused as too old")
	}

	// Storage is back for the relay: commits come through it again.
	cache.relayStorage.fail.Store(false)
	relayed := testutil.ToFloat64(metrics.ReadCacheCurrent.WithLabelValues("relayed"))
	d.put(t, "after-recovery", 100)
	d.flush(t)
	within(t, 3*time.Second, "the commit reaching the reader through the relay", func() bool {
		_, found, err := reader.Get(d.ctx, []byte("after-recovery"))
		return err == nil && found
	})
	if testutil.ToFloat64(metrics.ReadCacheCurrent.WithLabelValues("relayed")) == relayed {
		t.Fatal("the commit after recovery did not come through the relay")
	}
}
