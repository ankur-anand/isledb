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
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/cacheclient"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// writeChanges writes two change batches, each of several blocks, and
// returns the changes in order.
func writeChanges(t *testing.T, ctx context.Context, writer *Writer) []Change {
	t.Helper()
	var want []Change
	for batch := range 2 {
		for i := range 1200 {
			key := fmt.Appendf(nil, "key-%d-%04d", batch, i)
			value := make([]byte, 200)
			_, _ = rand.Read(value)
			if _, err := writer.Put(ctx, key, value); err != nil {
				t.Fatal(err)
			}
			want = append(want, Change{Key: key, Value: value})
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return want
}

// readFeed reads the feed from its oldest change, a page at a time.
func readFeed(t *testing.T, ctx context.Context, reader *ChangeReader) []Change {
	t.Helper()
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got []Change
	for cursor := bounds.Oldest; cursor != bounds.Head; {
		page, err := reader.Read(ctx, cursor, ChangeReadOptions{MaxChanges: 300, MaxBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, page.Changes...)
		cursor = page.Next
	}
	return got
}

func sameChanges(t *testing.T, got, want []Change) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("read %d changes, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("change %d: key %s, want %s", i, got[i].Key, want[i].Key)
		}
	}
}

func TestChangeReaderReadsBatchesThroughTheReadCache(t *testing.T) {
	ctx := context.Background()
	store, db, writer := openChangeReaderTestDB(t, "change-read-cache")
	want := writeChanges(t, ctx, writer)
	server := readCacheServer(t, store)

	read := func(url string) *ReaderMetrics {
		t.Helper()
		metrics := DefaultReaderMetrics(nil)
		reader, err := db.OpenChangeReader(ctx, ChangeReaderOptions{
			ReadCache: ReadCacheOptions{URL: url, Timeout: time.Second},
			Metrics:   metrics,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		sameChanges(t, readFeed(t, ctx, reader), want)
		return metrics
	}

	metrics := read(server.URL)
	if n := testutil.ToFloat64(metrics.ReadCacheReads); n == 0 {
		t.Fatal("no change batch read went through the cache")
	}
	if n := fallbacks(metrics); n != 0 {
		t.Fatalf("%v reads fell back with the cache up: a batch's size is wrong", n)
	}
	if server.objects.Stats().StorageReads == 0 {
		t.Fatal("the cache read nothing from storage")
	}

	damaged := func(r *http.Request) bool { return strings.Contains(r.URL.Path, "/changes/") }
	server.damage.Store(&damaged)
	metrics = read(server.URL)
	if n := testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(reasonChecksumMismatch)); n == 0 {
		t.Fatal("damaged batch bytes from the cache were not read again from storage")
	}

	server.Close()
	metrics = read(server.URL)
	if n := testutil.ToFloat64(metrics.ReadCacheFallbacks.WithLabelValues(cacheclient.ReasonUnavailable)); n == 0 {
		t.Fatal("no fallback counted with the cache down")
	}
}

func TestOpenChangeReaderRefusesBadOptions(t *testing.T) {
	ctx := context.Background()
	_, db, _ := openChangeReaderTestDB(t, "change-read-cache-options")
	if _, err := db.OpenChangeReader(ctx, ChangeReaderOptions{ReadCache: ReadCacheOptions{URL: "isledb-cache:8090"}}); !errors.Is(err, ErrInvalidReaderOptions) {
		t.Fatalf("bad URL: %v, want ErrInvalidReaderOptions", err)
	}
	if _, err := db.OpenChangeReader(ctx, ChangeReaderOptions{}, ChangeReaderOptions{}); err == nil {
		t.Fatal("two ChangeReaderOptions accepted")
	}
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("no options: %v", err)
	}
	_ = reader.Close()
}

func TestChangeCursorCompare(t *testing.T) {
	oldest := ChangeCursor{}
	a, aMid, b := changeCursorAt(5, 0), changeCursorAt(5, 3), changeCursorAt(6, 0)
	for _, c := range []struct {
		x, y ChangeCursor
		want int
	}{
		{oldest, oldest, 0}, {oldest, a, -1}, {a, oldest, 1},
		{a, a, 0}, {a, aMid, -1}, {aMid, b, -1}, {b, a, 1},
	} {
		if got := c.x.Compare(c.y); got != c.want {
			t.Errorf("%q vs %q: %d, want %d", c.x, c.y, got, c.want)
		}
	}
}

// TestChangeReaderReadsCurrentFromStorageForACursorPastTheCachesCopy: a
// cursor from a newer CURRENT than the cache holds, as another reader can
// hand out, is read against storage's CURRENT, not refused.
func TestChangeReaderReadsCurrentFromStorageForACursorPastTheCachesCopy(t *testing.T) {
	ctx := context.Background()
	store, db, writer := openChangeReaderTestDB(t, "change-read-cache-behind")
	put := func(key string) {
		t.Helper()
		if _, err := writer.Put(ctx, []byte(key), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	put("a")

	// A cache whose copy of CURRENT stays where it is polled once, here.
	disk, err := diskcache.Open(diskcache.Options{Dir: t.TempDir(), MaxBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	objects, err := readcache.NewObjects(store, disk, readcache.ObjectsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	relay := readcache.NewRelay(manifest.NewBlobStoreBackend(store), time.Hour)
	if err := relay.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	handler, err := readcache.NewHandler(objects, map[string]*readcache.Relay{store.Prefix(): relay}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	cached, err := db.OpenChangeReader(ctx, ChangeReaderOptions{ReadCache: ReadCacheOptions{URL: server.URL, Timeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cached.Close() }()
	if _, err := cached.Bounds(ctx); err != nil { // holds the cache's view, at a
		t.Fatal(err)
	}

	put("b")
	direct, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = direct.Close() }()
	bounds, err := direct.Bounds(ctx)
	if err != nil {
		t.Fatal(err)
	}

	page, err := cached.Read(ctx, bounds.Head, ChangeReadOptions{})
	if err != nil {
		t.Fatalf("read from storage's head with the cache behind it: %v", err)
	}
	if !page.CaughtUp() || len(page.Changes) != 0 || page.Next != bounds.Head {
		t.Fatalf("page %+v, want caught up at %q", page, bounds.Head)
	}
	if got := cached.LoadedHead(); got != bounds.Head {
		t.Fatalf("loaded head %q, want %q", got, bounds.Head)
	}
	if _, err := cached.Read(ctx, changeCursorAt(bounds.Head.entry+5, 0), ChangeReadOptions{}); !errors.Is(err, ErrInvalidChangeCursor) {
		t.Fatalf("a cursor past storage's head: %v, want ErrInvalidChangeCursor", err)
	}
}
