package isledb

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/cacheclient"
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
