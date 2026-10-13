package cacheclient

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
)

type fixture struct {
	key     string
	data    []byte
	url     string
	noCache atomic.Int32
}

func newFixture(t *testing.T, wrap func(http.Handler) http.Handler) *fixture {
	t.Helper()
	store := blobstore.NewMemory("db")
	f := &fixture{key: store.SSTPath("000001.sst"), data: bytes.Repeat([]byte("isle"), 100_000)}
	if _, err := store.Write(context.Background(), f.key, f.data); err != nil {
		t.Fatal(err)
	}
	disk, err := diskcache.Open(diskcache.Options{Dir: t.TempDir(), MaxBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	objects, err := readcache.NewObjects(store, disk, readcache.ObjectsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := readcache.NewHandler(objects, map[string]*readcache.Relay{
		"db": readcache.NewRelay(manifest.NewBlobStoreBackend(store), time.Hour),
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(readcache.HeaderNoCache) != "" {
			f.noCache.Add(1)
		}
		h.ServeHTTP(w, r)
	})
	if wrap != nil {
		handler = wrap(handler)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	f.url = server.URL
	return f
}

func client(t *testing.T, opts Options) *Client {
	t.Helper()
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func reason(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

func TestReadRangeThroughTheCache(t *testing.T) {
	f := newFixture(t, nil)
	c := client(t, Options{URL: f.url + "/"})
	size := int64(len(f.data))
	got, err := c.ReadRange(context.Background(), f.key, size, 1000, 5000, true)
	if err != nil || !bytes.Equal(got, f.data[1000:6000]) {
		t.Fatalf("read: %v", err)
	}
	if _, err := c.ReadRange(context.Background(), f.key, size, 0, 10, false); err != nil {
		t.Fatal(err)
	}
	if n := f.noCache.Load(); n != 1 {
		t.Fatalf("%d requests asked not to fill the cache, want the 1 read with fill false", n)
	}
}

func TestReadRangeFailureReasons(t *testing.T) {
	f := newFixture(t, nil)
	size := int64(len(f.data))
	c := client(t, Options{URL: f.url})
	missing := blobstore.NewMemory("db").SSTPath("missing.sst")
	for _, tc := range []struct {
		name string
		key  string
		size int64
		want string
	}{
		{"missing object", missing, 10, ReasonNotFound},
		{"wrong size", f.key, size + 1, ReasonSizeMismatch},
		{"another database", blobstore.NewMemory("other").SSTPath("000001.sst"), 10, ReasonRefused},
	} {
		if _, err := c.ReadRange(context.Background(), tc.key, tc.size, 0, 1, true); reason(err) != tc.want {
			t.Errorf("%s: %v, want reason %s", tc.name, err, tc.want)
		}
	}

	locked := newFixture(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer right" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	if _, err := client(t, Options{URL: locked.url, Token: "wrong"}).ReadRange(context.Background(), locked.key, size, 0, 1, true); reason(err) != ReasonRefused {
		t.Errorf("wrong token: %v, want refused", err)
	}
	if _, err := client(t, Options{URL: locked.url, Token: "right"}).ReadRange(context.Background(), locked.key, size, 0, 1, true); err != nil {
		t.Errorf("right token: %v", err)
	}

	slow := newFixture(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(time.Second):
			case <-r.Context().Done():
			}
			next.ServeHTTP(w, r)
		})
	})
	if _, err := client(t, Options{URL: slow.url, Timeout: 50 * time.Millisecond}).ReadRange(context.Background(), slow.key, size, 0, 1, true); reason(err) != ReasonTimeout {
		t.Errorf("slow cache: %v, want timeout", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client(t, Options{URL: slow.url}).ReadRange(ctx, slow.key, size, 0, 1, true); !errors.Is(err, context.Canceled) {
		t.Errorf("caller gone: %v, want the caller's context error", err)
	}

	closed := httptest.NewServer(nil)
	closed.Close()
	if _, err := client(t, Options{URL: closed.URL}).ReadRange(context.Background(), f.key, size, 0, 1, true); reason(err) != ReasonUnavailable {
		t.Errorf("no cache listening: %v, want unavailable", err)
	}
}

func TestNewRefusesBadOptions(t *testing.T) {
	for _, opts := range []Options{{URL: ""}, {URL: "isledb-cache:8090"}, {URL: "ftp://cache"}, {URL: "http://cache", Timeout: -time.Second}} {
		if _, err := New(opts); err == nil {
			t.Errorf("New(%+v) accepted", opts)
		}
	}
}

func TestCloseClosesOnlyItsOwnConnections(t *testing.T) {
	var closed atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-9/10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, 10))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	read := func(c *Client) {
		t.Helper()
		if _, err := c.ReadRange(context.Background(), "db/sstable/x/y", 10, 0, 10, true); err != nil {
			t.Fatal(err)
		}
	}
	waitClosed := func(want int32) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for closed.Load() != want && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := closed.Load(); got != want {
			t.Fatalf("%d connections closed, want %d", got, want)
		}
	}

	own := client(t, Options{URL: server.URL})
	read(own)
	own.Close()
	waitClosed(1)

	shared := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	passed := client(t, Options{URL: server.URL, HTTPClient: shared})
	read(passed)
	passed.Close()
	time.Sleep(50 * time.Millisecond)
	waitClosed(1)
	shared.CloseIdleConnections()
}
