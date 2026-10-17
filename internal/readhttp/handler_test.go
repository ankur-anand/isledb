package readhttp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ankur-anand/isledb"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

type fixture struct {
	ctx     context.Context
	bucket  *blob.Bucket
	writer  *isledb.Writer
	reader  *isledb.Reader
	handler *Handler
	server  *httptest.Server
}

// newFixture serves a reader over a fresh in-memory bucket, with a writer to
// commit through. The reader refreshes only when told to.
func newFixture(t *testing.T, opts Options) *fixture {
	t.Helper()
	f := &fixture{ctx: context.Background(), bucket: memblob.OpenBucket(nil)}
	t.Cleanup(func() { _ = f.bucket.Close() })
	open := func() *isledb.DB {
		db, err := isledb.OpenBucket(f.ctx, f.bucket, "mem", isledb.DBOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	wopts := isledb.DefaultWriterOptions()
	wopts.Flush.Interval = 0
	var err error
	if f.writer, err = open().OpenWriter(f.ctx, wopts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.writer.Close(f.ctx) })
	ropts := isledb.DefaultReaderOpenOptions(t.TempDir())
	ropts.Views.RefreshAfter = time.Hour
	if f.reader, err = open().OpenReader(f.ctx, ropts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.reader.Close() })
	if opts.MaxConcurrent == 0 {
		opts.MaxConcurrent = 8
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 10 * time.Second
	}
	if f.handler, err = NewHandler(f.reader, opts); err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(f.handler)
	t.Cleanup(f.server.Close)
	return f
}

// commit writes key=value, waits for it, and refreshes the reader; it
// returns the revision the writer gave.
func (f *fixture) commit(t *testing.T, key, value string) uint64 {
	t.Helper()
	seq, err := f.writer.Put(f.ctx, []byte(key), []byte(value))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	position, err := f.writer.WaitCommitted(f.ctx, seq)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.reader.Refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	return uint64(position)
}

type reply struct {
	status int
	header http.Header
	body   []byte
}

func (f *fixture) do(t *testing.T, method, path string) reply {
	t.Helper()
	req, err := http.NewRequest(method, f.server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return reply{resp.StatusCode, resp.Header, body}
}

func (r reply) wantError(t *testing.T, status int, code string) {
	t.Helper()
	var e errorResponse
	_ = json.Unmarshal(r.body, &e)
	if r.status != status || e.Error != code {
		t.Fatalf("got %d %s, want %d with error %q", r.status, r.body, status, code)
	}
}

func (r reply) revision(t *testing.T) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(r.header.Get(HeaderRevision), 10, 64)
	if err != nil {
		t.Fatalf("%s %q: %v", HeaderRevision, r.header.Get(HeaderRevision), err)
	}
	return v
}

func kvPathFor(key string) string { return "/v1/kv/" + url.PathEscape(key) }

func TestGetReturnsTheValueAndItsRevision(t *testing.T) {
	f := newFixture(t, Options{})
	revision := f.commit(t, "user/42", `{"name":"a"}`)

	r := f.do(t, http.MethodGet, kvPathFor("user/42"))
	if r.status != http.StatusOK || string(r.body) != `{"name":"a"}` {
		t.Fatalf("GET: %d %q", r.status, r.body)
	}
	if r.header.Get("Content-Type") != "application/octet-stream" || r.revision(t) != revision {
		t.Fatalf("headers %v, want octet-stream and revision %d", r.header, revision)
	}

	head := f.do(t, http.MethodHead, kvPathFor("user/42"))
	if head.status != http.StatusOK || len(head.body) != 0 || head.header.Get("Content-Length") != "12" || head.revision(t) != revision {
		t.Fatalf("HEAD: %d %q %v", head.status, head.body, head.header)
	}

	missing := f.do(t, http.MethodGet, kvPathFor("nope"))
	missing.wantError(t, http.StatusNotFound, ErrorKeyNotFound)
	if missing.revision(t) != revision {
		t.Fatalf("404 at revision %d, want %d", missing.revision(t), revision)
	}
	if head := f.do(t, http.MethodHead, kvPathFor("nope")); head.status != http.StatusNotFound || head.revision(t) != revision {
		t.Fatalf("HEAD of a missing key: %d %v", head.status, head.header)
	}
}

func TestKeysAreTakenAsSent(t *testing.T) {
	f := newFixture(t, Options{})
	for _, key := range []string{"a//b", "../x", "with space", "\xff\x00"} {
		f.commit(t, key, "v:"+key)
	}
	for _, key := range []string{"a//b", "../x", "with space", "\xff\x00"} {
		if r := f.do(t, http.MethodGet, kvPathFor(key)); r.status != http.StatusOK || string(r.body) != "v:"+key {
			t.Fatalf("GET %q: %d %q", key, r.status, r.body)
		}
	}
	f.commit(t, "user/42", "v")
	if r := f.do(t, http.MethodGet, "/v1/kv/user/42"); r.status != http.StatusOK || string(r.body) != "v" {
		t.Fatalf("GET with an unencoded slash: %d %q", r.status, r.body)
	}
	f.commit(t, "empty", "")
	if r := f.do(t, http.MethodGet, kvPathFor("empty")); r.status != http.StatusOK || len(r.body) != 0 {
		t.Fatalf("empty value: %d %q", r.status, r.body)
	}
}

func TestReaderRefusesWhatItDoesNotServe(t *testing.T) {
	f := newFixture(t, Options{})
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPost} {
		r := f.do(t, method, kvPathFor("k"))
		r.wantError(t, http.StatusMethodNotAllowed, ErrorMethod)
		if r.header.Get("Allow") != "GET, HEAD" {
			t.Fatalf("%s: Allow %q", method, r.header.Get("Allow"))
		}
	}
	f.do(t, http.MethodGet, "/v1/kvx").wantError(t, http.StatusNotFound, ErrorNotFound)
	f.do(t, http.MethodGet, "/v1/writes/1.1").wantError(t, http.StatusNotFound, ErrorNotFound)
	f.do(t, http.MethodGet, "/v1/kv/").wantError(t, http.StatusBadRequest, ErrorBadRequest)
	f.do(t, http.MethodGet, kvPathFor("k")+"?min_revision=1").wantError(t, http.StatusBadRequest, ErrorBadRequest)
}

func TestReadsPastTheLimitAre503(t *testing.T) {
	f := newFixture(t, Options{MaxConcurrent: 1})
	f.commit(t, "k", "v")
	f.handler.slots <- struct{}{} // one read in progress
	r := f.do(t, http.MethodGet, kvPathFor("k"))
	r.wantError(t, http.StatusServiceUnavailable, ErrorBackpressure)
	if r.header.Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}
	<-f.handler.slots
	if r := f.do(t, http.MethodGet, kvPathFor("k")); r.status != http.StatusOK {
		t.Fatalf("after the read finished: %d %s", r.status, r.body)
	}
	if f.handler.InProgress() != 0 {
		t.Fatalf("%d reads still counted in progress", f.handler.InProgress())
	}
}

func TestClosedReaderIs503(t *testing.T) {
	f := newFixture(t, Options{})
	f.commit(t, "k", "v")
	if err := f.reader.Close(); err != nil {
		t.Fatal(err)
	}
	r := f.do(t, http.MethodGet, kvPathFor("k"))
	r.wantError(t, http.StatusServiceUnavailable, ErrorUnavailable)
	if r.header.Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}
}

func TestNewHandlerRefusesBadOptions(t *testing.T) {
	f := newFixture(t, Options{})
	if _, err := NewHandler(nil, Options{MaxConcurrent: 1, ReadTimeout: time.Second}); err == nil {
		t.Fatal("nil reader accepted")
	}
	if _, err := NewHandler(f.reader, Options{ReadTimeout: time.Second}); err == nil {
		t.Fatal("zero max concurrent accepted")
	}
	if _, err := NewHandler(f.reader, Options{MaxConcurrent: 1}); err == nil {
		t.Fatal("zero read timeout accepted")
	}
}

func TestRouteNamesEveryEndpoint(t *testing.T) {
	for _, c := range []struct{ method, path, want string }{
		{http.MethodGet, "/v1/kv/k", "kv_get"},
		{http.MethodHead, "/v1/kv/k", "kv_head"},
		{http.MethodGet, "/v1/kv?prefix=a", "kv_scan"},
		{http.MethodPost, "/v1/kv", "other"},
		{http.MethodPut, "/v1/kv/k", "other"},
		{http.MethodGet, "/v1/writes/1.1", "other"},
	} {
		if got := Route(httptest.NewRequest(c.method, c.path, nil)); got != c.want {
			t.Errorf("%s %s: %q, want %q", c.method, c.path, got, c.want)
		}
	}
}

// TestReadThatRunsOutOfTimeGivesUpItsSlot: a read past its deadline is
// answered 503 at once and frees its slot, so a storage that hangs cannot
// hold every slot.
func TestReadThatRunsOutOfTimeGivesUpItsSlot(t *testing.T) {
	f := newFixture(t, Options{ReadTimeout: time.Nanosecond})
	f.commit(t, "k", "v") // a new SST: the read must go to storage
	r := f.do(t, http.MethodGet, kvPathFor("k"))
	r.wantError(t, http.StatusServiceUnavailable, ErrorUnavailable)
	if r.header.Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}
	if f.handler.InProgress() != 0 {
		t.Fatalf("%d reads still hold a slot", f.handler.InProgress())
	}
}

func TestEveryAnswerIsNotCached(t *testing.T) {
	f := newFixture(t, Options{})
	f.commit(t, "k", "v")
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, kvPathFor("k")},
		{http.MethodHead, kvPathFor("k")},
		{http.MethodGet, kvPathFor("missing")},
		{http.MethodGet, "/v1/kv?prefix=k"},
		{http.MethodGet, "/v1/kv?limit=0"},
		{http.MethodPut, kvPathFor("k")},
		{http.MethodGet, "/v1/other"},
	} {
		if got := f.do(t, c.method, c.path).header.Get("Cache-Control"); got != "no-cache" {
			t.Errorf("%s %s: Cache-Control %q, want no-cache", c.method, c.path, got)
		}
	}
}

func TestMalformedQuerySaysSo(t *testing.T) {
	f := newFixture(t, Options{})
	r := f.do(t, http.MethodGet, kvPathFor("k")+"?a=%zz")
	r.wantError(t, http.StatusBadRequest, ErrorBadRequest)
	var e errorResponse
	_ = json.Unmarshal(r.body, &e)
	if e.Message != "the query is not encoded correctly" {
		t.Fatalf("message %q", e.Message)
	}
}

// TestStorageFailureIs503: a read that storage fails is retryable, not a
// server bug.
func TestStorageFailureIs503(t *testing.T) {
	f := newFixture(t, Options{})
	f.commit(t, "k", "v") // a new SST: the read must go to storage
	if err := f.bucket.Close(); err != nil {
		t.Fatal(err)
	}
	r := f.do(t, http.MethodGet, kvPathFor("k"))
	r.wantError(t, http.StatusServiceUnavailable, ErrorUnavailable)
	if r.header.Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}
}
