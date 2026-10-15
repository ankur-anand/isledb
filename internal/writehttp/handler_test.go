package writehttp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

type fixture struct {
	ctx    context.Context
	bucket *blob.Bucket
	writer *isledb.Writer
	server *httptest.Server
}

// newFixture serves a writer over a fresh in-memory bucket. flush is the
// writer's flush interval: zero leaves writes pending until Flush.
func newFixture(t *testing.T, flush time.Duration, opts Options, mutate ...func(*isledb.WriterOptions)) *fixture {
	t.Helper()
	f := &fixture{ctx: context.Background(), bucket: memblob.OpenBucket(nil)}
	t.Cleanup(func() { _ = f.bucket.Close() })
	f.writer, f.server = f.serve(t, flush, opts, mutate...)
	return f
}

// serve opens a writer on f's bucket and serves it.
func (f *fixture) serve(t *testing.T, flush time.Duration, opts Options, mutate ...func(*isledb.WriterOptions)) (*isledb.Writer, *httptest.Server) {
	t.Helper()
	db, err := isledb.OpenBucket(f.ctx, f.bucket, "mem", isledb.DBOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	wopts := isledb.DefaultWriterOptions()
	wopts.Flush.Interval = flush
	for _, m := range mutate {
		m(&wopts)
	}
	writer, err := db.OpenWriter(f.ctx, wopts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(f.ctx) })
	if opts.CommitWait == 0 {
		opts.CommitWait = 10 * time.Second
	}
	if opts.MaxValueBytes == 0 {
		opts.MaxValueBytes = 1 << 20
	}
	if opts.MaxBodyBytes == 0 {
		opts.MaxBodyBytes = 4 << 20
	}
	if opts.MaxInflightBytes == 0 {
		opts.MaxInflightBytes = 64 << 20
	}
	if opts.BodyReadTimeout == 0 {
		opts.BodyReadTimeout = 10 * time.Second
	}
	handler, err := NewHandler(writer, opts)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return writer, server
}

// reader opens a Reader over f's bucket, at its latest view.
func (f *fixture) reader(t *testing.T) *isledb.Reader {
	t.Helper()
	db, err := isledb.OpenBucket(f.ctx, f.bucket, "mem", isledb.DBOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	reader, err := db.OpenReader(f.ctx, isledb.DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	if err := reader.Refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	return reader
}

type reply struct {
	status int
	header http.Header
	body   map[string]string
}

func do(t *testing.T, server *httptest.Server, method, path string, body []byte, header ...string) reply {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(header); i += 2 {
		req.Header.Add(header[i], header[i+1])
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("%s %s: content type %q, want application/json; body %s", method, path, ct, raw)
	}
	var out map[string]string
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s %s: body %s is not a JSON object of strings: %v", method, path, raw, err)
	}
	return reply{status: resp.StatusCode, header: resp.Header, body: out}
}

func (r reply) want(t *testing.T, status int, code string) {
	t.Helper()
	if r.status != status || r.body["error"] != code {
		t.Fatalf("got %d %v, want %d with error %q", r.status, r.body, status, code)
	}
}

func kvPathFor(key string) string { return "/v1/kv/" + url.PathEscape(key) }

// revision is the view position a response's revision names.
func revision(t *testing.T, r reply) isledb.ViewPosition {
	t.Helper()
	p, err := strconv.ParseUint(r.body["revision"], 10, 64)
	if err != nil {
		t.Fatalf("revision %q: %v", r.body["revision"], err)
	}
	return isledb.ViewPosition(p)
}

func TestPutIsVisibleAtItsRevision(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{})
	r := do(t, f.server, http.MethodPut, kvPathFor("user/42")+"?ttl=1h", []byte("v1"))
	if r.status != http.StatusOK || r.body["write_id"] == "" {
		t.Fatalf("PUT: %d %v", r.status, r.body)
	}
	if want := fmt.Sprintf("%d.%d", f.writer.Epoch(), f.writer.CommittedSequence()); r.body["write_id"] != want {
		t.Fatalf("write_id %q, want %q", r.body["write_id"], want)
	}
	reader := f.reader(t)
	if got, want := reader.ViewPosition(), revision(t, r); got < want {
		t.Fatalf("reader at %d, below the write's revision %d", got, want)
	}
	if v, ok, err := reader.Get(f.ctx, []byte("user/42")); err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get: %q %t %v", v, ok, err)
	}

	r = do(t, f.server, http.MethodDelete, kvPathFor("user/42"), nil)
	if r.status != http.StatusOK {
		t.Fatalf("DELETE: %d %v", r.status, r.body)
	}
	if _, ok, err := f.reader(t).Get(f.ctx, []byte("user/42")); err != nil || ok {
		t.Fatalf("after DELETE: found %t, %v", ok, err)
	}
}

func TestKeysAreTakenAsSent(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{})
	keys := []string{"a/b", "a//b", "..", ".", "a/../b", "\x00", "\xff\xfe", "sp ace?#%"}
	for i, key := range keys {
		value := fmt.Append(nil, i)
		if i == 0 {
			value = nil // an empty value is a value
		}
		if r := do(t, f.server, http.MethodPut, kvPathFor(key), value); r.status != http.StatusOK {
			t.Fatalf("PUT %q: %d %v", key, r.status, r.body)
		}
	}
	reader := f.reader(t)
	for i, key := range keys {
		want := fmt.Sprint(i)
		if i == 0 {
			want = ""
		}
		if v, ok, err := reader.Get(f.ctx, []byte(key)); err != nil || !ok || string(v) != want {
			t.Fatalf("key %q: %q %t %v, want %q", key, v, ok, err, want)
		}
	}
}

func TestBatchAppliesAtomically(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{})
	do(t, f.server, http.MethodPut, kvPathFor("gone"), []byte("x"))
	body, _ := json.Marshal(map[string]any{"ops": []map[string]any{
		{"op": "put", "key": []byte("order/1"), "value": []byte("o")},
		{"op": "put", "key": []byte("stock/7"), "value": []byte("s"), "ttl": "1h"},
		{"op": "delete", "key": []byte("gone")},
	}})
	r := do(t, f.server, http.MethodPost, "/v1/kv", body)
	if r.status != http.StatusOK {
		t.Fatalf("POST: %d %v", r.status, r.body)
	}
	reader := f.reader(t)
	for key, want := range map[string]string{"order/1": "o", "stock/7": "s"} {
		if v, ok, err := reader.Get(f.ctx, []byte(key)); err != nil || !ok || string(v) != want {
			t.Fatalf("%s: %q %t %v", key, v, ok, err)
		}
	}
	if _, ok, _ := reader.Get(f.ctx, []byte("gone")); ok {
		t.Fatal("the batch's delete did not apply")
	}
}

func TestBadRequestsAreRefused(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{})
	for _, c := range []struct {
		name, body, code string
	}{
		{"not JSON", `nope`, ErrorBadRequest},
		{"unknown field", `{"ops":[],"extra":1}`, ErrorBadRequest},
		{"two objects", `{"ops":[]}{"ops":[]}`, ErrorBadRequest},
		{"trailing ]", `{"ops":[{"op":"put","key":"aw=="}]}]`, ErrorBadRequest},
		{"trailing }", `{"ops":[{"op":"put","key":"aw=="}]}}`, ErrorBadRequest},
		{"unknown op", `{"ops":[{"op":"merge","key":"aw=="}]}`, ErrorBadRequest},
		{"delete with value", `{"ops":[{"op":"delete","key":"aw==","value":"dg=="}]}`, ErrorBadRequest},
		{"garbled ttl", `{"ops":[{"op":"put","key":"aw==","ttl":"soon"}]}`, ErrorBadRequest},
		{"empty batch", `{"ops":[]}`, ErrorInvalidMutation},
		{"empty key", `{"ops":[{"op":"put","value":"dg=="}]}`, ErrorInvalidMutation},
		{"negative ttl", `{"ops":[{"op":"put","key":"aw==","ttl":"-1s"}]}`, ErrorInvalidMutation},
	} {
		t.Run(c.name, func(t *testing.T) {
			do(t, f.server, http.MethodPost, "/v1/kv", []byte(c.body)).want(t, http.StatusBadRequest, c.code)
		})
	}
	do(t, f.server, http.MethodPut, kvPathFor("k")+"?ttl=soon", []byte("v")).want(t, http.StatusBadRequest, ErrorBadRequest)
	do(t, f.server, http.MethodPut, kvPathFor("k")+"?ttl=-1s", []byte("v")).want(t, http.StatusBadRequest, ErrorInvalidMutation)
	// Query parameters are checked, so a misspelled one is not silently ignored.
	for _, path := range []string{
		kvPathFor("k") + "?tll=1h",        // misspelled
		kvPathFor("k") + "?ttl=1h&ttl=2h", // twice
		kvPathFor("k") + "?ttl=%zz",       // malformed
	} {
		do(t, f.server, http.MethodPut, path, []byte("v")).want(t, http.StatusBadRequest, ErrorBadRequest)
	}
	do(t, f.server, http.MethodDelete, kvPathFor("k")+"?ttl=1h", nil).want(t, http.StatusBadRequest, ErrorBadRequest)
	do(t, f.server, http.MethodPost, "/v1/kv?ttl=1h", []byte(`{"ops":[]}`)).want(t, http.StatusBadRequest, ErrorBadRequest)
	// Of several unknown parameters, the same one is named every time.
	for range 5 {
		r := do(t, f.server, http.MethodDelete, kvPathFor("k")+"?zz=1&aa=1&mm=1", nil)
		if r.body["message"] != `unknown query parameter "aa"` {
			t.Fatalf("message %q, want the first by name", r.body["message"])
		}
	}
	// A single PUT's error speaks of its key, not of a batch it never sent.
	r := do(t, f.server, http.MethodPut, "/v1/kv/", []byte("v"))
	r.want(t, http.StatusBadRequest, ErrorInvalidMutation)
	if strings.Contains(r.body["message"], "mutation 0") || !strings.Contains(r.body["message"], "empty key") {
		t.Fatalf("PUT with an empty key: message %q", r.body["message"])
	}
}

func TestMethodsAndPaths(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{})
	r := do(t, f.server, http.MethodGet, kvPathFor("k"), nil)
	r.want(t, http.StatusMethodNotAllowed, ErrorMethod)
	if r.header.Get("Allow") != "PUT, DELETE" {
		t.Fatalf("Allow %q", r.header.Get("Allow"))
	}
	do(t, f.server, http.MethodPut, "/v1/kv", nil).want(t, http.StatusMethodNotAllowed, ErrorMethod)
	do(t, f.server, http.MethodPost, "/v1/writes/1.1", nil).want(t, http.StatusMethodNotAllowed, ErrorMethod)
	do(t, f.server, http.MethodGet, "/v2/kv/k", nil).want(t, http.StatusNotFound, ErrorNotFound)
	for _, id := range []string{"abc", "1", "0.1", "1.0", "1.x", "-1.2"} {
		do(t, f.server, http.MethodGet, "/v1/writes/"+id, nil).want(t, http.StatusBadRequest, ErrorBadWriteID)
	}
	// A sequence this writer never handed out.
	do(t, f.server, http.MethodGet, fmt.Sprintf("/v1/writes/%d.999999", f.writer.Epoch()), nil).
		want(t, http.StatusBadRequest, ErrorBadWriteID)
}

func TestAsyncWriteThenItsStatus(t *testing.T) {
	f := newFixture(t, 0, Options{})
	r := do(t, f.server, http.MethodPut, kvPathFor("k"), []byte("v"), "Prefer", "respond-async")
	id := r.body["write_id"]
	if r.status != http.StatusAccepted || id == "" || r.body["revision"] != "" {
		t.Fatalf("async PUT: %d %v", r.status, r.body)
	}
	if r.header.Get("Location") != "/v1/writes/"+id || r.header.Get("Preference-Applied") != "respond-async" {
		t.Fatalf("headers %v", r.header)
	}

	r = do(t, f.server, http.MethodGet, "/v1/writes/"+id, nil)
	if r.status != http.StatusOK || r.body["status"] != "pending" || r.header.Get("Retry-After") == "" {
		t.Fatalf("status before commit: %d %v %v", r.status, r.body, r.header)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	r = do(t, f.server, http.MethodGet, "/v1/writes/"+id, nil)
	if r.status != http.StatusOK || r.body["status"] != "committed" {
		t.Fatalf("status after commit: %d %v", r.status, r.body)
	}
	if got := f.reader(t).ViewPosition(); got < revision(t, r) {
		t.Fatalf("reader at %d, below the committed write's revision %s", got, r.body["revision"])
	}
}

func TestUnconfirmedWriteIs504WithItsID(t *testing.T) {
	f := newFixture(t, 0, Options{CommitWait: 200 * time.Millisecond})
	r := do(t, f.server, http.MethodPut, kvPathFor("k"), []byte("v"), "Prefer", "wait=abc, handling=lenient")
	r.want(t, http.StatusGatewayTimeout, ErrorOutcomeUnknown)
	if r.body["write_id"] == "" || r.header.Get("Location") != "/v1/writes/"+r.body["write_id"] {
		t.Fatalf("504 without its write: %v %v", r.body, r.header)
	}
	if r.header.Get("Preference-Applied") != "" {
		t.Fatalf("a malformed wait was applied: %q", r.header.Get("Preference-Applied"))
	}
	if s := do(t, f.server, http.MethodGet, "/v1/writes/"+r.body["write_id"], nil); s.body["status"] != "pending" {
		t.Fatalf("status %v, want pending", s.body)
	}

	// Prefer: wait shortens the server's wait, never lengthens it.
	f = newFixture(t, 0, Options{CommitWait: 10 * time.Second})
	start := time.Now()
	r = do(t, f.server, http.MethodPut, kvPathFor("k"), []byte("v"), "Prefer", "wait=1")
	r.want(t, http.StatusGatewayTimeout, ErrorOutcomeUnknown)
	if took := time.Since(start); took < time.Second || took > 5*time.Second {
		t.Fatalf("wait=1 answered after %s", took)
	}
	if r.header.Get("Preference-Applied") != "wait=1" {
		t.Fatalf("Preference-Applied %q", r.header.Get("Preference-Applied"))
	}
}

func TestTooLargeIs413(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{MaxValueBytes: 8, MaxBodyBytes: 64})
	do(t, f.server, http.MethodPut, kvPathFor("k"), []byte("123456789")).want(t, http.StatusRequestEntityTooLarge, ErrorTooLarge)
	do(t, f.server, http.MethodPost, "/v1/kv", bytes.Repeat([]byte(" "), 65)).want(t, http.StatusRequestEntityTooLarge, ErrorTooLarge)

	// A batch past what one memtable holds.
	f = newFixture(t, 10*time.Millisecond, Options{MaxBodyBytes: 4 << 20}, func(o *isledb.WriterOptions) {
		o.Memtable.TargetBytes = 64 << 10
		o.Values.MaxValueBytes = 1 << 10
	})
	var ops []map[string]any
	for i := range 2000 {
		ops = append(ops, map[string]any{"op": "put", "key": fmt.Appendf(nil, "k%05d", i), "value": []byte("v")})
	}
	body, _ := json.Marshal(map[string]any{"ops": ops})
	do(t, f.server, http.MethodPost, "/v1/kv", body).want(t, http.StatusRequestEntityTooLarge, ErrorTooLarge)
}

func TestBackpressureIs503(t *testing.T) {
	f := newFixture(t, 0, Options{}, func(o *isledb.WriterOptions) {
		o.Memtable.TargetBytes = 16 << 10
		o.Memtable.MaxPendingMemtables = 1
	})
	value := bytes.Repeat([]byte("v"), 4<<10)
	for i := range 100 {
		r := do(t, f.server, http.MethodPut, kvPathFor(fmt.Sprint(i)), value, "Prefer", "respond-async")
		if r.status == http.StatusAccepted {
			continue
		}
		r.want(t, http.StatusServiceUnavailable, ErrorBackpressure)
		if r.header.Get("Retry-After") == "" || r.body["write_id"] != "" {
			t.Fatalf("backpressure: %v %v", r.header, r.body)
		}
		return
	}
	t.Fatal("no backpressure after 100 uncommitted writes")
}

func TestFencedWriterAnswersHonestly(t *testing.T) {
	f := newFixture(t, 0, Options{})
	// A synchronous write left waiting: no flush interval.
	waiting := make(chan reply, 1)
	go func() { waiting <- do(t, f.server, http.MethodPut, kvPathFor("k"), []byte("old"), "Prefer", "wait=30") }()
	deadline := time.Now().Add(5 * time.Second)
	for f.writer.AcceptedSequence() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the write was never accepted")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Another writer takes over; the first learns of it on its next commit.
	next, nextServer := f.serve(t, 10*time.Millisecond, Options{})
	if err := f.writer.Flush(f.ctx); err == nil {
		t.Fatal("Flush after the takeover succeeded")
	}
	r := <-waiting
	r.want(t, http.StatusServiceUnavailable, ErrorNotCommitted)
	id := r.body["write_id"]
	if id == "" {
		t.Fatal("not_committed without its write id")
	}

	// The new writer settles it; the old one sends callers there.
	if s := do(t, nextServer, http.MethodGet, "/v1/writes/"+id, nil); s.status != http.StatusOK || s.body["status"] != "not_committed" {
		t.Fatalf("new writer on %s: %d %v", id, s.status, s.body)
	}
	do(t, f.server, http.MethodGet, fmt.Sprintf("/v1/writes/%d.1", next.Epoch()), nil).want(t, http.StatusServiceUnavailable, ErrorUnavailable)
	do(t, f.server, http.MethodPut, kvPathFor("k"), []byte("again")).want(t, http.StatusServiceUnavailable, ErrorUnavailable)

	if r := do(t, nextServer, http.MethodPut, kvPathFor("k"), []byte("new")); r.status != http.StatusOK {
		t.Fatalf("PUT on the new writer: %d %v", r.status, r.body)
	}
	if v, _, _ := f.reader(t).Get(f.ctx, []byte("k")); string(v) != "new" {
		t.Fatalf("value %q, want new", v)
	}
}

func TestParsePrefer(t *testing.T) {
	for _, c := range []struct {
		in    []string
		async bool
		wait  time.Duration
	}{
		{nil, false, 0},
		{[]string{"respond-async"}, true, 0},
		{[]string{"Respond-Async, wait=5"}, true, 5 * time.Second},
		{[]string{"handling=lenient", `wait="3"; x=y`}, false, 3 * time.Second},
		{[]string{"wait=0", "wait=-1", "wait=abc"}, false, 0},
		{[]string{"return=minimal"}, false, 0},
	} {
		if got := parsePrefer(c.in); got.async != c.async || got.wait != c.wait {
			t.Errorf("parsePrefer(%q) = %+v, want async %t wait %s", c.in, got, c.async, c.wait)
		}
	}
}

func TestNewHandlerRefusesBadOptions(t *testing.T) {
	good := Options{CommitWait: time.Second, MaxValueBytes: 1, MaxBodyBytes: 1, MaxInflightBytes: 1, BodyReadTimeout: time.Second}
	if _, err := NewHandler(nil, good); err == nil {
		t.Fatal("nil writer accepted")
	}
	f := newFixture(t, 0, Options{})
	if _, err := NewHandler(f.writer, good); err != nil {
		t.Fatalf("good options: %v", err)
	}
	noWait, noValue, smallInflight, noTimeout := good, good, good, good
	noWait.CommitWait = 0
	noValue.MaxValueBytes = 0
	smallInflight.MaxBodyBytes = 2
	noTimeout.BodyReadTimeout = 0
	for _, opts := range []Options{noWait, noValue, smallInflight, noTimeout} {
		if _, err := NewHandler(f.writer, opts); err == nil || !strings.HasPrefix(err.Error(), "writehttp:") {
			t.Fatalf("options %+v: %v", opts, err)
		}
	}
}

// stalledPut starts a PUT of a body of length bytes that sends first and
// then waits until the returned function sends the rest; the reply arrives on
// the channel. A negative length sends the body chunked, without
// Content-Length.
func stalledPut(t *testing.T, server *httptest.Server, path string, length int64, first []byte) (finish func(rest []byte), reply <-chan *http.Response) {
	t.Helper()
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, server.URL+path, pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = length
	out := make(chan *http.Response, 1)
	go func() {
		resp, err := server.Client().Do(req)
		if err != nil {
			out <- nil
			return
		}
		out <- resp
	}()
	sent := make(chan struct{})
	go func() {
		_, _ = pw.Write(first)
		close(sent)
	}()
	return func(rest []byte) {
		<-sent
		_, _ = pw.Write(rest)
		_ = pw.Close()
	}, out
}

func TestBodiesInFlightAreBounded(t *testing.T) {
	// One PUT of the whole value limit takes the whole budget.
	const limit = 1 << 10
	f := newFixture(t, 10*time.Millisecond, Options{MaxValueBytes: limit, MaxBodyBytes: limit, MaxInflightBytes: limit})
	handler := f.server.Config.Handler.(*Handler)
	value := bytes.Repeat([]byte("v"), limit)
	finish, reply := stalledPut(t, f.server, kvPathFor("slow"), limit, value[:10])
	deadline := time.Now().Add(5 * time.Second)
	for handler.InflightBytes() != limit {
		if time.Now().After(deadline) {
			t.Fatalf("the slow body reserved %d bytes, want %d", handler.InflightBytes(), limit)
		}
		time.Sleep(5 * time.Millisecond)
	}

	r := do(t, f.server, http.MethodPut, kvPathFor("k"), []byte("v"))
	r.want(t, http.StatusServiceUnavailable, ErrorBackpressure)
	if r.header.Get("Retry-After") == "" {
		t.Fatal("backpressure without Retry-After")
	}

	finish(value[10:])
	resp := <-reply
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("the slow PUT: %v", resp)
	}
	_ = resp.Body.Close()
	if n := handler.InflightBytes(); n != 0 {
		t.Fatalf("%d bytes still reserved after the request", n)
	}
	if v, _, _ := f.reader(t).Get(f.ctx, []byte("slow")); !bytes.Equal(v, value) {
		t.Fatalf("the slow value arrived as %d bytes", len(v))
	}
	if r := do(t, f.server, http.MethodPut, kvPathFor("k"), []byte("v")); r.status != http.StatusOK {
		t.Fatalf("after the budget freed: %d %v", r.status, r.body)
	}
}

func TestBodyWithoutLengthIs411(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{})
	for _, c := range []struct{ method, path string }{{http.MethodPut, kvPathFor("k")}, {http.MethodPost, "/v1/kv"}} {
		req, _ := http.NewRequest(c.method, f.server.URL+c.path, io.MultiReader(strings.NewReader(`{"ops":[]}`)))
		req.ContentLength = -1 // chunked
		resp, err := f.server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]string
		_ = json.NewDecoder(resp.Body).Decode(&body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusLengthRequired || body["error"] != ErrorLengthRequired {
			t.Fatalf("%s %s chunked: %d %v, want 411 %s", c.method, c.path, resp.StatusCode, body, ErrorLengthRequired)
		}
	}
	if n := f.server.Config.Handler.(*Handler).InflightBytes(); n != 0 {
		t.Fatalf("%d bytes reserved by refused bodies", n)
	}
}

func TestSlowBodyIs408(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{BodyReadTimeout: 200 * time.Millisecond})
	finish, reply := stalledPut(t, f.server, kvPathFor("slow"), 100, []byte("part"))
	resp := <-reply
	finish(nil)
	if resp == nil {
		t.Fatal("no response to a stalled body")
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if resp.StatusCode != http.StatusRequestTimeout || body["error"] != ErrorBodyTimeout {
		t.Fatalf("stalled body: %d %v, want 408 %s", resp.StatusCode, body, ErrorBodyTimeout)
	}
	if n := f.server.Config.Handler.(*Handler).InflightBytes(); n != 0 {
		t.Fatalf("%d bytes still reserved after a timed-out body", n)
	}
}

// A body read timeout shorter than the wait for the commit must not end the
// request: the write is answered once committed, not as an empty 200.
func TestBodyTimeoutDoesNotEndTheCommitWait(t *testing.T) {
	f := newFixture(t, 250*time.Millisecond, Options{BodyReadTimeout: 50 * time.Millisecond})
	for _, value := range [][]byte{nil, []byte("v")} {
		r := do(t, f.server, http.MethodPut, kvPathFor("k"), value)
		if r.status != http.StatusOK || r.body["revision"] == "" {
			t.Fatalf("PUT of %d bytes: %d %v, want 200 with a revision", len(value), r.status, r.body)
		}
		if f.writer.CommittedSequence() < f.writer.AcceptedSequence() {
			t.Fatalf("answered before its commit: accepted %d, committed %d",
				f.writer.AcceptedSequence(), f.writer.CommittedSequence())
		}
	}
}

// rawStalled sends head, a request with part of its body, then stalls, and
// returns the status line of the answer and how long it took.
func rawStalled(t *testing.T, addr, head string) (string, time.Duration) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte(head)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("no answer to a stalled request after %s: %v", time.Since(start), err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	took := time.Since(start)
	if !resp.Close {
		t.Fatalf("%s kept the connection with its body unread", resp.Status)
	}
	// The server lets go of the connection too, rather than waiting for the
	// rest of the body.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("%s: the server held the connection after answering: %v", resp.Status, err)
	}
	return resp.Status, took
}

// A request refused before its body is read is answered at once, even when
// the client stalls mid-body.
func TestRefusedStalledRequestsAreAnsweredAtOnce(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{BodyReadTimeout: 10 * time.Second, MaxValueBytes: 1 << 10})
	addr := f.server.Listener.Addr().String()
	for _, c := range []struct{ name, head, status string }{
		{"chunked", "PUT /v1/kv/k HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel", "411 Length Required"},
		{"bad ttl", "PUT /v1/kv/k?ttl=soon HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nhel", "400 Bad Request"},
		{"too large", "PUT /v1/kv/k HTTP/1.1\r\nHost: x\r\nContent-Length: 2000\r\n\r\nhel", "413 Request Entity Too Large"},
		{"wrong method", "PATCH /v1/kv/k HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nhel", "405 Method Not Allowed"},
	} {
		status, took := rawStalled(t, addr, c.head)
		if status != c.status || took > time.Second {
			t.Fatalf("%s: %q after %s, want %q at once", c.name, status, took, c.status)
		}
	}
}

func TestTTLAndWriteIDEdges(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{})
	// A ttl that is present must not be empty, in the query or in a batch.
	r := do(t, f.server, http.MethodPut, kvPathFor("k")+"?ttl=", []byte("v"))
	r.want(t, http.StatusBadRequest, ErrorBadRequest)
	if !strings.Contains(r.body["message"], "empty") {
		t.Fatalf("empty ttl: %q", r.body["message"])
	}
	do(t, f.server, http.MethodPost, "/v1/kv", []byte(`{"ops":[{"op":"put","key":"aw==","value":"dg==","ttl":""}]}`)).
		want(t, http.StatusBadRequest, ErrorBadRequest)
	do(t, f.server, http.MethodPost, "/v1/kv", []byte(`{"ops":[{"op":"delete","key":"aw==","ttl":null}]}`)).
		want(t, http.StatusOK, "")

	// A write id is URL-decoded like a key.
	put := do(t, f.server, http.MethodPut, kvPathFor("k"), []byte("v"))
	encoded := strings.Replace(put.body["write_id"], ".", "%2E", 1)
	if s := do(t, f.server, http.MethodGet, "/v1/writes/"+encoded, nil); s.status != http.StatusOK || s.body["status"] != "committed" {
		t.Fatalf("encoded id %s: %d %v", encoded, s.status, s.body)
	}

	// A fresh writer says it has issued nothing, not a backwards range.
	fresh := newFixture(t, 10*time.Millisecond, Options{})
	s := do(t, fresh.server, http.MethodGet, fmt.Sprintf("/v1/writes/%d.1", fresh.writer.Epoch()), nil)
	s.want(t, http.StatusBadRequest, ErrorBadWriteID)
	if !strings.Contains(s.body["message"], "none yet") {
		t.Fatalf("fresh writer: %q", s.body["message"])
	}
}

func TestBatchErrorsSayWhatIsWrong(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{})
	for body, want := range map[string]string{
		`{"ops":[{"op":"put","key":"!!","value":"dg=="}]}`: "base64",
		`{"ops":[{"op":"put","key":1}]}`:                   "key",
		`{"ops":[],"extra":1}`:                             "extra",
		`{"ops":[]}]`:                                      "data after",
	} {
		r := do(t, f.server, http.MethodPost, "/v1/kv", []byte(body))
		r.want(t, http.StatusBadRequest, ErrorBadRequest)
		if !strings.Contains(r.body["message"], want) {
			t.Errorf("%s: message %q, want it to mention %q", body, r.body["message"], want)
		}
	}
}

// hideController hides the server's connection, so a read deadline cannot be
// set on it.
type hideController struct{ http.ResponseWriter }

func TestBodyRefusedWhenItsReadCannotBeBounded(t *testing.T) {
	f := newFixture(t, 10*time.Millisecond, Options{})
	handler := f.server.Config.Handler.(*Handler)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(hideController{w}, r)
	}))
	defer server.Close()
	r := do(t, server, http.MethodPut, kvPathFor("k"), []byte("v"))
	r.want(t, http.StatusInternalServerError, ErrorInternal)
	if n := handler.InflightBytes(); n != 0 {
		t.Fatalf("%d bytes reserved for a body that could not be bounded", n)
	}
	if r := do(t, server, http.MethodDelete, kvPathFor("k"), nil); r.status != http.StatusOK {
		t.Fatalf("a request without a body: %d %v", r.status, r.body)
	}
}

func TestRouteNamesEveryEndpoint(t *testing.T) {
	for _, c := range []struct{ method, path, want string }{
		{http.MethodPut, "/v1/kv/a%2Fb", "kv_put"},
		{http.MethodDelete, "/v1/kv/a", "kv_delete"},
		{http.MethodPost, "/v1/kv", "kv_batch"},
		{http.MethodGet, "/v1/writes/1.2", "write_status"},
		{http.MethodGet, "/v1/kv/a", "other"},
		{http.MethodGet, "/metrics", "other"},
	} {
		r := httptest.NewRequest(c.method, c.path, nil)
		if got := Route(r); got != c.want {
			t.Errorf("%s %s: %q, want %q", c.method, c.path, got, c.want)
		}
	}
}
