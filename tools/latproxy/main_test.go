package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func startProxy(t *testing.T, minDelay, maxDelay time.Duration) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, "host="+r.Host+" path="+r.URL.Path)
	}))
	t.Cleanup(upstream.Close)
	handler, err := newProxy(upstream.URL, minDelay, maxDelay)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	return proxy, &hits
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func readStats(t *testing.T, base string) Stats {
	t.Helper()
	var s Stats
	if err := json.Unmarshal([]byte(get(t, base+"/_latproxy/stats")), &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestForwardsAfterDelayKeepingHost(t *testing.T) {
	proxy, hits := startProxy(t, 30*time.Millisecond, 30*time.Millisecond)
	start := time.Now()
	body := get(t, proxy.URL+"/bucket/authz/sstable/a.sst")
	if took := time.Since(start); took < 30*time.Millisecond {
		t.Fatalf("forwarded after %s, want at least the 30ms delay", took)
	}
	if hits.Load() != 1 {
		t.Fatalf("target saw %d requests, want 1", hits.Load())
	}
	// The client's Host reaches the target unchanged, as S3 signatures need.
	if want := "host=" + strings.TrimPrefix(proxy.URL, "http://"); !strings.HasPrefix(body, want) {
		t.Fatalf("target saw %q, want it to start with %q", body, want)
	}
}

func TestCountsByKindAndResets(t *testing.T) {
	proxy, _ := startProxy(t, 0, 0)
	paths := []string{
		"/bucket/authz/manifest/CURRENT",
		"/bucket/authz/manifest/CURRENT",
		"/bucket/authz/sstable/a.sst",
		"/bucket/authz/manifest/log/00000001",
		"/bucket?list-type=2",
	}
	bodies := map[string]int64{}
	for _, p := range paths {
		bodies[kindOf(strings.SplitN(p, "?", 2)[0])] += int64(len(get(t, proxy.URL+p)))
	}
	s := readStats(t, proxy.URL)
	for kind, want := range map[string]int64{"current": 2, "sst": 1, "manifest": 1, "other": 1} {
		if s[kind].Requests != want || s[kind].Bytes != bodies[kind] {
			t.Errorf("%s: %+v, want %d requests and %d bytes", kind, s[kind], want, bodies[kind])
		}
	}

	resp, err := http.Post(proxy.URL+"/_latproxy/reset", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	for kind, c := range readStats(t, proxy.URL) {
		if c.Requests != 0 || c.Bytes != 0 {
			t.Errorf("%s after reset: %+v, want zero", kind, c)
		}
	}
}

func TestCancelledWhileDelayedIsNotForwarded(t *testing.T) {
	proxy, hits := startProxy(t, time.Second, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, proxy.URL+"/bucket/authz/sstable/a.sst", nil)
	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatal("request finished, want it cancelled during the delay")
	}
	time.Sleep(1200 * time.Millisecond) // past the delay the request would have waited
	if hits.Load() != 0 || readStats(t, proxy.URL)["sst"].Requests != 0 {
		t.Fatalf("a cancelled request reached the target (%d hits) or was counted", hits.Load())
	}
}

func TestRejectsBadSettings(t *testing.T) {
	for _, tc := range []struct {
		target   string
		min, max time.Duration
	}{
		{"127.0.0.1:9000", 0, 0}, // not an absolute URL
		{"http://127.0.0.1:9000", -time.Millisecond, 0},
		{"http://127.0.0.1:9000", 40 * time.Millisecond, 15 * time.Millisecond},
	} {
		if _, err := newProxy(tc.target, tc.min, tc.max); err == nil {
			t.Errorf("newProxy(%q, %s, %s) accepted", tc.target, tc.min, tc.max)
		}
	}
}

func send(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestCountsUploadsByMethod(t *testing.T) {
	proxy, _ := startProxy(t, 0, 0)
	send(t, http.MethodPut, proxy.URL+"/bucket/authz/sstable/a.sst", strings.Repeat("x", 1000))
	send(t, http.MethodPut, proxy.URL+"/bucket/authz/sstable/b.sst", strings.Repeat("x", 500))
	get(t, proxy.URL+"/bucket/authz/sstable/a.sst")
	sst := readStats(t, proxy.URL)["sst"]
	if sst.Requests != 3 {
		t.Fatalf("sst requests %d, want 3", sst.Requests)
	}
	if put := sst.Methods["PUT"]; put.Requests != 2 || put.UploadedBytes != 1500 {
		t.Fatalf("PUT %+v, want 2 requests and 1500 uploaded bytes", put)
	}
	if g := sst.Methods["GET"]; g.Requests != 1 || g.UploadedBytes != 0 || g.Bytes == 0 {
		t.Fatalf("GET %+v, want 1 request with a response and no upload", g)
	}
}

func setFaults(t *testing.T, base, body string) int {
	t.Helper()
	code, _ := send(t, http.MethodPut, base+"/_latproxy/faults", body)
	return code
}

func TestInjectedRefusalIsNotForwarded(t *testing.T) {
	proxy, hits := startProxy(t, 0, 0)
	if code := setFaults(t, proxy.URL, `{"methods":["PUT"],"kinds":["sst"],"error_rate":1}`); code != http.StatusOK {
		t.Fatalf("setting faults: %d", code)
	}
	code, body := send(t, http.MethodPut, proxy.URL+"/bucket/authz/sstable/a.sst", "data")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "<Code>SlowDown</Code>") {
		t.Fatalf("got %d %q, want a 503 SlowDown", code, body)
	}
	if hits.Load() != 0 {
		t.Fatalf("a refused request reached the target")
	}
	// Requests the faults don't match pass.
	if code, _ := send(t, http.MethodGet, proxy.URL+"/bucket/authz/sstable/a.sst", ""); code != http.StatusOK || hits.Load() != 1 {
		t.Fatalf("an unmatched GET got %d, %d hits", code, hits.Load())
	}
	put := readStats(t, proxy.URL)["sst"].Methods["PUT"]
	if put.Injected != 1 || put.ServerErrors != 1 || put.UploadedBytes != 0 {
		t.Fatalf("PUT %+v, want one injected refusal and nothing uploaded", put)
	}
}

func TestDroppedAnswerIsForwarded(t *testing.T) {
	proxy, hits := startProxy(t, 0, 0)
	setFaults(t, proxy.URL, `{"kinds":["manifest"],"drop_rate":1}`)
	code, body := send(t, http.MethodPut, proxy.URL+"/bucket/authz/manifest/log/1", "entry")
	if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
		t.Fatalf("got %d %q, want a 500 InternalError", code, body)
	}
	if hits.Load() != 1 {
		t.Fatalf("target saw %d requests, want the dropped one forwarded", hits.Load())
	}
	put := readStats(t, proxy.URL)["manifest"].Methods["PUT"]
	if put.Dropped != 1 || put.UploadedBytes != int64(len("entry")) {
		t.Fatalf("PUT %+v, want one dropped answer with its body uploaded", put)
	}
	// Clearing the faults restores forwarding.
	setFaults(t, proxy.URL, `{}`)
	if code, _ := send(t, http.MethodPut, proxy.URL+"/bucket/authz/manifest/log/2", "e"); code != http.StatusOK {
		t.Fatalf("after clearing faults: %d", code)
	}
}

func TestRejectsBadFaults(t *testing.T) {
	proxy, _ := startProxy(t, 0, 0)
	for _, body := range []string{
		`{"error_rate":0.6,"drop_rate":0.6}`,
		`{"error_rate":-0.1}`,
		`{"methods":["PATCH"]}`,
		`{"kinds":["blob"]}`,
		`{"extra_delay":"soon"}`,
		`{"rate":1}`,
	} {
		if code := setFaults(t, proxy.URL, body); code != http.StatusBadRequest {
			t.Errorf("faults %s: %d, want 400", body, code)
		}
	}
	code, got := send(t, http.MethodGet, proxy.URL+"/_latproxy/faults", "")
	if code != http.StatusOK || strings.TrimSpace(got) != `{}` {
		t.Fatalf("faults after refusals: %d %q, want none set", code, got)
	}
}

func TestExtraDelay(t *testing.T) {
	proxy, _ := startProxy(t, 0, 0)
	setFaults(t, proxy.URL, `{"kinds":["current"],"extra_delay":"80ms"}`)
	start := time.Now()
	get(t, proxy.URL+"/bucket/authz/manifest/CURRENT")
	if took := time.Since(start); took < 80*time.Millisecond {
		t.Fatalf("took %s, want at least the 80ms extra delay", took)
	}
}
