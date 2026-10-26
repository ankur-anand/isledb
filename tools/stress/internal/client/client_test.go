package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func serve(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return New(s.URL, "tok")
}

func reply(status int, body string, header ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i+1 < len(header); i += 2 {
			w.Header().Set(header[i], header[i+1])
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func TestWriteOutcomes(t *testing.T) {
	for _, c := range []struct {
		name   string
		h      http.HandlerFunc
		want   Outcome
		rev    uint64
		id     string
		hasErr bool
	}{
		{"committed", reply(200, `{"write_id":"7.1","revision":"42"}`), Committed, 42, "7.1", false},
		{"committed without revision", reply(200, `{"write_id":"7.1"}`), Unknown, 0, "7.1", true},
		{"backpressure", reply(503, `{"error":"backpressure"}`, "Retry-After", "2"), Refused, 0, "", false},
		{"unavailable", reply(503, `{"error":"unavailable"}`), Refused, 0, "", false},
		{"not committed", reply(503, `{"error":"not_committed","write_id":"7.2"}`), NotCommitted, 0, "7.2", false},
		{"body timeout", reply(408, `{"error":"body_timeout"}`), Refused, 0, "", false},
		{"commit wait", reply(504, `{"error":"outcome_unknown","write_id":"7.3"}`), Unknown, 0, "7.3", false},
		{"proxy error", reply(502, `bad gateway`), Unknown, 0, "", true},
		{"too large", reply(413, `{"error":"too_large","message":"split it"}`), Invalid, 0, "", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := serve(t, c.h).Write(context.Background(), []byte(`{"ops":[]}`))
			if r.Outcome != c.want || r.Revision != c.rev || r.WriteID != c.id || (r.Err != nil) != c.hasErr {
				t.Fatalf("got %+v", r)
			}
			if c.name == "backpressure" && r.RetryAfter.Seconds() != 2 {
				t.Fatalf("Retry-After %s, want 2s", r.RetryAfter)
			}
		})
	}
}

func TestWriteSendsTheBatch(t *testing.T) {
	var got struct {
		Ops []map[string]any `json:"ops"`
	}
	var auth, ctype string
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		auth, ctype = r.Header.Get("Authorization"), r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = io.WriteString(w, `{"write_id":"1.1","revision":"1"}`)
	})
	body, err := EncodeBatch([]Op{{Key: []byte{0, 0xff}, Value: []byte("v")}, {Delete: true, Key: []byte("k")}})
	if err != nil {
		t.Fatal(err)
	}
	if r := c.Write(context.Background(), body); r.Outcome != Committed {
		t.Fatalf("%+v", r)
	}
	if auth != "Bearer tok" || ctype != "application/json" {
		t.Fatalf("headers: %q %q", auth, ctype)
	}
	if len(got.Ops) != 2 || got.Ops[0]["op"] != "put" || got.Ops[0]["key_base64"] != "AP8=" || got.Ops[0]["value_base64"] != "dg==" ||
		got.Ops[1]["op"] != "delete" || got.Ops[1]["value_base64"] != nil {
		t.Fatalf("sent %+v", got.Ops)
	}
}

func TestRefusedConnectionWasNeverSent(t *testing.T) {
	s := httptest.NewServer(http.NotFoundHandler())
	url := s.URL
	s.Close()
	if r := New(url, "").Write(context.Background(), []byte(`{}`)); r.Outcome != Refused {
		t.Fatalf("a refused connection: %+v, want refused", r)
	}
}

func TestGetEscapesKeysAndReadsRevisions(t *testing.T) {
	var path string
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.EscapedPath()
		w.Header().Set("Isledb-Revision", "9")
		if strings.HasSuffix(path, "%00") {
			w.WriteHeader(404)
			_, _ = io.WriteString(w, `{"error":"key_not_found"}`)
			return
		}
		_, _ = io.WriteString(w, "value")
	})
	v, found, rev, err := c.Get(context.Background(), []byte("a/b c\xff"))
	if err != nil || !found || rev != 9 || string(v) != "value" {
		t.Fatalf("%q %v %d %v", v, found, rev, err)
	}
	if path != "/v1/kv/a%2Fb%20c%FF" {
		t.Fatalf("path %q", path)
	}
	if _, found, rev, err := c.Get(context.Background(), []byte{0}); err != nil || found || rev != 9 {
		t.Fatalf("missing key: %v %d %v", found, rev, err)
	}
	busy := serve(t, reply(503, `{"error":"backpressure"}`))
	if _, _, _, err := busy.Get(context.Background(), []byte("k")); err != ErrRetry {
		t.Fatalf("503: %v, want ErrRetry", err)
	}
	norev := serve(t, reply(200, "v"))
	if _, _, _, err := norev.Get(context.Background(), []byte("k")); err == nil {
		t.Fatal("an answer without a revision was accepted")
	}
}

func TestScanDecodesTextAndBase64(t *testing.T) {
	var query string
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		w.Header().Set("Isledb-Revision", "5")
		_, _ = io.WriteString(w, `{"items":[{"key":"a","value":"x"},{"key_base64":"AP8=","value_base64":"AQ=="},{"key":"e","value":""}],"next_page":"tok"}`)
	})
	p, err := c.Scan(context.Background(), []byte{0xff, 'a'}, []byte("z"), 3, "")
	if err != nil {
		t.Fatal(err)
	}
	if p.Revision != 5 || p.Next != "tok" || len(p.Items) != 3 ||
		string(p.Items[0].Key) != "a" || string(p.Items[1].Key) != "\x00\xff" || p.Items[1].Value[0] != 1 ||
		p.Items[2].Value == nil || len(p.Items[2].Value) != 0 {
		t.Fatalf("%+v", p)
	}
	if query != "end=z&limit=3&start=%FFa" {
		t.Fatalf("query %q", query)
	}
}

func TestStatus(t *testing.T) {
	c := serve(t, reply(200, `{"status":"committed","revision":"77"}`))
	if s, err := c.Status(context.Background(), "7.1"); err != nil || s.Status != "committed" || s.Revision != 77 {
		t.Fatalf("%+v %v", s, err)
	}
	c = serve(t, reply(503, `{"error":"unavailable"}`, "Retry-After", "1"))
	if s, err := c.Status(context.Background(), "7.1"); err != nil || s.Status != "unavailable" || s.RetryAfter.Seconds() != 1 {
		t.Fatalf("%+v %v", s, err)
	}
}
