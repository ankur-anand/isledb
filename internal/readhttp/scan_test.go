package readhttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func (f *fixture) scan(t *testing.T, query string) (reply, scanResponse) {
	t.Helper()
	r := f.do(t, http.MethodGet, "/v1/kv?"+query)
	if r.status != http.StatusOK {
		t.Fatalf("scan %q: %d %s", query, r.status, r.body)
	}
	if ct := r.header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("scan %q: Content-Type %q", query, ct)
	}
	var resp scanResponse
	if err := json.Unmarshal(r.body, &resp); err != nil {
		t.Fatalf("scan %q: %v in %s", query, err, r.body)
	}
	return r, resp
}

func (f *fixture) commitAll(t *testing.T, kv ...string) uint64 {
	t.Helper()
	var revision uint64
	for i := 0; i < len(kv); i += 2 {
		revision = f.commit(t, kv[i], kv[i+1])
	}
	return revision
}

func itemKey(it scanItem) string {
	if it.Key != nil {
		return *it.Key
	}
	return string(it.KeyBase64)
}

func keysOf(items []scanItem) []string {
	keys := make([]string, len(items))
	for i, it := range items {
		keys[i] = itemKey(it)
	}
	return keys
}

func TestScanPrefixReturnsItsKeysInOrder(t *testing.T) {
	f := newFixture(t, Options{})
	revision := f.commitAll(t, "user/2", "b", "other/1", "x", "user/1", "a", "user/3", "c", "user0", "y")
	r, resp := f.scan(t, "prefix="+url.QueryEscape("user/"))
	if got := strings.Join(keysOf(resp.Items), ","); got != "user/1,user/2,user/3" {
		t.Fatalf("keys %s", got)
	}
	if *resp.Items[0].Value != "a" || *resp.Items[2].Value != "c" {
		t.Fatalf("values %s", r.body)
	}
	if resp.NextPage != "" || r.revision(t) != revision {
		t.Fatalf("next_page %q, revision %d, want none and %d", resp.NextPage, r.revision(t), revision)
	}
}

func TestScanWithoutMatchesIsAnEmptyList(t *testing.T) {
	f := newFixture(t, Options{})
	f.commit(t, "k", "v")
	r, resp := f.scan(t, "prefix=none")
	if !bytes.Contains(r.body, []byte(`"items":[]`)) || resp.NextPage != "" {
		t.Fatalf("body %s, want an empty items list and no next_page", r.body)
	}
}

func TestScanStartIsInclusiveEndExclusive(t *testing.T) {
	f := newFixture(t, Options{})
	f.commitAll(t, "a", "1", "b", "2", "c", "3", "d", "4")
	if _, resp := f.scan(t, "start=b&end=d"); strings.Join(keysOf(resp.Items), ",") != "b,c" {
		t.Fatalf("start=b&end=d: %v", keysOf(resp.Items))
	}
	if _, resp := f.scan(t, "start=c"); strings.Join(keysOf(resp.Items), ",") != "c,d" {
		t.Fatalf("start=c: %v", keysOf(resp.Items))
	}
	if _, resp := f.scan(t, "end=b"); strings.Join(keysOf(resp.Items), ",") != "a" {
		t.Fatalf("end=b: %v", keysOf(resp.Items))
	}
	if _, resp := f.scan(t, "limit=10"); len(resp.Items) != 4 {
		t.Fatalf("whole keyspace: %v", keysOf(resp.Items))
	}
}

// TestScanPagesCoverEveryKeyOnce follows next_page to the end.
func TestScanPagesCoverEveryKeyOnce(t *testing.T) {
	f := newFixture(t, Options{})
	var want []string
	for i := range 7 {
		key := fmt.Sprintf("p/%d", i)
		f.commit(t, key, "v")
		want = append(want, key)
	}
	f.commit(t, "q", "v")
	var got []string
	query, pages := "prefix=p%2F&limit=3", 0
	for {
		_, resp := f.scan(t, query)
		pages++
		got = append(got, keysOf(resp.Items)...)
		if resp.NextPage == "" {
			break
		}
		query = "prefix=p%2F&limit=3&page=" + resp.NextPage
	}
	if strings.Join(got, ",") != strings.Join(want, ",") || pages != 3 {
		t.Fatalf("%d pages of %v, want 3 of %v", pages, got, want)
	}
}

// TestScanPagesFollowWritesBetweenThem: each page reads the newest view; a
// key written behind the bookmark is not seen, one ahead of it is, and no key
// comes twice.
func TestScanPagesFollowWritesBetweenThem(t *testing.T) {
	f := newFixture(t, Options{})
	f.commitAll(t, "a", "1", "b", "2", "c", "3")
	r1, page1 := f.scan(t, "limit=2")
	if strings.Join(keysOf(page1.Items), ",") != "a,b" || page1.NextPage == "" {
		t.Fatalf("page 1: %v, next %q", keysOf(page1.Items), page1.NextPage)
	}
	f.commitAll(t, "aa", "new", "d", "4")
	r2, page2 := f.scan(t, "limit=2&page="+page1.NextPage)
	if strings.Join(keysOf(page2.Items), ",") != "c,d" || page2.NextPage != "" {
		t.Fatalf("page 2: %v, next %q", keysOf(page2.Items), page2.NextPage)
	}
	if r2.revision(t) <= r1.revision(t) {
		t.Fatalf("page 2 at revision %d, page 1 at %d", r2.revision(t), r1.revision(t))
	}
}

func TestScanTextAndBytes(t *testing.T) {
	f := newFixture(t, Options{})
	f.commitAll(t, "\xff\x00", "\xfe", "empty", "", "text", "<a&b>")
	r, resp := f.scan(t, "limit=10")
	if len(resp.Items) != 3 {
		t.Fatalf("items %s", r.body)
	}
	if bin := resp.Items[2]; bin.Key != nil || string(bin.KeyBase64) != "\xff\x00" || bin.Value != nil || string(bin.ValueBase64) != "\xfe" {
		t.Fatalf("binary item %+v, want key_base64 and value_base64", bin)
	}
	if empty := resp.Items[0]; empty.Value == nil || *empty.Value != "" {
		t.Fatalf("empty value %+v, want \"value\":\"\"", empty)
	}
	if !bytes.Contains(r.body, []byte(`"value":""`)) || !bytes.Contains(r.body, []byte(`"value":"<a&b>"`)) {
		t.Fatalf("body %s, want the empty value and the text sent as is", r.body)
	}
}

func TestScanPageEndsAtItsByteCap(t *testing.T) {
	f := newFixture(t, Options{})
	big := strings.Repeat("v", 400<<10)
	f.commitAll(t, "a", big, "b", big, "c", big, "d", strings.Repeat("w", MaxPageBytes+1))
	_, page1 := f.scan(t, "limit=10")
	if strings.Join(keysOf(page1.Items), ",") != "a,b" || page1.NextPage == "" {
		t.Fatalf("page 1: %v, want a,b and a next page", keysOf(page1.Items))
	}
	_, page2 := f.scan(t, "limit=10&page="+page1.NextPage)
	if strings.Join(keysOf(page2.Items), ",") != "c" {
		t.Fatalf("page 2: %v, want c alone", keysOf(page2.Items))
	}
	// A value past the cap still comes back, alone, so a scan never sticks.
	_, page3 := f.scan(t, "limit=10&page="+page2.NextPage)
	if strings.Join(keysOf(page3.Items), ",") != "d" || page3.NextPage != "" {
		t.Fatalf("page 3: %v, next %q", keysOf(page3.Items), page3.NextPage)
	}
}

func TestScanRefusesBadQueries(t *testing.T) {
	f := newFixture(t, Options{})
	f.commit(t, "a/1", "v")
	_, page := f.scan(t, "prefix=a%2F&limit=1")
	f.commit(t, "a/2", "v")
	_, page = f.scan(t, "prefix=a%2F&limit=1")
	if page.NextPage == "" {
		t.Fatal("no next page to test with")
	}
	for _, query := range []string{
		"prefix=a&start=a",
		"prefix=a&end=b",
		"limit=0",
		"limit=1001",
		"limit=ten",
		"prefix=a&prefix=b",
		"prefix=",
		"start=",
		"keys_only=true",
		"page=not-a-token",
		"page=" + encodePageToken(nil),
		"prefix=b%2F&page=" + page.NextPage, // a token from another scan
		"a=%zz",
	} {
		f.do(t, http.MethodGet, "/v1/kv?"+query).wantError(t, http.StatusBadRequest, ErrorBadRequest)
	}
}

func TestScanIsGETOnly(t *testing.T) {
	f := newFixture(t, Options{})
	for _, method := range []string{http.MethodHead, http.MethodPost, http.MethodDelete} {
		r := f.do(t, method, "/v1/kv")
		if r.status != http.StatusMethodNotAllowed || r.header.Get("Allow") != "GET" {
			t.Fatalf("%s /v1/kv: %d, Allow %q", method, r.status, r.header.Get("Allow"))
		}
	}
}

func TestScanThatRunsOutOfTimeGivesUpItsSlot(t *testing.T) {
	f := newFixture(t, Options{ReadTimeout: time.Nanosecond})
	f.commit(t, "k", "v") // a new SST: the scan must go to storage
	r := f.do(t, http.MethodGet, "/v1/kv?prefix=k")
	r.wantError(t, http.StatusServiceUnavailable, ErrorUnavailable)
	if f.handler.InProgress() != 0 {
		t.Fatalf("%d reads still hold a slot", f.handler.InProgress())
	}
}

func TestScanPastTheLimitIs503(t *testing.T) {
	f := newFixture(t, Options{MaxConcurrent: 1})
	f.handler.slots <- struct{}{}
	r := f.do(t, http.MethodGet, "/v1/kv?prefix=k")
	r.wantError(t, http.StatusServiceUnavailable, ErrorBackpressure)
	<-f.handler.slots
}

func TestPrefixEnd(t *testing.T) {
	for _, c := range []struct{ prefix, want string }{
		{"a", "b"},
		{"user/", "user0"},
		{"a\xff", "b"},
		{"a\xff\xff", "b"},
		{"\xff\xff", ""},
	} {
		if got := prefixEnd([]byte(c.prefix)); string(got) != c.want || (c.want == "" && got != nil) {
			t.Errorf("prefixEnd(%q) = %q, want %q", c.prefix, got, c.want)
		}
	}
}
