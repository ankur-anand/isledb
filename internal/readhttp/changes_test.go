package readhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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

type feedFixture struct {
	ctx     context.Context
	bucket  *blob.Bucket
	writer  *isledb.Writer
	reader  *isledb.Reader
	changes *Changes
	server  *httptest.Server
}

// newFeedFixture serves the change feed of a fresh database. With feed set,
// the writer turns the feed on before the server starts; otherwise the
// database has none until the test opens a writer that does. The reader
// loads new views only when the test refreshes it.
func newFeedFixture(t *testing.T, feed bool) *feedFixture {
	t.Helper()
	f := &feedFixture{ctx: context.Background(), bucket: memblob.OpenBucket(nil)}
	t.Cleanup(func() { _ = f.bucket.Close() })
	if feed {
		f.openWriter(t)
	}
	readerDB, err := isledb.OpenBucket(f.ctx, f.bucket, "mem", isledb.DBOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = readerDB.Close() })
	ropts := isledb.DefaultReaderOpenOptions(t.TempDir())
	ropts.Views.RefreshAfter = time.Hour
	if f.reader, err = readerDB.OpenReader(f.ctx, ropts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.reader.Close() })
	f.changes = NewChanges(func(ctx context.Context) (*isledb.ChangeReader, error) { return readerDB.OpenChangeReader(ctx) })
	t.Cleanup(func() { _ = f.changes.Close() })
	handler, err := NewHandler(f.reader, Options{MaxConcurrent: 8, ReadTimeout: 10 * time.Second, Changes: f.changes})
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(handler)
	t.Cleanup(f.server.Close)
	return f
}

func (f *feedFixture) openWriter(t *testing.T) {
	t.Helper()
	db, err := isledb.OpenBucket(f.ctx, f.bucket, "mem", isledb.DBOptions{
		ChangeFeed: &isledb.ChangeFeedOptions{Payload: isledb.ChangeFeedFullValues},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	wopts := isledb.DefaultWriterOptions()
	wopts.Flush.Interval = 0
	if f.writer, err = db.OpenWriter(f.ctx, wopts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.writer.Close(f.ctx) })
}

func (f *feedFixture) commit(t *testing.T, apply func(w *isledb.Writer) error) {
	t.Helper()
	if err := apply(f.writer); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func (f *feedFixture) get(t *testing.T, query string) (int, []byte) {
	t.Helper()
	resp, err := f.server.Client().Get(f.server.URL + "/v1/changes?" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}

func (f *feedFixture) page(t *testing.T, query string) changesResponse {
	t.Helper()
	status, body := f.get(t, query)
	if status != http.StatusOK {
		t.Fatalf("changes %q: %d %s", query, status, body)
	}
	var page changesResponse
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("changes %q: %v in %s", query, err, body)
	}
	return page
}

// refresh loads the newest view, as the reader does every
// views.refresh_after.
func (f *feedFixture) refresh(t *testing.T) {
	t.Helper()
	if err := f.reader.Refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// caughtUp refreshes the reader and reads the feed from the oldest change,
// returning the head the read ends at, which the reader's view also holds.
func (f *feedFixture) caughtUp(t *testing.T) string {
	t.Helper()
	f.refresh(t)
	page := f.page(t, "from=oldest&limit=10000")
	if !page.CaughtUp {
		t.Fatalf("reading from the oldest change: %+v, want caught up", page)
	}
	return page.Next
}

func TestChangesFromOldestThenCaughtUp(t *testing.T) {
	f := newFeedFixture(t, true)
	f.commit(t, func(w *isledb.Writer) error {
		if _, err := w.Put(f.ctx, []byte("user/1"), []byte("a")); err != nil {
			return err
		}
		_, err := w.Delete(f.ctx, []byte("user/2"))
		return err
	})
	head := f.caughtUp(t)

	page := f.page(t, "from=oldest")
	if len(page.Changes) != 2 || !page.CaughtUp || page.Next != head {
		t.Fatalf("page %+v, want 2 changes, caught up at %q", page, head)
	}
	put, del := page.Changes[0], page.Changes[1]
	if put.Op != "put" || *put.Key != "user/1" || put.Value == nil || *put.Value != "a" || put.Sequence == 0 {
		t.Fatalf("put %+v", put)
	}
	if del.Op != "delete" || *del.Key != "user/2" || del.Value != nil || del.ValueBase64 != nil || del.Sequence <= put.Sequence {
		t.Fatalf("delete %+v", del)
	}
	if put.Revision == 0 || del.Revision != put.Revision {
		t.Fatalf("revisions %d and %d, want one commit's, above 0", put.Revision, del.Revision)
	}
	resp, err := f.server.Client().Get(f.server.URL + "/v1/kv/user%2F1")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got := resp.Header.Get(HeaderRevision); got != strconv.FormatUint(put.Revision, 10) {
		t.Fatalf("a read after refreshing is at revision %s, want the change's %d", got, put.Revision)
	}
	if again := f.page(t, "from="+url.QueryEscape(page.Next)); len(again.Changes) != 0 || !again.CaughtUp || again.Next != page.Next {
		t.Fatalf("from the caught-up cursor: %+v", again)
	}
	if h := f.page(t, "from=head"); h.Next != head || !h.CaughtUp || len(h.Changes) != 0 {
		t.Fatalf("from=head: %+v, want caught up at %q", h, head)
	}
}

// TestCaughtUpPollsReadNoStorage: with storage gone, a consumer at the head
// is still answered, from memory; one behind it needs storage and is not.
func TestCaughtUpPollsReadNoStorage(t *testing.T) {
	f := newFeedFixture(t, true)
	f.commit(t, func(w *isledb.Writer) error { _, err := w.Put(f.ctx, []byte("k"), []byte("v")); return err })
	f.commit(t, func(w *isledb.Writer) error { _, err := w.Put(f.ctx, []byte("k2"), []byte("v")); return err })
	head := f.caughtUp(t)
	if err := f.bucket.Close(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if page := f.page(t, "from="+url.QueryEscape(head)); !page.CaughtUp || page.Next != head {
			t.Fatalf("caught-up poll with storage gone: %+v", page)
		}
	}
	if status, body := f.get(t, "from=oldest"); status != http.StatusServiceUnavailable {
		t.Fatalf("a read that needs storage: %d %s, want 503", status, body)
	}
}

// TestPollingConsumerSeesNewChanges: a consumer at the head stays caught up
// until the reader loads a view with new changes, then reads them.
func TestPollingConsumerSeesNewChanges(t *testing.T) {
	f := newFeedFixture(t, true)
	f.commit(t, func(w *isledb.Writer) error { _, err := w.Put(f.ctx, []byte("first"), []byte("1")); return err })
	f.caughtUp(t)
	cursor := f.page(t, "from=head").Next
	f.commit(t, func(w *isledb.Writer) error { _, err := w.Put(f.ctx, []byte("second"), []byte("2")); return err })
	if page := f.page(t, "from="+url.QueryEscape(cursor)); !page.CaughtUp || len(page.Changes) != 0 || page.Next != cursor {
		t.Fatalf("before the reader loads the commit: %+v, want caught up at %q", page, cursor)
	}
	f.refresh(t)
	page := f.page(t, "from="+url.QueryEscape(cursor))
	if len(page.Changes) != 1 || *page.Changes[0].Key != "second" || !page.CaughtUp {
		t.Fatalf("after the reader loads the commit: %+v, want only second", page)
	}
	if again := f.page(t, "from="+url.QueryEscape(page.Next)); !again.CaughtUp || len(again.Changes) != 0 {
		t.Fatalf("from the new head: %+v, want caught up", again)
	}
}

// TestChangesStartOnceTheWriterTurnsTheFeedOn: a reader started before the
// database has a feed answers 404, then serves the feed once it has one.
func TestChangesStartOnceTheWriterTurnsTheFeedOn(t *testing.T) {
	f := newFeedFixture(t, false)
	status, body := f.get(t, "from=oldest")
	if status != http.StatusNotFound || !bytes.Contains(body, []byte(ErrorChangeFeedOff)) {
		t.Fatalf("before the feed: %d %s, want 404 %s", status, body, ErrorChangeFeedOff)
	}
	f.openWriter(t)
	f.commit(t, func(w *isledb.Writer) error { _, err := w.Put(f.ctx, []byte("k"), []byte("v")); return err })
	f.refresh(t)
	if status, body := f.get(t, "from=oldest"); status != http.StatusOK || !bytes.Contains(body, []byte(`"key":"k"`)) {
		t.Fatalf("once the reader loads the feed: %d %s", status, body)
	}
}

func TestChangesCarryBytesAndExpiry(t *testing.T) {
	f := newFeedFixture(t, true)
	expires := time.Now().Add(time.Hour)
	f.commit(t, func(w *isledb.Writer) error {
		if _, err := w.Put(f.ctx, []byte("\xff\x00"), []byte("\xfe")); err != nil {
			return err
		}
		_, err := w.PutWithTTL(f.ctx, []byte("ttl"), []byte(""), time.Hour)
		return err
	})
	f.caughtUp(t)
	page := f.page(t, "from=oldest")
	if len(page.Changes) != 2 {
		t.Fatalf("changes %+v", page.Changes)
	}
	bin, ttl := page.Changes[0], page.Changes[1]
	if bin.Key != nil || string(bin.KeyBase64) != "\xff\x00" || string(bin.ValueBase64) != "\xfe" {
		t.Fatalf("binary change %+v", bin)
	}
	if ttl.Value == nil || *ttl.Value != "" || ttl.ExpiresAt == nil || ttl.ExpiresAt.Sub(expires).Abs() > time.Minute {
		t.Fatalf("ttl change %+v, want an empty value expiring near %s", ttl, expires)
	}
}

func TestChangesRefuseBadRequests(t *testing.T) {
	f := newFeedFixture(t, true)
	f.commit(t, func(w *isledb.Writer) error { _, err := w.Put(f.ctx, []byte("k"), []byte("v")); return err })
	f.caughtUp(t)
	for _, query := range []string{"", "from=nope", "from=oldest&limit=0", "from=oldest&limit=10001", "from=oldest&limit=x",
		"from=oldest&from=head", "from=oldest&wait=30s", "a=%zz"} {
		status, body := f.get(t, query)
		var e errorResponse
		_ = json.Unmarshal(body, &e)
		if status != http.StatusBadRequest || e.Error != ErrorBadRequest {
			t.Errorf("%q: %d %s, want 400", query, status, body)
		}
	}
	resp, err := f.server.Client().Post(f.server.URL+"/v1/changes", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "GET" {
		t.Fatalf("POST: %d Allow %q", resp.StatusCode, resp.Header.Get("Allow"))
	}
	if page := f.page(t, "from=oldest&limit=1"); len(page.Changes) != 1 {
		t.Fatalf("limit=1: %+v", page)
	}
}

func TestHandlerWithoutChangesAnswers404(t *testing.T) {
	f := newFixture(t, Options{})
	r := f.do(t, http.MethodGet, "/v1/changes?from=oldest")
	r.wantError(t, http.StatusNotFound, ErrorChangeFeedOff)
}

// TestCursorPastTheViewIsCaughtUpFromMemory: a cursor past the reader's
// view, from a page the feed read further, is answered with itself and no
// read until the view catches up; one past anything loaded goes to storage.
func TestCursorPastTheViewIsCaughtUpFromMemory(t *testing.T) {
	f := newFeedFixture(t, true)
	f.commit(t, func(w *isledb.Writer) error { _, err := w.Put(f.ctx, []byte("a"), []byte("1")); return err })
	viewHead := f.caughtUp(t)
	f.commit(t, func(w *isledb.Writer) error { _, err := w.Put(f.ctx, []byte("b"), []byte("2")); return err })
	page := f.page(t, "from=oldest") // the feed reads CURRENT again; the view stays at viewHead
	if len(page.Changes) != 2 || *page.Changes[1].Key != "b" || page.Next == viewHead {
		t.Fatalf("page past the view: %+v, want a and b", page)
	}
	ahead := page.Next

	if _, err := isledb.ParseChangeCursor(farCursor); err != nil {
		t.Fatal(err)
	}
	if status, body := f.get(t, "from="+url.QueryEscape(farCursor)); status != http.StatusBadRequest {
		t.Fatalf("a cursor past anything stored: %d %s, want 400", status, body)
	}

	if err := f.bucket.Close(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if again := f.page(t, "from="+url.QueryEscape(ahead)); !again.CaughtUp || len(again.Changes) != 0 || again.Next != ahead {
			t.Fatalf("poll past the view with storage gone: %+v, want caught up at %q", again, ahead)
		}
	}
	if h := f.page(t, "from=head"); h.Next != viewHead {
		t.Fatalf("from=head: %q, want the view's head %q", h.Next, viewHead)
	}
}

// farCursor is a cursor at manifest entry 2^40, past anything a test stores.
const farCursor = "cf1_AAABAAAAAAAAAAAAAAAAAA"

// TestCursorBetweenTheViewAndTheFeedGetsTheRest: a page that stops inside
// a batch the reader's view does not hold yet leaves a cursor between the
// view's head and the feed's; the next poll gets the rest of the batch, from
// what the feed has loaded, not a caught-up page.
func TestCursorBetweenTheViewAndTheFeedGetsTheRest(t *testing.T) {
	f := newFeedFixture(t, true)
	f.commit(t, func(w *isledb.Writer) error { _, err := w.Put(f.ctx, []byte("a"), []byte("1")); return err })
	f.caughtUp(t)
	f.commit(t, func(w *isledb.Writer) error {
		if _, err := w.Put(f.ctx, []byte("b"), []byte("2")); err != nil {
			return err
		}
		_, err := w.Put(f.ctx, []byte("c"), []byte("3"))
		return err
	})
	page := f.page(t, "from=oldest&limit=2") // a, then b: stops inside the batch of b and c
	if len(page.Changes) != 2 || *page.Changes[1].Key != "b" || page.CaughtUp {
		t.Fatalf("first page: %+v, want a and b, not caught up", page)
	}
	if err := f.bucket.Close(); err != nil {
		t.Fatal(err)
	}
	rest := f.page(t, "from="+url.QueryEscape(page.Next))
	if len(rest.Changes) != 1 || *rest.Changes[0].Key != "c" || !rest.CaughtUp {
		t.Fatalf("from inside the batch: %+v, want c", rest)
	}
}
