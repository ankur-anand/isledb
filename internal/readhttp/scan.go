package readhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"unicode/utf8"

	"github.com/ankur-anand/isledb"
)

const (
	// DefaultScanLimit is the items in a page when the request gives no limit.
	DefaultScanLimit = 100
	// MaxScanLimit is the most items a request may ask for.
	MaxScanLimit = 1000
	// MaxPageBytes caps the keys and values in a page; a page past it ends
	// early, but always holds at least one item.
	MaxPageBytes = 1 << 20
)

// pageTokenVersion leads every page token, so its contents can change later.
const pageTokenVersion = 1

type scanRequest struct {
	start, end []byte // [start, end); nil is unbounded
	limit      int
}

// parseScan reads a scan's query: prefix, or start and end; limit; and page,
// the token from the page before.
func parseScan(rawQuery string) (scanRequest, error) {
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return scanRequest{}, errors.New("the query is not encoded correctly")
	}
	for _, name := range slices.Sorted(maps.Keys(q)) {
		switch name {
		case "prefix", "start", "end", "limit", "page":
		default:
			return scanRequest{}, fmt.Errorf("unknown query parameter %q", name)
		}
		if len(q[name]) > 1 {
			return scanRequest{}, fmt.Errorf("%s given more than once", name)
		}
		if q.Get(name) == "" {
			return scanRequest{}, fmt.Errorf("%s is empty: give a value or leave it out", name)
		}
	}
	req := scanRequest{limit: DefaultScanLimit}
	if q.Has("prefix") {
		if q.Has("start") || q.Has("end") {
			return scanRequest{}, errors.New("give prefix, or start and end, not both")
		}
		req.start = []byte(q.Get("prefix"))
		req.end = prefixEnd(req.start)
	} else {
		if q.Has("start") {
			req.start = []byte(q.Get("start"))
		}
		if q.Has("end") {
			req.end = []byte(q.Get("end"))
		}
	}
	if q.Has("limit") {
		n, err := strconv.Atoi(q.Get("limit"))
		if err != nil || n < 1 || n > MaxScanLimit {
			return scanRequest{}, fmt.Errorf("limit %q, want 1 to %d", q.Get("limit"), MaxScanLimit)
		}
		req.limit = n
	}
	if q.Has("page") {
		next, err := decodePageToken(q.Get("page"))
		if err != nil || bytes.Compare(next, req.start) < 0 || (req.end != nil && bytes.Compare(next, req.end) >= 0) {
			return scanRequest{}, errors.New("page is not a token from this scan: send the same prefix, start and end with it")
		}
		req.start = next
	}
	return req, nil
}

// prefixEnd is the first key past every key starting with prefix, or nil
// when there is none.
func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func encodePageToken(next []byte) string {
	return base64.RawURLEncoding.EncodeToString(append([]byte{pageTokenVersion}, next...))
}

func decodePageToken(token string) ([]byte, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(b) < 2 || b[0] != pageTokenVersion {
		return nil, errors.New("bad page token")
	}
	return b[1:], nil
}

// scanItem is a key and value as text when they are valid UTF-8, else as
// base64, the same fields the writer's batch takes.
type scanItem struct {
	Key         *string `json:"key,omitempty"`
	KeyBase64   []byte  `json:"key_base64,omitempty"`
	Value       *string `json:"value,omitempty"`
	ValueBase64 []byte  `json:"value_base64,omitempty"`
}

type scanResponse struct {
	Items    []scanItem `json:"items"`
	NextPage string     `json:"next_page,omitempty"`
}

func newScanItem(key, value []byte) scanItem {
	var item scanItem
	item.Key, item.KeyBase64 = textOrBase64(key)
	item.Value, item.ValueBase64 = textOrBase64(value)
	return item
}

// textOrBase64 returns b as text when it is valid UTF-8, else as bytes to
// send as base64.
func textOrBase64(b []byte) (*string, []byte) {
	if utf8.Valid(b) {
		s := string(b)
		return &s, nil
	}
	return nil, bytes.Clone(b)
}

// serveScan answers one page from a snapshot. The page is built before it is
// sent, so a read that fails partway is still answered 503.
func (h *Handler) serveScan(w http.ResponseWriter, r *http.Request, req scanRequest) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()
	snap, err := h.reader.Snapshot(ctx)
	if err != nil {
		readFailed(w, err)
		return
	}
	defer func() { _ = snap.Close() }()
	it, err := snap.NewIterator(ctx, isledb.IteratorOptions{MinKey: req.start, MaxKey: req.end})
	if err != nil {
		readFailed(w, err)
		return
	}
	defer func() { _ = it.Close() }()
	resp := scanResponse{Items: []scanItem{}}
	size := 0
	for it.Next() {
		key, value := it.Key(), it.Value()
		if len(resp.Items) == req.limit || (len(resp.Items) > 0 && size+len(key)+len(value) > MaxPageBytes) {
			resp.NextPage = encodePageToken(key)
			break
		}
		resp.Items = append(resp.Items, newScanItem(key, value))
		size += len(key) + len(value)
	}
	if err := it.Err(); err != nil {
		readFailed(w, err)
		return
	}
	w.Header().Set(HeaderRevision, strconv.FormatUint(uint64(snap.ViewPosition()), 10))
	writeJSON(w, http.StatusOK, resp)
}
