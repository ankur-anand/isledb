// Package readhttp serves reads of an isledb Reader over HTTP: one key, or one
// page of a scan, at a time, from the reader's current view.
package readhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ankur-anand/isledb"
)

// Error codes, the "error" field of an error body.
const (
	ErrorBadRequest    = "bad_request"   // a malformed request
	ErrorKeyNotFound   = "key_not_found" // the key does not exist, or expired
	ErrorNotFound      = "not_found"     // no such endpoint
	ErrorMethod        = "method_not_allowed"
	ErrorBackpressure  = "backpressure"    // too many reads in progress; retry
	ErrorUnavailable   = "unavailable"     // the read could not be answered now; retry
	ErrorChangeFeedOff = "change_feed_off" // the database has no change feed
	ErrorCursorExpired = "cursor_expired"  // changes after the cursor are no longer kept
)

// HeaderRevision names the view position that answered a read.
const HeaderRevision = "Isledb-Revision"

// Options configure a Handler.
type Options struct {
	// MaxConcurrent bounds the reads in progress; past it, a read is answered
	// 503. It bounds memory to about MaxConcurrent largest values, or pages of
	// a scan: MaxPageBytes plus one value.
	MaxConcurrent int
	// ReadTimeout bounds a read, storage included: one that hangs gives up
	// its slot and is answered 503.
	ReadTimeout time.Duration
	// Changes, if set, serves the change feed at /v1/changes; nil answers
	// 404 there.
	Changes *Changes
}

// Handler serves the read API:
//
//	GET  /v1/kv/{key}   the value, as the body
//	HEAD /v1/kv/{key}   the same, without the body
//	GET  /v1/kv?prefix=  one page of the keys in a range, as JSON
//	GET  /v1/changes?from=  one page of the change feed, as JSON
//
// Each read is answered from a snapshot of the reader's current view, whose
// position is sent as Isledb-Revision, also with a 404. A scan's pages are
// each from their own snapshot: a key written or deleted between pages may
// or may not be seen, but no key is seen twice. Paths are read as
// sent, without the cleaning an http.ServeMux would do, so keys may hold "//"
// or "..". It does not authenticate: the server that mounts it must.
type Handler struct {
	reader      *isledb.Reader
	slots       chan struct{}
	readTimeout time.Duration
	changes     *Changes
}

// NewHandler returns a Handler reading through reader.
func NewHandler(reader *isledb.Reader, opts Options) (*Handler, error) {
	switch {
	case reader == nil:
		return nil, errors.New("readhttp: nil reader")
	case opts.MaxConcurrent <= 0:
		return nil, fmt.Errorf("readhttp: max concurrent %d, want above 0", opts.MaxConcurrent)
	case opts.ReadTimeout <= 0:
		return nil, fmt.Errorf("readhttp: read timeout %s, want above 0", opts.ReadTimeout)
	}
	return &Handler{reader: reader, slots: make(chan struct{}, opts.MaxConcurrent), readTimeout: opts.ReadTimeout, changes: opts.Changes}, nil
}

// InProgress is how many reads are being served now.
func (h *Handler) InProgress() int { return len(h.slots) }

const (
	kvPath      = "/v1/kv/"
	scanPath    = "/v1/kv"
	changesPath = "/v1/changes"
)

// Route names the endpoint r is for, as a metrics label: kv_get, kv_head,
// kv_scan, changes, or other.
func Route(r *http.Request) string {
	path := r.URL.EscapedPath()
	if r.Method == http.MethodGet {
		switch path {
		case scanPath:
			return "kv_scan"
		case changesPath:
			return "changes"
		}
	}
	if strings.HasPrefix(path, kvPath) {
		switch r.Method {
		case http.MethodGet:
			return "kv_get"
		case http.MethodHead:
			return "kv_head"
		}
	}
	return "other"
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// A read answers for one moment; a cache in front must ask again, or it
	// would keep "missing" after the key is written.
	w.Header().Set("Cache-Control", "no-cache")
	path := r.URL.EscapedPath()
	switch {
	case path == changesPath:
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeError(w, http.StatusMethodNotAllowed, ErrorMethod, "the change feed is read with GET")
			return
		}
		if h.changes == nil {
			writeError(w, http.StatusNotFound, ErrorChangeFeedOff, "this reader does not serve the change feed")
			return
		}
		req, err := parseChanges(r.URL.RawQuery)
		if err != nil {
			writeError(w, http.StatusBadRequest, ErrorBadRequest, err.Error())
			return
		}
		if h.takeSlot(w) {
			defer h.releaseSlot()
			h.serveChanges(w, r, req)
		}
	case path == scanPath:
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeError(w, http.StatusMethodNotAllowed, ErrorMethod, "a scan is a GET; send writes to the writer")
			return
		}
		req, err := parseScan(r.URL.RawQuery)
		if err != nil {
			writeError(w, http.StatusBadRequest, ErrorBadRequest, err.Error())
			return
		}
		if h.takeSlot(w) {
			defer h.releaseSlot()
			h.serveScan(w, r, req)
		}
	case strings.HasPrefix(path, kvPath):
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeError(w, http.StatusMethodNotAllowed, ErrorMethod, "a reader only reads; send writes to the writer")
			return
		}
		key, err := url.PathUnescape(path[len(kvPath):])
		switch {
		case err != nil:
			writeError(w, http.StatusBadRequest, ErrorBadRequest, "key is not percent-encoded correctly")
			return
		case key == "":
			writeError(w, http.StatusBadRequest, ErrorBadRequest, "empty key")
			return
		}
		switch q, err := url.ParseQuery(r.URL.RawQuery); {
		case err != nil:
			writeError(w, http.StatusBadRequest, ErrorBadRequest, "the query is not encoded correctly")
			return
		case len(q) > 0:
			writeError(w, http.StatusBadRequest, ErrorBadRequest,
				fmt.Sprintf("no query parameters are taken, got %q", slices.Sorted(maps.Keys(q))))
			return
		}
		if h.takeSlot(w) {
			defer h.releaseSlot()
			h.serveKey(w, r, []byte(key))
		}
	default:
		writeError(w, http.StatusNotFound, ErrorNotFound, "no such endpoint")
	}
}

// takeSlot takes a slot for a read, or answers 503 when none is free.
func (h *Handler) takeSlot(w http.ResponseWriter) bool {
	select {
	case h.slots <- struct{}{}:
		return true
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, ErrorBackpressure, "too many reads in progress; retry")
		return false
	}
}

func (h *Handler) releaseSlot() { <-h.slots }

func (h *Handler) serveKey(w http.ResponseWriter, r *http.Request, key []byte) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()
	snap, err := h.reader.Snapshot(ctx)
	if err != nil {
		readFailed(w, err)
		return
	}
	defer func() { _ = snap.Close() }()
	value, found, err := snap.Get(ctx, key)
	if err != nil {
		readFailed(w, err)
		return
	}
	w.Header().Set(HeaderRevision, strconv.FormatUint(uint64(snap.ViewPosition()), 10))
	if !found {
		writeError(w, http.StatusNotFound, ErrorKeyNotFound, "no such key")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(value)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(value)
	}
}

// readFailed answers a read that could not be answered, always 503: none of
// the causes is the client's, and a retry, here or on another replica, may
// succeed. The expected ones, a deadline, an expired view or a closed
// reader, are not logged; anything else, such as storage failing, is.
func readFailed(w http.ResponseWriter, err error) {
	w.Header().Set("Retry-After", "1")
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusServiceUnavailable, ErrorUnavailable, "the read did not finish in time; retry")
	case errors.Is(err, isledb.ErrReadViewExpired), errors.Is(err, isledb.ErrSnapshotExpired),
		errors.Is(err, isledb.ErrReaderClosed), errors.Is(err, isledb.ErrChangeReaderClosed):
		writeError(w, http.StatusServiceUnavailable, ErrorUnavailable, "no current view to read from; retry")
	default:
		// Not the key or path: keys can be sensitive, and logs travel.
		slog.Warn("readhttp: read failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, ErrorUnavailable, "the read failed; retry")
	}
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
