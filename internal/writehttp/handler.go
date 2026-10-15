// Package writehttp serves an isledb Writer over HTTP: single-key puts and
// deletes, atomic batches, and the outcome of a write by its id.
package writehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/isledb"
)

// Error codes, the "error" field of an error body.
const (
	ErrorBadRequest      = "bad_request"      // a malformed request
	ErrorBodyTimeout     = "body_timeout"     // the body did not arrive in time
	ErrorInvalidMutation = "invalid_mutation" // a key, value or TTL the writer refuses
	ErrorTooLarge        = "too_large"        // the body or the batch is too large
	ErrorLengthRequired  = "length_required"  // a body without Content-Length
	ErrorBackpressure    = "backpressure"     // too many writes waiting or being read; retry
	ErrorUnavailable     = "unavailable"      // the writer takes no writes now; retry
	ErrorNotCommitted    = "not_committed"    // accepted, never committed; safe to resend
	ErrorOutcomeUnknown  = "outcome_unknown"  // accepted, outcome not known; ask its status
	ErrorBadWriteID      = "bad_write_id"     // a write id this writer cannot answer for
	ErrorNotFound        = "not_found"        // no such endpoint
	ErrorMethod          = "method_not_allowed"
	ErrorInternal        = "internal"
)

// Options configure a Handler.
type Options struct {
	// CommitWait is the longest a write waits for its commit before it is
	// answered 504; a client's Prefer: wait can only shorten it.
	CommitWait time.Duration
	// MaxValueBytes caps a PUT body; MaxBodyBytes a POST body.
	MaxValueBytes int64
	MaxBodyBytes  int64
	// MaxInflightBytes bounds the memory of requests being read and
	// applied; a request past it is answered 503 before its body is read. A
	// PUT counts its body, a batch BatchReserveFactor times its body, for its
	// decoded copy and the batch built from it. At least MaxValueBytes and
	// BatchReserveFactor times MaxBodyBytes.
	MaxInflightBytes int64
	// BodyReadTimeout is how long a request has to send its body; past it,
	// the request is answered 408.
	BodyReadTimeout time.Duration
}

// BatchReserveFactor is how many times its body a batch request reserves of
// the in-flight budget: the body, the JSON decoder's copy of it, the decoded
// keys and values, and the batch built from them.
const BatchReserveFactor = 4

// Handler serves the write API:
//
//	PUT    /v1/kv/{key}?ttl=10s      write one key; the body is the value
//	DELETE /v1/kv/{key}              delete one key
//	POST   /v1/kv                    apply a JSON batch atomically
//	GET    /v1/writes/{epoch}.{seq}  whether a write committed
//
// Writes are answered once committed, or at once with Prefer: respond-async.
// Every response is JSON. Paths are read as sent, without the cleaning an
// http.ServeMux would do, so keys may hold "//" or "..". It does not
// authenticate: the server that mounts it must.
type Handler struct {
	writer          *isledb.Writer
	commitWait      time.Duration
	maxValueBytes   int64
	maxBodyBytes    int64
	bodyReadTimeout time.Duration

	inflightMu  sync.Mutex
	inflight    int64 // bytes reserved by bodies being read or applied
	maxInflight int64
}

// NewHandler returns a Handler writing through writer.
func NewHandler(writer *isledb.Writer, opts Options) (*Handler, error) {
	switch {
	case writer == nil:
		return nil, errors.New("writehttp: nil writer")
	case opts.CommitWait <= 0:
		return nil, fmt.Errorf("writehttp: commit wait %s, want above 0", opts.CommitWait)
	case opts.MaxValueBytes <= 0 || opts.MaxBodyBytes <= 0:
		return nil, fmt.Errorf("writehttp: body limits %d and %d, want above 0", opts.MaxValueBytes, opts.MaxBodyBytes)
	case opts.MaxInflightBytes < max(opts.MaxValueBytes, BatchReserveFactor*opts.MaxBodyBytes):
		return nil, fmt.Errorf("writehttp: in-flight limit %d is below the value limit %d or %d times the body limit %d: such a request could never be read",
			opts.MaxInflightBytes, opts.MaxValueBytes, BatchReserveFactor, opts.MaxBodyBytes)
	case opts.BodyReadTimeout <= 0:
		return nil, fmt.Errorf("writehttp: body read timeout %s, want above 0", opts.BodyReadTimeout)
	}
	return &Handler{writer: writer, commitWait: opts.CommitWait,
		maxValueBytes: opts.MaxValueBytes, maxBodyBytes: opts.MaxBodyBytes,
		bodyReadTimeout: opts.BodyReadTimeout, maxInflight: opts.MaxInflightBytes}, nil
}

// InflightBytes is how many body bytes are reserved now, by requests being
// read or applied.
func (h *Handler) InflightBytes() int64 {
	h.inflightMu.Lock()
	defer h.inflightMu.Unlock()
	return h.inflight
}

// reserve takes n bytes of the in-flight budget, or reports that they would
// pass it.
func (h *Handler) reserve(n int64) bool {
	h.inflightMu.Lock()
	defer h.inflightMu.Unlock()
	if h.inflight+n > h.maxInflight {
		return false
	}
	h.inflight += n
	return true
}

func (h *Handler) release(n int64) {
	h.inflightMu.Lock()
	h.inflight -= n
	h.inflightMu.Unlock()
}

const (
	kvPath     = "/v1/kv"
	writesPath = "/v1/writes/"
)

// Route names the endpoint r is for, as a metrics label: kv_put, kv_delete,
// kv_batch, write_status, or other.
func Route(r *http.Request) string {
	path := r.URL.EscapedPath()
	switch {
	case path == kvPath && r.Method == http.MethodPost:
		return "kv_batch"
	case strings.HasPrefix(path, kvPath+"/") && r.Method == http.MethodPut:
		return "kv_put"
	case strings.HasPrefix(path, kvPath+"/") && r.Method == http.MethodDelete:
		return "kv_delete"
	case strings.HasPrefix(path, writesPath) && r.Method == http.MethodGet:
		return "write_status"
	default:
		return "other"
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength != 0 {
		// The body read timeout runs from the start, so neither reading the
		// body nor discarding it after an early answer can stall. Until the
		// body is read, an HTTP/1 answer closes the connection, so the server
		// sends it at once instead of first discarding the body.
		if r.ProtoMajor == 1 {
			w.Header().Set("Connection", "close")
		}
		rc := http.NewResponseController(w)
		if err := rc.SetReadDeadline(time.Now().Add(h.bodyReadTimeout)); err != nil {
			// Without a deadline a stalled body would hold its reservation
			// for ever: refuse it instead.
			slog.Error("writehttp: cannot bound the body read", "error", err)
			writeError(w, http.StatusInternalServerError, ErrorInternal, "the request body cannot be read", "")
			return
		}
		defer func() {
			// Answered without reading the body (readBody clears the header
			// once it has): end the connection now, not after the server
			// has waited out the body read timeout to discard it.
			if w.Header().Get("Connection") == "close" {
				_ = rc.SetReadDeadline(time.Now())
			}
		}()
	}
	path := r.URL.EscapedPath()
	switch {
	case path == kvPath:
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		if _, ok := query(w, r); ok {
			h.serveBatch(w, r)
		}
	case strings.HasPrefix(path, kvPath+"/"):
		key, err := url.PathUnescape(path[len(kvPath)+1:])
		if err != nil {
			writeError(w, http.StatusBadRequest, ErrorBadRequest, "key is not percent-encoded correctly", "")
			return
		}
		switch r.Method {
		case http.MethodPut:
			q, ok := query(w, r, "ttl")
			if !ok {
				return
			}
			var raw *string
			if q.Has("ttl") {
				v := q.Get("ttl")
				raw = &v
			}
			ttl, err := parseTTL(raw)
			if err != nil {
				writeError(w, http.StatusBadRequest, ErrorBadRequest, "ttl: "+err.Error(), "")
				return
			}
			h.servePut(w, r, []byte(key), ttl)
		case http.MethodDelete:
			if _, ok := query(w, r); ok {
				h.write(w, r, func() {}, func(ctx context.Context) (uint64, error) {
					return h.writer.Delete(ctx, []byte(key))
				})
			}
		default:
			methodNotAllowed(w, http.MethodPut+", "+http.MethodDelete)
		}
	case strings.HasPrefix(path, writesPath):
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		id, err := url.PathUnescape(path[len(writesPath):])
		if err != nil {
			writeError(w, http.StatusBadRequest, ErrorBadWriteID, "the write id is not URL-encoded correctly", "")
			return
		}
		if _, ok := query(w, r); ok {
			h.serveStatus(w, id)
		}
	default:
		writeError(w, http.StatusNotFound, ErrorNotFound, "no such endpoint", "")
	}
}

// query returns r's query parameters, answering 400 for a malformed query,
// a parameter not in allowed, or one given twice.
func query(w http.ResponseWriter, r *http.Request, allowed ...string) (url.Values, bool) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorBadRequest, "the query is not encoded correctly", "")
		return nil, false
	}
	for _, name := range slices.Sorted(maps.Keys(q)) { // the same error for the same query
		switch {
		case !slices.Contains(allowed, name):
			writeError(w, http.StatusBadRequest, ErrorBadRequest, fmt.Sprintf("unknown query parameter %q", name), "")
			return nil, false
		case len(q[name]) > 1:
			writeError(w, http.StatusBadRequest, ErrorBadRequest, fmt.Sprintf("query parameter %q given more than once", name), "")
			return nil, false
		}
	}
	return q, true
}

func (h *Handler) servePut(w http.ResponseWriter, r *http.Request, key []byte, ttl time.Duration) {
	value, release, ok := h.readBody(w, r, h.maxValueBytes, 1)
	if !ok {
		return
	}
	h.write(w, r, release, func(ctx context.Context) (uint64, error) {
		return h.writer.PutWithTTL(ctx, key, value, ttl)
	})
}

type batchRequest struct {
	Ops []batchOp `json:"ops"`
}

type batchOp struct {
	Op    string  `json:"op"`
	Key   []byte  `json:"key"`
	Value []byte  `json:"value"`
	TTL   *string `json:"ttl"`
}

// parseTTL reads a TTL given as a duration such as 10s. Absent means none;
// present and empty is an error, so an unset template variable is not taken
// as "never expires".
func parseTTL(raw *string) (time.Duration, error) {
	if raw == nil {
		return 0, nil
	}
	if *raw == "" {
		return 0, errors.New("empty; leave it out for no expiry")
	}
	ttl, err := time.ParseDuration(*raw)
	if err != nil {
		return 0, errors.New("want a duration such as 10s")
	}
	return ttl, nil
}

func (h *Handler) serveBatch(w http.ResponseWriter, r *http.Request) {
	body, release, ok := h.readBody(w, r, h.maxBodyBytes, BatchReserveFactor)
	if !ok {
		return
	}
	applied := false
	defer func() {
		if !applied {
			release()
		}
	}()
	var req batchRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, ErrorBadRequest, "want one JSON object {\"ops\":[...]}: "+err.Error(), "")
		return
	}
	// The second Decode must find nothing: More alone misses a trailing ]
	// or }.
	if dec.Decode(&struct{}{}) != io.EOF {
		writeError(w, http.StatusBadRequest, ErrorBadRequest, "data after the JSON object", "")
		return
	}
	b := isledb.NewBatch()
	size := 0
	for _, op := range req.Ops {
		size += len(op.Key) + len(op.Value)
	}
	b.Grow(size)
	for i, op := range req.Ops {
		switch op.Op {
		case "put":
			ttl, err := parseTTL(op.TTL)
			if err != nil {
				writeError(w, http.StatusBadRequest, ErrorBadRequest, fmt.Sprintf("op %d: ttl: %v", i, err), "")
				return
			}
			b.PutWithTTL(op.Key, op.Value, ttl)
		case "delete":
			if op.Value != nil || op.TTL != nil {
				writeError(w, http.StatusBadRequest, ErrorBadRequest,
					fmt.Sprintf("op %d: a delete has no value or ttl", i), "")
				return
			}
			b.Delete(op.Key)
		default:
			writeError(w, http.StatusBadRequest, ErrorBadRequest,
				fmt.Sprintf("op %d: op %q, want put or delete", i, op.Op), "")
			return
		}
	}
	applied = true
	h.write(w, r, release, func(ctx context.Context) (uint64, error) {
		return h.writer.Apply(ctx, b)
	})
}

// readBody reads r's body after reserving factor times its Content-Length in
// the in-flight budget. The caller calls release once done with the body.
func (h *Handler) readBody(w http.ResponseWriter, r *http.Request, limit, factor int64) (body []byte, release func(), ok bool) {
	size := r.ContentLength
	switch {
	case size < 0:
		writeError(w, http.StatusLengthRequired, ErrorLengthRequired, "send Content-Length: a body of unknown length is not accepted", "")
		return nil, nil, false
	case size > limit:
		writeError(w, http.StatusRequestEntityTooLarge, ErrorTooLarge, fmt.Sprintf("body above %d bytes", limit), "")
		return nil, nil, false
	case !h.reserve(factor * size):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, ErrorBackpressure, "too many request bodies in flight; retry", "")
		return nil, nil, false
	}
	release = func() { h.release(factor * size) }
	body = make([]byte, size)
	_, err := io.ReadFull(r.Body, body)
	switch {
	case err == nil:
		// The body is read: lift the deadline, which would otherwise end
		// the request while it waits for its commit, and keep the connection.
		_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
		w.Header().Del("Connection")
		return body, release, true
	case errors.Is(err, os.ErrDeadlineExceeded):
		writeError(w, http.StatusRequestTimeout, ErrorBodyTimeout,
			fmt.Sprintf("the body did not arrive within %s", h.bodyReadTimeout), "")
	default:
		writeError(w, http.StatusBadRequest, ErrorBadRequest, "could not read the body", "")
	}
	release()
	return nil, nil, false
}

type writeResponse struct {
	WriteID  string `json:"write_id"`
	Revision string `json:"revision,omitempty"`
}

// write makes a write with apply, then answers once it commits. It calls
// release as soon as apply returns: the body is not needed while waiting.
func (h *Handler) write(w http.ResponseWriter, r *http.Request, release func(), apply func(context.Context) (uint64, error)) {
	seq, err := apply(r.Context())
	release()
	if err != nil {
		h.applyError(w, r, err)
		return
	}
	epoch := h.writer.Epoch()
	id := formatWriteID(epoch, seq)
	prefs := parsePrefer(r.Header.Values("Prefer"))
	if prefs.async {
		w.Header().Set("Preference-Applied", "respond-async")
		w.Header().Set("Location", writesPath+id)
		writeJSON(w, http.StatusAccepted, writeResponse{WriteID: id})
		return
	}
	wait := h.commitWait
	if prefs.wait > 0 && prefs.wait < wait {
		wait = prefs.wait
		w.Header().Set("Preference-Applied", "wait="+strconv.Itoa(int(wait/time.Second)))
	}
	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()
	position, err := h.writer.WaitCommitted(ctx, seq)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, writeResponse{WriteID: id, Revision: formatRevision(position)})
	case errors.Is(err, isledb.ErrFenced):
		// Usually never committed, but a takeover this writer learned of late
		// leaves it unknown: CommitStatus tells which.
		switch status, _ := h.writer.CommitStatus(epoch, seq); status {
		case isledb.CommitNever:
			writeError(w, http.StatusServiceUnavailable, ErrorNotCommitted,
				"another writer took over before this write committed; it never will, so resending is safe", id)
		case isledb.CommitDone:
			resp := writeResponse{WriteID: id}
			if position, err := h.writer.WaitCommitted(context.Background(), seq); err == nil {
				resp.Revision = formatRevision(position)
			}
			writeJSON(w, http.StatusOK, resp)
		default:
			h.outcomeUnknown(w, id)
		}
	default:
		// The wait ran out, the request ended, or the writer closed before
		// confirming: the write may still land. Answer even when the client
		// may be gone, since returning without an answer sends an empty 200.
		h.outcomeUnknown(w, id)
	}
}

func (h *Handler) outcomeUnknown(w http.ResponseWriter, id string) {
	w.Header().Set("Location", writesPath+id)
	writeError(w, http.StatusGatewayTimeout, ErrorOutcomeUnknown,
		"the write was accepted but its commit is not confirmed; ask "+writesPath+id, id)
}

// applyError answers a write the writer refused. Every path answers: a
// handler that returns without one is sent as an empty 200.
func (h *Handler) applyError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, isledb.ErrInvalidMutation):
		writeError(w, http.StatusBadRequest, ErrorInvalidMutation, err.Error(), "")
	case errors.Is(err, isledb.ErrBatchTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, ErrorTooLarge, err.Error(), "")
	case errors.Is(err, isledb.ErrBackpressure):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, ErrorBackpressure, "too many writes waiting to commit; retry", "")
	case errors.Is(err, isledb.ErrWritesStopped), errors.Is(err, isledb.ErrWriterClosed), errors.Is(err, isledb.ErrFenced):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, ErrorUnavailable, "this writer takes no writes now; retry", "")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, ErrorUnavailable, "the request ended before the write was accepted; retry", "")
	default:
		slog.Warn("writehttp: apply", "error", err)
		writeError(w, http.StatusInternalServerError, ErrorInternal, "the write was not accepted", "")
	}
}

type statusResponse struct {
	Status   string `json:"status"`
	Revision string `json:"revision,omitempty"`
}

func (h *Handler) serveStatus(w http.ResponseWriter, raw string) {
	epoch, seq, ok := parseWriteID(raw)
	if !ok {
		writeError(w, http.StatusBadRequest, ErrorBadWriteID, "want a write id such as 7.10442", "")
		return
	}
	status, err := h.writer.CommitStatus(epoch, seq)
	switch {
	case errors.Is(err, isledb.ErrFenced):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, ErrorUnavailable, "a later writer took over and can answer; retry", "")
		return
	case err != nil:
		writeError(w, http.StatusBadRequest, ErrorBadWriteID, err.Error(), "")
		return
	}
	if status == isledb.CommitUnknown && epoch == h.writer.Epoch() {
		// This writer stopped without learning where its commits ended; the
		// writer that replaced it can tell.
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, ErrorUnavailable, "this writer was replaced and the one that replaced it can answer; retry", "")
		return
	}
	resp := statusResponse{Status: status.String()}
	switch status {
	case isledb.CommitDone:
		if epoch == h.writer.Epoch() {
			// Committed already, so this returns at once.
			if position, err := h.writer.WaitCommitted(context.Background(), seq); err == nil {
				resp.Revision = formatRevision(position)
			}
		}
	case isledb.CommitPending:
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, http.StatusOK, resp)
}

func formatWriteID(epoch, seq uint64) string { return fmt.Sprintf("%d.%d", epoch, seq) }

func parseWriteID(s string) (epoch, seq uint64, ok bool) {
	e, q, found := strings.Cut(s, ".")
	if !found {
		return 0, 0, false
	}
	epoch, err1 := strconv.ParseUint(e, 10, 64)
	seq, err2 := strconv.ParseUint(q, 10, 64)
	if err1 != nil || err2 != nil || epoch == 0 || seq == 0 {
		return 0, 0, false
	}
	return epoch, seq, true
}

// formatRevision writes a view position as a revision: a decimal string, as
// a JSON number could exceed what JavaScript holds exactly.
func formatRevision(p isledb.ViewPosition) string { return strconv.FormatUint(uint64(p), 10) }

type preferences struct {
	async bool
	wait  time.Duration
}

// parsePrefer reads respond-async and wait=N (seconds) from Prefer headers.
// Unknown or malformed preferences are ignored.
func parsePrefer(values []string) preferences {
	var p preferences
	for _, value := range values {
		for pref := range strings.SplitSeq(value, ",") {
			pref, _, _ = strings.Cut(pref, ";") // parameters, unused here
			name, arg, _ := strings.Cut(strings.TrimSpace(pref), "=")
			name, arg = strings.ToLower(strings.TrimSpace(name)), strings.Trim(strings.TrimSpace(arg), `"`)
			switch name {
			case "respond-async":
				p.async = true
			case "wait":
				if n, err := strconv.ParseUint(arg, 10, 32); err == nil && n > 0 {
					p.wait = time.Duration(n) * time.Second
				}
			}
		}
	}
	return p
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	WriteID string `json:"write_id,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, message, writeID string) {
	writeJSON(w, status, errorResponse{Error: code, Message: message, WriteID: writeID})
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, ErrorMethod, "allowed: "+allow, "")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
