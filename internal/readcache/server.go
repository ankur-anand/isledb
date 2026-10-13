package readcache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// Response headers of GET /v1/current.
const (
	// HeaderPosition is the log position of the CURRENT returned.
	HeaderPosition = "Isledb-Position"
	// HeaderAgeMillis is how long before responding the cache read that
	// CURRENT from storage, in milliseconds, rounded up. A reader takes its
	// view's load time as the time it sent the request minus this age.
	HeaderAgeMillis = "Isledb-Age-Ms"
)

// Handler serves the cache's API for a set of databases, each named by its
// prefix ("" for the bucket root) and with its own Relay:
//
//	GET /v1/current?db=P            the copy of P's CURRENT, with its
//	                                position and age
//	GET /v1/current?db=P&after=N    its position only, once the position
//	                                passes N or after the long-poll timeout
//	GET /v1/object/{key}            bytes of an immutable object of a
//	                                database served, by optional Range
//
// db may be left out when there is one database; the root is db=/. A long
// poll is a doorbell: a client that sees a new position reads CURRENT with a
// plain GET, so CURRENT reaches readers by one path. Both answer 503 at once
// before the relay has read CURRENT once, and a long poll that times out
// answers with a position not past N.
//
// An object request answers 206 for a range and 200 for the whole object,
// 400 for a bad or wrong size, 403 for a key that is not an immutable object
// of a database served (see Owner), 404 for a missing object, 405 for HEAD,
// 416 for a range outside it and 502 when storage fails.
//
// It does not authenticate: the server that mounts it must.
type Handler struct {
	relays   map[string]*Relay
	objects  *Objects
	longPoll time.Duration
	mux      *http.ServeMux
	stop     context.Context
	stopAll  context.CancelFunc
	held     atomic.Int64
}

// NewHandler serves the databases relays names, by clean prefix (see
// CleanPrefix; "" for the root), and their objects from objects. Without
// objects, only CURRENT is served.
func NewHandler(objects *Objects, relays map[string]*Relay, longPoll time.Duration) (*Handler, error) {
	if len(relays) == 0 {
		return nil, errors.New("readcache: no databases")
	}
	for prefix := range relays {
		if prefix == "" {
			continue
		}
		clean, err := CleanPrefix(prefix)
		if err != nil {
			return nil, err
		}
		if clean != prefix {
			return nil, fmt.Errorf("readcache: database prefix %q is not clean: use %q", prefix, clean)
		}
	}
	h := &Handler{relays: relays, objects: objects, longPoll: longPoll, mux: http.NewServeMux()}
	h.stop, h.stopAll = context.WithCancel(context.Background())
	h.mux.HandleFunc("GET /v1/current", h.serveCurrent)
	if objects != nil {
		h.mux.HandleFunc("GET /v1/object/{key...}", h.serveObject)
	}
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// LongPollsHeld is how many long polls are waiting now.
func (h *Handler) LongPollsHeld() int64 { return h.held.Load() }

// StopLongPolls answers every held long poll now, and every later one at
// once, with the position as it is, so that a server shutting down need not
// wait for them to time out.
func (h *Handler) StopLongPolls() { h.stopAll() }

func (h *Handler) serveCurrent(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	name := query.Get("db")
	if name == "/" {
		name = ""
	}
	relay, ok := h.relays[name]
	switch {
	case !query.Has("db") && len(h.relays) == 1:
		for _, only := range h.relays {
			relay = only
		}
	case !query.Has("db"):
		http.Error(w, "db is required: this cache serves several databases", http.StatusBadRequest)
		return
	case !ok:
		http.Error(w, "unknown database", http.StatusNotFound)
		return
	}
	after := query.Get("after")
	if after == "" {
		current, ok := relay.Current()
		if !ok {
			http.Error(w, "CURRENT not read yet", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(HeaderPosition, strconv.FormatUint(current.Position, 10))
		w.Header().Set(HeaderAgeMillis, strconv.FormatInt(ceilMillis(current.Age), 10))
		_, _ = w.Write(current.Data)
		return
	}
	n, err := strconv.ParseUint(after, 10, 64)
	if err != nil {
		http.Error(w, "after must be a log position", http.StatusBadRequest)
		return
	}
	// With no copy yet, there is nothing to wait for: answer at once so the
	// client retries rather than hold for the whole timeout.
	if _, loaded := relay.Current(); !loaded {
		http.Error(w, "CURRENT not read yet", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.longPoll)
	defer cancel()
	defer context.AfterFunc(h.stop, cancel)()
	h.held.Add(1)
	current, _ := relay.Wait(ctx, n)
	h.held.Add(-1)
	w.Header().Set(HeaderPosition, strconv.FormatUint(current.Position, 10))
	w.WriteHeader(http.StatusOK)
}

// ceilMillis rounds up, so a reader never thinks a copy younger than it is.
func ceilMillis(d time.Duration) int64 {
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// Request and response headers of GET /v1/object/{key}.
const (
	// HeaderObjectSize on a request gives the object's size in bytes, so the
	// cache need not ask storage for it. Chunks are kept per key and size: a
	// wrong size reads none of another request's chunks, and a read from
	// storage rejects it.
	HeaderObjectSize = "Isledb-Object-Size"
	// HeaderNoCache on a request asks the cache to serve the object without
	// keeping what it reads from storage, as a scan does.
	HeaderNoCache = "Isledb-No-Cache"
	// HeaderCache on a response says how its first 4 MiB were served: hit,
	// shared or miss (see CacheStatus).
	HeaderCache = "Isledb-Cache"
	// HeaderError on an error response names its kind for clients, where
	// the status alone does not: ErrorSizeMismatch on a 400.
	HeaderError = "Isledb-Error"
)

// ErrorSizeMismatch is HeaderError's value when Isledb-Object-Size is not
// the object's size in storage.
const ErrorSizeMismatch = "size-mismatch"

func (h *Handler) serveObject(w http.ResponseWriter, r *http.Request) {
	objects := h.objects
	// The GET pattern also matches HEAD, which would read and keep the
	// object only for its body to be dropped. No client needs it.
	if r.Method == http.MethodHead {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "use GET", http.StatusMethodNotAllowed)
		return
	}
	key := r.PathValue("key")
	if owner, ok := Owner(key); !ok || h.relays[owner] == nil {
		http.Error(w, "not an immutable object of a database served", http.StatusForbidden)
		return
	}
	var size int64
	if given := r.Header.Get(HeaderObjectSize); given != "" {
		n, err := strconv.ParseInt(given, 10, 64)
		if err != nil || n <= 0 || n > MaxObjectSize {
			http.Error(w, HeaderObjectSize+" must be a size in bytes, at most 5 TiB", http.StatusBadRequest)
			return
		}
		size = n
	} else {
		n, err := objects.Size(r.Context(), key)
		if err != nil {
			objectError(w, r, key, err)
			return
		}
		size = n
	}
	offset, length, partial, ok := parseRange(r.Header.Get("Range"), size)
	if !ok {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	started := false
	ready := func(status CacheStatus) {
		started = true
		header := w.Header()
		header.Set("Content-Type", "application/octet-stream")
		header.Set("Accept-Ranges", "bytes")
		header.Set("Content-Length", strconv.FormatInt(length, 10))
		header.Set(HeaderCache, string(status))
		if partial {
			header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, size))
			w.WriteHeader(http.StatusPartialContent)
		}
	}
	fill := r.Header.Get(HeaderNoCache) == ""
	err := objects.WriteRange(r.Context(), w, key, size, offset, length, fill, ready)
	switch {
	case err == nil:
	case !started:
		objectError(w, r, key, err)
	case r.Context().Err() == nil:
		// The status is sent; a short body tells the client it failed.
		slog.Warn("readcache: serve object", "key", key, "error", err)
	}
}

func objectError(w http.ResponseWriter, r *http.Request, key string, err error) {
	switch {
	case r.Context().Err() != nil:
	case errors.Is(err, blobstore.ErrNotFound):
		http.Error(w, "object not found", http.StatusNotFound)
	case errors.Is(err, ErrSizeMismatch):
		w.Header().Set(HeaderError, ErrorSizeMismatch)
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		// The details stay in the log: they can name buckets and providers.
		slog.Warn("readcache: serve object", "key", key, "error", err)
		http.Error(w, "storage error", http.StatusBadGateway)
	}
}

// parseRange parses a single-range Range header for an object of size.
// Without a header it returns the whole object.
func parseRange(header string, size int64) (offset, length int64, partial, ok bool) {
	if header == "" {
		return 0, size, false, true
	}
	spec, found := strings.CutPrefix(header, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false, false
	}
	first, last, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false, false
	}
	if first == "" {
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil || n <= 0 || size == 0 {
			return 0, 0, false, false
		}
		n = min(n, size)
		return size - n, n, true, true
	}
	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false, false
	}
	end := size - 1
	if last != "" {
		if end, err = strconv.ParseInt(last, 10, 64); err != nil || end < start {
			return 0, 0, false, false
		}
		end = min(end, size-1)
	}
	return start, end - start + 1, true, true
}
