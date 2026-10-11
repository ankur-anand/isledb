package readcache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
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

// Handler serves the relay's copy of CURRENT:
//
//	GET /v1/current            the copy now
//	GET /v1/current?after=N    the copy once its position passes N, or after
//	                           longPoll, whichever comes first
//
// Both answer 200 with CURRENT's bytes and the position and age headers, and
// 503 before the relay has read CURRENT once. A long poll that times out
// answers with the copy as it is, at a position not past N.
func Handler(relay *Relay, longPoll time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/current", func(w http.ResponseWriter, r *http.Request) {
		var (
			current Current
			ok      bool
		)
		if after := r.URL.Query().Get("after"); after != "" {
			n, err := strconv.ParseUint(after, 10, 64)
			if err != nil {
				http.Error(w, "after must be a log position", http.StatusBadRequest)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), longPoll)
			defer cancel()
			current, ok = relay.Wait(ctx, n)
		} else {
			current, ok = relay.Current()
		}
		if !ok {
			http.Error(w, "CURRENT not read yet", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(HeaderPosition, strconv.FormatUint(current.Position, 10))
		w.Header().Set(HeaderAgeMillis, strconv.FormatInt(ceilMillis(current.Age), 10))
		_, _ = w.Write(current.Data)
	})
	return mux
}

// ceilMillis rounds up, so a reader never thinks a copy younger than it is.
func ceilMillis(d time.Duration) int64 {
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// HeaderNoCache on a request for an object asks the cache to serve it
// without keeping what it reads from storage, as a scan does.
const HeaderNoCache = "Isledb-No-Cache"

// ObjectHandler serves byte ranges of immutable objects:
//
//	GET /v1/object/{key}    with an optional Range: bytes=a-b, a- or -n
//
// It answers 206 for a range and 200 for the whole object, 403 for a key
// that is not immutable, 404 for a missing object and 416 for a range
// outside it.
func ObjectHandler(objects *Objects) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/object/{key...}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if !Immutable(key) {
			http.Error(w, "not an immutable object", http.StatusForbidden)
			return
		}
		size, err := objects.Size(r.Context(), key)
		if errors.Is(err, blobstore.ErrNotFound) {
			http.Error(w, "object not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, "storage: "+err.Error(), http.StatusBadGateway)
			return
		}
		offset, length, partial, ok := parseRange(r.Header.Get("Range"), size)
		if !ok {
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
			http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		if partial {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, size))
			w.WriteHeader(http.StatusPartialContent)
		}
		fill := r.Header.Get(HeaderNoCache) == ""
		if err := objects.WriteRange(r.Context(), w, key, size, offset, length, fill); err != nil && r.Context().Err() == nil {
			// The status is sent; a short body tells the client it failed.
			slog.Warn("readcache: serve object", "key", key, "error", err)
		}
	})
	return mux
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
