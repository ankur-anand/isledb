package readcache

import (
	"context"
	"net/http"
	"strconv"
	"time"
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
