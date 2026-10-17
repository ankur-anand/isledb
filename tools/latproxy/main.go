// Command latproxy forwards HTTP requests to an S3-compatible server after a
// random delay, standing in for object storage reached over a network, and
// counts the requests and bytes that pass through, by kind of object, so a
// benchmark can see how many reads reached storage.
//
//	latproxy -listen :9100 -target http://silo:9000 -min 15ms -max 40ms
//
// GET /_latproxy/stats returns the counts as JSON; POST /_latproxy/reset
// zeroes them. No S3 bucket name can start with "_", so these paths never
// shadow a bucket.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9100", "address to serve")
	target := flag.String("target", "http://127.0.0.1:9000", "server to forward to")
	minDelay := flag.Duration("min", 15*time.Millisecond, "shortest delay before forwarding a request")
	maxDelay := flag.Duration("max", 40*time.Millisecond, "longest delay before forwarding a request")
	flag.Parse()
	handler, err := newProxy(*target, *minDelay, *maxDelay)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("latproxy: %s -> %s, delay %s to %s", *listen, *target, *minDelay, *maxDelay)
	log.Fatal(http.ListenAndServe(*listen, handler))
}

// The kinds of object counted separately: IsleDB's CURRENT, which readers
// poll; SSTs; the rest of the manifest; and anything else.
var kinds = []string{"current", "sst", "manifest", "other"}

func kindOf(path string) string {
	switch {
	case strings.HasSuffix(path, "/manifest/CURRENT"):
		return "current"
	case strings.Contains(path, "/sstable/"):
		return "sst"
	case strings.Contains(path, "/manifest/"):
		return "manifest"
	default:
		return "other"
	}
}

type counter struct {
	requests atomic.Int64
	bytes    atomic.Int64
}

// Stats are the counts since the last reset: requests forwarded and response
// bytes returned, by kind of object.
type Stats map[string]struct {
	Requests int64 `json:"requests"`
	Bytes    int64 `json:"bytes"`
}

type proxy struct {
	forward            *httputil.ReverseProxy
	minDelay, maxDelay time.Duration
	counts             map[string]*counter
}

func newProxy(target string, minDelay, maxDelay time.Duration) (http.Handler, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, errors.New("target must be an absolute URL, such as http://127.0.0.1:9000")
	}
	if minDelay < 0 || maxDelay < minDelay {
		return nil, errors.New("delays must satisfy 0 <= min <= max")
	}
	p := &proxy{
		minDelay: minDelay,
		maxDelay: maxDelay,
		counts:   make(map[string]*counter, len(kinds)),
	}
	for _, k := range kinds {
		p.counts[k] = &counter{}
	}
	// The Host header is passed on as the client sent it: S3 clients sign it.
	p.forward = httputil.NewSingleHostReverseProxy(u)
	p.forward.Transport = &http.Transport{MaxIdleConnsPerHost: 512}
	return p, nil
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/_latproxy/stats":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.stats())
		return
	case "/_latproxy/reset":
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		for _, c := range p.counts {
			c.requests.Store(0)
			c.bytes.Store(0)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// A client that gives up while waiting never reaches the target, and is
	// not counted.
	if err := sleep(r.Context(), p.minDelay+rand.N(p.maxDelay-p.minDelay+1)); err != nil {
		return
	}
	c := p.counts[kindOf(r.URL.Path)]
	c.requests.Add(1)
	p.forward.ServeHTTP(&countingWriter{ResponseWriter: w, bytes: &c.bytes}, r)
}

func (p *proxy) stats() Stats {
	s := make(Stats, len(p.counts))
	for k, c := range p.counts {
		e := s[k]
		e.Requests, e.Bytes = c.requests.Load(), c.bytes.Load()
		s[k] = e
	}
	return s
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type countingWriter struct {
	http.ResponseWriter
	bytes *atomic.Int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes.Add(int64(n))
	return n, err
}

// Flush lets the reverse proxy stream a response as it arrives.
func (w *countingWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
