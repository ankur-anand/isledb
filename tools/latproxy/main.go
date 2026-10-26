// Command latproxy forwards HTTP requests to an S3-compatible server after a
// random delay, standing in for object storage reached over a network, and
// counts the requests and bytes that pass through, by kind of object and by
// method, so a benchmark can see how much reached storage. It can also inject
// faults: refuse requests, or forward them and lose the answer.
//
//	latproxy -listen 127.0.0.1:9100 -target http://127.0.0.1:9000 -min 15ms -max 40ms
//
// GET /_latproxy/stats returns the counts as JSON; POST /_latproxy/reset
// zeroes them. GET /_latproxy/faults returns the faults in force and PUT
// /_latproxy/faults replaces them. No S3 bucket name can start with "_", so
// these paths never shadow a bucket.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
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

// The methods counted separately; any other is counted as OTHER.
var methods = []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete, "OTHER"}

func methodOf(m string) string {
	if slices.Contains(methods, m) {
		return m
	}
	return "OTHER"
}

type counter struct {
	requests     atomic.Int64
	bytes        atomic.Int64 // response bytes
	uploaded     atomic.Int64 // request body bytes
	serverErrors atomic.Int64 // 5xx answers, injected ones included
	injected     atomic.Int64 // refused without forwarding
	dropped      atomic.Int64 // forwarded, then answered with an error
}

func (c *counter) reset() {
	for _, v := range []*atomic.Int64{&c.requests, &c.bytes, &c.uploaded, &c.serverErrors, &c.injected, &c.dropped} {
		v.Store(0)
	}
}

// Stats are the counts since the last reset, by kind of object. Requests and
// Bytes, the response bytes, cover every method; Methods breaks them down.
type Stats map[string]KindStats

type KindStats struct {
	Requests int64                  `json:"requests"`
	Bytes    int64                  `json:"bytes"`
	Methods  map[string]MethodStats `json:"methods"`
}

type MethodStats struct {
	Requests int64 `json:"requests"`
	// Bytes are the response bytes returned; UploadedBytes the request body
	// bytes sent on.
	Bytes         int64 `json:"bytes"`
	UploadedBytes int64 `json:"uploaded_bytes"`
	ServerErrors  int64 `json:"server_errors"`
	Injected      int64 `json:"injected"`
	Dropped       int64 `json:"dropped"`
}

// Faults are injected into the requests they match: those whose method is in
// Methods and kind of object in Kinds, either list empty for all.
type Faults struct {
	Methods []string `json:"methods,omitempty"`
	Kinds   []string `json:"kinds,omitempty"`
	// ErrorRate is the share of matching requests answered 503 SlowDown
	// without being forwarded: storage refused them, and they had no effect.
	ErrorRate float64 `json:"error_rate,omitempty"`
	// DropRate is the share forwarded and then answered 500 InternalError
	// instead of the server's answer: they took effect, but the client
	// cannot tell.
	DropRate float64 `json:"drop_rate,omitempty"`
	// ExtraDelay is added to the delay of matching requests.
	ExtraDelay Duration `json:"extra_delay,omitempty"`
}

func (f *Faults) validate() error {
	for _, m := range f.Methods {
		if methodOf(m) != m || m == "OTHER" {
			return fmt.Errorf("method %q: want one of GET, HEAD, PUT, POST, DELETE", m)
		}
	}
	for _, k := range f.Kinds {
		if !slices.Contains(kinds, k) {
			return fmt.Errorf("kind %q: want one of %s", k, strings.Join(kinds, ", "))
		}
	}
	if f.ErrorRate < 0 || f.DropRate < 0 || f.ErrorRate+f.DropRate > 1 {
		return errors.New("error_rate and drop_rate must be at least 0 and add up to at most 1")
	}
	if f.ExtraDelay < 0 {
		return errors.New("extra_delay is negative")
	}
	return nil
}

func (f *Faults) matches(method, kind string) bool {
	return (len(f.Methods) == 0 || slices.Contains(f.Methods, method)) && (len(f.Kinds) == 0 || slices.Contains(f.Kinds, kind))
}

// Duration is a time.Duration written as a string such as "250ms".
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

type proxy struct {
	forward            *httputil.ReverseProxy
	minDelay, maxDelay time.Duration
	counts             map[string]map[string]*counter // kind, method

	mu     sync.Mutex
	faults Faults
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
		counts:   make(map[string]map[string]*counter, len(kinds)),
	}
	for _, k := range kinds {
		p.counts[k] = make(map[string]*counter, len(methods))
		for _, m := range methods {
			p.counts[k][m] = &counter{}
		}
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
		for _, byMethod := range p.counts {
			for _, c := range byMethod {
				c.reset()
			}
		}
		w.WriteHeader(http.StatusNoContent)
		return
	case "/_latproxy/faults":
		p.serveFaults(w, r)
		return
	}
	method, kind := methodOf(r.Method), kindOf(r.URL.Path)
	p.mu.Lock()
	f := p.faults
	p.mu.Unlock()
	delay := p.minDelay + rand.N(p.maxDelay-p.minDelay+1)
	matched := f.matches(method, kind)
	if matched {
		delay += time.Duration(f.ExtraDelay)
	}
	// A client that gives up while waiting never reaches the target, and is
	// not counted.
	if err := sleep(r.Context(), delay); err != nil {
		return
	}
	c := p.counts[kind][method]
	c.requests.Add(1)
	roll := rand.Float64()
	if matched && roll < f.ErrorRate {
		c.injected.Add(1)
		c.serverErrors.Add(1)
		writeS3Error(w, http.StatusServiceUnavailable, "SlowDown", "latproxy: injected refusal")
		return
	}
	if r.Body != nil {
		r.Body = &countingReader{ReadCloser: r.Body, bytes: &c.uploaded}
	}
	if matched && roll < f.ErrorRate+f.DropRate {
		// The server's answer is read and thrown away.
		p.forward.ServeHTTP(&discardWriter{header: http.Header{}}, r)
		c.dropped.Add(1)
		c.serverErrors.Add(1)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "latproxy: injected lost answer")
		return
	}
	cw := &countingWriter{ResponseWriter: w, bytes: &c.bytes}
	p.forward.ServeHTTP(cw, r)
	if cw.status >= 500 {
		c.serverErrors.Add(1)
	}
}

func (p *proxy) serveFaults(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
	case http.MethodPut:
		var f Faults
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&f); err != nil {
			http.Error(w, "faults: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := f.validate(); err != nil {
			http.Error(w, "faults: "+err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.faults = f
		p.mu.Unlock()
		log.Printf("latproxy: faults %+v", f)
	default:
		http.Error(w, "use GET or PUT", http.StatusMethodNotAllowed)
		return
	}
	p.mu.Lock()
	f := p.faults
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(f)
}

func (p *proxy) stats() Stats {
	s := make(Stats, len(p.counts))
	for k, byMethod := range p.counts {
		ks := KindStats{Methods: make(map[string]MethodStats, len(byMethod))}
		for m, c := range byMethod {
			ms := MethodStats{
				Requests:      c.requests.Load(),
				Bytes:         c.bytes.Load(),
				UploadedBytes: c.uploaded.Load(),
				ServerErrors:  c.serverErrors.Load(),
				Injected:      c.injected.Load(),
				Dropped:       c.dropped.Load(),
			}
			ks.Requests += ms.Requests
			ks.Bytes += ms.Bytes
			if ms != (MethodStats{}) {
				ks.Methods[m] = ms
			}
		}
		s[k] = ks
	}
	return s
}

// writeS3Error answers as S3 does, so clients treat an injected fault as
// they would a real one.
func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, message)
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

type countingReader struct {
	io.ReadCloser
	bytes *atomic.Int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytes.Add(int64(n))
	return n, err
}

type countingWriter struct {
	http.ResponseWriter
	bytes  *atomic.Int64
	status int
}

func (w *countingWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *countingWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
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

// discardWriter takes a forwarded answer that the client never sees.
type discardWriter struct{ header http.Header }

func (w *discardWriter) Header() http.Header         { return w.header }
func (w *discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardWriter) WriteHeader(int)             {}
