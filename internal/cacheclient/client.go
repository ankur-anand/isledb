// Package cacheclient reads immutable objects through an isledb read cache.
// Every failure is reported with a reason, for the caller to read storage
// instead.
package cacheclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ankur-anand/isledb/internal/readcache"
)

// DefaultTimeout is Options.Timeout when it is zero.
const DefaultTimeout = 10 * time.Second

// Options configures a Client.
type Options struct {
	// URL is the cache's API, such as https://isledb-cache:8090.
	URL string
	// Token is sent as a bearer token when not empty.
	Token string
	// HTTPClient carries the requests; nil selects one with connection
	// reuse suited to many concurrent reads.
	HTTPClient *http.Client
	// Timeout bounds one read through the cache. Zero selects
	// DefaultTimeout.
	Timeout time.Duration
}

// Client reads through one cache. It is safe for concurrent use.
type Client struct {
	base    string
	token   string
	http    *http.Client
	ownHTTP bool // http was made here, so Close may close its connections
	timeout time.Duration
	breaker *breaker
}

func New(opts Options) (*Client, error) {
	u, err := url.Parse(opts.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("cacheclient: URL %q: want http:// or https:// and a host", opts.URL)
	}
	if opts.Timeout < 0 {
		return nil, fmt.Errorf("cacheclient: negative timeout %s", opts.Timeout)
	}
	if opts.Timeout == 0 {
		opts.Timeout = DefaultTimeout
	}
	client, own := opts.HTTPClient, opts.HTTPClient == nil
	if own {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConnsPerHost = 128
		client = &http.Client{Transport: transport}
	}
	return &Client{base: strings.TrimSuffix(opts.URL, "/"), token: opts.Token, http: client, ownHTTP: own,
		timeout: opts.Timeout, breaker: newBreaker()}, nil
}

// Close closes the idle connections of the HTTP client New made. One passed
// in Options.HTTPClient may be shared, and is left alone.
func (c *Client) Close() {
	if c.ownHTTP {
		c.http.CloseIdleConnections()
	}
}

// Reasons a read through the cache failed.
const (
	ReasonTimeout      = "timeout"       // the cache did not answer in time
	ReasonUnavailable  = "unavailable"   // no connection, or a 5xx
	ReasonNotFound     = "not_found"     // the cache found no such object
	ReasonSizeMismatch = "size_mismatch" // the size sent is not the object's
	ReasonRefused      = "refused"       // 401, 403 or another 4xx: misconfiguration
	ReasonInvalid      = "invalid"       // a response the cache should not give
)

// Error is a failed read through the cache.
type Error struct {
	Reason string
	Err    error
}

func (e *Error) Error() string { return "read cache: " + e.Reason + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// ReadRange reads [offset, offset+length) of key, an object of size bytes.
// With fill false, the cache is asked not to keep what it reads from
// storage, as for a scan. A failure is an *Error, unless ctx itself ended,
// when it is ctx's error. After the cache has timed out or been unavailable
// a few reads in a row, reads fail at once with ReasonBypassed for a few
// seconds, until one read finds it answering again.
func (c *Client) ReadRange(ctx context.Context, key string, size, offset, length int64, fill bool) ([]byte, error) {
	if length <= 0 || offset < 0 || offset+length > size {
		return nil, &Error{ReasonInvalid, fmt.Errorf("range [%d, %d) outside %s of size %d", offset, offset+length, key, size)}
	}
	ok, probe := c.breaker.allow(time.Now())
	if !ok {
		return nil, &Error{ReasonBypassed, errBypassed}
	}
	data, err := c.readRange(ctx, key, size, offset, length, fill)
	c.breaker.record(time.Now(), probe, err)
	return data, err
}

func (c *Client) readRange(ctx context.Context, key string, size, offset, length int64, fill bool) ([]byte, error) {
	readCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(readCtx, http.MethodGet, c.base+"/v1/object/"+escapeKey(key), nil)
	if err != nil {
		return nil, &Error{ReasonInvalid, err}
	}
	req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(offset+length-1, 10))
	req.Header.Set(readcache.HeaderObjectSize, strconv.FormatInt(size, 10))
	if !fill {
		req.Header.Set(readcache.HeaderNoCache, "1")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.transportError(ctx, readCtx, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if err := statusError(resp); err != nil {
		return nil, err
	}
	if total := contentTotal(resp.Header.Get("Content-Range")); total != size {
		return nil, &Error{ReasonInvalid, fmt.Errorf("Content-Range %q, want an object of %d bytes", resp.Header.Get("Content-Range"), size)}
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(resp.Body, data); err != nil {
		return nil, c.transportError(ctx, readCtx, err)
	}
	return data, nil
}

func (c *Client) transportError(ctx, readCtx context.Context, err error) error {
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(readCtx.Err(), context.DeadlineExceeded):
		return &Error{ReasonTimeout, err}
	default:
		return &Error{ReasonUnavailable, err}
	}
}

func statusError(resp *http.Response) error {
	if resp.StatusCode == http.StatusPartialContent {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	err := fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return &Error{ReasonNotFound, err}
	case resp.Header.Get(readcache.HeaderError) == readcache.ErrorSizeMismatch:
		return &Error{ReasonSizeMismatch, err}
	case resp.StatusCode >= 500:
		return &Error{ReasonUnavailable, err}
	case resp.StatusCode >= 400:
		return &Error{ReasonRefused, err}
	default:
		return &Error{ReasonInvalid, err}
	}
}

// contentTotal returns the total size in a Content-Range "bytes a-b/total",
// or -1.
func contentTotal(header string) int64 {
	_, total, ok := strings.Cut(header, "/")
	if !ok {
		return -1
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}
