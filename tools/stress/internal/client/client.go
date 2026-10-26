// Package client speaks the isledb writer and reader HTTP APIs: only what
// the stress test needs, and every answer mapped to what it means for the
// write or read that got it.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Outcome is what an answer means for a write.
type Outcome int

const (
	// Committed: the write is saved, at Result.Revision.
	Committed Outcome = iota
	// Refused: nothing was written; send the same request again later.
	Refused
	// NotCommitted: the write was accepted but will never be saved; send it
	// again.
	NotCommitted
	// Unknown: the write may or may not be saved; ask about Result.WriteID,
	// or, without one, the outcome cannot be learned.
	Unknown
	// Invalid: the request can never succeed; the run is misconfigured.
	Invalid
)

func (o Outcome) String() string {
	return [...]string{"committed", "refused", "not_committed", "unknown", "invalid"}[o]
}

// Result is the answer to a write.
type Result struct {
	Outcome    Outcome
	WriteID    string
	Revision   uint64
	Status     int
	Code       string // the error code of an error answer
	RetryAfter time.Duration
	Err        error // set for Unknown without an answer, and for Invalid
}

// Op is one put or delete of a batch.
type Op struct {
	Delete bool
	Key    []byte
	Value  []byte
}

type wireOp struct {
	Op    string `json:"op"`
	Key   []byte `json:"key_base64"`
	Value []byte `json:"value_base64,omitempty"`
}

type writeAnswer struct {
	WriteID  string `json:"write_id"`
	Revision string `json:"revision"`
	Status   string `json:"status"`
	Error    string `json:"error"`
	Message  string `json:"message"`
}

// Client talks to one writer or reader.
type Client struct {
	Base  string // e.g. http://127.0.0.1:8070
	Token string
	HTTP  *http.Client
}

func New(base, token string) *Client {
	return &Client{Base: strings.TrimSuffix(base, "/"), Token: token, HTTP: &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: 256, IdleConnTimeout: time.Minute},
	}}
}

// answer is a response, read whole.
type answer struct {
	status int
	header http.Header
	body   []byte
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, header http.Header) (answer, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, r)
	if err != nil {
		return answer{}, err
	}
	maps.Copy(req.Header, header)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return answer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return answer{status: resp.StatusCode, header: resp.Header, body: b}, err
}

func retryAfter(h http.Header) time.Duration {
	s, err := strconv.Atoi(h.Get("Retry-After"))
	if err != nil || s < 0 {
		return 0
	}
	return time.Duration(s) * time.Second
}

// EncodeBatch returns the request body of a batch.
func EncodeBatch(ops []Op) ([]byte, error) {
	wire := make([]wireOp, len(ops))
	for i, op := range ops {
		wire[i] = wireOp{Op: "put", Key: op.Key, Value: op.Value}
		if op.Delete {
			wire[i] = wireOp{Op: "delete", Key: op.Key}
		}
	}
	return json.Marshal(struct {
		Ops []wireOp `json:"ops"`
	}{wire})
}

// Write sends a batch body from EncodeBatch and waits for its commit.
func (c *Client) Write(ctx context.Context, body []byte) Result {
	a, err := c.do(ctx, http.MethodPost, "/v1/kv", body, nil)
	if err != nil {
		if neverSent(err) {
			return Result{Outcome: Refused, Err: err}
		}
		// The request may have reached the writer: its outcome is unknown,
		// and without a write ID it can't be asked about.
		return Result{Outcome: Unknown, Err: err}
	}
	return writeResult(a)
}

// neverSent reports whether err means the request never left: the
// connection could not be made, as while the writer restarts.
func neverSent(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

func writeResult(ans answer) Result {
	var a writeAnswer
	_ = json.Unmarshal(ans.body, &a)
	status, b := ans.status, ans.body
	r := Result{Status: status, Code: a.Error, WriteID: a.WriteID, RetryAfter: retryAfter(ans.header)}
	switch {
	case status == http.StatusOK:
		rev, err := strconv.ParseUint(a.Revision, 10, 64)
		if err != nil || rev == 0 {
			return Result{Outcome: Unknown, WriteID: a.WriteID, Status: status, Err: fmt.Errorf("committed without a revision: %q", b)}
		}
		r.Outcome, r.Revision = Committed, rev
	case status == http.StatusServiceUnavailable && a.Error == "not_committed":
		r.Outcome = NotCommitted
	case status == http.StatusServiceUnavailable, status == http.StatusRequestTimeout:
		r.Outcome = Refused
	case status == http.StatusGatewayTimeout:
		r.Outcome = Unknown
	case status >= 500:
		// An answer the API does not list, from a proxy or a crashing
		// writer: the write may have been applied.
		r.Outcome, r.Err = Unknown, fmt.Errorf("writer answered %d: %s", status, bytes.TrimSpace(b))
	default:
		r.Outcome, r.Err = Invalid, fmt.Errorf("writer answered %d %s: %s", status, a.Error, a.Message)
	}
	return r
}

// WriteStatus is the answer to asking about a write.
type WriteStatus struct {
	// Status is committed, pending, not_committed or unknown; or
	// unavailable when this writer can't tell and another may.
	Status     string
	Revision   uint64
	RetryAfter time.Duration
}

// Status asks the writer what became of a write.
func (c *Client) Status(ctx context.Context, writeID string) (WriteStatus, error) {
	ans, err := c.do(ctx, http.MethodGet, "/v1/writes/"+url.PathEscape(writeID), nil, nil)
	if err != nil {
		return WriteStatus{}, err
	}
	b := ans.body
	var a writeAnswer
	_ = json.Unmarshal(b, &a)
	ws := WriteStatus{Status: a.Status, RetryAfter: retryAfter(ans.header)}
	switch ans.status {
	case http.StatusOK:
		if ws.Status == "committed" {
			if ws.Revision, err = strconv.ParseUint(a.Revision, 10, 64); err != nil {
				return WriteStatus{}, fmt.Errorf("status of %s: committed without a revision: %q", writeID, b)
			}
		}
		return ws, nil
	case http.StatusServiceUnavailable:
		ws.Status = "unavailable"
		return ws, nil
	default:
		return WriteStatus{}, fmt.Errorf("status of %s: %d %s: %s", writeID, ans.status, a.Error, a.Message)
	}
}

// escapeKey percent-encodes every byte that is not a letter, digit, '-',
// '_', '.' or '~', which the reader API says is always safe.
func escapeKey(key []byte) string {
	var b strings.Builder
	b.Grow(len(key) * 3)
	const hex = "0123456789ABCDEF"
	for _, c := range key {
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&15])
		}
	}
	return b.String()
}

// ErrRetry is a read the reader could not answer now: retry it.
var ErrRetry = errors.New("reader busy or unavailable")

func revision(h http.Header) (uint64, error) {
	v := h.Get("Isledb-Revision")
	rev, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("answer without a revision (Isledb-Revision %q)", v)
	}
	return rev, nil
}

// Get reads one key and returns its value, whether it was found, and the
// revision the reader answered from.
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, rev uint64, err error) {
	ans, err := c.do(ctx, http.MethodGet, "/v1/kv/"+escapeKey(key), nil, nil)
	if err != nil {
		return nil, false, 0, err
	}
	b := ans.body
	switch ans.status {
	case http.StatusOK, http.StatusNotFound:
		if rev, err = revision(ans.header); err != nil {
			return nil, false, 0, err
		}
		if ans.status == http.StatusNotFound {
			var a writeAnswer
			if json.Unmarshal(b, &a) != nil || a.Error != "key_not_found" {
				return nil, false, 0, fmt.Errorf("404 that is not a missing key: %s", b)
			}
			return nil, false, rev, nil
		}
		return b, true, rev, nil
	case http.StatusServiceUnavailable:
		return nil, false, 0, ErrRetry
	default:
		return nil, false, 0, fmt.Errorf("reader answered %d: %s", ans.status, bytes.TrimSpace(b))
	}
}

// Item is one key and value of a scan page.
type Item struct {
	Key, Value []byte
}

type scanItem struct {
	Key         *string `json:"key"`
	KeyBase64   []byte  `json:"key_base64"`
	Value       *string `json:"value"`
	ValueBase64 []byte  `json:"value_base64"`
}

// Page is one page of a scan.
type Page struct {
	Items    []Item
	Revision uint64
	// Next is the token for the next page; empty on the last.
	Next string
}

// Scan reads one page of the keys in [start, end), end nil for no bound,
// from page, empty for the first.
func (c *Client) Scan(ctx context.Context, start, end []byte, limit int, page string) (Page, error) {
	q := url.Values{}
	if start != nil {
		q.Set("start", string(start))
	}
	if end != nil {
		q.Set("end", string(end))
	}
	q.Set("limit", strconv.Itoa(limit))
	if page != "" {
		q.Set("page", page)
	}
	ans, err := c.do(ctx, http.MethodGet, "/v1/kv?"+q.Encode(), nil, nil)
	if err != nil {
		return Page{}, err
	}
	b := ans.body
	switch ans.status {
	case http.StatusOK:
	case http.StatusServiceUnavailable:
		return Page{}, ErrRetry
	default:
		return Page{}, fmt.Errorf("reader answered %d: %s", ans.status, bytes.TrimSpace(b))
	}
	rev, err := revision(ans.header)
	if err != nil {
		return Page{}, err
	}
	var a struct {
		Items    []scanItem `json:"items"`
		NextPage string     `json:"next_page"`
	}
	if err := json.Unmarshal(b, &a); err != nil {
		return Page{}, fmt.Errorf("scan page: %w", err)
	}
	p := Page{Revision: rev, Next: a.NextPage, Items: make([]Item, len(a.Items))}
	for i, it := range a.Items {
		p.Items[i] = Item{Key: it.KeyBase64, Value: it.ValueBase64}
		if it.Key != nil {
			p.Items[i].Key = []byte(*it.Key)
		}
		if it.Value != nil {
			p.Items[i].Value = []byte(*it.Value)
		}
		if p.Items[i].Value == nil {
			p.Items[i].Value = []byte{}
		}
	}
	return p, nil
}
