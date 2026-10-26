package runner

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// BucketUsage is what the database's prefix holds, by kind of object.
type BucketUsage struct {
	Time    time.Time        `json:"time"`
	Seconds float64          `json:"seconds"`
	Objects map[string]int64 `json:"objects"`
	Bytes   map[string]int64 `json:"bytes"`
}

func objectKind(rel string) string {
	switch {
	case strings.HasPrefix(rel, "sstable/"):
		return "sst"
	case strings.HasPrefix(rel, "manifest/gc/"):
		return "gc"
	case strings.HasPrefix(rel, "manifest/"):
		return "manifest"
	case strings.HasPrefix(rel, "changes/"):
		return "change_feed"
	default:
		return "other"
	}
}

// lister lists what a database's prefix holds.
type lister interface {
	list(ctx context.Context, prefix string, add func(rel string, size int64)) error
}

// newLister returns a lister for the spec's bucket: a directory for file://,
// S3's ListObjectsV2 for s3://, straight to storage, not through a proxy,
// so listing never counts as a role's traffic. It returns nil, with a
// reason, for buckets it can't list.
func newLister(s StorageSpec) (lister, string) {
	raw := strings.ReplaceAll(s.BucketURL, "{endpoint}", s.Endpoint)
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err.Error()
	}
	switch u.Scheme {
	case "file":
		return dirLister{root: u.Path}, ""
	case "s3":
		q := u.Query()
		region := q.Get("region")
		if region == "" {
			region = os.Getenv("AWS_REGION")
		}
		if region == "" {
			region = "us-east-1"
		}
		c := &s3Lister{bucket: u.Host, region: region, endpoint: q.Get("endpoint"),
			pathStyle: q.Get("use_path_style") == "true" || q.Get("endpoint") != "",
			access:    os.Getenv("AWS_ACCESS_KEY_ID"), secret: os.Getenv("AWS_SECRET_ACCESS_KEY"), token: os.Getenv("AWS_SESSION_TOKEN")}
		if c.access == "" || c.secret == "" {
			return nil, "set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to list the bucket"
		}
		if c.endpoint == "" {
			c.endpoint = "https://s3." + region + ".amazonaws.com"
		}
		return c, ""
	default:
		return nil, "listing " + u.Scheme + ":// buckets is not supported"
	}
}

func listUsage(ctx context.Context, l lister, prefix string) (BucketUsage, error) {
	start := time.Now()
	u := BucketUsage{Time: start.UTC(), Objects: map[string]int64{}, Bytes: map[string]int64{}}
	err := l.list(ctx, strings.TrimSuffix(prefix, "/")+"/", func(rel string, size int64) {
		k := objectKind(rel)
		u.Objects[k]++
		u.Bytes[k] += size
	})
	u.Seconds = time.Since(start).Seconds()
	return u, err
}

type dirLister struct{ root string }

func (d dirLister) list(_ context.Context, prefix string, add func(string, int64)) error {
	base := filepath.Join(d.root, filepath.FromSlash(prefix))
	err := filepath.WalkDir(base, func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		fi, err := e.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(base, path)
		add(filepath.ToSlash(rel), fi.Size())
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

type s3Lister struct {
	bucket, region, endpoint string
	pathStyle                bool
	access, secret, token    string
	now                      func() time.Time
	client                   http.Client
}

type listResult struct {
	Contents []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
}

func (c *s3Lister) list(ctx context.Context, prefix string, add func(string, int64)) error {
	base, err := url.Parse(strings.TrimSuffix(c.endpoint, "/"))
	if err != nil {
		return err
	}
	path := "/"
	if c.pathStyle {
		path = "/" + c.bucket
	} else {
		base.Host = c.bucket + "." + base.Host
	}
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.Scheme+"://"+base.Host+path+"?"+canonicalQuery(q), nil)
		if err != nil {
			return err
		}
		now := time.Now
		if c.now != nil {
			now = c.now
		}
		c.sign(req, now())
		resp, err := c.client.Do(req)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("list s3://%s/%s: %s: %s", c.bucket, prefix, resp.Status, strings.TrimSpace(string(body)))
		}
		var r listResult
		if err := xml.Unmarshal(body, &r); err != nil {
			return fmt.Errorf("list s3://%s/%s: %w", c.bucket, prefix, err)
		}
		for _, o := range r.Contents {
			add(strings.TrimPrefix(o.Key, prefix), o.Size)
		}
		if !r.IsTruncated || r.NextContinuationToken == "" {
			return nil
		}
		token = r.NextContinuationToken
	}
}

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// sign adds AWS Signature Version 4 headers to a request without a body.
// It signs Host, every X-Amz-* header, and Range when present.
func (c *s3Lister) sign(req *http.Request, now time.Time) {
	amzDate := now.UTC().Format("20060102T150405Z")
	day := amzDate[:8]
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	if c.token != "" {
		req.Header.Set("X-Amz-Security-Token", c.token)
	}
	headers := map[string]string{"host": req.URL.Host}
	for k, v := range req.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-") || lk == "range" {
			headers[lk] = strings.TrimSpace(strings.Join(v, ","))
		}
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, k := range names {
		canonHeaders.WriteString(k + ":" + headers[k] + "\n")
	}
	signed := strings.Join(names, ";")
	canonical := strings.Join([]string{
		req.Method,
		uriEncode(req.URL.Path, false),
		canonicalQuery(req.URL.Query()),
		canonHeaders.String(),
		signed,
		emptySHA256,
	}, "\n")
	scope := day + "/" + c.region + "/s3/aws4_request"
	sum := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	key := mac([]byte("AWS4"+c.secret), day)
	key = mac(key, c.region)
	key = mac(key, "s3")
	key = mac(key, "aws4_request")
	sig := hex.EncodeToString(mac(key, toSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.access+"/"+scope+", SignedHeaders="+signed+", Signature="+sig)
}

func mac(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

// uriEncode percent-encodes s as SigV4 wants: every byte but unreserved
// ones, and '/' only when encodeSlash.
func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	const hexd = "0123456789ABCDEF"
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexd[c>>4])
			b.WriteByte(hexd[c&15])
		}
	}
	return b.String()
}

// canonicalQuery is q with keys sorted and keys and values encoded as
// SigV4 wants; the same string is sent, so what is signed is what arrives.
func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := append([]string(nil), q[k]...)
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}
	return strings.Join(parts, "&")
}
