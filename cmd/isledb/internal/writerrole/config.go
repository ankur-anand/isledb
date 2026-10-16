// Package writerrole is the isledb writer role: it holds the writer fence for
// one database and serves writes to it over HTTP.
package writerrole

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
	"github.com/ankur-anand/isledb/internal/readcache"
	"github.com/ankur-anand/isledb/internal/writehttp"
)

// Config is the writer's section of the configuration.
type Config struct {
	Server service.ServerConfig `yaml:",inline"`

	// Prefix is the database's prefix in the bucket, "/" for the root.
	Prefix string `yaml:"prefix"`
	// OwnerID names this writer in the fence; empty takes the host name.
	OwnerID    string     `yaml:"owner_id"`
	Flush      Flush      `yaml:"flush"`
	Memtable   Memtable   `yaml:"memtable"`
	Values     Values     `yaml:"values"`
	ChangeFeed ChangeFeed `yaml:"change_feed"`
	Policy     Policy     `yaml:"policy"`
	Writes     Writes     `yaml:"writes"`

	onGCS bool // the bucket is on Google Cloud Storage; set by ApplyCommon
}

type Flush struct {
	// Interval is how often buffered writes commit: a synchronous write
	// waits about this long, and each commit is a write to storage. Unset, it
	// is 1s on Google Cloud Storage and 250ms elsewhere.
	Interval *service.Duration `yaml:"interval"`
}

type Memtable struct {
	TargetBytes service.ByteSize `yaml:"target_bytes"`
	MaxPending  int              `yaml:"max_pending"`
}

type Values struct {
	MaxKeyBytes   int              `yaml:"max_key_bytes"`
	MaxValueBytes service.ByteSize `yaml:"max_value_bytes"`
}

type ChangeFeed struct {
	// Payload, keys or values, turns the database's change feed on, or
	// confirms it is on with that payload. Empty leaves the database as it is:
	// the writer follows whatever feed it has, or none.
	Payload string `yaml:"payload"`
}

type Policy struct {
	// MaxPinnedViewAge is the database's limit on how long a reader may keep
	// a view, fixed when the database is first written: it must match it, or
	// the writer refuses to start. Zero is the library's default.
	MaxPinnedViewAge service.Duration `yaml:"max_pinned_view_age"`
}

type Writes struct {
	// CommitWait is the longest a write waits for its commit before it is
	// answered 504.
	CommitWait service.Duration `yaml:"commit_wait"`
	// MaxRequestBytes caps a batch request's body.
	MaxRequestBytes service.ByteSize `yaml:"max_request_bytes"`
	// MaxInflightBytes bounds the request bodies held at once; past it,
	// writes are answered 503 before their body is read.
	MaxInflightBytes service.ByteSize `yaml:"max_inflight_bytes"`
	// BodyReadTimeout is how long a request has to send its body.
	BodyReadTimeout service.Duration `yaml:"body_read_timeout"`
}

const (
	// maxCommitWait leaves a waiting write time to be answered within the
	// server's 30s shutdown.
	maxCommitWait = 20 * time.Second
	// The library's limits, checked here so --check catches them too.
	maxKeyBytes     = 1<<16 - 1 - 8 // the memtable appends an 8-byte sequence
	maxOwnerIDBytes = 256

	defaultFlushInterval = 250 * time.Millisecond
	// minGCSFlushInterval: Google Cloud Storage allows about one update per
	// second to an object, and every commit updates CURRENT.
	minGCSFlushInterval = time.Second
)

func NewConfig() service.Config {
	defaults := isledb.DefaultWriterOptions()
	return &Config{
		Server: service.DefaultServer(":8070", "127.0.0.1:8071"),
		Memtable: Memtable{
			TargetBytes: service.ByteSize(defaults.Memtable.TargetBytes),
			MaxPending:  defaults.Memtable.MaxPendingMemtables,
		},
		Values: Values{
			MaxKeyBytes:   defaults.Values.MaxKeyBytes,
			MaxValueBytes: service.ByteSize(defaults.Values.MaxValueBytes),
		},
		// A batch fits one memtable, 16MiB by default: about 22MiB as base64
		// JSON, so 32MiB leaves room and no more.
		Writes: Writes{
			CommitWait:       service.Duration(10 * time.Second),
			MaxRequestBytes:  32 << 20,
			MaxInflightBytes: 256 << 20,
			BodyReadTimeout:  service.Duration(time.Minute),
		},
	}
}

func (c *Config) ServerConfig() *service.ServerConfig { return &c.Server }

func (c *Config) Settings() []service.Setting {
	return append(c.Server.Settings(),
		service.Setting{Flag: "prefix", Path: "prefix", Usage: "prefix of the database to write, \"/\" for the bucket root",
			Set: func(v string) error { c.Prefix = v; return nil }},
		service.Setting{Flag: "owner-id", Path: "owner_id", Usage: "name of this writer in the fence; default the host name",
			Set: func(v string) error { c.OwnerID = v; return nil }},
		service.Setting{Flag: "flush-interval", Path: "flush.interval", Usage: "how often buffered writes commit; synchronous writes wait about this long; default 250ms, 1s on gs:// and at least 1s there",
			Set: func(v string) error {
				d := new(service.Duration)
				if err := d.Parse(v); err != nil {
					return err
				}
				c.Flush.Interval = d
				return nil
			}},
		service.Setting{Flag: "memtable-bytes", Path: "memtable.target_bytes", Usage: "memtable size that starts a new one, e.g. 16MiB", Set: c.Memtable.TargetBytes.Parse},
		service.Setting{Flag: "max-pending-memtables", Path: "memtable.max_pending", Usage: "memtables waiting to commit before writes get 503",
			Set: func(v string) error { return parseInt(v, &c.Memtable.MaxPending) }},
		service.Setting{Flag: "max-key-bytes", Path: "values.max_key_bytes", Usage: "largest key accepted",
			Set: func(v string) error { return parseInt(v, &c.Values.MaxKeyBytes) }},
		service.Setting{Flag: "max-value-bytes", Path: "values.max_value_bytes", Usage: "largest value accepted, e.g. 16MiB", Set: c.Values.MaxValueBytes.Parse},
		service.Setting{Flag: "change-feed", Path: "change_feed.payload", Usage: "turn the database's change feed on, keys or values; empty keeps what the database has",
			Set: func(v string) error { c.ChangeFeed.Payload = v; return nil }},
		service.Setting{Flag: "max-pinned-view-age", Path: "policy.max_pinned_view_age", Usage: "the database's limit on how long a reader keeps a view; empty for the default, 1h", Set: c.Policy.MaxPinnedViewAge.Parse},
		service.Setting{Flag: "commit-wait", Path: "writes.commit_wait", Usage: "longest a write waits for its commit, at most 20s", Set: c.Writes.CommitWait.Parse},
		service.Setting{Flag: "max-request-bytes", Path: "writes.max_request_bytes", Usage: "largest batch request body, e.g. 32MiB", Set: c.Writes.MaxRequestBytes.Parse},
		service.Setting{Flag: "max-inflight-bytes", Path: "writes.max_inflight_bytes", Usage: "request bodies held at once before writes get 503, e.g. 256MiB", Set: c.Writes.MaxInflightBytes.Parse},
		service.Setting{Flag: "body-read-timeout", Path: "writes.body_read_timeout", Usage: "how long a request has to send its body", Set: c.Writes.BodyReadTimeout.Parse},
	)
}

// ApplyCommon sets the defaults that depend on the bucket.
func (c *Config) ApplyCommon(common service.Common) {
	u, err := url.Parse(common.BucketURL)
	c.onGCS = err == nil && u.Scheme == "gs"
	if c.Flush.Interval == nil {
		d := service.Duration(defaultFlushInterval)
		if c.onGCS {
			d = service.Duration(minGCSFlushInterval)
		}
		c.Flush.Interval = &d
	}
}

// flushInterval returns the configured interval. ApplyCommon has set it.
func (c *Config) flushInterval() time.Duration {
	if c.Flush.Interval == nil {
		return defaultFlushInterval
	}
	return time.Duration(*c.Flush.Interval)
}

func parseInt(v string, into *int) error {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return fmt.Errorf("%q is not a whole number", v)
	}
	*into = n
	return nil
}

// prefix returns the database's prefix as its keys spell it, "" for the root.
// Validate has checked it.
func (c *Config) prefix() string {
	p, _ := readcache.CleanPrefix(c.Prefix)
	return p
}

// changeFeed returns the database option for the configured change feed.
// Validate has checked it.
func (c *Config) changeFeed() *isledb.ChangeFeedOptions {
	switch c.ChangeFeed.Payload {
	case "keys":
		return &isledb.ChangeFeedOptions{Payload: isledb.ChangeFeedKeysOnly}
	case "values":
		return &isledb.ChangeFeedOptions{Payload: isledb.ChangeFeedFullValues}
	default:
		return nil
	}
}

func (c *Config) Validate() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if c.Server.Listen == "" {
		fail("listen is required")
	}
	if _, err := readcache.CleanPrefix(c.Prefix); err != nil || strings.TrimSpace(c.Prefix) != c.Prefix {
		fail("prefix %q: want a prefix such as prod/policy, or \"/\" for the bucket root", c.Prefix)
	}
	switch flush := c.flushInterval(); {
	case flush <= 0:
		fail("flush.interval must be positive: writes wait for a commit")
	case c.onGCS && flush < minGCSFlushInterval:
		fail("flush.interval %s is below %s, which Google Cloud Storage does not support: it allows about one update per second to the object each commit writes",
			flush, minGCSFlushInterval)
	case c.Writes.CommitWait > 0 && flush >= time.Duration(c.Writes.CommitWait):
		fail("flush.interval (%s) must be below writes.commit_wait (%s), or writes are answered 504 before their commit",
			flush, time.Duration(c.Writes.CommitWait))
	}
	if len(c.OwnerID) > maxOwnerIDBytes {
		fail("owner_id is above %d bytes", maxOwnerIDBytes)
	}
	if c.Policy.MaxPinnedViewAge < 0 {
		fail("policy.max_pinned_view_age is negative")
	}
	if c.Memtable.TargetBytes <= 0 || c.Memtable.MaxPending <= 0 {
		fail("memtable.target_bytes and memtable.max_pending must be positive")
	}
	if c.Values.MaxKeyBytes <= 0 || c.Values.MaxValueBytes <= 0 {
		fail("values.max_key_bytes and values.max_value_bytes must be positive")
	}
	if c.Values.MaxKeyBytes > maxKeyBytes {
		fail("values.max_key_bytes is above %d, the most a key may be", maxKeyBytes)
	}
	switch c.ChangeFeed.Payload {
	case "", "keys", "values":
	default:
		fail("change_feed.payload %q: want keys, values, or empty to keep the database's", c.ChangeFeed.Payload)
	}
	if c.Writes.CommitWait <= 0 || time.Duration(c.Writes.CommitWait) > maxCommitWait {
		fail("writes.commit_wait must be above 0 and at most %s, so waiting writes are answered before shutdown ends", maxCommitWait)
	}
	if c.Writes.MaxRequestBytes < 1<<20 {
		fail("writes.max_request_bytes is below 1MiB")
	}
	if int64(c.Writes.MaxInflightBytes) < max(writehttp.BatchReserveFactor*int64(c.Writes.MaxRequestBytes), int64(c.Values.MaxValueBytes)) {
		fail("writes.max_inflight_bytes is below %d times writes.max_request_bytes, or values.max_value_bytes: such a request could never be read",
			writehttp.BatchReserveFactor)
	}
	if c.Writes.BodyReadTimeout <= 0 {
		fail("writes.body_read_timeout must be positive")
	}
	return errors.Join(append(errs, c.Server.Validate())...)
}
