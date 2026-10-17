// Package readerrole is the isledb reader role: it serves reads of one
// database over HTTP from its own Reader. Replicas share nothing.
package readerrole

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
)

// Config is the reader's section of the configuration.
type Config struct {
	Server service.ServerConfig `yaml:",inline"`

	// Prefix is the database's prefix in the bucket, "/" for the root.
	Prefix string `yaml:"prefix"`
	// CacheDir holds this replica's disk cache; one per replica.
	CacheDir  string    `yaml:"cache_dir"`
	Caches    Caches    `yaml:"caches"`
	Views     Views     `yaml:"views"`
	ReadCache ReadCache `yaml:"read_cache"`
	Reads     Reads     `yaml:"reads"`
}

type Caches struct {
	DiskBytes  service.ByteSize `yaml:"disk_bytes"`
	BlockBytes service.ByteSize `yaml:"block_bytes"`
	BloomBytes service.ByteSize `yaml:"bloom_bytes"`
}

type Views struct {
	// RefreshAfter is how often the reader loads the database's newest
	// view: reads are at most about this far behind the writer, or less with
	// a read cache, whose doorbell refreshes on each commit.
	RefreshAfter service.Duration `yaml:"refresh_after"`
}

type ReadCache struct {
	// URL is an isledb cache's API, such as https://isledb-cache:8090;
	// empty reads storage directly.
	URL string `yaml:"url"`
	// TokenFile holds the token to send it; empty sends none.
	TokenFile string `yaml:"token_file"`
	// CAFile holds the CA certificates that sign the cache's certificate;
	// empty trusts the system's.
	CAFile string `yaml:"ca_file"`
	// Cert and Key are the client certificate to present to a cache that
	// requires one.
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

type Reads struct {
	// MaxConcurrent bounds the reads in progress; past it, reads are
	// answered 503.
	MaxConcurrent int `yaml:"max_concurrent"`
}

// minRefreshAfter is the library's shortest refresh interval.
const minRefreshAfter = time.Second

func NewConfig() service.Config {
	defaults := isledb.DefaultReaderOpenOptions("")
	return &Config{
		Server: service.DefaultServer(":8080", "127.0.0.1:8081"),
		Caches: Caches{
			DiskBytes:  service.ByteSize(defaults.DiskCacheSize),
			BlockBytes: service.ByteSize(defaults.BlockCacheSize),
			BloomBytes: service.ByteSize(defaults.BloomCacheSize),
		},
		Views: Views{RefreshAfter: service.Duration(time.Second)},
		Reads: Reads{MaxConcurrent: 256},
	}
}

func (c *Config) ServerConfig() *service.ServerConfig { return &c.Server }

func (c *Config) Settings() []service.Setting {
	return append(c.Server.Settings(),
		service.Setting{Flag: "prefix", Path: "prefix", Usage: "prefix of the database to read, \"/\" for the bucket root",
			Set: func(v string) error { c.Prefix = v; return nil }},
		service.Setting{Flag: "cache-dir", Path: "cache_dir", Usage: "directory for this replica's disk cache",
			Set: func(v string) error { c.CacheDir = v; return nil }},
		service.Setting{Flag: "disk-cache-bytes", Path: "caches.disk_bytes", Usage: "disk cache budget, e.g. 8GiB", Set: c.Caches.DiskBytes.Parse},
		service.Setting{Flag: "block-cache-bytes", Path: "caches.block_bytes", Usage: "in-memory block cache, e.g. 256MiB", Set: c.Caches.BlockBytes.Parse},
		service.Setting{Flag: "bloom-cache-bytes", Path: "caches.bloom_bytes", Usage: "in-memory Bloom filter cache", Set: c.Caches.BloomBytes.Parse},
		service.Setting{Flag: "refresh-after", Path: "views.refresh_after", Usage: "how often the newest view is loaded; reads are about this far behind", Set: c.Views.RefreshAfter.Parse},
		service.Setting{Flag: "read-cache-url", Path: "read_cache.url", Usage: "an isledb cache to read through, e.g. https://isledb-cache:8090",
			Set: func(v string) error { c.ReadCache.URL = v; return nil }},
		service.Setting{Flag: "read-cache-token-file", Path: "read_cache.token_file", Usage: "file holding the token for the read cache",
			Set: func(v string) error { c.ReadCache.TokenFile = v; return nil }},
		service.Setting{Flag: "read-cache-ca-file", Path: "read_cache.ca_file", Usage: "CA file that signs the read cache's certificate; empty trusts the system's",
			Set: func(v string) error { c.ReadCache.CAFile = v; return nil }},
		service.Setting{Flag: "read-cache-cert", Path: "read_cache.cert", Usage: "client certificate file for a read cache that requires one",
			Set: func(v string) error { c.ReadCache.Cert = v; return nil }},
		service.Setting{Flag: "read-cache-key", Path: "read_cache.key", Usage: "key file of the read cache client certificate",
			Set: func(v string) error { c.ReadCache.Key = v; return nil }},
		service.Setting{Flag: "max-concurrent-reads", Path: "reads.max_concurrent", Usage: "reads in progress before reads get 503",
			Set: func(v string) error { return parseInt(v, &c.Reads.MaxConcurrent) }},
	)
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

func (c *Config) Validate() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if c.Server.Listen == "" {
		fail("listen is required")
	}
	if _, err := readcache.CleanPrefix(c.Prefix); err != nil || strings.TrimSpace(c.Prefix) != c.Prefix {
		fail("prefix %q: want a prefix such as prod/policy, or \"/\" for the bucket root", c.Prefix)
	}
	if strings.TrimSpace(c.CacheDir) == "" {
		fail("cache_dir is required: each replica needs a directory of its own")
	}
	// The library takes zero as its default size, not as off.
	if c.Caches.DiskBytes <= 0 || c.Caches.BlockBytes <= 0 || c.Caches.BloomBytes <= 0 {
		fail("caches.disk_bytes, caches.block_bytes and caches.bloom_bytes must be positive: the caches cannot be turned off")
	}
	if time.Duration(c.Views.RefreshAfter) < minRefreshAfter {
		fail("views.refresh_after must be at least %s", minRefreshAfter)
	}
	tlsSet := c.ReadCache.CAFile != "" || c.ReadCache.Cert != "" || c.ReadCache.Key != ""
	if c.ReadCache.URL != "" {
		u, err := url.Parse(c.ReadCache.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			fail("read_cache.url %q: want http:// or https:// and a host", c.ReadCache.URL)
		} else if tlsSet && u.Scheme != "https" {
			fail("read_cache.ca_file, cert and key need an https:// read_cache.url")
		}
	} else if c.ReadCache.TokenFile != "" || tlsSet {
		fail("read_cache.token_file, ca_file, cert and key need read_cache.url")
	}
	if (c.ReadCache.Cert == "") != (c.ReadCache.Key == "") {
		fail("read_cache.cert and read_cache.key go together")
	}
	if c.Reads.MaxConcurrent <= 0 {
		fail("reads.max_concurrent must be positive")
	}
	return errors.Join(append(errs, c.Server.Validate())...)
}
