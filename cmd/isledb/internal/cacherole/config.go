// Package cacherole is the isledb cache role: a regional read cache that
// relays CURRENT for each database it serves and serves their immutable
// objects from chunks kept on disk.
package cacherole

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
	"github.com/ankur-anand/isledb/internal/readcache"
)

// Config is the cache's section of the configuration.
type Config struct {
	Server service.ServerConfig `yaml:",inline"`

	Disk      Disk       `yaml:"disk"`
	Reads     Reads      `yaml:"reads"`
	Current   Current    `yaml:"current"`
	Databases []Database `yaml:"databases"`
}

type Disk struct {
	Dir   string           `yaml:"dir"`
	Bytes service.ByteSize `yaml:"bytes"`
}

type Reads struct {
	MaxBytes service.ByteSize `yaml:"max_bytes"`
	MaxWait  service.Duration `yaml:"max_wait"`
}

type Current struct {
	Poll     service.Duration `yaml:"poll"`
	LongPoll service.Duration `yaml:"long_poll"`
}

// Database is one database served, by its prefix in the bucket. A database
// at the root of the bucket is written "/"; an empty prefix is refused, so a
// blank value never names it. Each database is served only its own objects,
// not those of a database nested under it or of the root.
type Database struct {
	Prefix string `yaml:"prefix"`
	// CurrentPoll overrides current.poll for this database.
	CurrentPoll service.Duration `yaml:"current_poll,omitempty"`
}

func NewConfig() service.Config {
	return &Config{
		Server:  service.DefaultServer(":8090", "127.0.0.1:8091"),
		Reads:   Reads{MaxBytes: 256 << 20, MaxWait: service.Duration(readcache.DefaultMaxReadWait)},
		Current: Current{Poll: service.Duration(500 * time.Millisecond), LongPoll: service.Duration(30 * time.Second)},
	}
}

func (c *Config) ServerConfig() *service.ServerConfig { return &c.Server }

func (c *Config) Settings() []service.Setting {
	return append(c.Server.Settings(),
		service.Setting{Flag: "db", Path: "databases", Usage: "prefix of a database to serve, \"/\" for the bucket root; repeat for several", List: true,
			Set:   func(v string) error { c.Databases = append(c.Databases, Database{Prefix: v}); return nil },
			Reset: func() { c.Databases = nil }},
		service.Setting{Flag: "disk-dir", Path: "disk.dir", Usage: "directory for cached chunks, on local disk", Set: func(v string) error { c.Disk.Dir = v; return nil }},
		service.Setting{Flag: "disk-bytes", Path: "disk.bytes", Usage: "disk budget, e.g. 200GiB", Set: c.Disk.Bytes.Parse},
		service.Setting{Flag: "max-read-bytes", Path: "reads.max_bytes", Usage: "bytes read from storage at once, e.g. 256MiB", Set: c.Reads.MaxBytes.Parse},
		service.Setting{Flag: "max-read-wait", Path: "reads.max_wait", Usage: "longest a storage read waits for max-read-bytes", Set: c.Reads.MaxWait.Parse},
		service.Setting{Flag: "current-poll", Path: "current.poll", Usage: "how often CURRENT is read from storage", Set: c.Current.Poll.Parse},
		service.Setting{Flag: "long-poll", Path: "current.long_poll", Usage: "longest a long poll is held, at most 30s", Set: c.Current.LongPoll.Parse},
	)
}

// prefix returns db's prefix as the bucket names it: "" for the root.
// prefix returns db's prefix as its keys spell it, "" for the root. Validate
// has checked it.
func (db Database) prefix() string {
	p, _ := readcache.CleanPrefix(db.Prefix)
	return p
}

// name is db's prefix for logs and metrics: "/" for the root.
func (db Database) name() string {
	if p := db.prefix(); p != "" {
		return p
	}
	return "/"
}

func (c *Config) Validate() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if c.Server.Listen == "" {
		fail("listen is required")
	}
	if len(c.Databases) == 0 {
		fail("at least one database (db) is required")
	}
	seen := map[string]bool{}
	for _, db := range c.Databases {
		if _, err := readcache.CleanPrefix(strings.TrimSpace(db.Prefix)); err != nil || strings.TrimSpace(db.Prefix) != db.Prefix {
			fail("database %q: want a prefix such as prod/policy, or \"/\" for the bucket root", db.Prefix)
			continue
		}
		if seen[db.prefix()] {
			fail("database %q is listed twice", db.Prefix)
		}
		seen[db.prefix()] = true
		if db.CurrentPoll < 0 {
			fail("database %q: current_poll is negative", db.Prefix)
		}
	}
	if c.Disk.Dir == "" {
		fail("disk.dir is required")
	}
	if c.Disk.Bytes <= 0 {
		fail("disk.bytes is required")
	}
	if c.Reads.MaxBytes < 4<<20 {
		fail("reads.max_bytes is below 4MiB, the largest single read")
	}
	if c.Reads.MaxWait <= 0 {
		fail("reads.max_wait must be positive")
	}
	if c.Current.Poll <= 0 || c.Current.LongPoll <= 0 {
		fail("current.poll and current.long_poll must be positive")
	}
	if time.Duration(c.Current.LongPoll) > readcache.MaxLongPoll {
		fail("current.long_poll is above %s: readers give up on a long poll not answered by then", readcache.MaxLongPoll)
	}
	return errors.Join(append(errs, c.Server.Validate())...)
}
