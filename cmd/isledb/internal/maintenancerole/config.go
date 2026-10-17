// Package maintenancerole is the isledb maintenance role: it holds the
// maintenance fence for one database and compacts it, checkpoints its
// manifest, and deletes what is no longer needed. It serves no API, only the
// admin listener.
package maintenancerole

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
	"github.com/ankur-anand/isledb/internal/readcache"
)

// Config is the maintenance section of the configuration.
type Config struct {
	Server service.ServerConfig `yaml:",inline"`

	// Prefix is the database's prefix in the bucket, "/" for the root.
	Prefix string `yaml:"prefix"`
	// ScratchDir holds the inputs of compactions in progress; empty uses the
	// user's cache directory.
	ScratchDir string     `yaml:"scratch_dir"`
	ChangeFeed ChangeFeed `yaml:"change_feed"`
}

type ChangeFeed struct {
	// RetainFor is how long change-feed history is kept; zero keeps it all.
	RetainFor service.Duration `yaml:"retain_for"`
}

func NewConfig() service.Config {
	cfg := &Config{Server: service.DefaultServer("", "127.0.0.1:8061")}
	cfg.Server.ShutdownDrain = 0 // no traffic to drain
	return cfg
}

func (c *Config) ServerConfig() *service.ServerConfig { return &c.Server }

func (c *Config) Settings() []service.Setting {
	return append(c.Server.Settings(),
		service.Setting{Flag: "prefix", Path: "prefix", Usage: "prefix of the database to maintain, \"/\" for the bucket root",
			Set: func(v string) error { c.Prefix = v; return nil }},
		service.Setting{Flag: "scratch-dir", Path: "scratch_dir", Usage: "directory for compaction inputs; empty uses the user's cache directory",
			Set: func(v string) error { c.ScratchDir = v; return nil }},
		service.Setting{Flag: "change-feed-retain-for", Path: "change_feed.retain_for", Usage: "how long change-feed history is kept, e.g. 168h; 0 keeps it all",
			Set: c.ChangeFeed.RetainFor.Parse},
	)
}

// prefix returns the database's prefix as its keys spell it, "" for the root.
// Validate has checked it.
func (c *Config) prefix() string {
	p, _ := readcache.CleanPrefix(c.Prefix)
	return p
}

func (c *Config) options() isledb.MaintenanceOptions {
	opts := isledb.DefaultMaintenanceOptions()
	opts.SSTCompaction.ScratchDir = c.ScratchDir
	if c.ChangeFeed.RetainFor > 0 {
		opts.ChangeFeedRetention = &isledb.ChangeFeedRetentionOptions{RetainFor: time.Duration(c.ChangeFeed.RetainFor)}
	}
	return opts
}

func (c *Config) Validate() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if c.Server.Listen != "" || c.Server.Auth != (service.AuthConfig{}) || c.Server.TLS != (service.TLSConfig{}) {
		fail("listen, auth and tls don't apply: maintenance serves no API, only admin_listen")
	}
	if _, err := readcache.CleanPrefix(c.Prefix); err != nil || strings.TrimSpace(c.Prefix) != c.Prefix {
		fail("prefix %q: want a prefix such as prod/policy, or \"/\" for the bucket root", c.Prefix)
	}
	if c.ChangeFeed.RetainFor < 0 {
		fail("change_feed.retain_for is negative")
	}
	return errors.Join(append(errs, c.Server.Validate())...)
}
