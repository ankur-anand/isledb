// Package maintenancerole is the isledb maintenance role: it holds the
// maintenance fence for one database and compacts it, checkpoints its
// manifest, and deletes what is no longer needed. It serves no API, only the
// admin listener.
package maintenancerole

import (
	"errors"
	"fmt"
	"math"
	"strconv"
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
	Compaction Compaction `yaml:"compaction"`
}

// Compaction tunes how much compaction rewrites against how many files a read
// checks. NewConfig fills it with the library's defaults.
type Compaction struct {
	// BaseLevelBytes is the target size of L1.
	BaseLevelBytes service.ByteSize `yaml:"base_level_bytes"`
	// LevelGrowth is the size ratio between successive levels, at least 2.
	LevelGrowth int `yaml:"level_growth"`
	// TargetSSTBytes is the size of the files compaction writes.
	TargetSSTBytes service.ByteSize `yaml:"target_sst_bytes"`
	// L0ToBaseRatio is the share of the L1 bytes L0 overlaps that L0 must
	// hold before it merges into L1.
	L0ToBaseRatio float64 `yaml:"l0_to_base_ratio"`
	// L0MaxDepth is the number of L0 files covering one key that makes L0
	// merge into L1 whatever its size.
	L0MaxDepth int `yaml:"l0_max_depth"`
	// L0MaxAge is the age of L0's oldest data that makes L0 merge into L1
	// whatever its size.
	L0MaxAge service.Duration `yaml:"l0_max_age"`
}

type ChangeFeed struct {
	// RetainFor is how long change-feed history is kept; zero keeps it all.
	RetainFor service.Duration `yaml:"retain_for"`
}

func NewConfig() service.Config {
	d := isledb.DefaultMaintenanceOptions().SSTCompaction
	cfg := &Config{
		Server: service.DefaultServer("", "127.0.0.1:8061"),
		Compaction: Compaction{
			BaseLevelBytes: service.ByteSize(d.BaseLevelBytes),
			LevelGrowth:    d.LevelGrowthFactor,
			TargetSSTBytes: service.ByteSize(d.TargetSSTBytes),
			L0ToBaseRatio:  d.L0ToBaseRatio,
			L0MaxDepth:     d.L0MaxDepth,
			L0MaxAge:       service.Duration(d.L0MaxAge),
		},
	}
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
		service.Setting{Flag: "base-level-bytes", Path: "compaction.base_level_bytes", Usage: "target size of L1, e.g. 512MiB",
			Set: c.Compaction.BaseLevelBytes.Parse},
		service.Setting{Flag: "level-growth", Path: "compaction.level_growth", Usage: "size ratio between successive levels, at least 2",
			Set: func(v string) error { return parseInt(v, &c.Compaction.LevelGrowth) }},
		service.Setting{Flag: "target-sst-bytes", Path: "compaction.target_sst_bytes", Usage: "size of the files compaction writes, e.g. 64MiB",
			Set: c.Compaction.TargetSSTBytes.Parse},
		service.Setting{Flag: "l0-to-base-ratio", Path: "compaction.l0_to_base_ratio", Usage: "share of the L1 bytes L0 overlaps that L0 holds before it merges into L1",
			Set: func(v string) error { return parseFloat(v, &c.Compaction.L0ToBaseRatio) }},
		service.Setting{Flag: "l0-max-depth", Path: "compaction.l0_max_depth", Usage: "L0 files covering one key that make L0 merge into L1",
			Set: func(v string) error { return parseInt(v, &c.Compaction.L0MaxDepth) }},
		service.Setting{Flag: "l0-max-age", Path: "compaction.l0_max_age", Usage: "age of L0's oldest data that makes L0 merge into L1, e.g. 30m",
			Set: c.Compaction.L0MaxAge.Parse},
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

func parseFloat(v string, into *float64) error {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return fmt.Errorf("%q is not a number", v)
	}
	*into = f
	return nil
}

// prefix returns the database's prefix as its keys spell it, "" for the root.
// Validate has checked it.
func (c *Config) prefix() string {
	p, _ := readcache.CleanPrefix(c.Prefix)
	return p
}

func (c *Config) options() isledb.MaintenanceOptions {
	opts := isledb.DefaultMaintenanceOptions()
	sc := &opts.SSTCompaction
	sc.ScratchDir = c.ScratchDir
	sc.BaseLevelBytes = int64(c.Compaction.BaseLevelBytes)
	sc.LevelGrowthFactor = c.Compaction.LevelGrowth
	sc.TargetSSTBytes = int64(c.Compaction.TargetSSTBytes)
	sc.L0ToBaseRatio = c.Compaction.L0ToBaseRatio
	sc.L0MaxDepth = c.Compaction.L0MaxDepth
	sc.L0MaxAge = time.Duration(c.Compaction.L0MaxAge)
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
	cc := c.Compaction
	if cc.BaseLevelBytes <= 0 || cc.TargetSSTBytes <= 0 {
		fail("compaction.base_level_bytes and compaction.target_sst_bytes must be positive")
	}
	if cc.LevelGrowth < 2 {
		fail("compaction.level_growth must be at least 2")
	}
	if !(cc.L0ToBaseRatio > 0) || math.IsInf(cc.L0ToBaseRatio, 0) {
		fail("compaction.l0_to_base_ratio must be a positive number")
	}
	if cc.L0MaxDepth <= 0 || cc.L0MaxAge <= 0 {
		fail("compaction.l0_max_depth and compaction.l0_max_age must be positive")
	}
	return errors.Join(append(errs, c.Server.Validate())...)
}
