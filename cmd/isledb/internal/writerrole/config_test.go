package writerrole

import (
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
)

func load(args []string, vars map[string]string) (*Config, error) {
	cfg := NewConfig().(*Config)
	lookup := func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	_, _, err := service.Load("writer", []string{"writer"}, cfg, args, lookup)
	return cfg, err
}

var base = []string{"-bucket-url", "file:///b", "-prefix", "prod/a", "-insecure"}

func TestConfigDefaultsAndOverrides(t *testing.T) {
	cfg, err := load(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.prefix() != "prod/a" || cfg.changeFeed() != nil || time.Duration(cfg.Flush.Interval) != 250*time.Millisecond {
		t.Fatalf("defaults: %+v", cfg)
	}
	cfg, err = load(append(base, "-prefix", "/", "-change-feed", "values", "-max-pending-memtables", "2"),
		map[string]string{"ISLEDB_WRITER_WRITES_COMMIT_WAIT": "5s"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.prefix() != "" || cfg.changeFeed().Payload != isledb.ChangeFeedFullValues ||
		cfg.Memtable.MaxPending != 2 || time.Duration(cfg.Writes.CommitWait) != 5*time.Second {
		t.Fatalf("overrides: %+v", cfg)
	}
}

func TestConfigRefusesMistakes(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"no prefix", []string{"-bucket-url", "file:///b", "-insecure"}, "prefix"},
		{"dot segment", []string{"-prefix", "a/../b"}, "prefix"},
		{"no flush", []string{"-flush-interval", "0s"}, "flush.interval"},
		{"long wait", []string{"-commit-wait", "25s"}, "writes.commit_wait"},
		{"feed", []string{"-change-feed", "all"}, "change_feed.payload"},
		{"small request", []string{"-max-request-bytes", "1KiB"}, "writes.max_request_bytes"},
		{"pending", []string{"-max-pending-memtables", "0"}, "memtable"},
		{"not a number", []string{"-max-key-bytes", "lots"}, "whole number"},
		{"inflight below a request", []string{"-max-inflight-bytes", "8MiB"}, "writes.max_inflight_bytes"},
		{"no body timeout", []string{"-body-read-timeout", "0s"}, "writes.body_read_timeout"},
		{"flush past the wait", []string{"-flush-interval", "10s", "-commit-wait", "10s"}, "flush.interval"},
		{"long key", []string{"-max-key-bytes", "65528"}, "values.max_key_bytes"},
		{"long owner", []string{"-owner-id", strings.Repeat("o", 257)}, "owner_id"},
		{"negative view age", []string{"-max-pinned-view-age", "-1s"}, "policy.max_pinned_view_age"},
	} {
		t.Run(c.name, func(t *testing.T) {
			args := c.args
			if c.name != "no prefix" {
				args = append(append([]string{}, base...), c.args...)
			}
			cfg, err := load(args, nil)
			if err == nil {
				err = cfg.Validate()
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want one naming %q", err, c.want)
			}
		})
	}
}
