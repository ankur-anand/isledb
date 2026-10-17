package readerrole

import (
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
)

func load(args []string, vars map[string]string) (*Config, error) {
	cfg := NewConfig().(*Config)
	lookup := func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	_, _, err := service.Load("reader", []string{"reader"}, cfg, args, lookup)
	return cfg, err
}

var base = []string{"-bucket-url", "file:///b", "-prefix", "prod/a", "-cache-dir", "/var/cache/isledb", "-insecure"}

func TestConfigDefaultsAndOverrides(t *testing.T) {
	cfg, err := load(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.prefix() != "prod/a" || time.Duration(cfg.Views.RefreshAfter) != time.Second ||
		cfg.Reads.MaxConcurrent != 256 || cfg.Server.Listen != ":8080" || cfg.Caches.DiskBytes <= 0 {
		t.Fatalf("defaults: %+v", cfg)
	}
	cfg, err = load(append(base, "-prefix", "/", "-read-cache-url", "https://cache:8090", "-max-concurrent-reads", "8"),
		map[string]string{"ISLEDB_READER_VIEWS_REFRESH_AFTER": "5s"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.prefix() != "" || cfg.ReadCache.URL != "https://cache:8090" || cfg.Reads.MaxConcurrent != 8 ||
		time.Duration(cfg.Views.RefreshAfter) != 5*time.Second {
		t.Fatalf("overrides: %+v", cfg)
	}
}

func TestConfigRefusesMistakes(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"no prefix", []string{"-bucket-url", "file:///b", "-cache-dir", "/c", "-insecure"}, "prefix"},
		{"no cache dir", []string{"-bucket-url", "file:///b", "-prefix", "a", "-insecure"}, "cache_dir"},
		{"dot segment", []string{"-prefix", "a/../b"}, "prefix"},
		{"fast refresh", []string{"-refresh-after", "500ms"}, "views.refresh_after"},
		{"no disk cache", []string{"-disk-cache-bytes", "0"}, "caches.disk_bytes"},
		{"no block cache", []string{"-block-cache-bytes", "0"}, "caches.block_bytes"},
		{"no bloom cache", []string{"-bloom-cache-bytes", "0"}, "caches.bloom_bytes"},
		{"cache url", []string{"-read-cache-url", "cache:8090"}, "read_cache.url"},
		{"token without url", []string{"-read-cache-token-file", "/t"}, "read_cache.token_file"},
		{"ca without url", []string{"-read-cache-ca-file", "/ca.pem"}, "need read_cache.url"},
		{"ca over http", []string{"-read-cache-url", "http://cache:8090", "-read-cache-ca-file", "/ca.pem"}, "https://"},
		{"cert without key", []string{"-read-cache-url", "https://cache:8090", "-read-cache-cert", "/c.pem"}, "go together"},
		{"no reads", []string{"-max-concurrent-reads", "0"}, "reads.max_concurrent"},
		{"not a number", []string{"-max-concurrent-reads", "many"}, "whole number"},
	} {
		t.Run(c.name, func(t *testing.T) {
			args := c.args
			if !strings.HasPrefix(c.name, "no prefix") && c.name != "no cache dir" {
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
