package cacherole

import (
	"strings"
	"testing"

	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
)

func load(args []string, vars map[string]string) (*Config, error) {
	cfg := NewConfig().(*Config)
	lookup := func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	_, _, err := service.Load("cache", []string{"cache"}, cfg, args, lookup)
	return cfg, err
}

var base = []string{"-bucket-url", "file:///b", "-disk-dir", "/d", "-disk-bytes", "1GiB", "-insecure"}

func TestDatabasePrefixes(t *testing.T) {
	cfg, err := load(append(base, "-db", "/prod/a/", "-db", "/"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{cfg.Databases[0].prefix(), cfg.Databases[1].prefix()}; got[0] != "prod/a" || got[1] != "" {
		t.Fatalf("prefixes %q, want prod/a and the root", got)
	}
	cfg, err = load(base, map[string]string{"ISLEDB_CACHE_DATABASES": "prod/a,"})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Databases) != 1 {
		t.Fatalf("a trailing comma added a database: %+v", cfg.Databases)
	}
}

func TestConfigRefusesMistakes(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"empty prefix", append(base, "-db", ""), "bucket root"},
		{"blank prefix", append(base, "-db", "  "), "bucket root"},
		{"empty segment", append(base, "-db", "prod//a"), "bucket root"},
		{"dot-dot segment", append(base, "-db", "prod/../a"), "bucket root"},
		{"spaces around", append(base, "-db", " prod/a"), "bucket root"},
		{"no database", base, "database"},
		{"database twice", append(base, "-db", "a", "-db", "/a/"), "listed twice"},
		{"no authentication", []string{"-bucket-url", "file:///b", "-disk-dir", "/d", "-disk-bytes", "1GiB", "-db", "a"}, "no authentication"},
		{"key without certificate", append(base, "-db", "a", "-tls-key", "k.pem"), "tls.cert and tls.key"},
		{"read budget below one read", append(base, "-db", "a", "-max-read-bytes", "1MiB"), "max_bytes"},
		{"long poll above the readers' wait", append(base, "-db", "a", "-long-poll", "31s"), "long_poll"},
		{"no bucket", []string{"-disk-dir", "/d", "-disk-bytes", "1GiB", "-insecure", "-db", "a"}, "bucket_url"},
	} {
		if _, err := load(c.args, nil); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error naming %q", c.name, err, c.want)
		}
	}
}
