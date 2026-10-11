package service

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testConfig is a role's section with one value, one list and the server.
type testConfig struct {
	Server ServerConfig `yaml:",inline"`
	Size   ByteSize     `yaml:"size"`
	Names  []string     `yaml:"names"`
}

func (c *testConfig) ServerConfig() *ServerConfig { return &c.Server }
func (c *testConfig) Validate() error             { return c.Server.Validate() }
func (c *testConfig) Start(context.Context, Common, *slog.Logger) (*Service, error) {
	return &Service{}, nil
}

func (c *testConfig) Settings() []Setting {
	return append(c.Server.Settings(),
		Setting{Flag: "size", Path: "size", Set: c.Size.Parse},
		Setting{Flag: "name", Path: "names", List: true,
			Set:   func(v string) error { c.Names = append(c.Names, v); return nil },
			Reset: func() { c.Names = nil }})
}

func newTestConfig() *testConfig {
	return &testConfig{Server: DefaultServer(":1", "127.0.0.1:2")}
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "isledb.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func lookup(vars map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := vars[k]
		return v, ok
	}
}

const file = `
bucket_url: file:///b
test:
  size: 1GiB
  names: [a, b]
  auth:
    insecure: true
other:
  anything: goes
`

func TestLoadFileThenEnvironmentThenFlags(t *testing.T) {
	path := writeFile(t, file)
	load := func(args []string, vars map[string]string) (*testConfig, Common) {
		t.Helper()
		cfg := newTestConfig()
		common, _, err := Load("test", []string{"test", "other"}, cfg, append([]string{"-config", path}, args...), lookup(vars))
		if err != nil {
			t.Fatal(err)
		}
		return cfg, common
	}

	cfg, common := load(nil, nil)
	if common.BucketURL != "file:///b" || cfg.Size != 1<<30 || strings.Join(cfg.Names, ",") != "a,b" || cfg.Server.Listen != ":1" {
		t.Fatalf("file over defaults: %+v %+v", common, cfg)
	}
	vars := map[string]string{"ISLEDB_TEST_SIZE": "2GiB", "ISLEDB_TEST_NAMES": "x,, y,", "ISLEDB_BUCKET_URL": "file:///env"}
	cfg, common = load(nil, vars)
	if common.BucketURL != "file:///env" || cfg.Size != 2<<30 || strings.Join(cfg.Names, ",") != "x,y" {
		t.Fatalf("environment over file, empty list entries skipped: %+v %+v", common, cfg)
	}
	cfg, _ = load([]string{"-size", "3GiB", "-name", "z"}, vars)
	if cfg.Size != 3<<30 || strings.Join(cfg.Names, ",") != "z" {
		t.Fatalf("flags over environment: %+v", cfg)
	}
	cfg, _ = load(nil, map[string]string{"ISLEDB_TEST_ADMIN_LISTEN": ""})
	if cfg.Server.AdminListen != "" {
		t.Fatalf("an environment variable set empty was ignored: admin_listen %q", cfg.Server.AdminListen)
	}
}

func TestLoadRefusesUnknownKeys(t *testing.T) {
	for name, content := range map[string]string{
		"unknown top-level key":             file + "bukcet_url: x\n",
		"unknown key in the role's section": strings.Replace(file, "size: 1GiB", "sise: 1GiB", 1),
	} {
		_, _, err := Load("test", []string{"test", "other"}, newTestConfig(), []string{"-config", writeFile(t, content)}, lookup(nil))
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestEnvName(t *testing.T) {
	for _, c := range [][3]string{{"cache", "disk.bytes", "ISLEDB_CACHE_DISK_BYTES"}, {"", "bucket_url", "ISLEDB_BUCKET_URL"}, {"cache", "auth.token_file", "ISLEDB_CACHE_AUTH_TOKEN_FILE"}} {
		if got := EnvName(c[0], c[1]); got != c[2] {
			t.Errorf("EnvName(%q, %q) = %q, want %q", c[0], c[1], got, c[2])
		}
	}
}

func TestByteSizes(t *testing.T) {
	for in, want := range map[string]ByteSize{"4096": 4096, "512KiB": 512 << 10, "256MiB": 256 << 20, "200GiB": 200 << 30, "2TiB": 2 << 40} {
		var b ByteSize
		if err := b.Parse(in); err != nil || b != want {
			t.Errorf("Parse(%q) = %d, %v; want %d", in, b, err, want)
		}
	}
	for _, in := range []string{"", "-1", "1GB", "9223372036854775807TiB"} {
		var b ByteSize
		if err := b.Parse(in); err == nil {
			t.Errorf("Parse(%q) accepted %d", in, b)
		}
	}
	if s := ByteSize(256 << 20).String(); s != "256MiB" {
		t.Errorf("String() = %q, want 256MiB", s)
	}
}
