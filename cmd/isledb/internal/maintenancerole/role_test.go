package maintenancerole

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
)

func load(args []string) (*Config, error) {
	cfg := NewConfig().(*Config)
	_, _, err := service.Load("maintenance", []string{"maintenance"}, cfg, args, func(string) (string, bool) { return "", false })
	return cfg, err
}

func TestConfigDefaultsAndOptions(t *testing.T) {
	cfg, err := load([]string{"-bucket-url", "file:///b", "-prefix", "prod/a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Listen != "" || cfg.Server.AdminListen != "127.0.0.1:8061" || cfg.prefix() != "prod/a" {
		t.Fatalf("defaults: %+v", cfg)
	}
	if opts := cfg.options(); opts.ChangeFeedRetention != nil || opts.SSTCompaction.ScratchDir != "" {
		t.Fatalf("by default history is kept and scratch is the cache directory: %+v", opts)
	}
	cfg, err = load([]string{"-bucket-url", "file:///b", "-prefix", "/", "-scratch-dir", "/scratch", "-change-feed-retain-for", "168h"})
	if err != nil {
		t.Fatal(err)
	}
	opts := cfg.options()
	if cfg.prefix() != "" || opts.SSTCompaction.ScratchDir != "/scratch" ||
		opts.ChangeFeedRetention == nil || opts.ChangeFeedRetention.RetainFor != 168*time.Hour {
		t.Fatalf("options: %+v", opts)
	}
}

func TestConfigRefusesMistakes(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"no prefix", nil, "prefix"},
		{"dot segment", []string{"-prefix", "a/../b"}, "prefix"},
		{"an API", []string{"-prefix", "a", "-listen", ":8060"}, "serves no API"},
		{"auth", []string{"-prefix", "a", "-auth-token-file", "/t"}, "serves no API"},
		{"tls", []string{"-prefix", "a", "-tls-cert", "/c", "-tls-key", "/k"}, "serves no API"},
		{"negative retention", []string{"-prefix", "a", "-change-feed-retain-for", "-1h"}, "change_feed.retain_for"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := load(append([]string{"-bucket-url", "file:///b"}, c.args...))
			if err == nil {
				err = cfg.Validate()
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want one naming %q", err, c.want)
			}
		})
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func metric(body, name string) float64 {
	m := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + ` (\S+)$`).FindStringSubmatch(body)
	if m == nil {
		return -1
	}
	v, _ := strconv.ParseFloat(m[1], 64)
	return v
}

// serve runs the role on bucket and returns its admin address and a stop
// function that waits for it.
func serve(t *testing.T, bucket string) (string, func()) {
	t.Helper()
	cfg := NewConfig().(*Config)
	cfg.Server.AdminListen = "127.0.0.1:0"
	cfg.Server.ShutdownDrain = 0
	cfg.Prefix = "prod/a"
	cfg.ScratchDir = t.TempDir()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	addrs := make(chan net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(ctx, Role, cfg, service.Common{BucketURL: bucket}, slog.New(slog.NewTextHandler(io.Discard, nil)),
			func(_, admin net.Addr) { addrs <- admin })
	}()
	select {
	case a := <-addrs:
		return "http://" + a.String(), func() {
			stop()
			if err := <-done; err != nil {
				t.Errorf("Serve: %v", err)
			}
		}
	case err := <-done:
		t.Fatalf("Serve: %v", err)
	}
	return "", nil
}

func within(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, d)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestMaintenanceCompactsWhatTheWriterSaves: a writer saves enough L0 files
// to need compaction; the role compacts them, the writer applies the result,
// and the metrics show it.
func TestMaintenanceCompactsWhatTheWriterSaves(t *testing.T) {
	ctx := context.Background()
	bucket := "file://" + t.TempDir()
	db, err := isledb.Open(ctx, bucket, isledb.DBOptions{Prefix: "prod/a"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	wopts := isledb.DefaultWriterOptions()
	wopts.Flush.Interval = 0
	// The writer merges its own saves and hands maintenance the result; a
	// short age limit makes that happen within the test.
	wopts.L0Merge.TargetBytes = 1 << 20
	wopts.L0Merge.MaxAge = 200 * time.Millisecond
	writer, err := db.OpenWriter(ctx, wopts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close(ctx) }()
	// Every save writes both ends of the key range, so the files overlap and
	// compaction rewrites them rather than moving them.
	save := func(i int) {
		for _, key := range []string{"a", "z"} {
			if _, err := writer.Put(ctx, []byte(key), fmt.Appendf(nil, "v%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 10 {
		save(i)
	}

	admin, stop := serve(t, bucket)
	defer stop()
	if code, _ := get(t, admin+"/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz %d, want 200", code)
	}
	// The writer applies maintenance's commands as it commits.
	i := 10
	within(t, 30*time.Second, "a compaction job", func() bool {
		save(i)
		i++
		_, body := get(t, admin+"/metrics")
		return metric(body, "isledb_maintenance_compaction_jobs_total") > 0
	})
	_, body := get(t, admin+"/metrics")
	for _, name := range []string{"isledb_maintenance_cycles_total", "isledb_maintenance_compaction_rewritten_bytes_total"} {
		if metric(body, name) <= 0 {
			t.Fatalf("%s is %v, want above 0:\n%s", name, metric(body, name), body)
		}
	}
	if metric(body, "isledb_maintenance_last_cycle_timestamp_seconds") < float64(time.Now().Add(-time.Minute).Unix()) {
		t.Fatalf("last cycle too old:\n%s", body)
	}
}

// TestMaintenanceTakenOverStaysUpNotReady: another process claims the
// maintenance fence; this one stops working and reports not ready, but keeps
// running, so it does not take the fence back.
func TestMaintenanceTakenOverStaysUpNotReady(t *testing.T) {
	ctx := context.Background()
	bucket := "file://" + t.TempDir()
	admin, stop := serve(t, bucket)
	defer stop()
	if code, _ := get(t, admin+"/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz %d, want 200", code)
	}
	if _, body := get(t, admin+"/metrics"); metric(body, "isledb_maintenance_running") != 1 {
		t.Fatalf("running is not 1:\n%s", body)
	}

	db, err := isledb.Open(ctx, bucket, isledb.DBOptions{Prefix: "prod/a"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	other, err := db.OpenMaintenance(ctx, isledb.DefaultMaintenanceOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close(ctx) }()

	within(t, 20*time.Second, "the role reporting not ready", func() bool {
		code, _ := get(t, admin+"/readyz")
		return code == http.StatusServiceUnavailable
	})
	if code, _ := get(t, admin+"/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz %d, want 200: the process stays up", code)
	}
	if _, body := get(t, admin+"/metrics"); metric(body, "isledb_maintenance_running") != 0 {
		t.Fatalf("running is not 0 after the takeover:\n%s", body)
	}
}
