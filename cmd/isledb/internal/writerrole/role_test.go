package writerrole

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
)

func call(t *testing.T, method, url, token, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func TestWriterRoleServesAndCommitsOnShutdown(t *testing.T) {
	bucket := "file://" + t.TempDir()
	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte("secret-one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := NewConfig().(*Config)
	cfg.Server.Listen, cfg.Server.AdminListen = "127.0.0.1:0", "127.0.0.1:0"
	cfg.Server.Auth.TokenFile = tokens
	cfg.Server.ShutdownDrain = 0
	cfg.Prefix = "prod/a"
	// Commits come rarely, so the last write below is still waiting when
	// shutdown starts.
	cfg.Flush.Interval = every(15 * time.Second)
	cfg.Writes.CommitWait = service.Duration(20 * time.Second)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	addrs := make(chan [2]net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(ctx, Role, cfg, service.Common{BucketURL: bucket}, slog.New(slog.NewTextHandler(io.Discard, nil)),
			func(api, admin net.Addr) { addrs <- [2]net.Addr{api, admin} })
	}()
	a := <-addrs
	api, admin := "http://"+a[0].String(), "http://"+a[1].String()

	if code, _ := call(t, http.MethodGet, admin+"/readyz", "", ""); code != http.StatusOK {
		t.Fatalf("/readyz %d, want 200", code)
	}
	if code, _ := call(t, http.MethodPut, api+"/v1/kv/k", "", "v"); code != http.StatusUnauthorized {
		t.Fatalf("PUT without a token: %d, want 401", code)
	}
	// A synchronous write waits for a commit that only shutdown brings.
	waiting := make(chan string, 1)
	go func() {
		code, body := call(t, http.MethodPut, api+"/v1/kv/last", "secret-one", "w")
		if code != http.StatusOK {
			body = "status " + http.StatusText(code) + ": " + body
		}
		waiting <- body
	}()
	_, metrics := call(t, http.MethodGet, admin+"/metrics", "", "")
	for _, want := range []string{
		`isledb_writer_http_requests_total{code="401",route="kv_put"} 1`,
		`isledb_writer_status{status="open"} 1`,
		`isledb_writer_puts_total`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics lack %q:\n%s", want, metrics)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(metrics, "isledb_writer_accepted_sequence 1") {
		if time.Now().After(deadline) {
			t.Fatalf("the waiting write was never accepted:\n%s", metrics)
		}
		time.Sleep(10 * time.Millisecond)
		_, metrics = call(t, http.MethodGet, admin+"/metrics", "", "")
	}

	stop()
	if body := <-waiting; !strings.Contains(body, `"revision"`) {
		t.Fatalf("the write waiting at shutdown: %s, want it committed", body)
	}
	if err := <-done; err != nil {
		t.Fatalf("Serve: %v", err)
	}

	db, err := isledb.Open(context.Background(), bucket, isledb.DBOptions{Prefix: "prod/a"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	reader, err := db.OpenReader(context.Background(), isledb.DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	for key, want := range map[string]string{"last": "w"} {
		if v, ok, err := reader.Get(context.Background(), []byte(key)); err != nil || !ok || string(v) != want {
			t.Fatalf("%s after shutdown: %q %t %v", key, v, ok, err)
		}
	}
}

func TestWriterRoleMatchesTheDatabasePolicy(t *testing.T) {
	bucket := "file://" + t.TempDir()
	ctx := context.Background()
	// An application wrote the database first, with its own policy.
	db, err := isledb.Open(ctx, bucket, isledb.DBOptions{Prefix: "app", Policy: isledb.StorePolicy{MaxPinnedViewAge: 2 * time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := db.OpenWriter(ctx, isledb.DefaultWriterOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	start := func(age time.Duration) error {
		cfg := NewConfig().(*Config)
		cfg.Server.Listen = "127.0.0.1:0"
		cfg.Server.Auth.Insecure = true
		cfg.Prefix = "app"
		cfg.Policy.MaxPinnedViewAge = service.Duration(age)
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		svc, err := cfg.Start(ctx, service.Common{BucketURL: bucket}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err == nil {
			err = svc.Close()
		}
		return err
	}
	if err := start(0); !errors.Is(err, isledb.ErrStorePolicyMismatch) {
		t.Fatalf("default policy on a 2h database: %v, want ErrStorePolicyMismatch", err)
	}
	if err := start(2 * time.Hour); err != nil {
		t.Fatalf("matching policy: %v", err)
	}
}

// A replacement writer that cannot start must leave the running one alone.
func TestFailedReplacementLeavesTheWriterRunning(t *testing.T) {
	bucket := "file://" + t.TempDir()
	newCfg := func() *Config {
		cfg := NewConfig().(*Config)
		cfg.Server.Listen, cfg.Server.AdminListen = "127.0.0.1:0", ""
		cfg.Server.Auth.Insecure = true
		cfg.Server.ShutdownDrain = 0
		cfg.Prefix = "app"
		cfg.Flush.Interval = every(20 * time.Millisecond)
		return cfg
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	addrs := make(chan net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(ctx, Role, newCfg(), service.Common{BucketURL: bucket}, quiet,
			func(api, _ net.Addr) { addrs <- api })
	}()
	api := "http://" + (<-addrs).String()
	if code, body := call(t, http.MethodPut, api+"/v1/kv/k", "", "before"); code != http.StatusOK {
		t.Fatalf("PUT before: %d %s", code, body)
	}

	broken := newCfg()
	broken.Server.Auth.Insecure = false
	broken.Server.Auth.TokenFile = filepath.Join(t.TempDir(), "missing")
	if err := service.Serve(context.Background(), Role, broken, service.Common{BucketURL: bucket}, quiet, nil); err == nil {
		t.Fatal("the replacement started without its token file")
	}

	if code, body := call(t, http.MethodPut, api+"/v1/kv/k", "", "after"); code != http.StatusOK {
		t.Fatalf("PUT after the failed replacement: %d %s, want 200", code, body)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}
