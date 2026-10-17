package readerrole

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
)

func call(t *testing.T, method, url, token string) (int, http.Header, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(raw)
}

func TestReaderRoleServesReads(t *testing.T) {
	ctx := context.Background()
	bucket := "file://" + t.TempDir()

	// The writer, here the library, commits before the reader starts.
	db, err := isledb.Open(ctx, bucket, isledb.DBOptions{Prefix: "prod/a"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	wopts := isledb.DefaultWriterOptions()
	wopts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, wopts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close(ctx) }()
	seq, err := writer.Put(ctx, []byte("user/42"), []byte(`{"name":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	revision, err := writer.WaitCommitted(ctx, seq)
	if err != nil {
		t.Fatal(err)
	}

	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := NewConfig().(*Config)
	cfg.Server.Listen, cfg.Server.AdminListen = "127.0.0.1:0", "127.0.0.1:0"
	cfg.Server.Auth.TokenFile = tokens
	cfg.Server.ShutdownDrain = 0
	cfg.Prefix = "prod/a"
	cfg.CacheDir = t.TempDir()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	serveCtx, stop := context.WithCancel(ctx)
	defer stop()
	addrs := make(chan [2]net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(serveCtx, Role, cfg, service.Common{BucketURL: bucket}, slog.New(slog.NewTextHandler(io.Discard, nil)),
			func(api, admin net.Addr) { addrs <- [2]net.Addr{api, admin} })
	}()
	a := <-addrs
	api, admin := "http://"+a[0].String(), "http://"+a[1].String()

	if code, _, _ := call(t, http.MethodGet, admin+"/readyz", ""); code != http.StatusOK {
		t.Fatalf("/readyz %d, want 200", code)
	}
	if code, _, _ := call(t, http.MethodGet, api+"/v1/kv/user%2F42", ""); code != http.StatusUnauthorized {
		t.Fatalf("GET without a token: %d, want 401", code)
	}
	code, header, body := call(t, http.MethodGet, api+"/v1/kv/user%2F42", "secret")
	if code != http.StatusOK || body != `{"name":"a"}` || header.Get("Isledb-Revision") != strconv.FormatUint(uint64(revision), 10) {
		t.Fatalf("GET: %d %q revision %q, want 200, the value and %d", code, body, header.Get("Isledb-Revision"), revision)
	}
	if code, _, body := call(t, http.MethodPut, api+"/v1/kv/user%2F42", "secret"); code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT to a reader: %d %s, want 405", code, body)
	}

	// A later commit reaches the reader by its refresh interval, 1s.
	seq, err = writer.Put(ctx, []byte("later"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if code, _, _ := call(t, http.MethodGet, api+"/v1/kv/later", "secret"); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a later commit never reached the reader")
		}
		time.Sleep(50 * time.Millisecond)
	}

	_, _, metrics := call(t, http.MethodGet, admin+"/metrics", "")
	for _, want := range []string{
		`isledb_reader_http_requests_total{code="200",route="kv_get"}`,
		`isledb_reader_http_requests_total{code="401",route="kv_get"} 1`,
		`isledb_reader_view_position`,
		`isledb_reader_get_total`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics lack %q:\n%s", want, metrics)
		}
	}
	stop()
	if err := <-done; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

// TestReaderStartsBeforeTheWriter: a reader of a database nobody has written
// yet starts, answers 404 at revision 0, and serves the data once the writer
// creates it.
func TestReaderStartsBeforeTheWriter(t *testing.T) {
	ctx := context.Background()
	bucket := "file://" + t.TempDir()
	cfg := NewConfig().(*Config)
	cfg.Server.Listen, cfg.Server.AdminListen = "127.0.0.1:0", "127.0.0.1:0"
	cfg.Server.Auth.Insecure = true
	cfg.Server.ShutdownDrain = 0
	cfg.Prefix = "new"
	cfg.CacheDir = t.TempDir()
	serveCtx, stop := context.WithCancel(ctx)
	defer stop()
	addrs := make(chan [2]net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(serveCtx, Role, cfg, service.Common{BucketURL: bucket}, slog.New(slog.NewTextHandler(io.Discard, nil)),
			func(api, admin net.Addr) { addrs <- [2]net.Addr{api, admin} })
	}()
	a := <-addrs
	api, admin := "http://"+a[0].String(), "http://"+a[1].String()
	if code, _, _ := call(t, http.MethodGet, admin+"/readyz", ""); code != http.StatusOK {
		t.Fatalf("/readyz before any write: %d, want 200", code)
	}
	code, header, _ := call(t, http.MethodGet, api+"/v1/kv/k", "")
	if code != http.StatusNotFound || header.Get("Isledb-Revision") != "0" {
		t.Fatalf("GET before any write: %d at revision %q, want 404 at 0", code, header.Get("Isledb-Revision"))
	}

	db, err := isledb.Open(ctx, bucket, isledb.DBOptions{Prefix: "new"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	wopts := isledb.DefaultWriterOptions()
	wopts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, wopts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close(ctx) }()
	if _, err := writer.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if code, _, body := call(t, http.MethodGet, api+"/v1/kv/k", ""); code == http.StatusOK && body == "v" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first write never reached the reader")
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}
