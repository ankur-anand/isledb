package cacherole

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
)

func call(t *testing.T, url, token string, header ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

func TestCacheRoleServesAndShutsDownCleanly(t *testing.T) {
	bucket := "file://" + t.TempDir()
	store, err := blobstore.Open(context.Background(), bucket, "prod/a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	current, _ := json.Marshal(manifest.Current{LayoutVersion: manifest.LayoutVersion, Format: manifest.CurrentFormat, NextSeq: 7, NextEpoch: 1})
	sst := bytes.Repeat([]byte("isle"), 100_000)
	for key, data := range map[string][]byte{store.ManifestPath(): current, store.SSTPath("000001.sst"): sst} {
		if _, err := store.Write(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
	}
	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte("secret-one\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := NewConfig().(*Config)
	cfg.Server.Listen, cfg.Server.AdminListen = "127.0.0.1:0", "127.0.0.1:0"
	cfg.Server.Auth.TokenFile = tokens
	cfg.Server.ShutdownDrain = 0
	cfg.Disk = Disk{Dir: t.TempDir(), Bytes: 64 << 20}
	cfg.Databases = []Database{{Prefix: "prod/a"}}
	cfg.Current = Current{Poll: service.Duration(20 * time.Millisecond), LongPoll: service.Duration(10 * time.Second)}
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
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if resp, _ := call(t, admin+"/readyz", ""); resp.StatusCode == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never ready")
		}
	}

	if resp, _ := call(t, api+"/v1/current", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: status %d, want 401", resp.StatusCode)
	}
	if resp, _ := call(t, api+"/v1/current", "secret-one"); resp.Header.Get(readcache.HeaderPosition) != "7" {
		t.Fatalf("CURRENT: status %d, position %q", resp.StatusCode, resp.Header.Get(readcache.HeaderPosition))
	}
	resp, body := call(t, api+"/v1/object/"+store.SSTPath("000001.sst"), "secret-one",
		"Range", "bytes=1000-1999", readcache.HeaderObjectSize, "400000")
	if resp.StatusCode != http.StatusPartialContent || body != string(sst[1000:2000]) {
		t.Fatalf("object: status %d, %d bytes", resp.StatusCode, len(body))
	}
	other := blobstore.NewMemory("prod/b").SSTPath("000001.sst")
	if resp, _ := call(t, api+"/v1/object/"+other, "secret-one"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("object of a database not served: status %d, want 403", resp.StatusCode)
	}
	_, metrics := call(t, admin+"/metrics", "")
	for _, want := range []string{
		`isledb_cache_current_position{db="prod/a"} 7`,
		`isledb_cache_object_responses_total{status="miss"} 1`,
		"isledb_cache_storage_reads_total 1",
		`isledb_cache_http_requests_total{code="401",route="current"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics lack %q", want)
		}
	}

	held := make(chan *http.Response, 1)
	go func() {
		resp, _ := call(t, api+"/v1/current?after=7", "secret-one")
		held <- resp
	}()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	stop()
	select {
	case resp := <-held:
		if resp.StatusCode != http.StatusOK || resp.Header.Get(readcache.HeaderPosition) != "7" {
			t.Fatalf("held long poll at shutdown: status %d, position %q", resp.StatusCode, resp.Header.Get(readcache.HeaderPosition))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a held long poll was not answered at shutdown")
	}
	if err := <-done; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("shutdown took %s with a long poll held, want well under the 10s long poll", took)
	}
}
