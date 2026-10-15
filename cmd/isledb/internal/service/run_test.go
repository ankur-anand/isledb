package service

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type fakeRole struct {
	testConfig
	starts, shutdowns, closes atomic.Int32
}

func (f *fakeRole) Start(context.Context, Common, *slog.Logger) (*Service, error) {
	f.starts.Add(1)
	return &Service{
		API:        http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }),
		Ready:      func() bool { return true },
		OnShutdown: func() { f.shutdowns.Add(1) },
		Close:      func() error { f.closes.Add(1); return nil },
	}, nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func status(t *testing.T, url, token string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestServeAuthenticatesReportsReadyAndShutsDownInOrder(t *testing.T) {
	tokens := writeFile(t, "# ops\nsecret-one\n")
	f := &fakeRole{testConfig: *newTestConfig()}
	f.Server.Listen, f.Server.AdminListen = "127.0.0.1:0", "127.0.0.1:0"
	f.Server.Auth.TokenFile = tokens
	f.Server.ShutdownDrain = Duration(300 * time.Millisecond)

	ctx, stop := context.WithCancel(context.Background())
	addrs := make(chan [2]net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Role{Name: "test"}, f, Common{}, quiet(), func(api, admin net.Addr) { addrs <- [2]net.Addr{api, admin} })
	}()
	a := <-addrs
	api, admin := "http://"+a[0].String(), "http://"+a[1].String()

	if s := status(t, api+"/", ""); s != http.StatusUnauthorized {
		t.Fatalf("no token: %d, want 401", s)
	}
	if s := status(t, api+"/", "secret-one"); s != http.StatusOK {
		t.Fatalf("token: %d, want 200", s)
	}
	if s := status(t, admin+"/readyz", ""); s != http.StatusOK {
		t.Fatalf("readyz: %d, want 200", s)
	}

	if err := os.WriteFile(tokens, []byte("secret-two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := NewTokenAuth(tokens)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer secret-one")
	if auth.Allows(req) {
		t.Fatal("a token removed from the file is still allowed")
	}

	stop()
	time.Sleep(100 * time.Millisecond)
	if s := status(t, admin+"/readyz", ""); s != http.StatusServiceUnavailable {
		t.Fatalf("readyz while draining: %d, want 503", s)
	}
	if n := f.shutdowns.Load(); n != 0 {
		t.Fatal("OnShutdown ran before the drain passed")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if f.shutdowns.Load() != 1 || f.closes.Load() != 1 {
		t.Fatalf("OnShutdown ran %d times, Close %d; want once each", f.shutdowns.Load(), f.closes.Load())
	}
}

// A server that cannot run must fail before Start: a role such as the writer
// takes over its database there, and would take it from a healthy one only
// to exit.
func TestServeFailsBeforeStartWhenItCannotRun(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	for _, c := range []struct {
		name string
		set  func(*fakeRole)
	}{
		{"admin address taken", func(f *fakeRole) { f.Server.AdminListen = taken.Addr().String() }},
		{"API address taken", func(f *fakeRole) { f.Server.Listen = taken.Addr().String() }},
		{"token file missing", func(f *fakeRole) {
			f.Server.Auth.Insecure = false
			f.Server.Auth.TokenFile = t.TempDir() + "/missing"
		}},
		{"TLS key missing", func(f *fakeRole) {
			f.Server.TLS.Cert, f.Server.TLS.Key = t.TempDir()+"/cert.pem", t.TempDir()+"/key.pem"
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeRole{testConfig: *newTestConfig()}
			f.Server.Listen, f.Server.AdminListen = "127.0.0.1:0", "127.0.0.1:0"
			f.Server.Auth.Insecure = true
			c.set(f)
			if err := Serve(context.Background(), Role{Name: "test"}, f, Common{}, quiet(), nil); err == nil {
				t.Fatal("Serve started")
			}
			if f.starts.Load() != 0 || f.closes.Load() != 0 {
				t.Fatalf("Start ran %d times and Close %d, want neither", f.starts.Load(), f.closes.Load())
			}
		})
	}
}

// A request refused for its token is answered at once, even when its client
// stalls mid-body: the server must not wait to read a body it will not use.
func TestUnauthorizedStalledBodyIsAnsweredAtOnce(t *testing.T) {
	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := &fakeRole{testConfig: *newTestConfig()}
	f.Server.Listen, f.Server.AdminListen = "127.0.0.1:0", ""
	f.Server.Auth.TokenFile = tokens
	f.Server.ShutdownDrain = 0
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	addrs := make(chan net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Role{Name: "test"}, f, Common{}, quiet(), func(api, _ net.Addr) { addrs <- api })
	}()
	conn, err := net.Dial("tcp", (<-addrs).String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = conn.Write([]byte("PUT /x HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\nabc"))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no answer after %s: %v", time.Since(start), err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || !resp.Close || time.Since(start) > time.Second {
		t.Fatalf("%s, close %t, after %s; want 401 at once, closing", resp.Status, resp.Close, time.Since(start))
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
