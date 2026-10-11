package service

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type fakeRole struct {
	testConfig
	shutdowns, closes atomic.Int32
}

func (f *fakeRole) Start(context.Context, Common, *slog.Logger) (*Service, error) {
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

func TestServeCleansUpWhenTheAdminAddressIsTaken(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	f := &fakeRole{testConfig: *newTestConfig()}
	f.Server.Listen, f.Server.AdminListen = "127.0.0.1:0", taken.Addr().String()
	f.Server.Auth.Insecure = true
	if err := Serve(context.Background(), Role{Name: "test"}, f, Common{}, quiet(), nil); err == nil {
		t.Fatal("Serve started with its admin address taken")
	}
	if f.closes.Load() != 1 {
		t.Fatalf("Close ran %d times, want once", f.closes.Load())
	}
}
