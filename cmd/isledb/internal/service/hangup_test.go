//go:build unix

package service

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestHangupDoesNotStopTheProcess sends this process SIGHUP, whose default
// would end it: once keepHangups has run, before a token reloader starts and
// after one stops, as during start and shutdown. Without the fix the test
// binary dies.
func TestHangupDoesNotStopTheProcess(t *testing.T) {
	keepHangups()
	hangup := func() {
		t.Helper()
		if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // time for a default action to land
	}
	hangup()

	file := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(file, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tokens, err := NewTokenAuth(file)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { reloadOnHangup(ctx, tokens, slog.New(slog.NewTextHandler(io.Discard, nil))); close(done) }()
	hangup()
	cancel()
	<-done // the reloader has stopped listening
	hangup()
}
