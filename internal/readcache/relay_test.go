package readcache

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/manifest"
)

// currentBytes encodes a valid CURRENT at position; epoch varies the bytes at
// one position, as a checkpoint does.
func currentBytes(t *testing.T, position, epoch uint64) []byte {
	t.Helper()
	data, err := json.Marshal(manifest.Current{
		LayoutVersion: manifest.LayoutVersion, Format: manifest.CurrentFormat,
		NextSeq: position, NextEpoch: epoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// fakeSource serves fixed CURRENT bytes, or an error.
type fakeSource struct {
	mu   sync.Mutex
	data []byte
	err  error
}

func (s *fakeSource) set(data []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.err = data, err
}

func (s *fakeSource) ReadCurrent(context.Context) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data, "etag", s.err
}

func soon(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestRelayTakesNewerAndCheckpointsRefusesOlder(t *testing.T) {
	src := &fakeSource{}
	r := NewRelay(src, time.Hour)
	if _, ok := r.Current(); ok {
		t.Fatal("copy before the first read")
	}
	poll := func() {
		t.Helper()
		if err := r.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	src.set(currentBytes(t, 5, 1), nil)
	poll()
	if c, _ := r.Current(); c.Position != 5 {
		t.Fatalf("position %d, want 5", c.Position)
	}

	src.set(currentBytes(t, 3, 1), nil) // the store went back
	if err := r.Poll(context.Background()); err == nil {
		t.Fatal("Poll accepted a CURRENT behind the copy without an error")
	}
	if c, _ := r.Current(); c.Position != 5 {
		t.Fatalf("position %d after an older CURRENT, want 5", c.Position)
	}

	checkpoint := currentBytes(t, 5, 2) // same position, new bytes
	src.set(checkpoint, nil)
	poll()
	if c, _ := r.Current(); c.Position != 5 || string(c.Data) != string(checkpoint) {
		t.Fatalf("copy %s, want the checkpoint's CURRENT", c.Data)
	}
}

func TestRelayKeepsCopyAndAgesWhenStorageFails(t *testing.T) {
	src := &fakeSource{}
	src.set(currentBytes(t, 7, 1), nil)
	r := NewRelay(src, time.Hour)
	if err := r.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.set(nil, errors.New("storage unreachable"))
	time.Sleep(20 * time.Millisecond)
	if err := r.Poll(context.Background()); err == nil {
		t.Fatal("Poll succeeded with storage failing")
	}
	c, ok := r.Current()
	if !ok || c.Position != 7 || c.Age < 20*time.Millisecond {
		t.Fatalf("copy %+v, want position 7 with an age of at least 20ms", c)
	}

	src.set(currentBytes(t, 7, 1), nil) // a successful read of the same CURRENT renews it
	if err := r.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c, _ := r.Current(); c.Age >= 20*time.Millisecond {
		t.Fatalf("age %s after a fresh read, want it reset", c.Age)
	}
}

func TestRelayWaitReturnsOnAdvanceOrWhenDone(t *testing.T) {
	src := &fakeSource{}
	src.set(currentBytes(t, 1, 1), nil)
	r := NewRelay(src, time.Hour)
	if err := r.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := make(chan Current, 1)
	waitCtx := soon(t)
	go func() {
		c, _ := r.Wait(waitCtx, 1)
		got <- c
	}()
	select {
	case <-got:
		t.Fatal("Wait returned before the position passed 1")
	case <-time.After(50 * time.Millisecond):
	}
	src.set(currentBytes(t, 2, 1), nil)
	if err := r.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-got:
		if c.Position != 2 {
			t.Fatalf("Wait returned position %d, want 2", c.Position)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after the position advanced")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if c, ok := r.Wait(ctx, 2); !ok || c.Position != 2 {
		t.Fatalf("Wait at timeout = %+v, %t; want the current copy", c, ok)
	}
}

func TestRelayRunPollsUntilStopped(t *testing.T) {
	src := &fakeSource{}
	src.set(currentBytes(t, 1, 1), nil)
	r := NewRelay(src, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	src.set(currentBytes(t, 4, 1), nil)
	if c, _ := r.Wait(soon(t), 3); c.Position != 4 {
		t.Fatalf("position %d, want Run to pick up 4", c.Position)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}

// slowSource takes delay to answer, as storage does under load.
type slowSource struct {
	*fakeSource
	delay time.Duration
}

func (s slowSource) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	time.Sleep(s.delay)
	return s.fakeSource.ReadCurrent(ctx)
}

func TestRelayAgeIncludesTheReadsDuration(t *testing.T) {
	src := &fakeSource{}
	src.set(currentBytes(t, 1, 1), nil)
	r := NewRelay(slowSource{fakeSource: src, delay: 100 * time.Millisecond}, time.Hour)
	if err := r.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Storage answered at some moment during the 100ms read, so the copy is at
	// least as old as the read took, never younger.
	if c, _ := r.Current(); c.Age < 100*time.Millisecond {
		t.Fatalf("age %s right after a 100ms read, want at least 100ms", c.Age)
	}
}

// racingSource answers its first read slowly with an older CURRENT and later
// reads at once with a newer one, as storage does when a commit lands while a
// slow read is in flight.
type racingSource struct {
	mu    sync.Mutex
	calls int
	older []byte
	newer []byte
}

func (s *racingSource) ReadCurrent(context.Context) ([]byte, string, error) {
	s.mu.Lock()
	s.calls++
	first := s.calls == 1
	s.mu.Unlock()
	if first {
		time.Sleep(100 * time.Millisecond)
		return s.older, "etag", nil
	}
	return s.newer, "etag", nil
}

func TestRelayOverlappingPollsDoNotReportTheStoreWentBack(t *testing.T) {
	src := &racingSource{older: currentBytes(t, 1, 1), newer: currentBytes(t, 2, 1)}
	r := NewRelay(src, time.Hour)
	errs := make(chan error, 2)
	go func() { errs <- r.Poll(context.Background()) }()
	time.Sleep(20 * time.Millisecond) // the first poll is now in its slow read
	go func() { errs <- r.Poll(context.Background()) }()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("overlapping polls: %v", err)
		}
	}
	if c, _ := r.Current(); c.Position != 2 {
		t.Fatalf("position %d, want 2", c.Position)
	}
}
