package blobstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gocloud.dev/blob/gcsblob"
)

func TestPacerSpacesWritesOfOneKey(t *testing.T) {
	const gap = 50 * time.Millisecond
	p := newOverwritePacer(gap)
	ctx := context.Background()
	var ends []time.Time
	for range 3 {
		if err := p.do(ctx, "CURRENT", func() error { ends = append(ends, time.Now()); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < len(ends); i++ {
		if d := ends[i].Sub(ends[i-1]); d < gap {
			t.Fatalf("writes %d and %d %s apart, want at least %s", i-1, i, d, gap)
		}
	}
}

func TestPacerLetsOtherKeysThrough(t *testing.T) {
	p := newOverwritePacer(time.Hour)
	ctx := context.Background()
	for i := range 3 {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		err := p.do(ctx, fmt.Sprintf("key-%d", i), func() error { return nil })
		cancel()
		if err != nil {
			t.Fatalf("first write of key-%d waited: %v", i, err)
		}
	}
}

func TestPacerWaitEndsWithTheContext(t *testing.T) {
	p := newOverwritePacer(time.Hour)
	if err := p.do(context.Background(), "k", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	wrote := false
	err := p.do(ctx, "k", func() error { wrote = true; return nil })
	if !errors.Is(err, context.DeadlineExceeded) || wrote {
		t.Fatalf("err %v, wrote %v; want the deadline and no write", err, wrote)
	}
}

func TestPacerCountsFailedWrites(t *testing.T) {
	const gap = 50 * time.Millisecond
	p := newOverwritePacer(gap)
	ctx := context.Background()
	refused := errors.New("refused")
	if err := p.do(ctx, "k", func() error { return refused }); !errors.Is(err, refused) {
		t.Fatalf("err %v, want the write's", err)
	}
	failedAt := time.Now()
	_ = p.do(ctx, "k", func() error { return nil })
	if d := time.Since(failedAt); d < gap-5*time.Millisecond {
		t.Fatalf("a write %s after a failed one, want at least %s", d, gap)
	}
}

func TestPacerRunsConcurrentWritesOfOneKeyInTurn(t *testing.T) {
	const gap = 20 * time.Millisecond
	p := newOverwritePacer(gap)
	var running, overlapped atomic.Int32
	var wg sync.WaitGroup
	start := time.Now()
	for range 5 {
		wg.Go(func() {
			_ = p.do(context.Background(), "k", func() error {
				if running.Add(1) > 1 {
					overlapped.Add(1)
				}
				time.Sleep(time.Millisecond)
				running.Add(-1)
				return nil
			})
		})
	}
	wg.Wait()
	if overlapped.Load() > 0 {
		t.Fatal("two writes of one key ran at once")
	}
	if d := time.Since(start); d < 4*gap {
		t.Fatalf("5 writes took %s, want at least %s", d, 4*gap)
	}
}

func TestPacerForgetsIdleKeysOnly(t *testing.T) {
	const gap = 20 * time.Millisecond
	p := newOverwritePacer(gap)
	ctx := context.Background()
	for i := range pacerSweepAt {
		_ = p.do(ctx, fmt.Sprintf("old-%d", i), func() error { return nil })
	}
	time.Sleep(gap)
	_ = p.do(ctx, "recent", func() error { return nil })
	_ = p.do(ctx, "new", func() error { return nil }) // sweeps
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.keys["recent"]; !ok || len(p.keys) != 2 {
		t.Fatalf("after the sweep: %d keys, recent kept %v; want recent and new", len(p.keys), ok)
	}
}

func TestGCSStoreSpacesWritesOfOneObject(t *testing.T) {
	plain := setupFakeGCS(t)
	bucket, err := gcsblob.OpenBucket(context.Background(), nil, fakeGCSBucket, &gcsblob.Options{Client: fakeGCSServer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	s := New(bucket, fakeGCSBucket, plain.prefix)
	if s.pacer == nil || s.pacer.gap != GCSOverwriteGap {
		t.Fatalf("a GCS store has pacer %+v, want one with a %s gap", s.pacer, GCSOverwriteGap)
	}
	if NewMemory("p").pacer != nil {
		t.Fatal("a memory store is paced")
	}
	ctx := context.Background()
	attrs, err := s.WriteIfMatch(ctx, "paced/CURRENT", []byte("1"), "")
	if err != nil {
		t.Fatal(err)
	}
	first := time.Now()
	if _, err := s.WriteIfMatch(ctx, "paced/CURRENT", []byte("2"), fmt.Sprint(attrs.Generation)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(first); d < GCSOverwriteGap {
		t.Fatalf("second write of one object %s after the first, want at least %s", d, GCSOverwriteGap)
	}
}
