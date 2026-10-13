package cacheclient

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// hangingFixture is a cache that, while hang is set, holds every request
// until the client gives up.
func hangingFixture(t *testing.T) (*fixture, *atomic.Bool) {
	t.Helper()
	var hang atomic.Bool
	f := newFixture(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hang.Load() {
				<-r.Context().Done()
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	return f, &hang
}

func timed(c *Client, f *fixture) (string, time.Duration) {
	start := time.Now()
	_, err := c.ReadRange(context.Background(), f.key, int64(len(f.data)), 0, 10, true)
	return reason(err), time.Since(start)
}

func TestBreakerSkipsACacheThatStoppedAnsweringThenProbes(t *testing.T) {
	f, hang := hangingFixture(t)
	c := client(t, Options{URL: f.url, Timeout: 50 * time.Millisecond})
	c.breaker.cooldown = 200 * time.Millisecond

	hang.Store(true)
	for i := range breakerTrips {
		if r, _ := timed(c, f); r != ReasonTimeout {
			t.Fatalf("read %d: reason %q, want timeout", i, r)
		}
	}
	if r, took := timed(c, f); r != ReasonBypassed || took > 20*time.Millisecond {
		t.Fatalf("after %d timeouts: reason %q after %s, want bypassed at once", breakerTrips, r, took)
	}

	time.Sleep(c.breaker.cooldown)
	probe := make(chan string, 1)
	go func() { r, _ := timed(c, f); probe <- r }()
	time.Sleep(10 * time.Millisecond)
	if r, took := timed(c, f); r != ReasonBypassed || took > 20*time.Millisecond {
		t.Fatalf("while the probe is out: reason %q after %s, want bypassed at once", r, took)
	}
	if r := <-probe; r != ReasonTimeout {
		t.Fatalf("probe of a hanging cache: reason %q, want timeout", r)
	}
	if r, _ := timed(c, f); r != ReasonBypassed {
		t.Fatalf("after a failed probe: reason %q, want bypassed for another cooldown", r)
	}

	hang.Store(false)
	time.Sleep(c.breaker.cooldown)
	if r, _ := timed(c, f); r != "" {
		t.Fatalf("probe of an answering cache: reason %q, want a read", r)
	}
	if r, _ := timed(c, f); r != "" {
		t.Fatalf("after a good probe: reason %q, want reads through the cache", r)
	}
}

func TestBreakerIgnoresAnswersAndAbandonedReads(t *testing.T) {
	f, hang := hangingFixture(t)
	size := int64(len(f.data))
	c := client(t, Options{URL: f.url, Timeout: 50 * time.Millisecond})
	for range 2 * breakerTrips {
		if _, err := c.ReadRange(context.Background(), f.key, size+1, 0, 1, true); reason(err) != ReasonSizeMismatch {
			t.Fatalf("wrong size: %v, want size_mismatch every time", err)
		}
	}

	hang.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 2 * breakerTrips {
		_, _ = c.ReadRange(ctx, f.key, size, 0, 1, true)
	}
	if r, _ := timed(c, f); r != ReasonTimeout {
		t.Fatalf("after reads their callers abandoned: reason %q, want the cache tried", r)
	}
}

func TestBreakerCountsOnlyFailuresInARow(t *testing.T) {
	f, hang := hangingFixture(t)
	c := client(t, Options{URL: f.url, Timeout: 50 * time.Millisecond})
	for range 3 {
		hang.Store(true)
		for range breakerTrips - 1 {
			timed(c, f)
		}
		hang.Store(false)
		if r, _ := timed(c, f); r != "" {
			t.Fatalf("a read between timeouts: reason %q, want a read through the cache", r)
		}
	}
}
