package stats

import (
	"testing"
	"time"
)

func TestQuantilesAreWithinABucket(t *testing.T) {
	var h Histogram
	for i := 1; i <= 1000; i++ {
		h.Observe(time.Duration(i) * time.Millisecond)
	}
	s := h.Snapshot()
	for _, c := range []struct {
		q    float64
		want time.Duration
	}{{0.5, 500 * time.Millisecond}, {0.99, 990 * time.Millisecond}, {1, time.Second}} {
		got := s.Quantile(c.q)
		if got < c.want || float64(got) > float64(c.want)*growth {
			t.Errorf("q%.2f = %s, want within a bucket above %s", c.q, got, c.want)
		}
	}
	if (Snapshot{}).Quantile(0.5) != 0 {
		t.Fatal("an empty histogram has a quantile")
	}
	if l := s.Latency(); l.Count != 1000 || l.Mean < 500 || l.Mean > 501 {
		t.Fatalf("latency %+v", l)
	}
}

func TestSubAndMerge(t *testing.T) {
	var a, b Histogram
	a.Observe(time.Millisecond)
	before := a.Snapshot()
	a.Observe(time.Hour) // past the last bucket
	if d := a.Snapshot().Sub(before); d.Count != 1 || d.Quantile(0.5) < 20*time.Minute {
		t.Fatalf("sub: %d observations, median %s", d.Count, d.Quantile(0.5))
	}
	b.Observe(5 * time.Microsecond)
	b.Merge(&a)
	if s := b.Snapshot(); s.Count != 3 || s.Quantile(0) > minNanos {
		t.Fatalf("merge: %d observations, min %s", s.Count, s.Quantile(0))
	}
}
