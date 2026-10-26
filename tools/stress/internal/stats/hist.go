// Package stats has the counters and latency histograms the stress test
// reports.
package stats

import (
	"math"
	"sync/atomic"
	"time"
)

// Histogram counts durations in buckets about 4% wide, from 10µs to about
// 20 minutes. It is safe for concurrent use and never allocates.
type Histogram struct {
	buckets [nBuckets]atomic.Int64
	sum     atomic.Int64 // nanoseconds
}

const (
	minNanos  = 10_000
	growth    = 1.04
	nBuckets  = 480
	logGrowth = 0.03922071315328133 // math.Log(growth)
)

func bucketOf(d time.Duration) int {
	if d <= minNanos {
		return 0
	}
	i := int(math.Log(float64(d)/minNanos)/logGrowth) + 1
	return min(i, nBuckets-1)
}

// upper returns the largest duration bucket i holds.
func upper(i int) time.Duration {
	return time.Duration(minNanos * math.Pow(growth, float64(i)))
}

func (h *Histogram) Observe(d time.Duration) {
	h.buckets[bucketOf(d)].Add(1)
	h.sum.Add(int64(d))
}

// Snapshot is a histogram's counts at one moment.
type Snapshot struct {
	Buckets [nBuckets]int64
	Count   int64
	Sum     time.Duration
}

func (h *Histogram) Snapshot() Snapshot {
	var s Snapshot
	for i := range h.buckets {
		s.Buckets[i] = h.buckets[i].Load()
		s.Count += s.Buckets[i]
	}
	s.Sum = time.Duration(h.sum.Load())
	return s
}

// Sub returns the observations made between earlier and s.
func (s Snapshot) Sub(earlier Snapshot) Snapshot {
	var d Snapshot
	for i := range s.Buckets {
		d.Buckets[i] = s.Buckets[i] - earlier.Buckets[i]
		d.Count += d.Buckets[i]
	}
	d.Sum = s.Sum - earlier.Sum
	return d
}

// Quantile returns the upper bound of the bucket holding quantile q, 0 with
// no observations.
func (s Snapshot) Quantile(q float64) time.Duration {
	if s.Count == 0 {
		return 0
	}
	rank := int64(math.Ceil(q * float64(s.Count)))
	rank = max(rank, 1)
	var seen int64
	for i, c := range s.Buckets {
		if seen += c; seen >= rank {
			return upper(i)
		}
	}
	return upper(nBuckets - 1)
}

// Latency is the summary of a snapshot the reports carry.
type Latency struct {
	Count int64   `json:"count"`
	P50   float64 `json:"p50_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Mean  float64 `json:"mean_ms"`
}

func ms(d time.Duration) float64 { return math.Round(float64(d)/1e3) / 1e3 }

func (s Snapshot) Latency() Latency {
	l := Latency{Count: s.Count, P50: ms(s.Quantile(0.5)), P99: ms(s.Quantile(0.99)), P999: ms(s.Quantile(0.999))}
	if s.Count > 0 {
		l.Mean = ms(s.Sum / time.Duration(s.Count))
	}
	return l
}

// Merge adds o's observations to h.
func (h *Histogram) Merge(o *Histogram) {
	for i := range o.buckets {
		if n := o.buckets[i].Load(); n != 0 {
			h.buckets[i].Add(n)
		}
	}
	h.sum.Add(o.sum.Load())
}

// Cumulative returns, for each bound, how many observations were at most
// it, as a Prometheus histogram's buckets count them. A bucket counts
// toward a bound its upper edge does not pass, so a count can fall short by
// what one 4% bucket holds.
func (s Snapshot) Cumulative(bounds []time.Duration) []int64 {
	out := make([]int64, len(bounds))
	var seen int64
	b := 0
	for i, c := range s.Buckets {
		for b < len(bounds) && upper(i) > bounds[b] {
			out[b] = seen
			b++
		}
		seen += c
	}
	for ; b < len(bounds); b++ {
		out[b] = seen
	}
	return out
}
