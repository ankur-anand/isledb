// Package read reads from isledb readers at a target rate, gets and scans,
// and hands every answer to the verifier.
package read

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/client"
	"github.com/ankur-anand/isledb/tools/stress/internal/ledger"
	"github.com/ankur-anand/isledb/tools/stress/internal/stats"
	"github.com/ankur-anand/isledb/tools/stress/internal/verify"
	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

// Config is one group of readers.
type Config struct {
	Name     string
	Schedule *workload.Schedule
	Shape    workload.Shape
	Ledger   *ledger.Ledger
	Verifier *verify.Verifier
	// Readers are the isledb readers to spread reads over.
	Readers []*client.Client
	// Rate is operations a second; 0 is unlimited. With a rate, reads are
	// due at their times whether or not the workers keep up: one a worker
	// starts late counts its wait in its response time, and one due more
	// than MaxLag ago is dropped and counted, never sent.
	Rate    float64
	MaxLag  time.Duration
	Workers int
	// ScanLimit is the most keys a scan asks for; with ScanMin, each asks
	// for a number between the two.
	ScanRatio          float64
	ScanMin, ScanLimit int
	// Dist picks the keys gets read: uniform, zipf (low indexes hot) or
	// latest (the most recently inserted).
	Dist         string
	ZipfS        float64
	LatestWindow uint64
	Log          *slog.Logger
}

// Counters are a reader group's running totals.
type Counters struct {
	Gets, Scans, ScanItems atomic.Int64
	Retries, Errors        atomic.Int64
	// Get and Scan are service times, from sending a read to its answer;
	// GetResponse and ScanResponse response times, from when it was due.
	Get, Scan                 stats.Histogram
	GetResponse, ScanResponse stats.Histogram
	// Offered is the reads the rate made due; Dropped those never sent
	// because they were due more than MaxLag ago, and Late those sent more
	// than lateAfter after they were due. Busy is the workers reading now.
	Offered, Dropped, Late atomic.Int64
	Busy                   atomic.Int64
}

// DefaultWorkers is a group's workers when its config gives none.
const DefaultWorkers = 16

// lateAfter is how long after it was due a read counts as started late.
const lateAfter = 10 * time.Millisecond

type Reader struct {
	cfg  Config
	due  *arrivals
	C    Counters
	next atomic.Uint64
	// lastErr is logged at most once a second.
	logMu  sync.Mutex
	logged time.Time
}

func New(cfg Config) (*Reader, error) {
	if len(cfg.Readers) == 0 {
		return nil, errors.New("read: no readers")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers
	}
	if cfg.ScanLimit <= 0 {
		cfg.ScanLimit = 100
	}
	if cfg.ScanMin < 0 || cfg.ScanMin > cfg.ScanLimit {
		return nil, fmt.Errorf("read: scan_min %d: want 0 to scan_limit %d", cfg.ScanMin, cfg.ScanLimit)
	}
	if cfg.MaxLag <= 0 {
		cfg.MaxLag = 5 * time.Second
	}
	switch cfg.Dist {
	case "":
		cfg.Dist = "uniform"
	case "uniform", "zipf", "latest":
	default:
		return nil, fmt.Errorf("read: key distribution %q: want uniform, zipf or latest", cfg.Dist)
	}
	if cfg.ZipfS <= 1 {
		cfg.ZipfS = 1.1
	}
	if cfg.LatestWindow == 0 {
		cfg.LatestWindow = 1 << 20
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	r := &Reader{cfg: cfg}
	if cfg.Rate > 0 {
		r.due = &arrivals{interval: time.Duration(float64(time.Second) / cfg.Rate), maxLag: cfg.MaxLag}
	}
	return r, nil
}

// arrivals hands out the times reads are due, one each interval, whether or
// not the workers keep up.
type arrivals struct {
	interval, maxLag time.Duration
	mu               sync.Mutex
	next             time.Time
}

// take waits for the next read's time and returns it, with how many reads
// before it were dropped for being due more than maxLag ago.
func (a *arrivals) take(ctx context.Context) (time.Time, int64, error) {
	a.mu.Lock()
	now := time.Now()
	if a.next.IsZero() {
		a.next = now
	}
	var dropped int64
	if lag := now.Sub(a.next); lag > a.maxLag {
		dropped = int64((lag - a.maxLag + a.interval - 1) / a.interval)
		a.next = a.next.Add(time.Duration(dropped) * a.interval)
	}
	due := a.next
	a.next = a.next.Add(a.interval)
	a.mu.Unlock()
	if d := time.Until(due); d > 0 && !sleep(ctx, d) {
		return due, dropped, ctx.Err()
	}
	return due, dropped, ctx.Err()
}

func (r *Reader) reader() *client.Client {
	rs := r.cfg.Readers
	return rs[r.next.Add(1)%uint64(len(rs))]
}

// written returns how many keys have been sent: those of the rounds before
// the newest batch sent, and the inserts of its round up to it.
func (r *Reader) written() uint64 {
	sent := r.cfg.Ledger.MaxSent()
	if sent < 0 {
		return 0
	}
	s := r.cfg.Schedule
	return s.KeysBy(s.Batch(uint64(sent)))
}

func (r *Reader) logErr(msg string, args ...any) {
	r.logMu.Lock()
	defer r.logMu.Unlock()
	if time.Since(r.logged) < time.Second {
		return
	}
	r.logged = time.Now()
	r.cfg.Log.Warn(msg, args...)
}

// Run reads until ctx ends.
func (r *Reader) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for w := range r.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.work(ctx, uint64(w))
		}()
	}
	wg.Wait()
}

func (r *Reader) work(ctx context.Context, worker uint64) {
	w := &picker{rng: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), worker))}
	for {
		due := time.Now()
		if r.due != nil {
			var dropped int64
			var err error
			if due, dropped, err = r.due.take(ctx); err != nil {
				return
			}
			r.C.Offered.Add(1 + dropped)
			r.C.Dropped.Add(dropped)
			if time.Since(due) > lateAfter {
				r.C.Late.Add(1)
			}
		} else if ctx.Err() != nil {
			return
		}
		n := r.written()
		if n == 0 {
			if !sleep(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		r.C.Busy.Add(1)
		r.read(ctx, w, n, due)
		r.C.Busy.Add(-1)
	}
}

// picker is one worker's choice of what to read.
type picker struct {
	rng   *rand.Rand
	zipf  *rand.Zipf
	zipfN uint64
}

// read does one get or scan of the first n keys, due at due.
func (r *Reader) read(ctx context.Context, w *picker, n uint64, due time.Time) {
	if w.rng.Float64() < r.cfg.ScanRatio {
		limit := r.cfg.ScanLimit
		if r.cfg.ScanMin > 0 {
			limit = r.cfg.ScanMin + w.rng.IntN(r.cfg.ScanLimit-r.cfg.ScanMin+1)
		}
		// From a written key, so the scan lands among keys in every shape:
		// random bytes would start past every sequential key.
		r.scan(ctx, r.cfg.Shape.AppendKey(nil, w.rng.Uint64N(n)), limit, due)
		return
	}
	var idx uint64
	switch r.cfg.Dist {
	case "zipf":
		if w.zipf == nil || w.zipfN != n {
			w.zipf, w.zipfN = rand.NewZipf(w.rng, r.cfg.ZipfS, 1, n-1), n
		}
		idx = w.zipf.Uint64()
	case "latest":
		// Keys are inserted in index order, so the newest have the highest.
		idx = n - 1 - w.rng.Uint64N(min(r.cfg.LatestWindow, n))
	default:
		idx = w.rng.Uint64N(n)
	}
	r.get(ctx, idx, due)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *Reader) get(ctx context.Context, idx uint64, due time.Time) {
	key := r.cfg.Shape.AppendKey(nil, idx)
	start := time.Now()
	value, found, rev, err := r.reader().Get(ctx, key)
	switch {
	case ctx.Err() != nil:
		return
	case errors.Is(err, client.ErrRetry):
		r.C.Retries.Add(1)
		return
	case err != nil:
		r.C.Errors.Add(1)
		r.logErr("get failed", "group", r.cfg.Name, "key", fmt.Sprintf("%x", key), "error", err)
		return
	}
	r.C.Get.Observe(time.Since(start))
	r.C.GetResponse.Observe(time.Since(due))
	r.C.Gets.Add(1)
	r.cfg.Verifier.Get(idx, rev, value, found)
}

func items(p client.Page) []verify.Item {
	out := make([]verify.Item, len(p.Items))
	for i, it := range p.Items {
		out[i] = verify.Item{Key: it.Key, Value: it.Value}
	}
	return out
}

// scan reads one page of at most limit keys from lo.
func (r *Reader) scan(ctx context.Context, lo []byte, limit int, due time.Time) {
	start := time.Now()
	p, err := r.reader().Scan(ctx, lo, nil, limit, "")
	switch {
	case ctx.Err() != nil:
		return
	case errors.Is(err, client.ErrRetry):
		r.C.Retries.Add(1)
		return
	case err != nil:
		r.C.Errors.Add(1)
		r.logErr("scan failed", "group", r.cfg.Name, "from", fmt.Sprintf("%x", lo), "error", err)
		return
	}
	r.C.Scan.Observe(time.Since(start))
	r.C.ScanResponse.Observe(time.Since(due))
	r.C.Scans.Add(1)
	r.C.ScanItems.Add(int64(len(p.Items)))
	r.cfg.Verifier.Page(lo, nil, p.Revision, items(p), p.Next != "")
}

// FullScan reads every key once, the keyspace split into parts ranges
// scanned in parallel, and checks every page. It returns the keys seen.
func FullScan(ctx context.Context, readers []*client.Client, v *verify.Verifier, parts, limit int) (int64, error) {
	parts = max(parts, 1)
	bound := func(i int) []byte {
		if i == 0 || i == parts {
			return nil
		}
		return binary.BigEndian.AppendUint64(nil, uint64(i)*(math.MaxUint64/uint64(parts)))
	}
	var seen atomic.Int64
	errs := make([]error, parts)
	var wg sync.WaitGroup
	for i := range parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := readers[i%len(readers)]
			lo, hi := bound(i), bound(i+1)
			from := lo
			for {
				var p client.Page
				var err error
				for attempt := range 20 {
					if p, err = c.Scan(ctx, from, hi, limit, ""); err == nil || ctx.Err() != nil {
						break
					}
					sleep(ctx, time.Duration(attempt+1)*100*time.Millisecond)
				}
				if err != nil {
					errs[i] = fmt.Errorf("full scan from %x: %w", from, err)
					return
				}
				v.Page(from, hi, p.Revision, items(p), p.Next != "")
				seen.Add(int64(len(p.Items)))
				if p.Next == "" || len(p.Items) == 0 {
					return
				}
				from = append(bytes.Clone(p.Items[len(p.Items)-1].Key), 0)
			}
		}()
	}
	wg.Wait()
	return seen.Load(), errors.Join(errs...)
}
