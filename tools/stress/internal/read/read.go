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
	"github.com/ankur-anand/isledb/tools/stress/internal/load"
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
	// Rate is operations a second; 0 is unlimited.
	Rate      float64
	Workers   int
	ScanRatio float64
	ScanLimit int
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
	Get, Scan              stats.Histogram
}

type Reader struct {
	cfg  Config
	pace *load.Pacer
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
		cfg.Workers = 16
	}
	if cfg.ScanLimit <= 0 {
		cfg.ScanLimit = 100
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
	return &Reader{cfg: cfg, pace: load.NewPacer(cfg.Rate)}, nil
}

func (r *Reader) Pacer() *load.Pacer { return r.pace }

func (r *Reader) reader() *client.Client {
	rs := r.cfg.Readers
	return rs[r.next.Add(1)%uint64(len(rs))]
}

// written returns how many keys have been sent: all of them once the insert
// round is over.
func (r *Reader) written() uint64 {
	s := r.cfg.Schedule
	sent := r.cfg.Ledger.MaxSent()
	if sent < 0 {
		return 0
	}
	if b := s.Batch(uint64(sent)); b.Round == 0 {
		return b.Start + b.Count
	}
	return s.N
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
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), worker))
	var zipf *rand.Zipf
	var zipfN uint64
	for r.pace.Take(ctx, 1) == nil {
		n := r.written()
		if n == 0 {
			if !sleep(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		if rng.Float64() < r.cfg.ScanRatio {
			var lo [8]byte
			binary.BigEndian.PutUint64(lo[:], rng.Uint64())
			r.scan(ctx, lo[:])
			continue
		}
		var idx uint64
		switch r.cfg.Dist {
		case "zipf":
			if zipf == nil || zipfN != n {
				zipf, zipfN = rand.NewZipf(rng, r.cfg.ZipfS, 1, n-1), n
			}
			idx = zipf.Uint64()
		case "latest":
			idx = r.cfg.Schedule.Index(0, n-1-rng.Uint64N(min(r.cfg.LatestWindow, n)))
		default:
			idx = rng.Uint64N(n)
		}
		r.get(ctx, idx)
	}
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

func (r *Reader) get(ctx context.Context, idx uint64) {
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

// scan reads one page from lo.
func (r *Reader) scan(ctx context.Context, lo []byte) {
	start := time.Now()
	p, err := r.reader().Scan(ctx, lo, nil, r.cfg.ScanLimit, "")
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
