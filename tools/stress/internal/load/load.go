// Package load sends a schedule's batches to an isledb writer and records
// what became of each in the ledger.
package load

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/client"
	"github.com/ankur-anand/isledb/tools/stress/internal/ledger"
	"github.com/ankur-anand/isledb/tools/stress/internal/stats"
	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

// Config is one run of the load: batches [From, To) of a schedule.
type Config struct {
	Schedule *workload.Schedule
	Shape    workload.Shape
	Ledger   *ledger.Ledger
	Writer   *client.Client
	From, To uint64
	// Rate is key and value bytes a second; 0 is unlimited.
	Rate    float64
	Workers int
	// StatusTimeout bounds how long a write whose outcome is unknown is
	// asked about before it is sent again.
	StatusTimeout time.Duration
	Log           *slog.Logger
}

// Counters are the load's running totals.
type Counters struct {
	Batches      atomic.Int64 // committed
	Keys         atomic.Int64
	Bytes        atomic.Int64 // key and value bytes committed
	Attempts     atomic.Int64
	Refused      atomic.Int64 // 503 backpressure or unavailable: nothing written
	NotSaved     atomic.Int64 // accepted, never saved
	Unknown      atomic.Int64 // outcome asked about
	Unresolved   atomic.Int64 // outcome never learned; sent again
	Commit       stats.Histogram
	BytesByRound sync.Map // uint32 -> *atomic.Int64
}

func (c *Counters) addRoundBytes(round uint32, n int64) {
	v, _ := c.BytesByRound.LoadOrStore(round, &atomic.Int64{})
	v.(*atomic.Int64).Add(n)
}

// Loader runs a Config.
type Loader struct {
	cfg    Config
	pace   *Pacer
	C      Counters
	paused atomic.Bool
	warned atomic.Int64
}

func New(cfg Config) (*Loader, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.StatusTimeout <= 0 {
		cfg.StatusTimeout = 2 * time.Minute
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.To < cfg.From {
		return nil, fmt.Errorf("load: batches [%d, %d)", cfg.From, cfg.To)
	}
	return &Loader{cfg: cfg, pace: NewPacer(cfg.Rate)}, nil
}

// warn logs at most once a second; the counters count every case.
func (l *Loader) warn(msg string, args ...any) {
	now := time.Now().UnixNano()
	last := l.warned.Load()
	if now-last < int64(time.Second) || !l.warned.CompareAndSwap(last, now) {
		return
	}
	l.cfg.Log.Warn(msg, args...)
}

// Pacer returns the pacer, whose rate can change while the load runs.
func (l *Loader) Pacer() *Pacer { return l.pace }

// Pause stops sending new batches until Resume.
func (l *Loader) Pause()  { l.paused.Store(true) }
func (l *Loader) Resume() { l.paused.Store(false) }

// ErrInvalid is a write the writer will never take: the run is
// misconfigured.
var ErrInvalid = errors.New("load: writer refused a batch as invalid")

// Run sends every batch in [From, To) not already committed, and returns
// when all have committed, or ctx ends, or a batch is invalid. A round
// starts only once every batch of the round before it has committed, so
// two batches writing the same key are never in flight together.
func (l *Loader) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	jobs := make(chan uint64, l.cfg.Workers)
	var wg sync.WaitGroup
	// done counts committed batches of the round being sent.
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	outstanding := map[uint32]int{}

	for range l.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for no := range jobs {
				err := l.send(ctx, no)
				if err != nil {
					cancel(err)
				}
				mu.Lock()
				outstanding[l.cfg.Schedule.Batch(no).Round]--
				cond.Broadcast()
				mu.Unlock()
			}
		}()
	}
	// Wake waiters when ctx ends.
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	})
	defer stop()

	s := l.cfg.Schedule
	earlierOutstanding := func(round uint32) bool {
		for r, n := range outstanding {
			if r < round && n > 0 {
				return true
			}
		}
		return false
	}
	dispatch := func(no uint64) bool {
		round := s.Batch(no).Round
		mu.Lock()
		for earlierOutstanding(round) && ctx.Err() == nil {
			cond.Wait()
		}
		mu.Unlock()
		for l.paused.Load() && ctx.Err() == nil {
			sleep(ctx, 100*time.Millisecond)
		}
		if l.pace.Take(ctx, float64(s.Bytes(s.Batch(no), 16))) != nil {
			return false
		}
		mu.Lock()
		outstanding[round]++
		mu.Unlock()
		select {
		case jobs <- no:
			return true
		case <-ctx.Done():
			mu.Lock()
			outstanding[round]--
			mu.Unlock()
			return false
		}
	}
	for no := l.cfg.From; no < l.cfg.To; no++ {
		if l.cfg.Ledger.Get(no).Revision != 0 {
			continue // committed by an earlier run
		}
		if !dispatch(no) {
			break
		}
	}
	close(jobs)
	wg.Wait()
	if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return ctx.Err()
}

// body builds batch no's request.
func (l *Loader) body(no uint64) ([]byte, workload.Batch, int64, error) {
	s := l.cfg.Schedule
	b := s.Batch(no)
	ops := make([]client.Op, 0, b.Count)
	var bytes int64
	for pos := b.Start; pos < b.Start+b.Count; pos++ {
		idx := s.Index(b.Round, pos)
		key := l.cfg.Shape.AppendKey(nil, idx)
		bytes += int64(len(key))
		if s.Deletes(b.Round, idx) {
			ops = append(ops, client.Op{Delete: true, Key: key})
			continue
		}
		v := workload.AppendValue(nil, s.Seed, idx, b.Round, s.Dist.Size(idx, b.Round))
		bytes += int64(len(v))
		ops = append(ops, client.Op{Key: key, Value: v})
	}
	body, err := client.EncodeBatch(ops)
	return body, b, bytes, err
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

// send sends batch no until it commits.
func (l *Loader) send(ctx context.Context, no uint64) error {
	body, b, bytes, err := l.body(no)
	if err != nil {
		return err
	}
	backoff := 20 * time.Millisecond
	wait := func(hint time.Duration) bool {
		d := max(hint, backoff)
		backoff = min(2*backoff, time.Second)
		return sleep(ctx, d)
	}
	for {
		att, err := l.cfg.Ledger.Begin(no)
		if err != nil {
			return err
		}
		l.C.Attempts.Add(1)
		start := time.Now()
		res := l.cfg.Writer.Write(ctx, body)
		if ctx.Err() != nil {
			// The request may have reached the writer.
			att.Unknown()
			return ctx.Err()
		}
		switch res.Outcome {
		case client.Committed:
			l.C.Commit.Observe(time.Since(start))
			return l.committed(att, b, bytes, res.Revision)
		case client.Refused:
			att.NotCommitted()
			l.C.Refused.Add(1)
			if !wait(res.RetryAfter) {
				return nil
			}
		case client.NotCommitted:
			att.NotCommitted()
			l.C.NotSaved.Add(1)
		case client.Unknown:
			l.C.Unknown.Add(1)
			rev, settled := l.resolve(ctx, res.WriteID)
			switch {
			case rev != 0:
				l.C.Commit.Observe(time.Since(start))
				return l.committed(att, b, bytes, rev)
			case settled:
				att.NotCommitted()
			default:
				att.Unknown()
				l.C.Unresolved.Add(1)
				l.warn("write outcome unknown; sending it again", "batch", no, "write_id", res.WriteID, "error", res.Err)
				if !wait(0) {
					return nil
				}
			}
		case client.Invalid:
			att.NotCommitted()
			return fmt.Errorf("%w: batch %d: %v", ErrInvalid, no, res.Err)
		}
	}
}

func (l *Loader) committed(att *ledger.Attempt, b workload.Batch, bytes int64, rev uint64) error {
	if err := att.Committed(rev); err != nil {
		return err
	}
	l.C.Batches.Add(1)
	l.C.Keys.Add(int64(b.Count))
	l.C.Bytes.Add(bytes)
	l.C.addRoundBytes(b.Round, bytes)
	return nil
}

// resolve asks what became of a write: its revision if it committed, or
// settled true if it never will. Neither means the outcome stayed unknown.
func (l *Loader) resolve(ctx context.Context, writeID string) (rev uint64, settled bool) {
	if writeID == "" {
		return 0, false
	}
	deadline := time.Now().Add(l.cfg.StatusTimeout)
	for time.Now().Before(deadline) {
		st, err := l.cfg.Writer.Status(ctx, writeID)
		if err != nil && ctx.Err() != nil {
			return 0, false
		}
		switch {
		case err != nil:
			l.cfg.Log.Debug("asking about a write failed", "write_id", writeID, "error", err)
		case st.Status == "committed":
			return st.Revision, true
		case st.Status == "not_committed":
			return 0, true
		case st.Status == "unknown":
			return 0, false
		}
		// pending, or unavailable while the writer restarts or is replaced
		if !sleep(ctx, max(st.RetryAfter, 200*time.Millisecond)) {
			return 0, false
		}
	}
	return 0, false
}

// Pacer spaces work to a rate of units a second; zero is unlimited. The
// rate can change while it runs.
type Pacer struct {
	mu   sync.Mutex
	rate float64
	next time.Time
}

func NewPacer(rate float64) *Pacer { return &Pacer{rate: rate} }

func (p *Pacer) SetRate(rate float64) {
	p.mu.Lock()
	p.rate = rate
	p.mu.Unlock()
}

func (p *Pacer) Rate() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rate
}

// Take waits for its turn to do units of work.
func (p *Pacer) Take(ctx context.Context, units float64) error {
	p.mu.Lock()
	if p.rate <= 0 {
		p.mu.Unlock()
		return ctx.Err()
	}
	now := time.Now()
	// After a stall, start again from now instead of bursting to catch up.
	if p.next.Before(now.Add(-time.Second)) {
		p.next = now
	}
	at := p.next
	p.next = p.next.Add(time.Duration(units / p.rate * float64(time.Second)))
	p.mu.Unlock()
	if d := time.Until(at); d > 0 && !sleep(ctx, d) {
		return ctx.Err()
	}
	return ctx.Err()
}

// Merge adds o's counts to c.
func (c *Counters) Merge(o *Counters) {
	for _, p := range [][2]*atomic.Int64{
		{&c.Batches, &o.Batches}, {&c.Keys, &o.Keys}, {&c.Bytes, &o.Bytes}, {&c.Attempts, &o.Attempts},
		{&c.Refused, &o.Refused}, {&c.NotSaved, &o.NotSaved}, {&c.Unknown, &o.Unknown}, {&c.Unresolved, &o.Unresolved},
	} {
		p[0].Add(p[1].Load())
	}
	c.Commit.Merge(&o.Commit)
	o.BytesByRound.Range(func(k, v any) bool {
		c.addRoundBytes(k.(uint32), v.(*atomic.Int64).Load())
		return true
	})
}

// RoundBytes returns the bytes committed by each round.
func (c *Counters) RoundBytes() map[uint32]int64 {
	out := map[uint32]int64{}
	c.BytesByRound.Range(func(k, v any) bool {
		out[k.(uint32)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}
