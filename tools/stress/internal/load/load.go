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
	// Quarantine is how long the next round waits after a write was lost
	// with its connection, before its answer: with no write ID to ask about,
	// it might still commit late, and must not land on top of the next
	// round's write of the same keys.
	Quarantine time.Duration
	// Hold, when set and true, stops new batches from being sent until it
	// is false again: the disk guard uses it.
	Hold func() bool
	Log  *slog.Logger
}

// Counters are the load's running totals.
type Counters struct {
	Batches atomic.Int64 // committed
	Keys    atomic.Int64
	Bytes   atomic.Int64 // key and value bytes committed
	// Live is the key and value bytes the database holds as far as the
	// committed batches go: what each key's newest state takes, none for a
	// deleted key. Space amplification is measured against it.
	Live         atomic.Int64
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
	warned atomic.Int64

	lostMu sync.Mutex
	lost   map[uint32]time.Time // round -> when a write of it was last lost
}

func New(cfg Config) (*Loader, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.Quarantine <= 0 {
		cfg.Quarantine = time.Minute
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.To < cfg.From {
		return nil, fmt.Errorf("load: batches [%d, %d)", cfg.From, cfg.To)
	}
	return &Loader{cfg: cfg, pace: &Pacer{rate: cfg.Rate}, lost: map[uint32]time.Time{}}, nil
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

// lostWrite records that a write of round was lost with its connection.
func (l *Loader) lostWrite(round uint32) {
	l.lostMu.Lock()
	l.lost[round] = time.Now()
	l.lostMu.Unlock()
}

// quarantineBefore returns how long round must still wait for writes lost
// in earlier rounds to have no chance left to commit.
func (l *Loader) quarantineBefore(round uint32) time.Duration {
	l.lostMu.Lock()
	defer l.lostMu.Unlock()
	var wait time.Duration
	for r, at := range l.lost {
		if r < round {
			wait = max(wait, time.Until(at.Add(l.cfg.Quarantine)))
		}
	}
	return wait
}

// ErrInvalid is a write the writer will never take: the run is
// misconfigured.
var ErrInvalid = errors.New("load: writer refused a batch as invalid")

// Run sends every batch in [From, To) not already committed, and returns
// when all have committed, or ctx ends, or a batch is invalid. A round
// starts only once every batch of the round before it has committed, and
// any write of it lost with its connection has been quarantined, so two
// writes of the same key are never in flight together.
func (l *Loader) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	jobs := make(chan uint64, l.cfg.Workers)
	var wg sync.WaitGroup
	// outstanding counts the batches of each round handed to workers and
	// not yet committed.
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
		for l.cfg.Hold != nil && l.cfg.Hold() && ctx.Err() == nil {
			sleep(ctx, time.Second)
		}
		if d := l.quarantineBefore(round); d > 0 {
			l.cfg.Log.Info("waiting for lost writes of the last round to expire", "round", round, "wait", d.Round(time.Second))
			if !sleep(ctx, d) {
				return false
			}
		}
		if l.pace.rate > 0 && l.pace.Take(ctx, float64(s.Bytes(s.Batch(no), 16))) != nil {
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
		if s.Written(s.Batch(no)) == 0 {
			continue // its positions hold no key its round writes
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
		if !s.Writes(b.Round, idx) {
			continue
		}
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
			if !wait(res.RetryAfter) {
				return nil
			}
		case client.Unknown:
			l.C.Unknown.Add(1)
			rev, settled := l.resolve(ctx, res.WriteID)
			switch {
			case rev != 0:
				l.C.Commit.Observe(time.Since(start))
				return l.committed(att, b, bytes, rev)
			case settled:
				att.NotCommitted()
				if !wait(0) {
					return nil
				}
			default:
				att.Unknown()
				l.lostWrite(b.Round)
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
	l.C.Keys.Add(int64(l.cfg.Schedule.Written(b)))
	l.C.Bytes.Add(bytes)
	l.C.Live.Add(l.liveDelta(b))
	l.C.addRoundBytes(b.Round, bytes)
	return nil
}

// liveDelta is how batch b changes the bytes the database holds: each key it
// writes replaces what the last round to write it left.
func (l *Loader) liveDelta(b workload.Batch) int64 {
	s := l.cfg.Schedule
	held := func(round uint32, idx uint64) int64 {
		if st := s.After(round, idx); st.Present {
			return int64(16 + s.Dist.Size(idx, round))
		}
		return 0
	}
	var d int64
	for pos := b.Start; pos < b.Start+b.Count; pos++ {
		idx := s.Index(b.Round, pos)
		if !s.Writes(b.Round, idx) {
			continue
		}
		d += held(b.Round, idx)
		if b.Round > 0 {
			if prev, ok := s.Last(b.Round-1, idx); ok {
				d -= held(prev, idx)
			}
		}
	}
	return d
}

// resolve asks what became of a write: its revision if it committed, or
// settled true if it never will. Neither means the outcome cannot be
// learned: there is no write ID, or the writer has forgotten it. While the
// writer says it is pending, or can't answer while it restarts or is
// replaced, it is asked again: the writer holding the database settles
// every write in the end, and giving up early would let a late commit of
// this attempt land after the next round's write of the same keys.
func (l *Loader) resolve(ctx context.Context, writeID string) (rev uint64, settled bool) {
	if writeID == "" {
		return 0, false
	}
	asked := time.Now()
	for {
		st, err := l.cfg.Writer.Status(ctx, writeID)
		if ctx.Err() != nil {
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
		if time.Since(asked) > time.Minute {
			l.warn("a write is still unsettled; asking again", "write_id", writeID, "status", st.Status, "error", err)
		}
		if !sleep(ctx, max(st.RetryAfter, 200*time.Millisecond)) {
			return 0, false
		}
	}
}

// Pacer spaces work to a rate of units a second; zero is unlimited.
type Pacer struct {
	rate float64
	mu   sync.Mutex
	next time.Time
}

func NewPacer(rate float64) *Pacer { return &Pacer{rate: rate} }

// Take waits for its turn to do units of work.
func (p *Pacer) Take(ctx context.Context, units float64) error {
	if p.rate <= 0 {
		return ctx.Err()
	}
	p.mu.Lock()
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
		{&c.Batches, &o.Batches}, {&c.Keys, &o.Keys}, {&c.Bytes, &o.Bytes}, {&c.Live, &o.Live}, {&c.Attempts, &o.Attempts},
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
