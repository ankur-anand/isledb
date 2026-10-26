package verify

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

// Failure kinds.
const (
	Missing     = "missing"     // a key that must be there was not
	Stale       = "stale"       // an older version than the newest at that revision
	Future      = "future"      // a version not yet written at that revision
	Resurrected = "resurrected" // a key deleted at that revision was there
	Corrupt     = "corrupt"     // a value that fails its own checks
	Unexpected  = "unexpected"  // a key no index has
	Order       = "order"       // scan results out of order or repeated
	Unsettled   = "unsettled"   // a read the ledger never settled, at the end
)

var kinds = []string{Missing, Stale, Future, Resurrected, Corrupt, Unexpected, Order, Unsettled}

// Item is one key and value of a scan page.
type Item struct {
	Key, Value []byte
}

// Failure is one wrong answer, with what is needed to chase it.
type Failure struct {
	Time     time.Time   `json:"time"`
	Kind     string      `json:"kind"`
	Op       string      `json:"op"`
	Index    uint64      `json:"index"`
	Key      string      `json:"key"`
	Revision uint64      `json:"revision"`
	Observed string      `json:"observed"`
	Allowed  []string    `json:"allowed,omitempty"`
	Detail   string      `json:"detail,omitempty"`
	Batches  []BatchInfo `json:"batches,omitempty"`
}

// Stats are the verifier's counts.
type Stats struct {
	Checked   int64            `json:"checked"`
	Queued    int64            `json:"queued"`
	Pending   int64            `json:"pending"`
	Truncated int64            `json:"truncated_scans"`
	Failures  map[string]int64 `json:"failures"`
}

func (s Stats) Failed() int64 {
	var n int64
	for _, c := range s.Failures {
		n += c
	}
	return n
}

// obs is one key's observed state, its value already checked.
type obs struct {
	idx uint64
	st  workload.State
}

type pending struct {
	rev  uint64
	op   string
	page bool
	// A get: one observation. A page: the observations in key order, the
	// bound the scan started at, and the last key it covers.
	items        []obs
	lo, hi, last []byte
	more         bool
}

// Verifier checks reads. Its methods are safe for concurrent use.
type Verifier struct {
	model *Model
	shape workload.Shape
	// MaxWalk bounds the indexes one page check enumerates; a page whose
	// range holds more is checked only for what it returned.
	MaxWalk int

	checked, queued, truncated atomic.Int64
	counts                     map[string]*atomic.Int64

	qmu   sync.Mutex
	queue []pending

	logMu sync.Mutex
	log   *json.Encoder
	// OnFailure, if set, is called for each failure after it is logged.
	OnFailure func(Failure)
}

// New returns a verifier writing every failure to log as a JSON line.
func New(model *Model, shape workload.Shape, log io.Writer) *Verifier {
	v := &Verifier{model: model, shape: shape, MaxWalk: 1 << 18, counts: map[string]*atomic.Int64{}}
	for _, k := range kinds {
		v.counts[k] = &atomic.Int64{}
	}
	if log != nil {
		v.log = json.NewEncoder(log)
	}
	return v
}

func (v *Verifier) Stats() Stats {
	v.qmu.Lock()
	n := int64(len(v.queue))
	v.qmu.Unlock()
	s := Stats{Checked: v.checked.Load(), Queued: v.queued.Load(), Pending: n, Truncated: v.truncated.Load(), Failures: map[string]int64{}}
	for k, c := range v.counts {
		s.Failures[k] = c.Load()
	}
	return s
}

func (v *Verifier) fail(f Failure) {
	f.Time = time.Now().UTC()
	f.Batches = v.model.Batches(f.Index)
	v.counts[f.Kind].Add(1)
	if v.log != nil {
		v.logMu.Lock()
		_ = v.log.Encode(f)
		v.logMu.Unlock()
	}
	if v.OnFailure != nil {
		v.OnFailure(f)
	}
}

func (v *Verifier) key(idx uint64) string { return hex.EncodeToString(v.shape.AppendKey(nil, idx)) }

func names(states []workload.State) []string {
	out := make([]string, len(states))
	for i, s := range states {
		out[i] = s.String()
	}
	return out
}

// observe checks a value's own integrity and returns the state it shows.
func (v *Verifier) observe(op string, idx, rev uint64, value []byte, found bool) (workload.State, bool) {
	if !found {
		return workload.State{}, true
	}
	version, err := workload.CheckValue(v.model.Schedule.Dist, idx, value)
	if err != nil {
		v.fail(Failure{Kind: Corrupt, Op: op, Index: idx, Key: v.key(idx), Revision: rev, Observed: fmt.Sprintf("%d bytes", len(value)), Detail: err.Error()})
		return workload.State{}, false
	}
	return workload.State{Present: true, Version: version}, true
}

// Get checks the answer to a read of idx at rev.
func (v *Verifier) Get(idx, rev uint64, value []byte, found bool) {
	st, ok := v.observe("get", idx, rev, value, found)
	if !ok {
		return
	}
	v.settle(pending{rev: rev, op: "get", items: []obs{{idx, st}}})
}

// Page checks one page of a scan of [lo, hi), hi nil for no bound,
// answered at rev: its items in key order, and whether more pages follow.
// Every key in [lo, last returned] that holds a value at rev must be there;
// with no more pages, every key in [lo, hi).
func (v *Verifier) Page(lo, hi []byte, rev uint64, items []Item, more bool) {
	p := pending{rev: rev, op: "scan", page: true, lo: bytes.Clone(lo), hi: bytes.Clone(hi), more: more}
	var prev []byte
	for _, it := range items {
		if prev != nil && bytes.Compare(prev, it.Key) >= 0 {
			v.fail(Failure{Kind: Order, Op: "scan", Key: hex.EncodeToString(it.Key), Revision: rev,
				Detail: "after " + hex.EncodeToString(prev)})
			return
		}
		if bytes.Compare(it.Key, lo) < 0 || (hi != nil && bytes.Compare(it.Key, hi) >= 0) {
			v.fail(Failure{Kind: Order, Op: "scan", Key: hex.EncodeToString(it.Key), Revision: rev,
				Detail: fmt.Sprintf("outside the scan's range [%x, %x)", lo, hi)})
			return
		}
		prev = it.Key
		idx, ok := v.shape.Index(it.Key)
		if !ok {
			v.fail(Failure{Kind: Unexpected, Op: "scan", Key: hex.EncodeToString(it.Key), Revision: rev})
			return
		}
		st, ok := v.observe("scan", idx, rev, it.Value, true)
		if !ok {
			return
		}
		p.items = append(p.items, obs{idx, st})
	}
	if len(items) > 0 {
		p.last = bytes.Clone(items[len(items)-1].Key)
	}
	v.settle(p)
}

// settle checks p now if its revision is settled, and queues it otherwise.
func (v *Verifier) settle(p pending) {
	if p.rev > v.model.Ledger.Settled() {
		v.queued.Add(1)
		v.qmu.Lock()
		v.queue = append(v.queue, p)
		v.qmu.Unlock()
		return
	}
	v.check(p)
}

func (v *Verifier) check(p pending) {
	v.checked.Add(1)
	if !p.page {
		o := p.items[0]
		allowed := v.model.expect(o.idx, p.rev, nil)
		if !slices.Contains(allowed, o.st) {
			v.fail(Failure{Kind: v.classify(o.idx, p.rev, o.st, allowed), Op: p.op, Index: o.idx, Key: v.key(o.idx), Revision: p.rev,
				Observed: o.st.String(), Allowed: names(allowed)})
		}
		return
	}
	hi := p.hi
	if p.more {
		if p.last == nil {
			return // an empty page with more to come covers nothing
		}
		hi = append(bytes.Clone(p.last), 0)
	}
	next, walked := 0, 0
	var allowed []workload.State
	v.shape.Walk(p.lo, hi, func(idx uint64) bool {
		if walked++; walked > v.MaxWalk {
			v.truncated.Add(1)
			return false
		}
		allowed = v.model.expect(idx, p.rev, allowed[:0])
		got := next < len(p.items) && p.items[next].idx == idx
		if got {
			o := p.items[next]
			next++
			if !slices.Contains(allowed, o.st) {
				v.fail(Failure{Kind: v.classify(idx, p.rev, o.st, allowed), Op: p.op, Index: idx, Key: v.key(idx), Revision: p.rev,
					Observed: o.st.String(), Allowed: names(allowed)})
			}
			return true
		}
		if !slices.Contains(allowed, workload.State{}) {
			v.fail(Failure{Kind: Missing, Op: p.op, Index: idx, Key: v.key(idx), Revision: p.rev,
				Observed: "absent", Allowed: names(allowed), Detail: "skipped by a scan"})
		}
		return true
	})
	if walked <= v.MaxWalk && next < len(p.items) {
		// Every returned key is a workload key in range, so the walk passes
		// it; reaching here means the page's order and the shape's disagree.
		o := p.items[next]
		v.fail(Failure{Kind: Order, Op: p.op, Index: o.idx, Key: v.key(o.idx), Revision: p.rev,
			Detail: "returned out of the keyspace's order"})
	}
}

// classify names how an observed state differs from the allowed ones.
func (v *Verifier) classify(idx, rev uint64, got workload.State, allowed []workload.State) string {
	if !got.Present {
		return Missing
	}
	var newest workload.State
	for _, a := range allowed {
		if a.Present && a.Version >= newest.Version {
			newest = a
		}
	}
	switch {
	case !newest.Present && v.writtenBy(idx, rev):
		return Resurrected
	case newest.Present && got.Version < newest.Version:
		return Stale
	default:
		return Future
	}
}

// writtenBy reports whether some batch wrote idx at or below rev: a key
// that must be absent then was deleted, not yet to be inserted.
func (v *Verifier) writtenBy(idx, rev uint64) bool {
	for _, b := range v.model.Batches(idx) {
		if b.Revision != 0 && b.Revision <= rev {
			return true
		}
	}
	return false
}

// Drain checks queued reads whose revisions have settled. It returns how
// many remain.
func (v *Verifier) Drain() int {
	settled := v.model.Ledger.Settled()
	v.qmu.Lock()
	var ready []pending
	keep := v.queue[:0]
	for _, p := range v.queue {
		if p.rev <= settled {
			ready = append(ready, p)
		} else {
			keep = append(keep, p)
		}
	}
	v.queue = keep
	n := len(keep)
	v.qmu.Unlock()
	for _, p := range ready {
		v.check(p)
	}
	return n
}

// Run drains the queue every interval until ctx ends.
func (v *Verifier) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			v.Drain()
		}
	}
}

// Finish drains what has settled and reports every read still queued as
// unsettled: the load has stopped, so it never will be.
func (v *Verifier) Finish() {
	v.Drain()
	v.qmu.Lock()
	left := v.queue
	v.queue = nil
	v.qmu.Unlock()
	for _, p := range left {
		f := Failure{Kind: Unsettled, Op: p.op, Revision: p.rev,
			Detail: fmt.Sprintf("the ledger settled only up to revision %d", v.model.Ledger.Settled())}
		if len(p.items) > 0 {
			f.Index, f.Key = p.items[0].idx, v.key(p.items[0].idx)
		}
		v.fail(f)
	}
}

// Summary is a one-line account of the counts, for logs.
func (s Stats) Summary() string {
	var parts []string
	for _, k := range kinds {
		if n := s.Failures[k]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", k, n))
		}
	}
	if len(parts) == 0 {
		parts = append(parts, "no failures")
	}
	return fmt.Sprintf("checked %d, pending %d, %s", s.Checked, s.Pending, strings.Join(parts, " "))
}
