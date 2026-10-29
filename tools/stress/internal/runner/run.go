package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/client"
	"github.com/ankur-anand/isledb/tools/stress/internal/ledger"
	"github.com/ankur-anand/isledb/tools/stress/internal/load"
	"github.com/ankur-anand/isledb/tools/stress/internal/read"
	"github.com/ankur-anand/isledb/tools/stress/internal/stats"
	"github.com/ankur-anand/isledb/tools/stress/internal/verify"
	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

// Options say what to run and where.
type Options struct {
	Spec *Spec
	// Isledb is the version under test: a binary, a git ref, or "." for the
	// working tree.
	Isledb string
	// Repo is the isledb repository, for git refs and for building latproxy.
	Repo string
	// Latproxy is the latency proxy binary; empty builds it from Repo.
	Latproxy string
	// Out is the directory each run's own directory goes under.
	Out string
	// Fetch updates origin's branches in Repo first, so origin/main is
	// current, and takes tags at the commit origin has them.
	Fetch bool
	// MetricsListen is where the runner serves /metrics for Prometheus: a
	// loopback address, or empty for none.
	MetricsListen string
	// TargetsDir is where the run writes its Prometheus scrape targets while
	// it runs; empty is Out/.prometheus.
	TargetsDir string
	Log        *slog.Logger
}

// RunInfo is run.json: everything that defines the run.
type RunInfo struct {
	Name      string    `json:"name"`
	Dir       string    `json:"dir"`
	Prefix    string    `json:"prefix"`
	Started   time.Time `json:"started"`
	Isledb    Binary    `json:"isledb"`
	Host      string    `json:"host"`
	CPUs      int       `json:"cpus"`
	Keys      uint64    `json:"keys"`
	MaxKeys   uint64    `json:"max_keys,omitempty"` // with inserts after the insert round
	Batches   uint64    `json:"batches_per_round"`
	MeanValue float64   `json:"mean_value_bytes"`
	Spec      *Spec     `json:"spec"`
}

// Sample is one row of series.jsonl.
type Sample struct {
	Time    time.Time `json:"time"`
	Elapsed float64   `json:"elapsed_s"`
	Phase   string    `json:"phase"`

	Load    LoadSample              `json:"load"`
	Readers map[string]ReaderSample `json:"readers"`
	Verify  verify.Stats            `json:"verify"`
	Ledger  LedgerSample            `json:"ledger"`

	Storage map[string]ProxyStats         `json:"storage,omitempty"`
	Amp     *Amplification                `json:"amplification,omitempty"`
	Isledb  map[string]map[string]float64 `json:"isledb,omitempty"`

	// Bucket is the latest listing of the database's prefix, and Space the
	// space amplification it shows: SST bytes over the bytes the database
	// holds.
	Bucket *BucketUsage `json:"bucket,omitempty"`
	Space  float64      `json:"space_amplification,omitempty"`
	Disk   *DiskSample  `json:"disk,omitempty"`
}

type DiskSample struct {
	Path    string  `json:"path"`
	UsedPct float64 `json:"used_pct"`
	Free    int64   `json:"free_bytes"`
	// Paused is whether the load is held because the disk is too full, and
	// PausedSeconds how long it has been held in all.
	Paused        bool    `json:"paused"`
	PausedSeconds float64 `json:"paused_seconds"`
}

type LoadSample struct {
	Bytes      int64            `json:"bytes"`
	Live       int64            `json:"live_bytes"`
	Keys       int64            `json:"keys"`
	Batches    int64            `json:"batches"`
	Attempts   int64            `json:"attempts"`
	Refused    int64            `json:"refused"`
	NotSaved   int64            `json:"not_committed"`
	Unknown    int64            `json:"unknown"`
	Unresolved int64            `json:"unresolved"`
	ByRound    map[uint32]int64 `json:"bytes_by_round"`
	Commit     stats.Latency    `json:"commit"`
}

type ReaderSample struct {
	Gets      int64 `json:"gets"`
	Scans     int64 `json:"scans"`
	ScanItems int64 `json:"scan_items"`
	Retries   int64 `json:"retries"`
	Errors    int64 `json:"errors"`
	// Offered is the reads the rate made due, Dropped those never sent for
	// being due too long ago, Late those started late; Busy the workers
	// reading when sampled.
	Offered int64 `json:"offered,omitempty"`
	Dropped int64 `json:"dropped,omitempty"`
	Late    int64 `json:"late,omitempty"`
	Busy    int64 `json:"busy_workers"`
	// Get and Scan are service times, from sending to the answer;
	// GetResponse and ScanResponse response times, from when each was due.
	Get          stats.Latency `json:"get"`
	Scan         stats.Latency `json:"scan"`
	GetResponse  stats.Latency `json:"get_response"`
	ScanResponse stats.Latency `json:"scan_response"`
}

type LedgerSample struct {
	Settled     uint64 `json:"settled_revision"`
	MaxRevision uint64 `json:"max_revision"`
	MaxSent     int64  `json:"max_batch_sent"`
}

// Amplification is what storage saw against what the load wrote.
type Amplification struct {
	// Write is every SST byte uploaded, by the writer and maintenance, per
	// byte written: RocksDB's definition, the first save of each byte
	// included.
	Write float64 `json:"write"`
	// Writer is the writer's own SST uploads per byte written: its saves and
	// the files it combines them into.
	Writer float64 `json:"writer"`
	// Compaction is maintenance's SST uploads per byte written.
	Compaction float64 `json:"compaction"`
}

func amplification(storage map[string]ProxyStats, written int64) *Amplification {
	w, m := storage["writer"], storage["maintenance"]
	if w == nil || m == nil || written == 0 {
		return nil
	}
	ws, ms := float64(w.uploaded("sst")), float64(m.uploaded("sst"))
	b := float64(written)
	return &Amplification{Write: (ws + ms) / b, Writer: ws / b, Compaction: ms / b}
}

// Summary is summary.json.
type Summary struct {
	Run         RunInfo           `json:"run"`
	Finished    time.Time         `json:"finished"`
	Phases      []PhaseResult     `json:"phases"`
	Checkpoints map[string]Sample `json:"checkpoints"`
	Settled     *Sample           `json:"settled,omitempty"`
	Final       Sample            `json:"final"`
	Failed      int64             `json:"verification_failures"`
	Error       string            `json:"error,omitempty"`
}

type PhaseResult struct {
	Name     string    `json:"name"`
	Started  time.Time `json:"started"`
	Seconds  float64   `json:"seconds"`
	Bytes    int64     `json:"bytes_written"`
	Error    string    `json:"error,omitempty"`
	Scanned  int64     `json:"keys_scanned,omitempty"`
	Settling string    `json:"settling,omitempty"`
}

type runState struct {
	opts     Options
	spec     *Spec
	info     RunInfo
	log      *slog.Logger
	sched    *workload.Schedule
	shape    workload.Shape
	ledger   *ledger.Ledger
	verifier *verify.Verifier
	cluster  *Cluster
	writer   *client.Client
	readers  []*client.Client
	loader   *load.Loader // the load of the phase running, if any
	groups   map[string]*read.Reader
	bins     map[string]Binary // by the version a restart names
	lister   lister
	usage    atomic.Pointer[BucketUsage]

	// The disk guard's state: whether it holds the load, since when, and
	// how long it held it before.
	diskHeld   atomic.Bool
	diskMu     sync.Mutex
	heldSince  time.Time
	heldBefore time.Duration
	lastDisk   *DiskSample
	statDisk   func(string) (size, free uint64, err error)
	diskEvery  time.Duration

	mu       sync.Mutex
	phase    string
	nextRnd  uint32
	counters *load.Counters // accumulates across phases
	started  time.Time
	series   *os.File
	summary  Summary
}

// Run runs the spec and returns its summary. The error is the run's
// failure: a role that died, a misconfiguration, or verification failures.
func Run(ctx context.Context, opts Options) (*Summary, error) {
	spec := opts.Spec
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	if opts.MetricsListen != "" {
		if err := CheckLoopback(opts.MetricsListen); err != nil {
			return nil, err
		}
	}
	if opts.TargetsDir == "" {
		opts.TargetsDir = filepath.Join(opts.Out, ".prometheus")
	}
	cache := filepath.Join(opts.Out, ".bin")
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return nil, err
	}
	if opts.Fetch {
		log.Info("fetching", "repo", opts.Repo)
		if err := FetchOrigin(ctx, opts.Repo); err != nil {
			return nil, err
		}
	}
	bin, err := ResolveIsledb(ctx, opts.Isledb, opts.Repo, cache, opts.Fetch)
	if err != nil {
		return nil, err
	}
	log.Info("isledb", "asked", opts.Isledb, "version", bin.Version, "commit", bin.Commit, "reports", bin.Reported, "binary", bin.Path)
	// Every version a role restarts at is built now, so a bad ref fails the
	// run before it starts, not hours in.
	bins := map[string]Binary{}
	for _, v := range spec.Versions() {
		if bins[v], err = ResolveIsledb(ctx, v, opts.Repo, cache, opts.Fetch); err != nil {
			return nil, fmt.Errorf("restart version %s: %w", v, err)
		}
	}
	latproxy := opts.Latproxy
	if latproxy == "" && spec.Storage.Latency != nil {
		latproxy = filepath.Join(cache, "latproxy")
		if err := run(ctx, opts.Repo, "go", "build", "-o", latproxy, "./tools/latproxy"); err != nil {
			return nil, err
		}
	}

	dist, err := workload.ParseSizeDist(spec.Workload.Values)
	if err != nil {
		return nil, err
	}
	n := spec.Workload.Keys
	if n == 0 {
		n = workload.KeysForBytes(dist, 16, int64(spec.Workload.Target))
	}
	sched, shape, err := newSchedule(spec, n, dist)
	if err != nil {
		return nil, err
	}

	start := time.Now().UTC()
	name := fmt.Sprintf("%s-%s-%s-%s", spec.Name, spec.SizeLabel(), safe(bin.Version), start.Format("20060102T150405Z"))
	dir := filepath.Join(opts.Out, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	host, _ := os.Hostname()
	st := &runState{opts: opts, spec: spec, log: log, sched: sched, shape: shape, started: start,
		counters: &load.Counters{}, groups: map[string]*read.Reader{}, bins: bins}
	st.info = RunInfo{Name: name, Dir: dir, Prefix: "stress/" + name, Started: start, Isledb: bin, Host: host,
		CPUs: runtime.NumCPU(), Keys: n, Batches: sched.BatchesPerRound(), MeanValue: dist.Mean(), Spec: spec}
	if m := sched.MaxKeys(); m > n {
		st.info.MaxKeys = m
	}
	st.summary = Summary{Run: st.info, Checkpoints: map[string]Sample{}}
	if err := writeJSON(filepath.Join(dir, "run.json"), st.info); err != nil {
		return nil, err
	}
	log.Info("run", "dir", dir, "isledb", bin.Version, "keys", n, "batches_per_round", sched.BatchesPerRound())

	if st.ledger, err = ledger.Open(filepath.Join(dir, "ledger")); err != nil {
		return nil, err
	}
	defer func() { _ = st.ledger.Close() }()
	failures, err := os.Create(filepath.Join(dir, "failures.jsonl"))
	if err != nil {
		return nil, err
	}
	defer func() { _ = failures.Close() }()
	st.verifier = verify.New(&verify.Model{Schedule: sched, Ledger: st.ledger}, shape, failures)
	var logged atomic.Int64
	st.verifier.OnFailure = func(f verify.Failure) {
		if logged.Add(1) <= 20 {
			log.Error("verification failed", "kind", f.Kind, "op", f.Op, "key", f.Key, "revision", f.Revision, "observed", f.Observed, "allowed", f.Allowed)
		}
	}
	if st.series, err = os.Create(filepath.Join(dir, "series.jsonl")); err != nil {
		return nil, err
	}
	defer func() { _ = st.series.Close() }()

	if u, err := url.Parse(spec.Storage.BucketURL); err == nil && u.Scheme == "file" {
		// A directory bucket must exist before the writer opens it.
		if err := os.MkdirAll(u.Path, 0o755); err != nil {
			return nil, err
		}
	}
	if l, why := newLister(spec.Storage); l != nil {
		st.lister = l
	} else {
		log.Warn("the bucket won't be listed: no space amplification", "reason", why)
	}
	st.cluster, err = newCluster(spec, dir, st.info.Prefix, latproxy, bin, log)
	if err != nil {
		return nil, err
	}
	defer st.cluster.Stop()
	runErr := st.run(ctx)
	st.finish(runErr)
	if runErr == nil && st.summary.Failed > 0 {
		runErr = fmt.Errorf("%d reads failed verification; see %s", st.summary.Failed, failures.Name())
	}
	return &st.summary, runErr
}

func safe(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9', r == '.', r == '-':
			return r
		}
		return '_'
	}, s)
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func (st *runState) setPhase(name string) {
	st.mu.Lock()
	st.phase = name
	st.mu.Unlock()
	st.log.Info("phase", "name", name)
}

func (st *runState) run(ctx context.Context) error {
	if err := st.cluster.Start(ctx); err != nil {
		return err
	}
	metricsCtx, stopMetrics := context.WithCancel(context.Background())
	defer stopMetrics()
	metricsAddr := ""
	if a := st.opts.MetricsListen; a != "" {
		var err error
		if metricsAddr, err = st.serveMetrics(metricsCtx, a); err != nil {
			return err
		}
	}
	targets, err := st.writeTargets(st.opts.TargetsDir, metricsAddr)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(targets) }()
	st.writer = client.New("http://"+st.cluster.Roles["writer"].Listen, "")
	for _, u := range st.cluster.ReaderURLs() {
		st.readers = append(st.readers, client.New(u, ""))
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// A role that dies on its own ends the run.
	watching := make(chan struct{})
	restarting := map[string]bool{}
	var restartMu sync.Mutex
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-watching:
				return
			case <-t.C:
			}
			restartMu.Lock()
			name, err := st.cluster.Exited()
			if err != nil && !restarting[name] {
				cancel(err)
			}
			restartMu.Unlock()
		}
	}()
	defer close(watching)

	go st.verifier.Run(ctx, 250*time.Millisecond)
	go st.sample(ctx)
	if st.spec.Storage.Disk != nil {
		go st.guardDisk(ctx)
	}
	if st.lister != nil {
		go st.listBucket(ctx)
	}

	for _, rs := range st.spec.Readers {
		r, err := read.New(read.Config{Name: rs.Name, Schedule: st.sched, Shape: st.shape, Ledger: st.ledger, Verifier: st.verifier,
			Readers: st.readers, Rate: rs.Rate, MaxLag: rs.MaxLag.D(), Workers: rs.Workers,
			ScanRatio: rs.ScanRatio, ScanMin: rs.ScanMin, ScanLimit: rs.ScanLimit,
			Dist: rs.Dist, ZipfS: rs.ZipfS, LatestWindow: rs.LatestWindow, Log: st.log})
		if err != nil {
			return err
		}
		st.groups[rs.Name] = r
	}
	// The readers read through every phase but a verify, which reads
	// everything itself; they start again after it.
	var readers sync.WaitGroup
	stopReads := func() {}
	startReaders := func() {
		readCtx, cancel := context.WithCancel(ctx)
		stopReads = cancel
		for _, r := range st.groups {
			readers.Add(1)
			go func() {
				defer readers.Done()
				r.Run(readCtx)
			}()
		}
	}
	stopReaders := func() {
		stopReads()
		readers.Wait()
	}
	startReaders()
	defer stopReaders()

	restart := func(ev RestartEvent) error {
		var bin *Binary
		if ev.Isledb != "" {
			b := st.bins[ev.Isledb]
			bin = &b
		}
		restartMu.Lock()
		restarting[ev.Role] = true
		restartMu.Unlock()
		defer func() {
			restartMu.Lock()
			delete(restarting, ev.Role)
			restartMu.Unlock()
		}()
		return st.cluster.Restart(ctx, ev.Role, ev.Kill, ev.Down.D(), bin)
	}

	for i, p := range st.spec.Phases {
		var res PhaseResult
		var err error
		started := time.Now()
		before := st.written()
		switch {
		case p.Load != nil:
			res.Name = "load"
			st.setPhase(res.Name)
			err = st.write(ctx, p.Load, 1, restart)
		case p.Update != nil:
			rounds := max(p.Update.Rounds, 1)
			res.Name = fmt.Sprintf("update x%d", rounds)
			st.setPhase(res.Name)
			if st.nextRnd == 0 {
				err = errors.New("update before load: the keys must be inserted first")
				break
			}
			err = st.write(ctx, p.Update, rounds, restart)
		case p.Settle != nil:
			res.Name = "settle"
			st.setPhase(res.Name)
			res.Settling, err = st.settle(ctx, p.Settle)
			s := st.takeSample(ctx)
			st.summary.Settled = &s
		case p.Verify != nil:
			res.Name = "verify"
			st.setPhase(res.Name)
			stopReaders()
			res.Scanned, err = st.verifyAll(ctx, p.Verify)
			startReaders()
		case p.Restart != nil:
			res.Name = "restart " + p.Restart.Role
			st.setPhase(res.Name)
			err = restart(*p.Restart)
		}
		res.Started, res.Seconds = started.UTC(), time.Since(started).Seconds()
		res.Bytes = st.written() - before
		if c := context.Cause(ctx); err == nil && c != nil {
			err = c
		}
		if err != nil {
			res.Error = err.Error()
		}
		st.summary.Phases = append(st.summary.Phases, res)
		if err != nil {
			return fmt.Errorf("phase %d (%s): %w", i+1, res.Name, err)
		}
	}
	return nil
}

// write runs rounds rounds from the next one, with the phase's events.
func (st *runState) write(ctx context.Context, p *WritePhase, rounds uint32, restart func(RestartEvent) error) error {
	rate := float64(st.spec.Workload.Rate)
	if p.Rate != nil {
		rate = float64(*p.Rate)
	}
	l, err := load.New(load.Config{Schedule: st.sched, Shape: st.shape, Ledger: st.ledger, Writer: st.writer,
		From: st.sched.FirstBatch(st.nextRnd), To: st.sched.FirstBatch(st.nextRnd + rounds),
		Rate: rate, Workers: st.spec.Workload.Workers, Log: st.log,
		Hold: st.diskHeld.Load})
	if err != nil {
		return err
	}
	st.mu.Lock()
	st.loader = l
	st.mu.Unlock()
	defer func() {
		st.mu.Lock()
		st.counters.Merge(&l.C)
		st.loader = nil
		st.mu.Unlock()
	}()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var events sync.WaitGroup
	for _, ev := range p.Events {
		events.Add(1)
		go func() {
			defer events.Done()
			select {
			case <-ctx.Done():
				return
			case <-time.After(ev.After.D()):
			}
			if err := restart(ev); err != nil {
				cancel(fmt.Errorf("restart %s: %w", ev.Role, err))
			}
		}()
	}
	for _, f := range p.Faults {
		events.Add(1)
		go func() {
			defer events.Done()
			select {
			case <-ctx.Done():
				return
			case <-time.After(f.After.D()):
			}
			if err := st.setFaults(ctx, f.Proxy, f.Set); err != nil {
				cancel(err)
			}
		}()
	}
	if len(p.Faults) > 0 {
		defer func() {
			for _, f := range p.Faults {
				if err := st.setFaults(context.Background(), f.Proxy, Faults{}); err != nil {
					st.log.Warn("clearing faults", "proxy", f.Proxy, "error", err)
				}
			}
		}()
	}
	err = l.Run(ctx)
	if c := context.Cause(ctx); c != nil && !errors.Is(c, context.Canceled) {
		err = c
	}
	cancel(nil)
	events.Wait()
	if err == nil {
		st.nextRnd += rounds
	}
	return err
}

// setFaults sets a proxy's injected faults; the zero Faults clears them.
func (st *runState) setFaults(ctx context.Context, proxy string, set Faults) error {
	p := st.cluster.Proxies[proxy]
	if p == nil {
		return fmt.Errorf("no %s proxy", proxy)
	}
	b, err := json.Marshal(set)
	if err != nil {
		return err
	}
	st.log.Info("faults", "proxy", proxy, "set", string(b))
	return p.SetFaults(ctx, string(b))
}

// settle waits until maintenance has uploaded nothing for Quiet, at most
// Max. Without proxies to count uploads, it waits Quiet.
func (st *runState) settle(ctx context.Context, p *SettlePhase) (string, error) {
	quiet, limit := p.Quiet.D(), p.Max.D()
	if quiet <= 0 {
		quiet = 2 * time.Minute
	}
	if limit <= 0 {
		limit = time.Hour
	}
	proxy := st.cluster.Proxies["maintenance"]
	if proxy == nil {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(quiet):
		}
		return "waited without upload counts", nil
	}
	deadline := time.Now().Add(limit)
	var last int64 = -1
	since := time.Now()
	for {
		s, err := proxy.Stats(ctx)
		if err != nil {
			return "", err
		}
		total := s.uploaded("sst") + s.uploaded("manifest")
		if total != last {
			last, since = total, time.Now()
		}
		if time.Since(since) >= quiet {
			return fmt.Sprintf("quiet for %s", quiet), nil
		}
		if time.Now().After(deadline) {
			return fmt.Sprintf("still uploading after %s", limit), nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// caughtUp waits until every reader answers at or above the newest revision
// the load committed at, so a verify checks everything written, not what
// a reader's view held a moment ago.
func (st *runState) caughtUp(ctx context.Context, timeout time.Duration) error {
	want := st.ledger.MaxRevision()
	key := st.shape.AppendKey(nil, 0)
	deadline := time.Now().Add(timeout)
	for _, r := range st.readers {
		for {
			_, _, rev, err := r.Get(ctx, key)
			if err == nil && rev >= want {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("reader %s still answers at revision %d after %s, below %d (error %v)", r.Base, rev, timeout, want, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

// verifyAll waits until the readers have caught up, then scans everything.
func (st *runState) verifyAll(ctx context.Context, p *VerifyPhase) (int64, error) {
	parts, limit := p.Parts, p.Limit
	if parts <= 0 {
		parts = 16
	}
	if limit <= 0 {
		limit = 1000
	}
	if err := st.caughtUp(ctx, 2*time.Minute); err != nil {
		return 0, err
	}
	n, err := read.FullScan(ctx, st.readers, st.verifier, parts, limit)
	if err != nil {
		return n, err
	}
	st.log.Info("full scan", "keys", n)
	return n, nil
}

// written returns the key and value bytes committed so far.
func (st *runState) written() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	n := st.counters.Bytes.Load()
	if st.loader != nil {
		n += st.loader.C.Bytes.Load()
	}
	return n
}

// totals returns the load's counts over every phase so far, and the phase.
func (st *runState) totals() (*load.Counters, string) {
	total := &load.Counters{}
	st.mu.Lock()
	defer st.mu.Unlock()
	total.Merge(st.counters)
	if st.loader != nil {
		total.Merge(&st.loader.C)
	}
	return total, st.phase
}

func (st *runState) takeSample(ctx context.Context) Sample { return st.sampleOf(ctx, true) }

// sampleOf measures everything now; with roles false, it leaves out the
// scrape of each role's metrics, which Prometheus does itself.
func (st *runState) sampleOf(ctx context.Context, roles bool) Sample {
	total, phase := st.totals()
	now := time.Now().UTC()
	s := Sample{Time: now, Elapsed: now.Sub(st.started).Seconds(), Phase: phase, Readers: map[string]ReaderSample{},
		Verify: st.verifier.Stats(),
		Ledger: LedgerSample{Settled: min(st.ledger.Settled(), st.ledger.MaxRevision()), MaxRevision: st.ledger.MaxRevision(), MaxSent: st.ledger.MaxSent()}}
	s.Load = LoadSample{Bytes: total.Bytes.Load(), Live: total.Live.Load(), Keys: total.Keys.Load(), Batches: total.Batches.Load(), Attempts: total.Attempts.Load(),
		Refused: total.Refused.Load(), NotSaved: total.NotSaved.Load(), Unknown: total.Unknown.Load(), Unresolved: total.Unresolved.Load(),
		ByRound: total.RoundBytes(), Commit: total.Commit.Snapshot().Latency()}
	for name, r := range st.groups {
		s.Readers[name] = ReaderSample{Gets: r.C.Gets.Load(), Scans: r.C.Scans.Load(), ScanItems: r.C.ScanItems.Load(),
			Retries: r.C.Retries.Load(), Errors: r.C.Errors.Load(),
			Offered: r.C.Offered.Load(), Dropped: r.C.Dropped.Load(), Late: r.C.Late.Load(), Busy: r.C.Busy.Load(),
			Get: r.C.Get.Snapshot().Latency(), Scan: r.C.Scan.Snapshot().Latency(),
			GetResponse: r.C.GetResponse.Snapshot().Latency(), ScanResponse: r.C.ScanResponse.Snapshot().Latency()}
	}
	if len(st.cluster.Proxies) > 0 {
		s.Storage = map[string]ProxyStats{}
		for g, p := range st.cluster.Proxies {
			if ps, err := p.Stats(ctx); err == nil {
				s.Storage[g] = ps
			}
		}
		s.Amp = amplification(s.Storage, s.Load.Bytes)
	}
	if roles {
		s.Isledb = map[string]map[string]float64{}
		for name, r := range st.cluster.Roles {
			if m, err := scrapeMetrics(ctx, r.Admin); err == nil {
				s.Isledb[name] = m
			}
		}
	}
	if u := st.usage.Load(); u != nil {
		s.Bucket = u
		if s.Load.Live > 0 {
			s.Space = float64(u.Bytes["sst"]) / float64(s.Load.Live)
		}
	}
	st.diskMu.Lock()
	if d := st.lastDisk; d != nil {
		c := *d
		c.PausedSeconds = st.heldBefore.Seconds()
		if c.Paused {
			c.PausedSeconds += time.Since(st.heldSince).Seconds()
		}
		s.Disk = &c
	}
	st.diskMu.Unlock()
	return s
}

// guardDisk holds the load while the disk under storage is fuller than the
// spec allows, and lets it go once it is 2 points below: maintenance keeps
// compacting and deleting meanwhile, so space comes back.
func (st *runState) guardDisk(ctx context.Context) {
	d := st.spec.Storage.Disk
	stat, every := st.statDisk, st.diskEvery
	if stat == nil {
		stat = diskUsage
	}
	if every <= 0 {
		every = 5 * time.Second
	}
	warned := false
	for {
		size, free, err := stat(d.Path)
		if err != nil {
			if !warned {
				warned = true
				st.log.Warn("disk guard can't read the disk, so it holds nothing", "path", d.Path, "error", err)
			}
		} else if size > 0 {
			used := 100 * float64(size-free) / float64(size)
			held := st.diskHeld.Load()
			st.diskMu.Lock()
			switch {
			case !held && used >= d.MaxUsedPct:
				st.diskHeld.Store(true)
				st.heldSince = time.Now()
				st.log.Warn("disk too full: writes held", "path", d.Path, "used_pct", fmt.Sprintf("%.1f", used), "limit_pct", d.MaxUsedPct)
			case held && used < d.MaxUsedPct-2:
				st.diskHeld.Store(false)
				st.heldBefore += time.Since(st.heldSince)
				st.log.Info("disk has room again: writes resume", "path", d.Path, "used_pct", fmt.Sprintf("%.1f", used))
			}
			st.lastDisk = &DiskSample{Path: d.Path, UsedPct: used, Free: int64(free), Paused: st.diskHeld.Load()}
			st.diskMu.Unlock()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}

// listBucket lists the database's prefix every ListEvery.
func (st *runState) listBucket(ctx context.Context) {
	for {
		u, err := listUsage(ctx, st.lister, st.info.Prefix)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			st.log.Warn("listing the bucket failed", "error", err)
		default:
			st.usage.Store(&u)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(st.spec.Storage.ListEvery.D()):
		}
	}
}

func (st *runState) sample(ctx context.Context) {
	enc := json.NewEncoder(st.series)
	checkpoints := append([]Size(nil), st.spec.Checkpoints...)
	sort.Slice(checkpoints, func(i, j int) bool { return checkpoints[i] < checkpoints[j] })
	t := time.NewTicker(st.spec.SampleEvery.D())
	defer t.Stop()
	var prev Sample
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s := st.takeSample(ctx)
		st.mu.Lock()
		_ = enc.Encode(s)
		for len(checkpoints) > 0 && s.Load.Bytes >= int64(checkpoints[0]) {
			st.summary.Checkpoints[fmt.Sprint(int64(checkpoints[0]))] = s
			checkpoints = checkpoints[1:]
		}
		st.mu.Unlock()
		st.log.Info("sample", st.progress(s, prev)...)
		prev = s
	}
}

// progress is the sample's log line: how far, how fast, how long to go,
// what storage pays, and whether every read so far was right.
func (st *runState) progress(s, prev Sample) []any {
	args := []any{"phase", s.Phase, "written", Size(s.Load.Bytes).String()}
	if dt := s.Elapsed - prev.Elapsed; dt > 0 && prev.Time != (time.Time{}) {
		rate := float64(s.Load.Bytes-prev.Load.Bytes) / dt
		args = append(args, "rate", Size(rate).String()+"/s")
		if planned := int64(st.spec.Planned()); rate > 0 && s.Load.Bytes < planned &&
			(strings.HasPrefix(s.Phase, "load") || strings.HasPrefix(s.Phase, "update")) {
			eta := time.Duration(float64(planned-s.Load.Bytes) / rate * float64(time.Second))
			if eta >= time.Minute {
				eta = eta.Round(time.Minute)
			} else {
				eta = eta.Round(time.Second)
			}
			args = append(args, "eta", eta.String())
		}
		if refused := s.Load.Refused - prev.Load.Refused; refused > 0 {
			args = append(args, "refused_per_s", fmt.Sprintf("%.1f", float64(refused)/dt))
		}
	}
	if s.Amp != nil {
		args = append(args, "write_amp", fmt.Sprintf("%.2f", s.Amp.Write))
	}
	if s.Space > 0 {
		args = append(args, "space_amp", fmt.Sprintf("%.2f", s.Space), "ssts", s.Bucket.Objects["sst"])
	}
	if s.Disk != nil {
		args = append(args, "disk_used", fmt.Sprintf("%.1f%%", s.Disk.UsedPct))
		if s.Disk.Paused {
			args = append(args, "writes_held", "disk full")
		}
	}
	return append(args, "commit_p99_ms", s.Load.Commit.P99, "verify", s.Verify.Summary())
}

func (st *runState) finish(runErr error) {
	st.verifier.Finish()
	_ = st.ledger.Flush()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if st.lister != nil {
		// The last listing may be minutes old: the end deserves a fresh one.
		if u, err := listUsage(ctx, st.lister, st.info.Prefix); err == nil {
			st.usage.Store(&u)
		}
	}
	final := st.takeSample(ctx)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.summary.Final = final
	st.summary.Finished = time.Now().UTC()
	st.summary.Failed = final.Verify.Failed()
	if runErr != nil {
		st.summary.Error = runErr.Error()
	}
	if err := writeJSON(filepath.Join(st.info.Dir, "summary.json"), st.summary); err != nil {
		st.log.Error("write summary", "error", err)
	}
	st.log.Info("finished", "dir", st.info.Dir, "verify", final.Verify.Summary(), "error", runErr)
}

func (z Size) String() string {
	const unit = 1024
	n := int64(z)
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%ciB", float64(n)/float64(div), "KMGTP"[exp])
}

// newSchedule returns the workload spec describes over n initial keys, and
// the shape of its keys, over every key its inserts add.
func newSchedule(spec *Spec, n uint64, dist workload.SizeDist) (*workload.Schedule, workload.Shape, error) {
	w := spec.Workload
	rounds := spec.UpdateRounds()
	var inserts uint64
	if rounds > 0 {
		inserts = n * w.InsertPermille / 1000
	}
	shape, err := workload.NewShape(w.Shape, n+uint64(rounds)*inserts, spec.Seed)
	if err != nil {
		return nil, nil, err
	}
	o := workload.Options{UpdatePermille: w.UpdatePermille, InsertsPerRound: inserts, Rounds: rounds}
	if h := w.Hot; h != nil {
		from, to := float64(h.FromPermille)/1000, float64(h.ToPermille)/1000
		o.Hot = func(idx uint64) bool { p := shape.Place(idx); return p >= from && p < to }
		o.HotPermille, o.HotShare = h.UpdatePermille, h.ToPermille-h.FromPermille
	}
	sched, err := workload.NewScheduleWith(n, w.BatchKeys, spec.Seed, w.DeletePermille, dist, o)
	return sched, shape, err
}

// groupWorkers returns how many workers reader group name runs.
func (st *runState) groupWorkers(name string) int {
	for _, rs := range st.spec.Readers {
		if rs.Name == name {
			if rs.Workers > 0 {
				return rs.Workers
			}
			return read.DefaultWorkers
		}
	}
	return 0
}
