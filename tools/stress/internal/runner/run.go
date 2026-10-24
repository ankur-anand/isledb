package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
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
	Log *slog.Logger
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
}

type LoadSample struct {
	Bytes      int64            `json:"bytes"`
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
	Gets      int64         `json:"gets"`
	Scans     int64         `json:"scans"`
	ScanItems int64         `json:"scan_items"`
	Retries   int64         `json:"retries"`
	Errors    int64         `json:"errors"`
	Get       stats.Latency `json:"get"`
	Scan      stats.Latency `json:"scan"`
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
	cache := filepath.Join(opts.Out, ".bin")
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return nil, err
	}
	bin, err := ResolveIsledb(ctx, opts.Isledb, opts.Repo, cache)
	if err != nil {
		return nil, err
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
	sched, err := workload.NewSchedule(n, spec.Workload.BatchKeys, spec.Seed, spec.Workload.DeletePermille, dist)
	if err != nil {
		return nil, err
	}
	shape, err := workload.NewShape(spec.Workload.Shape, n, spec.Seed)
	if err != nil {
		return nil, err
	}

	start := time.Now().UTC()
	name := fmt.Sprintf("%s-%s-%s", spec.Name, safe(bin.Version), start.Format("20060102T150405Z"))
	dir := filepath.Join(opts.Out, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	host, _ := os.Hostname()
	st := &runState{opts: opts, spec: spec, log: log, sched: sched, shape: shape, started: start,
		counters: &load.Counters{}, groups: map[string]*read.Reader{}}
	st.info = RunInfo{Name: name, Dir: dir, Prefix: "stress/" + name, Started: start, Isledb: bin, Host: host,
		CPUs: runtime.NumCPU(), Keys: n, Batches: sched.BatchesPerRound(), MeanValue: dist.Mean(), Spec: spec}
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
	logged := 0
	st.verifier.OnFailure = func(f verify.Failure) {
		if logged < 20 {
			logged++
			log.Error("verification failed", "kind", f.Kind, "op", f.Op, "key", f.Key, "revision", f.Revision, "observed", f.Observed, "allowed", f.Allowed)
		}
	}
	if st.series, err = os.Create(filepath.Join(dir, "series.jsonl")); err != nil {
		return nil, err
	}
	defer func() { _ = st.series.Close() }()

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

	readCtx, stopReads := context.WithCancel(ctx)
	defer stopReads()
	var readers sync.WaitGroup
	for _, rs := range st.spec.Readers {
		r, err := read.New(read.Config{Name: rs.Name, Schedule: st.sched, Shape: st.shape, Ledger: st.ledger, Verifier: st.verifier,
			Readers: st.readers, Rate: rs.Rate, Workers: rs.Workers, ScanRatio: rs.ScanRatio, ScanLimit: rs.ScanLimit, Dist: rs.Dist, Log: st.log})
		if err != nil {
			return err
		}
		st.groups[rs.Name] = r
		readers.Add(1)
		go func() {
			defer readers.Done()
			r.Run(readCtx)
		}()
	}
	stopReaders := func() {
		stopReads()
		readers.Wait()
	}
	defer stopReaders()

	restart := func(ev RestartEvent) error {
		var bin *Binary
		if ev.Isledb != "" {
			b, err := ResolveIsledb(ctx, ev.Isledb, st.opts.Repo, filepath.Join(st.opts.Out, ".bin"))
			if err != nil {
				return err
			}
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
	per := st.sched.BatchesPerRound()
	from := uint64(st.nextRnd) * per
	l, err := load.New(load.Config{Schedule: st.sched, Shape: st.shape, Ledger: st.ledger, Writer: st.writer,
		From: from, To: from + uint64(rounds)*per, Rate: rate, Workers: st.spec.Workload.Workers, Log: st.log})
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
				if err := st.setFaults(context.Background(), f.Proxy, nil); err != nil {
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

// setFaults sets a proxy's injected faults; nil clears them.
func (st *runState) setFaults(ctx context.Context, proxy string, set map[string]any) error {
	p := st.cluster.Proxies[proxy]
	if p == nil {
		return fmt.Errorf("no %s proxy", proxy)
	}
	if set == nil {
		set = map[string]any{}
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

// verifyAll waits for queued checks to settle, then scans everything.
func (st *runState) verifyAll(ctx context.Context, p *VerifyPhase) (int64, error) {
	parts, limit := p.Parts, p.Limit
	if parts <= 0 {
		parts = 16
	}
	if limit <= 0 {
		limit = 1000
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

func (st *runState) takeSample(ctx context.Context) Sample {
	total := &load.Counters{}
	st.mu.Lock()
	phase := st.phase
	total.Merge(st.counters)
	if st.loader != nil {
		total.Merge(&st.loader.C)
	}
	st.mu.Unlock()
	now := time.Now().UTC()
	s := Sample{Time: now, Elapsed: now.Sub(st.started).Seconds(), Phase: phase, Readers: map[string]ReaderSample{},
		Verify: st.verifier.Stats(),
		Ledger: LedgerSample{Settled: st.ledger.Settled(), MaxRevision: st.ledger.MaxRevision(), MaxSent: st.ledger.MaxSent()}}
	s.Load = LoadSample{Bytes: total.Bytes.Load(), Keys: total.Keys.Load(), Batches: total.Batches.Load(), Attempts: total.Attempts.Load(),
		Refused: total.Refused.Load(), NotSaved: total.NotSaved.Load(), Unknown: total.Unknown.Load(), Unresolved: total.Unresolved.Load(),
		ByRound: total.RoundBytes(), Commit: total.Commit.Snapshot().Latency()}
	for name, r := range st.groups {
		s.Readers[name] = ReaderSample{Gets: r.C.Gets.Load(), Scans: r.C.Scans.Load(), ScanItems: r.C.ScanItems.Load(),
			Retries: r.C.Retries.Load(), Errors: r.C.Errors.Load(), Get: r.C.Get.Snapshot().Latency(), Scan: r.C.Scan.Snapshot().Latency()}
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
	s.Isledb = map[string]map[string]float64{}
	for name, r := range st.cluster.Roles {
		if m, err := scrapeMetrics(ctx, r.Admin); err == nil {
			s.Isledb[name] = m
		}
	}
	return s
}

func (st *runState) sample(ctx context.Context) {
	enc := json.NewEncoder(st.series)
	checkpoints := append([]Size(nil), st.spec.Checkpoints...)
	sort.Slice(checkpoints, func(i, j int) bool { return checkpoints[i] < checkpoints[j] })
	t := time.NewTicker(st.spec.SampleEvery.D())
	defer t.Stop()
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
		amp := ""
		if s.Amp != nil {
			amp = fmt.Sprintf("%.2f", s.Amp.Write)
		}
		st.log.Info("sample", "phase", s.Phase, "written", Size(s.Load.Bytes).String(), "write_amp", amp,
			"commit_p99_ms", s.Load.Commit.P99, "verify", s.Verify.Summary())
	}
}

func (st *runState) finish(runErr error) {
	st.verifier.Finish()
	_ = st.ledger.Flush()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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
