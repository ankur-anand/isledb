package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/stats"
)

// CheckLoopback refuses a listen address another host could reach: the
// stress test serves only 127.0.0.1 or ::1, reached from elsewhere through
// an SSH tunnel.
func CheckLoopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("listen address %q: %w", addr, err)
	}
	if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("listen address %q: want a port number", addr)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("listen address %q: only loopback addresses such as 127.0.0.1 are allowed; reach it from elsewhere through an SSH tunnel", addr)
	}
	return nil
}

// promTarget is one entry of a Prometheus file_sd target file.
type promTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// writeTargets writes the run's scrape targets to dir/<run>.json, for a
// Prometheus that discovers its targets from that directory: the runner's
// own metrics and every role's admin listener, each labelled with the run,
// its role and the version under test. It returns the file, to remove when
// the run ends, so finished runs leave no targets behind.
func (st *runState) writeTargets(dir, runner string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	label := func(role string) map[string]string {
		return map[string]string{"run": st.info.Name, "role": role, "version": st.info.Isledb.Version}
	}
	var ts []promTarget
	if runner != "" {
		ts = append(ts, promTarget{Targets: []string{runner}, Labels: label("runner")})
	}
	names := make([]string, 0, len(st.cluster.Roles))
	for name := range st.cluster.Roles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		ts = append(ts, promTarget{Targets: []string{st.cluster.Roles[name].Admin}, Labels: label(name)})
	}
	b, err := json.MarshalIndent(ts, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, st.info.Name+".json")
	// Written whole and renamed, so Prometheus never reads half a file.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return "", err
	}
	return path, os.Rename(tmp, path)
}

// serveMetrics serves the runner's measurements at /metrics until ctx ends,
// and returns the address it listens on.
func (st *runState) serveMetrics(ctx context.Context, addr string) (string, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("metrics listener: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write(st.exposition(c))
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(l) }()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	st.log.Info("serving metrics", "address", l.Addr().String())
	return l.Addr().String(), nil
}

// latencyBounds are the bucket bounds of every latency histogram exported.
var latencyBounds = []time.Duration{
	time.Millisecond, 2500 * time.Microsecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond,
	50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
	time.Second, 2500 * time.Millisecond, 5 * time.Second, 10 * time.Second, 30 * time.Second,
}

// promFamilies collects metric families for the text format: each family's
// HELP and TYPE once, then its samples.
type promFamilies struct {
	order []string
	fam   map[string]*promFamily
}

type promFamily struct {
	help, kind string
	lines      []string
}

func (p *promFamilies) family(name, kind, help string) *promFamily {
	if p.fam == nil {
		p.fam = map[string]*promFamily{}
	}
	f := p.fam[name]
	if f == nil {
		f = &promFamily{help: help, kind: kind}
		p.fam[name] = f
		p.order = append(p.order, name)
	}
	return f
}

func promLabels(kv ...string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(kv[i] + "=" + strconv.Quote(kv[i+1]))
	}
	b.WriteByte('}')
	return b.String()
}

func promValue(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

func (p *promFamilies) add(kind, name, help string, v float64, kv ...string) {
	f := p.family(name, kind, help)
	f.lines = append(f.lines, name+promLabels(kv...)+" "+promValue(v))
}

func (p *promFamilies) gauge(name, help string, v float64, kv ...string) {
	p.add("gauge", name, help, v, kv...)
}

func (p *promFamilies) counter(name, help string, v float64, kv ...string) {
	p.add("counter", name, help, v, kv...)
}

func (p *promFamilies) histogram(name, help string, s stats.Snapshot, kv ...string) {
	f := p.family(name, "histogram", help)
	for i, n := range s.Cumulative(latencyBounds) {
		le := promValue(latencyBounds[i].Seconds())
		f.lines = append(f.lines, name+"_bucket"+promLabels(append(append([]string(nil), kv...), "le", le)...)+" "+strconv.FormatInt(n, 10))
	}
	f.lines = append(f.lines,
		name+"_bucket"+promLabels(append(append([]string(nil), kv...), "le", "+Inf")...)+" "+strconv.FormatInt(s.Count, 10),
		name+"_sum"+promLabels(kv...)+" "+promValue(s.Sum.Seconds()),
		name+"_count"+promLabels(kv...)+" "+strconv.FormatInt(s.Count, 10))
}

func (p *promFamilies) bytes() []byte {
	var b strings.Builder
	for _, name := range p.order {
		f := p.fam[name]
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, f.help, name, f.kind)
		for _, l := range f.lines {
			b.WriteString(l + "\n")
		}
	}
	return []byte(b.String())
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// exposition is the runner's metrics now. The run, role and version come
// from the scrape target's labels, so no metric repeats them, and storage
// traffic is labelled proxy, not role, which the target's label holds.
func (st *runState) exposition(ctx context.Context) []byte {
	s := st.sampleOf(ctx, false)
	total, _ := st.totals()
	var p promFamilies

	p.gauge("stress_run_info", "The run: its spec, size, and the commit under test.", 1,
		"spec", st.spec.Name, "size", st.spec.SizeLabel(), "commit", st.info.Isledb.Commit)
	p.gauge("stress_phase", "1 for the phase running.", 1, "phase", s.Phase)
	p.gauge("stress_target_bytes", "Key and value bytes the whole run plans to write.", float64(st.spec.Planned()))

	p.counter("stress_written_bytes_total", "Key and value bytes committed.", float64(s.Load.Bytes))
	p.gauge("stress_live_bytes", "Key and value bytes the database holds, as far as committed writes go.", float64(s.Load.Live))
	p.counter("stress_keys_written_total", "Keys committed.", float64(s.Load.Keys))
	p.counter("stress_write_attempts_total", "Write requests sent, resends included.", float64(s.Load.Attempts))
	for outcome, n := range map[string]int64{"committed": s.Load.Batches, "refused": s.Load.Refused, "not_committed": s.Load.NotSaved,
		"unknown": s.Load.Unknown, "unresolved": s.Load.Unresolved} {
		p.counter("stress_writes_total", "Write answers by outcome: committed; refused (503), nothing written; not_committed, never saved; unknown, asked about; unresolved, sent again without learning the outcome.",
			float64(n), "outcome", outcome)
	}
	p.histogram("stress_commit_seconds", "Time from sending a batch to learning it committed, as the client sees it.", total.Commit.Snapshot())

	for name, r := range st.groups {
		const help = "Reads by reader group, operation and result: ok, retry (503) or error."
		p.counter("stress_reads_total", help, float64(r.C.Gets.Load()), "group", name, "op", "get", "result", "ok")
		p.counter("stress_reads_total", help, float64(r.C.Scans.Load()), "group", name, "op", "scan", "result", "ok")
		p.counter("stress_reads_total", help, float64(r.C.Retries.Load()), "group", name, "op", "any", "result", "retry")
		p.counter("stress_reads_total", help, float64(r.C.Errors.Load()), "group", name, "op", "any", "result", "error")
		p.counter("stress_scan_items_total", "Keys scans returned.", float64(r.C.ScanItems.Load()), "group", name)
		p.histogram("stress_read_seconds", "Read service time: from sending a read to its answer.", r.C.Get.Snapshot(), "group", name, "op", "get")
		p.histogram("stress_read_seconds", "Read service time: from sending a read to its answer.", r.C.Scan.Snapshot(), "group", name, "op", "scan")
		p.histogram("stress_read_response_seconds", "Read response time: from when a read was due to its answer, the wait for a free worker included.",
			r.C.GetResponse.Snapshot(), "group", name, "op", "get")
		p.histogram("stress_read_response_seconds", "Read response time: from when a read was due to its answer, the wait for a free worker included.",
			r.C.ScanResponse.Snapshot(), "group", name, "op", "scan")
		p.counter("stress_reads_offered_total", "Reads the group's rate made due.", float64(r.C.Offered.Load()), "group", name)
		p.counter("stress_reads_dropped_total", "Reads never sent: due longer ago than max_lag when a worker was free.", float64(r.C.Dropped.Load()), "group", name)
		p.counter("stress_reads_late_total", "Reads started more than 10ms after they were due.", float64(r.C.Late.Load()), "group", name)
		p.gauge("stress_read_workers_busy", "Reader workers reading now.", float64(r.C.Busy.Load()), "group", name)
		p.gauge("stress_read_workers", "Reader workers in the group.", float64(st.groupWorkers(name)), "group", name)
	}

	v := s.Verify
	p.counter("stress_verify_checked_total", "Reads checked against what was written.", float64(v.Checked))
	p.counter("stress_verify_queued_total", "Reads that waited for their revision to settle before being checked.", float64(v.Queued))
	p.gauge("stress_verify_pending", "Reads waiting now for their revision to settle.", float64(v.Pending))
	p.counter("stress_verify_truncated_scans_total", "Scan pages checked only for what they returned: their range was too large to list.", float64(v.Truncated))
	kinds := make([]string, 0, len(v.Failures))
	for k := range v.Failures {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		p.counter("stress_verify_failures_total", "Wrong answers, by kind. Any is a failed run.", float64(v.Failures[k]), "kind", k)
	}
	p.gauge("stress_ledger_settled_revision", "Every write at or below this revision has a known outcome.", float64(s.Ledger.Settled))
	p.gauge("stress_ledger_max_revision", "The newest revision a write committed at.", float64(s.Ledger.MaxRevision))

	proxies := make([]string, 0, len(s.Storage))
	for g := range s.Storage {
		proxies = append(proxies, g)
	}
	sort.Strings(proxies)
	for _, g := range proxies {
		for kind, ks := range s.Storage[g] {
			for method, m := range ks.Methods {
				l := []string{"proxy", g, "kind", kind, "method", method}
				p.counter("stress_storage_requests_total", "Storage requests through each latency proxy.", float64(m.Requests), l...)
				p.counter("stress_storage_uploaded_bytes_total", "Request body bytes sent to storage.", float64(m.UploadedBytes), l...)
				p.counter("stress_storage_response_bytes_total", "Response bytes storage returned.", float64(m.Bytes), l...)
				p.counter("stress_storage_server_errors_total", "5xx answers, injected ones included.", float64(m.ServerErrors), l...)
				p.counter("stress_storage_injected_total", "Requests the proxy refused as an injected fault.", float64(m.Injected), l...)
				p.counter("stress_storage_dropped_total", "Requests forwarded whose answer the proxy replaced with an error.", float64(m.Dropped), l...)
			}
		}
	}
	if a := s.Amp; a != nil {
		p.gauge("stress_write_amplification", "SST bytes uploaded per key and value byte written, by who uploaded them.", a.Write, "by", "total")
		p.gauge("stress_write_amplification", "SST bytes uploaded per key and value byte written, by who uploaded them.", a.Writer, "by", "writer")
		p.gauge("stress_write_amplification", "SST bytes uploaded per key and value byte written, by who uploaded them.", a.Compaction, "by", "compaction")
	}
	if u := s.Bucket; u != nil {
		for kind, n := range u.Bytes {
			p.gauge("stress_bucket_bytes", "Bytes under the run's prefix at the last listing, by kind.", float64(n), "kind", kind)
			p.gauge("stress_bucket_objects", "Objects under the run's prefix at the last listing, by kind.", float64(u.Objects[kind]), "kind", kind)
		}
		p.gauge("stress_bucket_listed_timestamp_seconds", "When the bucket was last listed.", float64(u.Time.Unix()))
		if s.Space > 0 {
			p.gauge("stress_space_amplification", "SST bytes in the bucket per byte the database holds.", s.Space)
		}
	}
	if d := s.Disk; d != nil {
		p.gauge("stress_disk_used_ratio", "Share of the guarded disk in use.", d.UsedPct/100, "path", d.Path)
		p.gauge("stress_disk_free_bytes", "Free bytes on the guarded disk.", float64(d.Free), "path", d.Path)
		p.gauge("stress_disk_held", "1 while new writes are held because the disk is too full.", boolValue(d.Paused), "path", d.Path)
		p.counter("stress_disk_held_seconds_total", "Time new writes were held for the disk.", d.PausedSeconds, "path", d.Path)
	}
	return p.bytes()
}
