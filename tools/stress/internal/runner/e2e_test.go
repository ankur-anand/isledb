package runner

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

// TestRunAgainstTheWorkingTree builds isledb from this repository and runs
// a small spec against a directory bucket: inserts with the writer killed
// partway, a round of overwrites and deletes, and a full verify.
func TestRunAgainstTheWorkingTree(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs isledb")
	}
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skip("not in a git checkout")
	}
	repo := strings.TrimSpace(string(out))
	if _, err := os.Stat(filepath.Join(repo, "cmd", "isledb")); err != nil {
		t.Skip("not the isledb repository")
	}
	dir := t.TempDir()
	t.Cleanup(func() {
		if t.Failed() {
			logRoleLogs(t, filepath.Join(dir, "results"))
		}
	})
	bucket := filepath.Join(dir, "bucket")
	if err := os.MkdirAll(bucket, 0o755); err != nil {
		t.Fatal(err)
	}
	specPath := filepath.Join(dir, "spec.yaml")
	if err := os.WriteFile(specPath, []byte(`
name: e2e
workload: {keys: 4000, values: "fixed:256", batch_keys: 16, delete_permille: 200, workers: 4, rate: 2MiB}
readers: [{name: r, rate: 300, workers: 4, scan_ratio: 0.1, scan_limit: 50}]
storage: {bucket_url: "file://`+bucket+`"}
isledb: {readers: 1, config: {writer: {flush: {interval: 100ms}}}}
phases:
  - load: {events: [{after: 300ms, role: writer, kill: true}]}
  - verify: {parts: 4, limit: 200}
  - update: {rounds: 1, rate: 512KiB}
  - verify: {parts: 4, limit: 200}
sample_every: 500ms
`), 0o644); err != nil {
		t.Fatal(err)
	}
	spec, err := LoadSpec(specPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	sum, err := Run(ctx, Options{Spec: spec, Isledb: ".", Repo: repo, Out: filepath.Join(dir, "results"),
		Log: slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelWarn}))})
	if err != nil {
		t.Fatal(err)
	}
	final := sum.Final
	if final.Verify.Checked == 0 || final.Verify.Failed() != 0 || final.Verify.Pending != 0 {
		t.Fatalf("verify: %s", final.Verify.Summary())
	}
	if final.Load.Keys != 8000 {
		t.Fatalf("%d keys written, want two rounds of 4000", final.Load.Keys)
	}
	if final.Readers["r"].Gets == 0 || final.Readers["r"].Scans == 0 {
		t.Fatalf("readers did not read: %+v", final.Readers)
	}
	if len(sum.Phases) != 4 || sum.Phases[1].Scanned != 4000 || sum.Phases[3].Scanned < 3000 || sum.Phases[3].Scanned > 3400 {
		t.Fatalf("phases %+v: want the verifies to scan 4000 keys, then about 3200, 4000 less a fifth deleted", sum.Phases)
	}
	// The readers read again after the first verify.
	if gets := phaseGets(t, filepath.Join(sum.Run.Dir, "series.jsonl"), "update x1"); gets == 0 {
		t.Fatal("no gets during the update after a verify: the readers did not start again")
	}
	if !strings.Contains(sum.Run.Name, "-4000keys-") {
		t.Fatalf("run name %q lacks its size", sum.Run.Name)
	}
	if final.Load.Live <= 0 || final.Bucket == nil || final.Bucket.Bytes["sst"] == 0 || final.Space < 1 {
		t.Fatalf("live %d, bucket %+v, space amplification %.2f: want the final listing to show the SSTs", final.Load.Live, final.Bucket, final.Space)
	}
	for _, f := range []string{"run.json", "series.jsonl", "summary.json", "failures.jsonl", "logs/writer.log"} {
		if _, err := os.Stat(filepath.Join(sum.Run.Dir, f)); err != nil {
			t.Errorf("run directory lacks %s", f)
		}
	}
}

// TestPartialRoundsAgainstTheWorkingTree runs update rounds that write part
// of the keys, a hot range more often, delete some and insert new ones, and
// checks the run wrote and kept exactly the keys its schedule says. Killing
// the writer is TestRunAgainstTheWorkingTree's: a lost write holds the next
// round a minute.
func TestPartialRoundsAgainstTheWorkingTree(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs isledb")
	}
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skip("not in a git checkout")
	}
	repo := strings.TrimSpace(string(out))
	if _, err := os.Stat(filepath.Join(repo, "cmd", "isledb")); err != nil {
		t.Skip("not the isledb repository")
	}
	dir := t.TempDir()
	t.Cleanup(func() {
		if t.Failed() {
			logRoleLogs(t, filepath.Join(dir, "results"))
		}
	})
	bucket := filepath.Join(dir, "bucket")
	if err := os.MkdirAll(bucket, 0o755); err != nil {
		t.Fatal(err)
	}
	specPath := filepath.Join(dir, "spec.yaml")
	if err := os.WriteFile(specPath, []byte(`
name: e2e-partial
workload:
  keys: 3000
  values: "fixed:128"
  batch_keys: 8
  delete_permille: 200
  update_permille: 150
  insert_permille: 100
  hot: {to_permille: 200, update_permille: 600}
  workers: 4
  rate: 2MiB
readers: [{name: r, rate: 300, workers: 4, scan_ratio: 0.2, scan_min: 1, scan_limit: 60, dist: latest, latest_window: 500}]
storage: {bucket_url: "file://`+bucket+`"}
isledb: {readers: 1, config: {writer: {flush: {interval: 100ms}}}}
phases:
  - load: {}
  - update: {rounds: 4, rate: 512KiB}
  - verify: {parts: 4, limit: 200}
sample_every: 500ms
`), 0o644); err != nil {
		t.Fatal(err)
	}
	spec, err := LoadSpec(specPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	sum, err := Run(ctx, Options{Spec: spec, Isledb: ".", Repo: repo, Out: filepath.Join(dir, "results"),
		Log: slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelWarn}))})
	if err != nil {
		t.Fatal(err)
	}
	final := sum.Final
	if final.Verify.Checked == 0 || final.Verify.Failed() != 0 || final.Verify.Pending != 0 {
		t.Fatalf("verify: %s", final.Verify.Summary())
	}
	dist, _ := workload.ParseSizeDist(spec.Workload.Values)
	s, _, err := newSchedule(spec, spec.Workload.Keys, dist)
	if err != nil {
		t.Fatal(err)
	}
	var keys, live int64
	for no := range s.FirstBatch(5) {
		keys += int64(s.Written(s.Batch(no)))
	}
	for idx := range s.MaxKeys() {
		if r, ok := s.Last(4, idx); ok && s.After(r, idx).Present {
			live++
		}
	}
	if final.Load.Keys != keys {
		t.Fatalf("%d keys written, the schedule writes %d", final.Load.Keys, keys)
	}
	if got := sum.Phases[2].Scanned; got != live {
		t.Fatalf("the verify scanned %d keys, the schedule leaves %d", got, live)
	}
	if r := final.Readers["r"]; r.Offered == 0 || r.Gets == 0 || r.Scans == 0 || r.GetResponse.Count == 0 {
		t.Fatalf("readers %+v", r)
	}
}

// logRoleLogs logs the end of every role's log under results, which the
// temporary directory takes with it.
func logRoleLogs(t *testing.T, results string) {
	logs, _ := filepath.Glob(filepath.Join(results, "*", "logs", "*.log"))
	for _, path := range logs {
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		t.Logf("%s, last %d lines:\n%s", filepath.Base(path), min(len(lines), 40), strings.Join(lines[max(0, len(lines)-40):], "\n"))
	}
}

// phaseGets returns the gets the readers made during phase.
func phaseGets(t *testing.T, path, phase string) int64 {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := int64(-1), int64(-1)
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var s Sample
		if err := json.Unmarshal([]byte(line), &s); err != nil {
			t.Fatal(err)
		}
		if s.Phase != phase {
			continue
		}
		var gets int64
		for _, r := range s.Readers {
			gets += r.Gets
		}
		if lo < 0 {
			lo = gets
		}
		hi = gets
	}
	return hi - lo
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}
