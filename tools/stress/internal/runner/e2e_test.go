package runner

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
  - update: {rounds: 1}
  - verify: {parts: 4, limit: 200}
sample_every: 1s
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
	if len(sum.Phases) != 3 || sum.Phases[2].Scanned < 3000 || sum.Phases[2].Scanned > 3400 {
		t.Fatalf("phases %+v: want the verify to scan about 3200 keys, 4000 less a fifth deleted", sum.Phases)
	}
	for _, f := range []string{"run.json", "series.jsonl", "summary.json", "failures.jsonl", "logs/writer.log"} {
		if _, err := os.Stat(filepath.Join(sum.Run.Dir, f)); err != nil {
			t.Errorf("run directory lacks %s", f)
		}
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}
