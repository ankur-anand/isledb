// Command stress tests an isledb version end to end. It starts that
// version's writer, reader and maintenance processes against a bucket,
// drives them through their HTTP APIs, checks every answer a reader gives
// against what was written, and measures what storage saw. It does not
// import isledb, so it tests any version from the binary alone.
//
//	stress run -spec specs/default.yaml -target 20GiB -isledb v0.20.0
//	stress run -spec specs/default.yaml -target 1TiB -isledb origin/main -fetch
//	stress run -spec specs/local-smoke.yaml              # the working tree
//	stress compare results/a/summary.json results/b/summary.json
//
// Each run gets its own directory under -out: run.json says what ran,
// series.jsonl has a row of measurements every few seconds, failures.jsonl
// every wrong answer, summary.json the results, and logs/ every process's
// output.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/ankur-anand/isledb/tools/stress/internal/runner"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(ctx, os.Args[2:])
	case "compare":
		err = cmdCompare(os.Args[2:])
	default:
		usage()
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "stress:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: stress run -spec FILE [-target SIZE] [-rate SIZE] [-isledb VERSION] [-fetch] [-out DIR]\n       stress compare SUMMARY.json SUMMARY.json...")
	os.Exit(2)
}

// repoRoot finds the isledb repository this command was built in, from the
// working directory.
func repoRoot() string {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func cmdRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	specPath := fs.String("spec", "", "the run's spec (required)")
	isledb := fs.String("isledb", ".", "version under test: a binary, a git ref such as v0.20.0, or . for the working tree")
	repo := fs.String("repo", repoRoot(), "the isledb repository, for git refs and for building latproxy")
	latproxy := fs.String("latproxy", "", "latproxy binary; empty builds it from -repo")
	out := fs.String("out", "results", "directory each run's directory goes under")
	fetch := fs.Bool("fetch", false, "fetch origin in -repo first: origin/main is current, and a tag is the commit origin has it at")
	target := fs.String("target", "", "insert this many key and value bytes, e.g. 20GiB or 1TiB, instead of the spec's size")
	metrics := fs.String("metrics-listen", "127.0.0.1:9400", "where the runner serves /metrics for Prometheus, loopback only; empty for none")
	targets := fs.String("targets-dir", "", "where the run writes its Prometheus scrape targets while it runs; empty is -out/.prometheus")
	rate := fs.String("rate", "", "write this many bytes a second, e.g. 12MiB, 0 unlimited, instead of the spec's rate")
	_ = fs.Parse(args)
	if *specPath == "" {
		return errors.New("-spec is required")
	}
	var o runner.Overrides
	if *target != "" {
		t, err := runner.ParseSize(*target)
		if err != nil || t == 0 {
			return fmt.Errorf("-target %q: want a size such as 20GiB", *target)
		}
		o.Target = t
	}
	if *rate != "" {
		r, err := runner.ParseSize(*rate)
		if err != nil {
			return fmt.Errorf("-rate: %w", err)
		}
		o.Rate = &r
	}
	spec, err := runner.LoadSpecWith(*specPath, o)
	if err != nil {
		return err
	}
	absOut, err := filepath.Abs(*out)
	if err != nil {
		return err
	}
	_, err = runner.Run(ctx, runner.Options{Spec: spec, Isledb: *isledb, Repo: *repo, Latproxy: *latproxy, Out: absOut, Fetch: *fetch,
		MetricsListen: *metrics, TargetsDir: *targets})
	return err
}

func cmdCompare(paths []string) error {
	if len(paths) < 2 {
		return errors.New("compare needs at least two summary.json files")
	}
	var sums []*runner.Summary
	for _, p := range paths {
		s, err := runner.ReadSummary(p)
		if err != nil {
			return err
		}
		sums = append(sums, s)
	}
	runner.Compare(os.Stdout, sums)
	return nil
}
