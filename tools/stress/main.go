// Command stress tests an isledb version end to end. It starts that
// version's writer, reader and maintenance processes against a bucket,
// drives them through their HTTP APIs, checks every answer a reader gives
// against what was written, and measures what storage saw. It does not
// import isledb, so it tests any version from the binary alone.
//
//	stress run -spec specs/smoke.yaml -isledb v0.20.0
//	stress run -spec specs/smoke.yaml -isledb .          # the working tree
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
	fmt.Fprintln(os.Stderr, "usage: stress run -spec FILE -isledb VERSION [-out DIR]\n       stress compare SUMMARY.json SUMMARY.json...")
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
	_ = fs.Parse(args)
	if *specPath == "" {
		return errors.New("-spec is required")
	}
	spec, err := runner.LoadSpec(*specPath)
	if err != nil {
		return err
	}
	absOut, err := filepath.Abs(*out)
	if err != nil {
		return err
	}
	_, err = runner.Run(ctx, runner.Options{Spec: spec, Isledb: *isledb, Repo: *repo, Latproxy: *latproxy, Out: absOut})
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
