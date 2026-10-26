package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Binary is an isledb build.
type Binary struct {
	Path string `json:"path"`
	// Version names the build in results: the git ref it was built from,
	// or what the binary reports.
	Version string `json:"version"`
	// Reported is what "isledb version" prints, empty for versions without
	// the command.
	Reported string `json:"reported,omitempty"`
	Commit   string `json:"commit,omitempty"`
	Source   string `json:"source"` // what was asked for
}

// ResolveIsledb returns the isledb binary for want: a path to a binary; a
// git ref of the repository at repo, built once into cache; or "." for
// repo's working tree as it is, built every time.
//
// With fromOrigin, a tag is the commit origin has it at, even when a local
// tag of the same name differs: fetch with FetchOrigin first.
func ResolveIsledb(ctx context.Context, want, repo, cache string, fromOrigin bool) (Binary, error) {
	b := Binary{Source: want}
	if want == "" {
		want = "."
	}
	if fi, err := os.Stat(want); err == nil && !fi.IsDir() {
		b.Path = want
	} else if want == "." {
		b.Path = filepath.Join(cache, "isledb-worktree")
		if err := run(ctx, repo, "go", "build", "-o", b.Path, "./cmd/isledb"); err != nil {
			return b, fmt.Errorf("build isledb from %s: %w", repo, err)
		}
		if out, err := output(ctx, repo, "git", "rev-parse", "HEAD"); err == nil {
			b.Commit = out + "+worktree"
		}
	} else {
		commit := ""
		if fromOrigin {
			c, err := originTag(ctx, repo, want)
			if err != nil {
				return b, err
			}
			commit = c
		}
		if commit == "" {
			c, err := output(ctx, repo, "git", "rev-parse", "--verify", want+"^{commit}")
			if err != nil {
				return b, fmt.Errorf("isledb %q is neither a binary nor a git ref of %s: %w", want, repo, err)
			}
			commit = c
		}
		b.Commit = commit
		b.Path = filepath.Join(cache, "isledb-"+commit[:12])
		if _, err := os.Stat(b.Path); err != nil {
			tree, err := os.MkdirTemp(cache, "tree-")
			if err != nil {
				return b, err
			}
			defer func() {
				_ = run(context.Background(), repo, "git", "worktree", "remove", "--force", tree)
				_ = os.RemoveAll(tree)
			}()
			if err := run(ctx, repo, "git", "worktree", "add", "--detach", tree, commit); err != nil {
				return b, err
			}
			if err := run(ctx, tree, "go", "build", "-o", b.Path, "./cmd/isledb"); err != nil {
				return b, fmt.Errorf("build isledb %s: %w", want, err)
			}
		}
	}
	abs, err := filepath.Abs(b.Path)
	if err != nil {
		return b, err
	}
	b.Path = abs
	if v, err := output(ctx, "", b.Path, "version"); err == nil {
		b.Reported = strings.TrimPrefix(v, "isledb ")
	}
	switch {
	case want != "." && b.Commit != "":
		b.Version = want
	case b.Reported != "" && b.Reported != "(devel)":
		b.Version = b.Reported
	case b.Commit != "":
		b.Version = b.Commit[:12]
	default:
		b.Version = filepath.Base(b.Path)
	}
	return b, nil
}

// FetchOrigin updates the remote-tracking branches of origin in repo, so
// origin/main is current. Tags are left alone: fetching them would refuse,
// or overwrite, a local tag that differs from origin's.
func FetchOrigin(ctx context.Context, repo string) error {
	return run(ctx, repo, "git", "fetch", "--no-tags", "origin")
}

// originTag returns the commit origin's tag name points to, fetching it
// without creating or changing any local ref; "" when origin has no such
// tag. It warns when a local tag of the name points elsewhere.
func originTag(ctx context.Context, repo, name string) (string, error) {
	out, err := output(ctx, repo, "git", "ls-remote", "--tags", "origin", "refs/tags/"+name, "refs/tags/"+name+"^{}")
	if err != nil {
		return "", fmt.Errorf("ask origin for tag %s: %w", name, err)
	}
	commit := ""
	for _, line := range strings.Split(out, "\n") {
		hash, ref, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		// An annotated tag lists its own object, then, peeled, the commit.
		if ref == "refs/tags/"+name+"^{}" || commit == "" {
			commit = hash
		}
	}
	if commit == "" {
		return "", nil
	}
	if err := run(ctx, repo, "git", "fetch", "--no-tags", "origin", commit); err != nil {
		return "", fmt.Errorf("fetch tag %s from origin: %w", name, err)
	}
	if local, err := output(ctx, repo, "git", "rev-parse", "--verify", "--quiet", "refs/tags/"+name+"^{commit}"); err == nil && local != "" && local != commit {
		slog.Warn("the local tag differs from origin's; testing origin's", "tag", name, "local", local, "origin", commit)
	}
	return commit, nil
}

func command(ctx context.Context, dir, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	return cmd
}

func run(ctx context.Context, dir, name string, args ...string) error {
	cmd := command(ctx, dir, name, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, out)
	}
	return nil
}

func output(ctx context.Context, dir, name string, args ...string) (string, error) {
	cmd := command(ctx, dir, name, args...)
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return "", fmt.Errorf("%w: %s", err, ee.Stderr)
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// freePort returns a TCP port on 127.0.0.1 nothing listens on.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Proc is one child process.
type Proc struct {
	Name string
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	log  *os.File
}

// StartProc starts bin with args, its output appended to logPath.
func StartProc(name, bin string, args []string, logPath string, env []string) (*Proc, error) {
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	_, _ = fmt.Fprintf(f, "--- %s %s %s\n", time.Now().UTC().Format(time.RFC3339), bin, strings.Join(args, " "))
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = f, f
	cmd.Env = append(os.Environ(), env...)
	// Its own process group, so a Ctrl-C of the runner reaches the runner
	// only, which then stops its children in order.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	p := &Proc{Name: name, cmd: cmd, done: make(chan struct{}), log: f}
	go func() {
		p.err = cmd.Wait()
		_ = f.Close()
		close(p.done)
	}()
	return p, nil
}

// Exited reports whether the process has ended, and how.
func (p *Proc) Exited() (bool, error) {
	select {
	case <-p.done:
		return true, p.err
	default:
		return false, nil
	}
}

// Stop sends SIGTERM and waits up to timeout, then sends SIGKILL.
func (p *Proc) Stop(timeout time.Duration) error {
	if exited, _ := p.Exited(); exited {
		return nil
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
		return nil
	case <-time.After(timeout):
	}
	p.Kill()
	return fmt.Errorf("%s did not stop within %s and was killed", p.Name, timeout)
}

// Kill sends SIGKILL and waits for the process to end.
func (p *Proc) Kill() {
	if exited, _ := p.Exited(); exited {
		return
	}
	_ = p.cmd.Process.Kill()
	<-p.done
}

// waitReady polls url until it answers 200, the process exits, or timeout.
func waitReady(ctx context.Context, p *Proc, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	c := http.Client{Timeout: 2 * time.Second}
	for {
		if exited, err := p.Exited(); exited {
			return fmt.Errorf("%s exited before it was ready: %v; see %s", p.Name, err, p.log.Name())
		}
		if resp, err := c.Get(url); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not ready at %s after %s; see %s", p.Name, url, timeout, p.log.Name())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}
