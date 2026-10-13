//go:build integration && minio

package storageengine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	isledb "github.com/ankur-anand/isledb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const readCacheToken = "integration-token"

// cacheProcess is the isledb binary running the cache role.
type cacheProcess struct {
	bin, url, admin string
	args            []string
	log             *os.File
	cmd             *exec.Cmd
}

// startCacheProcess builds isledb and starts its cache role for the
// database at prefix in bucketURL, authenticating readCacheToken.
func startCacheProcess(t *testing.T, bucketURL, prefix string) *cacheProcess {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "isledb")
	build := exec.Command("go", "build", "-o", bin, "github.com/ankur-anand/isledb/cmd/isledb")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build isledb: %v\n%s", err, out)
	}
	tokens := filepath.Join(dir, "tokens")
	if err := os.WriteFile(tokens, []byte(readCacheToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(dir, "cache.log"))
	if err != nil {
		t.Fatal(err)
	}
	listen, admin := freeAddr(t), freeAddr(t)
	c := &cacheProcess{bin: bin, url: "http://" + listen, admin: "http://" + admin, log: log, args: []string{"cache",
		"--bucket-url", bucketURL,
		"--db", prefix,
		"--listen", listen,
		"--admin-listen", admin,
		"--auth-token-file", tokens,
		"--disk-dir", filepath.Join(dir, "chunks"),
		"--disk-bytes", "1GiB",
		"--current-poll", "100ms",
		"--long-poll", "2s",
		"--shutdown-drain", "0s",
	}}
	t.Cleanup(func() {
		c.stop(t)
		if t.Failed() {
			out, _ := os.ReadFile(log.Name())
			t.Logf("cache log:\n%s", out)
		}
		_ = log.Close()
	})
	c.start(t)
	return c
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// start runs the cache and waits until it reports ready.
func (c *cacheProcess) start(t *testing.T) {
	t.Helper()
	c.cmd = exec.Command(c.bin, c.args...)
	c.cmd.Stdout, c.cmd.Stderr = c.log, c.log
	if err := c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "the cache reporting ready", func() bool {
		resp, err := http.Get(c.admin + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
}

// kill ends the cache at once, as a crash would.
func (c *cacheProcess) kill(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = c.cmd.Wait()
}

func (c *cacheProcess) stop(t *testing.T) {
	if c.cmd == nil || c.cmd.ProcessState != nil {
		return
	}
	_ = c.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = c.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Error("the cache did not stop within 10s of an interrupt")
		_ = c.cmd.Process.Kill()
		<-done
	}
}

// metric returns the sum of a metric's samples on the cache's /metrics.
func (c *cacheProcess) metric(t *testing.T, name string) float64 {
	t.Helper()
	resp, err := http.Get(c.admin + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sum float64
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, name+" ") && !strings.HasPrefix(line, name+"{") {
			continue
		}
		v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
		if err != nil {
			t.Fatalf("metric line %q: %v", line, err)
		}
		sum += v
	}
	return sum
}

func waitFor(t *testing.T, d time.Duration, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(d); !done(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, d)
		}
	}
}

func fallbacks(m *isledb.ReaderMetrics, reason string) float64 {
	return testutil.ToFloat64(m.ReadCacheFallbacks.WithLabelValues(reason))
}

// TestReadCacheProcess runs the isledb cache binary in front of S3: readers
// read through it, commits reach them through it, and when it crashes or
// refuses a reader, reads go to storage and stay correct.
func TestReadCacheProcess(t *testing.T) {
	ctx := context.Background()
	bucketURL := newMinIOBucket(t)
	const prefix = "read-cache"
	open := func() *isledb.DB {
		t.Helper()
		db, err := isledb.Open(ctx, bucketURL, isledb.DBOptions{Prefix: prefix,
			ChangeFeed: &isledb.ChangeFeedOptions{Payload: isledb.ChangeFeedFullValues}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	// The writer and each reader have a handle of their own, as separate
	// processes would: commits reach readers only through S3 or the cache.
	writer, err := open().OpenWriter(ctx, isledb.DefaultWriterOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(ctx) })
	values := map[string][]byte{}
	put := func(key string, size int) {
		t.Helper()
		value := make([]byte, size)
		_, _ = rand.Read(value)
		values[key] = value
		if _, err := writer.Put(ctx, []byte(key), value); err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 2000 { // about 8 MiB: a large SST
		value := make([]byte, 4<<10)
		_, _ = rand.Read(value)
		key := fmt.Sprintf("key-%05d", i)
		values[key] = value
		if _, err := writer.Put(ctx, []byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	cache := startCacheProcess(t, bucketURL, prefix)
	openReader := func(token string) (*isledb.Reader, *isledb.ReaderMetrics) {
		t.Helper()
		metrics := isledb.DefaultReaderMetrics(nil)
		opts := isledb.DefaultReaderOpenOptions(t.TempDir())
		opts.Views.RefreshAfter = time.Hour // only the doorbell brings new commits
		opts.ReadCache = isledb.ReadCacheOptions{URL: cache.url, Token: token, Timeout: 2 * time.Second}
		opts.Metrics = metrics
		reader, err := open().OpenReader(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reader.Close() })
		return reader, metrics
	}
	readAll := func(reader *isledb.Reader) {
		t.Helper()
		for key, want := range values {
			got, found, err := reader.Get(ctx, []byte(key))
			if err != nil || !found || !bytes.Equal(got, want) {
				t.Fatalf("Get(%s): found %t, %v", key, found, err)
			}
		}
	}
	sees := func(reader *isledb.Reader, key string) func() bool {
		return func() bool {
			_, found, err := reader.Get(ctx, []byte(key))
			return err == nil && found
		}
	}

	// Reads go through the cache, which reads S3.
	reader, metrics := openReader(readCacheToken)
	readAll(reader)
	if testutil.ToFloat64(metrics.ReadCacheReads) == 0 {
		t.Fatal("no read went through the cache")
	}
	for _, reason := range []string{"timeout", "unavailable", "not_found", "size_mismatch", "refused", "invalid", "bypassed", "checksum_mismatch", "after_damage"} {
		if n := fallbacks(metrics, reason); n != 0 {
			t.Fatalf("%v reads fell back (%s) with the cache up", n, reason)
		}
	}
	if cache.metric(t, "isledb_cache_storage_reads_total") == 0 {
		t.Fatal("the cache read nothing from S3")
	}

	// A commit reaches the reader through the relay and the doorbell.
	put("committed", 100)
	waitFor(t, 5*time.Second, "the commit reaching the reader", sees(reader, "committed"))
	if testutil.ToFloat64(metrics.ReadCacheDoorbells.WithLabelValues("rang")) == 0 ||
		testutil.ToFloat64(metrics.ReadCacheCurrent.WithLabelValues("relayed")) == 0 {
		t.Fatal("the commit did not come through the doorbell and the relay")
	}

	// The change feed reads through the cache and wakes on a commit.
	feedMetrics := isledb.DefaultReaderMetrics(nil)
	feed, err := open().OpenChangeReader(ctx, isledb.ChangeReaderOptions{
		ReadCache: isledb.ReadCacheOptions{URL: cache.url, Token: readCacheToken, Timeout: 2 * time.Second},
		Metrics:   feedMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = feed.Close() })
	bounds, err := feed.Bounds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changes, cursor := 0, bounds.Oldest
	for cursor != bounds.Head {
		page, err := feed.Read(ctx, cursor, isledb.ChangeReadOptions{MaxChanges: 200})
		if err != nil {
			t.Fatal(err)
		}
		changes, cursor = changes+len(page.Changes), page.Next
	}
	if changes != len(values) {
		t.Fatalf("change feed: %d changes, want %d", changes, len(values))
	}
	waited := make(chan error, 1)
	go func() { waited <- feed.WaitForChanges(ctx, cursor) }()
	time.Sleep(100 * time.Millisecond)
	put("later", 100)
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForChanges did not return within 5s of a commit")
	}
	if testutil.ToFloat64(feedMetrics.ReadCacheReads) == 0 {
		t.Fatal("no change feed read went through the cache")
	}

	// A reader with a token the cache does not know reads S3 itself.
	refused, refusedMetrics := openReader("wrong-token")
	readAll(refused)
	if fallbacks(refusedMetrics, "refused") == 0 {
		t.Fatal("a wrong token was not counted as refused")
	}

	// The cache crashes: a new reader, with nothing on its own disk, reads
	// everything right from S3, and soon stops waiting on the cache.
	cache.kill(t)
	during, duringMetrics := openReader(readCacheToken)
	readAll(during)
	if fallbacks(duringMetrics, "unavailable") == 0 || fallbacks(duringMetrics, "bypassed") == 0 {
		t.Fatalf("cache down: unavailable %v, bypassed %v; want both",
			fallbacks(duringMetrics, "unavailable"), fallbacks(duringMetrics, "bypassed"))
	}

	waitFor(t, 5*time.Second, "the open reader's doorbell failing", func() bool {
		return testutil.ToFloat64(metrics.ReadCacheDoorbells.WithLabelValues("error")) > 0
	})

	// It restarts: the reader that was open all along gets commits through
	// it again, once its doorbell has backed off and retried. The commit
	// waits out any refresh already scheduled, so only the doorbell brings it.
	cache.start(t)
	time.Sleep(2 * time.Second)
	put("after-restart", 100)
	waitFor(t, 40*time.Second, "the commit after the restart reaching the reader", sees(reader, "after-restart"))
	after, afterMetrics := openReader(readCacheToken)
	readAll(after)
	if testutil.ToFloat64(afterMetrics.ReadCacheReads) == 0 {
		t.Fatal("no read went through the restarted cache")
	}
}
