package isledb

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
)

// storageRequest is one request the fake object store served, timed from
// arrival to response, the delay included.
type storageRequest struct {
	method     string
	sst        bool
	start, end time.Time
}

// delayedStorage adds a random 15–40 ms delay to every request, as latproxy
// does for the stress runs, and records each request while enabled.
type delayedStorage struct {
	enabled  atomic.Bool
	mu       sync.Mutex
	requests []storageRequest
}

func (d *delayedStorage) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !d.enabled.Load() {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		time.Sleep(15*time.Millisecond + rand.N(25*time.Millisecond))
		next.ServeHTTP(w, r)
		d.mu.Lock()
		d.requests = append(d.requests, storageRequest{
			method: r.Method,
			sst:    strings.Contains(r.URL.Path, "/sstable/"),
			start:  start,
			end:    time.Now(),
		})
		d.mu.Unlock()
	})
}

// BenchmarkCompactionRewriteDelayedS3 measures one L0 to L1 rewrite against
// object storage with network latency, and splits its time into the input
// download phase and the merge and upload phase. Within the second phase,
// upload_wait is how long the merge sat idle while an output was uploaded.
// The full_l1 case first fills L1, so the measured job rewrites L1 too, as
// it does once a database has been running for a while.
//
//	go test -run '^$' -bench CompactionRewriteDelayedS3 -benchtime 1x
func BenchmarkCompactionRewriteDelayedS3(b *testing.B) {
	cases := []struct {
		name     string
		l1Inputs int // L0 SSTs compacted into L1 before the measured job
		l0Inputs int
	}{
		{name: "empty_l1", l0Inputs: 8},
		{name: "full_l1", l1Inputs: 8, l0Inputs: 10},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				benchmarkCompactionRewrite(b, i, tc.l1Inputs, tc.l0Inputs)
			}
		})
	}
}

func benchmarkCompactionRewrite(b *testing.B, run, l1Inputs, l0Inputs int) {
	const (
		inputBytes = 64 << 20
		valueBytes = 1024
	)
	b.StopTimer()
	ctx := context.Background()
	storage := &delayedStorage{}
	bucketURL := setupFakeS3BucketURLWithHandler(b, storage.wrap)
	store, err := blobstore.Open(ctx, bucketURL, fmt.Sprintf("rewrite-%d", run))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if _, err := db.manifestStore.ClaimWriter(ctx, "bench-writer"); err != nil {
		b.Fatal(err)
	}
	opts := DefaultMaintenanceOptions()
	opts.SSTCompaction.L0TriggerSSTs = l0Inputs
	if l1Inputs > 0 {
		opts.SSTCompaction.L0TriggerSSTs = min(l1Inputs, l0Inputs)
	}
	// Room for the whole L1, so the planner keeps choosing L0 to L1.
	opts.SSTCompaction.BaseLevelBytes = 8 << 30
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer maintenance.Close(ctx)

	var seq uint64
	if l1Inputs > 0 {
		for f := 0; f < l1Inputs; f++ {
			addRandomL0SST(b, ctx, db, &seq, inputBytes, valueBytes)
		}
		stats, err := maintenance.RunOnce(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if stats.SSTCompaction.Jobs != 1 {
			b.Fatalf("filling L1: compaction stats=%+v", stats.SSTCompaction)
		}
		// Apply the staged job to CURRENT, as the writer would.
		head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
		if err != nil || head == nil || head.Pending == nil {
			b.Fatalf("filling L1: no staged job: head=%+v err=%v", head, err)
		}
		if _, err := db.manifestStore.ApplyMaintenanceCommand(ctx, *head.Pending); err != nil {
			b.Fatal(err)
		}
	}
	for f := 0; f < l0Inputs; f++ {
		addRandomL0SST(b, ctx, db, &seq, inputBytes, valueBytes)
	}

	storage.enabled.Store(true)
	b.StartTimer()
	start := time.Now()
	stats, err := maintenance.RunOnce(ctx)
	elapsed := time.Since(start)
	b.StopTimer()
	storage.enabled.Store(false)
	if err != nil {
		b.Fatal(err)
	}
	c := stats.SSTCompaction
	want := int64(l1Inputs+l0Inputs) * inputBytes * 9 / 10
	if c.Jobs != 1 || c.RewrittenBytes == 0 || c.ReadBytes < want {
		b.Fatalf("compaction stats=%+v scheduling=%+v, want one rewrite of every input", c, stats.Scheduling)
	}
	reportRewritePhases(b, storage.requests, elapsed, c)
}

func reportRewritePhases(b *testing.B, requests []storageRequest, elapsed time.Duration, c SSTCompactionStats) {
	var gets, puts []storageRequest
	for _, r := range requests {
		switch {
		case r.sst && r.method == http.MethodGet:
			gets = append(gets, r)
		case r.sst && r.method == http.MethodPut:
			puts = append(puts, r)
		}
	}
	if len(gets) == 0 || len(puts) == 0 {
		b.Fatalf("saw %d SST reads and %d SST writes", len(gets), len(puts))
	}
	slices.SortFunc(puts, func(x, y storageRequest) int { return x.start.Compare(y.start) })
	downloadStart, downloadEnd := gets[0].start, gets[0].end
	for _, r := range gets {
		if r.start.Before(downloadStart) {
			downloadStart = r.start
		}
		if r.end.After(downloadEnd) {
			downloadEnd = r.end
		}
	}
	var uploadWait time.Duration
	for _, r := range puts {
		uploadWait += r.end.Sub(r.start)
	}
	writePhase := puts[len(puts)-1].end.Sub(downloadEnd)
	seconds := elapsed.Seconds()
	b.ReportMetric(float64(c.ReadBytes)/seconds/1e6, "read_MB/s")
	b.ReportMetric(float64(c.RewrittenBytes)/seconds/1e6, "rewrite_MB/s")
	b.ReportMetric(downloadEnd.Sub(downloadStart).Seconds(), "download_s")
	b.ReportMetric(writePhase.Seconds(), "merge_upload_s")
	// Everything else: planning, opening inputs, and committing the result.
	b.ReportMetric((elapsed - downloadEnd.Sub(downloadStart) - writePhase).Seconds(), "other_s")
	b.ReportMetric(uploadWait.Seconds(), "upload_wait_s")
	b.ReportMetric(float64(len(gets)), "sst_gets")
	b.ReportMetric(float64(len(puts)), "sst_puts")
	b.ReportMetric(0, "ns/op")
}

// addRandomL0SST uploads an L0 SST of random keys spanning the whole key
// range, as random writes produce, and adds it to the manifest.
func addRandomL0SST(b *testing.B, ctx context.Context, db *DB, seq *uint64, size, valueBytes int) {
	count := size / (valueBytes + 32)
	keys := make([][]byte, count)
	for i := range keys {
		raw := make([]byte, 16)
		_, _ = cryptorand.Read(raw)
		keys[i] = []byte(hex.EncodeToString(raw))
	}
	slices.SortFunc(keys, bytes.Compare)
	entries := make([]internal.MemEntry, count)
	for i, key := range keys {
		*seq++
		value := make([]byte, valueBytes)
		_, _ = cryptorand.Read(value)
		entries[i] = internal.MemEntry{Key: key, Seq: *seq, Kind: internal.OpPut, Value: value}
	}
	seqLo, seqHi := *seq-uint64(count)+1, *seq
	// Old enough that maintenance drains it rather than leave it for the
	// writer to merge.
	identity := newSSTStreamIdentity(1, seqLo, seqHi, time.Now().Add(-time.Hour))
	result, err := writeSSTStreaming(ctx, &sliceSSTIter{entries: entries},
		sstWriterOptions{BloomBitsPerKey: 12, BlockSize: 16 << 10, Compression: "snappy"},
		identity,
		func(ctx context.Context, id string, r io.Reader) error {
			_, err := db.store.WriteReader(ctx, db.store.SSTPath(id), r, nil)
			return err
		})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.manifestStore.AppendAddSSTableWithFence(ctx, result.Meta); err != nil {
		b.Fatal(err)
	}
}
