package isledb

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

type s3ReadCounts struct {
	current       atomic.Int64
	manifestPages atomic.Int64
	changeBatches atomic.Int64
	lists         atomic.Int64
}

type s3ReadSnapshot struct {
	current       int64
	manifestPages int64
	changeBatches int64
	lists         int64
}

func (c *s3ReadCounts) observe(r *http.Request) {
	if r == nil || r.Method != http.MethodGet {
		return
	}
	if r.URL.Query().Get("list-type") != "" {
		c.lists.Add(1)
		return
	}
	path := r.URL.Path
	switch {
	case strings.HasSuffix(path, "/manifest/CURRENT"):
		c.current.Add(1)
	case strings.Contains(path, "/manifest/pages/"):
		c.manifestPages.Add(1)
	case strings.Contains(path, "/changes/"):
		c.changeBatches.Add(1)
	}
}

func (c *s3ReadCounts) reset() {
	c.current.Store(0)
	c.manifestPages.Store(0)
	c.changeBatches.Store(0)
	c.lists.Store(0)
}

func (c *s3ReadCounts) snapshot() s3ReadSnapshot {
	return s3ReadSnapshot{
		current:       c.current.Load(),
		manifestPages: c.manifestPages.Load(),
		changeBatches: c.changeBatches.Load(),
		lists:         c.lists.Load(),
	}
}

func TestS3E2E_ChangeReaderLargeBatchGETBudget(t *testing.T) {
	const (
		records   = 4096
		pageSize  = 127
		valueSize = 256
	)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	counts := &s3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(t, counts.observe)
	db, err := Open(ctx, bucketURL, DBOptions{
		Prefix:           fmt.Sprintf("e2e/change-get-budget-%d", time.Now().UnixNano()),
		EnableChangeFeed: true,
	})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	defer db.Close()

	writeSingleChangeBatch(t, ctx, db, records, valueSize)
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()

	counts.reset()
	cursor := ChangeCursor{}
	total := 0
	pages := 0
	for {
		page, err := reader.Read(ctx, cursor, ChangeReadOptions{
			MaxChanges: pageSize,
			MaxBytes:   1 << 20,
		})
		if err != nil {
			t.Fatalf("read page %d: %v", pages, err)
		}
		for _, change := range page.Changes {
			assertBenchmarkChange(t, change, total, valueSize)
			total++
		}
		pages++
		cursor = page.Next
		if page.CaughtUp() {
			break
		}
	}
	if total != records {
		t.Fatalf("changes=%d want=%d", total, records)
	}
	if pages <= 1 {
		t.Fatalf("pages=%d want multiple pages", pages)
	}

	got := counts.snapshot()
	if got.current != 1 || got.changeBatches != 1 || got.manifestPages != 0 || got.lists != 0 {
		t.Fatalf("object GETs=%+v want current=1 change_batches=1 manifest_pages=0 lists=0", got)
	}
}

func TestS3E2E_ChangeReaderManifestRotationAndRestart(t *testing.T) {
	const (
		flushes         = 70
		changesPerFlush = 3
	)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	counts := &s3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(t, counts.observe)
	prefix := fmt.Sprintf("e2e/change-rotation-%d", time.Now().UnixNano())
	store, err := blobstore.Open(ctx, bucketURL, prefix)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{changeFeedEnabled: true})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	writerOpts := testChangeWriterOptions()
	writerOpts.OwnerID = "change-reader-rotation-writer"
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}

	expected := make([]expectedChange, 0, flushes*changesPerFlush)
	for batch := 0; batch < flushes; batch++ {
		key := []byte(fmt.Sprintf("key-%03d", batch))
		value := []byte(fmt.Sprintf("value-%03d", batch))
		if err := writer.Put(ctx, key, value); err != nil {
			t.Fatalf("put batch %d: %v", batch, err)
		}
		expected = append(expected, expectedChange{operation: ChangePut, key: string(key), value: string(value)})

		ttlKey := []byte(fmt.Sprintf("ttl-%03d", batch))
		if err := writer.PutWithTTL(ctx, ttlKey, value, time.Hour); err != nil {
			t.Fatalf("put TTL batch %d: %v", batch, err)
		}
		expected = append(expected, expectedChange{
			operation: ChangePut,
			key:       string(ttlKey),
			value:     string(value),
			hasTTL:    true,
		})

		if err := writer.Delete(ctx, key); err != nil {
			t.Fatalf("delete batch %d: %v", batch, err)
		}
		expected = append(expected, expectedChange{operation: ChangeDelete, key: string(key)})
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush batch %d: %v", batch, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	current := readCurrentForTest(t, ctx, store)
	if len(current.IndexFrontier) == 0 {
		t.Fatalf("manifest did not rotate: active=%d frontier=0", len(current.ActiveEntries))
	}

	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open first change reader: %v", err)
	}
	counts.reset()
	cursor := ChangeCursor{}
	consumed := 0
	for consumed < 21 || cursor.index != 0 {
		page, err := reader.Read(ctx, cursor, ChangeReadOptions{MaxChanges: 7, MaxBytes: 1 << 20})
		if err != nil {
			t.Fatalf("read before restart: %v", err)
		}
		consumed = assertExpectedChanges(t, page.Changes, expected, consumed)
		cursor = page.Next
	}
	if cursor.index != 0 {
		t.Fatalf("restart cursor is inside a batch: %q", cursor)
	}
	checkpoint := cursor.String()
	if err := reader.Close(); err != nil {
		t.Fatalf("close first change reader: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close first DB: %v", err)
	}

	db, err = openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	defer db.Close()
	reader, err = db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open resumed change reader: %v", err)
	}
	defer reader.Close()
	cursor, err = ParseChangeCursor(checkpoint)
	if err != nil {
		t.Fatalf("parse checkpoint: %v", err)
	}
	for {
		page, err := reader.Read(ctx, cursor, ChangeReadOptions{MaxChanges: 7, MaxBytes: 1 << 20})
		if err != nil {
			t.Fatalf("read after restart: %v", err)
		}
		consumed = assertExpectedChanges(t, page.Changes, expected, consumed)
		cursor = page.Next
		if page.CaughtUp() {
			break
		}
	}
	if consumed != len(expected) {
		t.Fatalf("changes=%d want=%d", consumed, len(expected))
	}

	got := counts.snapshot()
	if got.manifestPages == 0 {
		t.Fatalf("manifest page GETs=%d want >0", got.manifestPages)
	}
	if got.changeBatches != flushes {
		t.Fatalf("change-batch GETs=%d want=%d", got.changeBatches, flushes)
	}
	if got.lists != 0 {
		t.Fatalf("LIST requests=%d want=0", got.lists)
	}
}

func BenchmarkFakeS3_ChangeReaderReplay_16384x256B(b *testing.B) {
	const (
		records   = 16_384
		valueSize = 256
		pageSize  = 1024
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	counts := &s3ReadCounts{}
	bucketURL := setupFakeS3BucketURLWithObserver(b, counts.observe)
	db, err := Open(ctx, bucketURL, DBOptions{
		Prefix:           fmt.Sprintf("bench/change-reader-%d", time.Now().UnixNano()),
		EnableChangeFeed: true,
	})
	if err != nil {
		b.Fatalf("open DB: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	writeSingleChangeBatch(b, ctx, db, records, valueSize)
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		b.Fatalf("open change reader: %v", err)
	}
	b.Cleanup(func() { _ = reader.Close() })

	for _, cache := range []string{"cold", "warm"} {
		b.Run(cache, func(b *testing.B) {
			if cache == "warm" {
				replayAllChanges(b, ctx, reader, records, pageSize)
			}
			counts.reset()
			b.ReportAllocs()
			b.SetBytes(int64(records * valueSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if cache == "cold" {
					clearChangeReaderBatchCache(reader)
				}
				replayAllChanges(b, ctx, reader, records, pageSize)
			}
			b.StopTimer()
			got := counts.snapshot()
			b.ReportMetric(float64(got.current)/float64(b.N), "current_GETs/op")
			b.ReportMetric(float64(got.changeBatches)/float64(b.N), "change_GETs/op")
			b.ReportMetric(float64(got.manifestPages)/float64(b.N), "page_GETs/op")
			b.ReportMetric(float64(b.N*records)/b.Elapsed().Seconds(), "records/s")
		})
	}
}

type expectedChange struct {
	operation ChangeOperation
	key       string
	value     string
	hasTTL    bool
}

func assertExpectedChanges(
	t testing.TB,
	changes []Change,
	expected []expectedChange,
	offset int,
) int {
	t.Helper()
	for _, change := range changes {
		if offset >= len(expected) {
			t.Fatalf("unexpected change at offset %d: %+v", offset, change)
		}
		want := expected[offset]
		if change.Sequence != uint64(offset+1) || change.Operation != want.operation ||
			string(change.Key) != want.key || string(change.Value) != want.value {
			t.Fatalf("change[%d]=%+v want=%+v", offset, change, want)
		}
		if change.ExpiresAt.IsZero() == want.hasTTL {
			t.Fatalf("change[%d] expires_at=%v want has_ttl=%v", offset, change.ExpiresAt, want.hasTTL)
		}
		offset++
	}
	return offset
}

func writeSingleChangeBatch(t testing.TB, ctx context.Context, db *DB, records, valueSize int) {
	t.Helper()
	opts := DefaultWriterOptions()
	opts.OwnerID = fmt.Sprintf("change-reader-e2e-%d", time.Now().UnixNano())
	opts.Flush.Interval = 0
	opts.Memtable.TargetBytes = 16 << 20
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	values := benchmarkChangeFeedValues(records, valueSize, true)
	for i := 0; i < records; i++ {
		if len(values[i]) >= 8 {
			binary.BigEndian.PutUint64(values[i], uint64(i))
		}
		if err := writer.Put(ctx, []byte(fmt.Sprintf("key-%08d", i)), values[i]); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

func assertBenchmarkChange(t testing.TB, change Change, index, valueSize int) {
	t.Helper()
	if change.Sequence != uint64(index+1) || change.Operation != ChangePut ||
		string(change.Key) != fmt.Sprintf("key-%08d", index) || len(change.Value) != valueSize ||
		binary.BigEndian.Uint64(change.Value) != uint64(index) {
		t.Fatalf("change[%d]=%+v", index, change)
	}
}

func replayAllChanges(t testing.TB, ctx context.Context, reader *ChangeReader, records, pageSize int) {
	t.Helper()
	cursor := ChangeCursor{}
	total := 0
	for {
		page, err := reader.Read(ctx, cursor, ChangeReadOptions{
			MaxChanges: pageSize,
			MaxBytes:   64 << 20,
		})
		if err != nil {
			t.Fatalf("read page: %v", err)
		}
		total += len(page.Changes)
		cursor = page.Next
		if page.CaughtUp() {
			break
		}
	}
	if total != records {
		t.Fatalf("changes=%d want=%d", total, records)
	}
}

func clearChangeReaderBatchCache(reader *ChangeReader) {
	reader.batchMu.Lock()
	reader.batchPath = ""
	reader.batchMeta = manifest.ChangeBatchMeta{}
	reader.batch = nil
	reader.batchEntry = 0
	reader.batchView = nil
	reader.batchMu.Unlock()
}
