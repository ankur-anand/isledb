package isledb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func TestChangeKeyAndValueDoNotAliasWritablePageCapacity(t *testing.T) {
	build := func() (Change, Change) {
		pageData := make([]byte, 0, 128)
		first := publicChange(changeRecord{Seq: 1, Kind: changePut, Key: []byte("first"), Value: []byte("value-one")}, &pageData)
		second := publicChange(changeRecord{Seq: 2, Kind: changePut, Key: []byte("second"), Value: []byte("value-two")}, &pageData)
		return first, second
	}

	t.Run("key append cannot overwrite its value", func(t *testing.T) {
		first, _ := build()
		wantValue := append([]byte(nil), first.Value...)
		_ = append(first.Key, '!')
		if !bytes.Equal(first.Value, wantValue) {
			t.Fatalf("append to Change.Key mutated its Value: got=%q want=%q", first.Value, wantValue)
		}
	})

	t.Run("value append cannot overwrite next key", func(t *testing.T) {
		first, second := build()
		wantSecondKey := append([]byte(nil), second.Key...)
		_ = append(first.Value, '!')
		if !bytes.Equal(second.Key, wantSecondKey) {
			t.Fatalf("append to first Change.Value mutated neighboring Change.Key: got=%q want=%q",
				second.Key, wantSecondKey)
		}
	})
}

func TestS3E2E_ChangeFeedPayloadModes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	bucketURL := setupFakeS3BucketURL(t)

	t.Run("default_disabled_keeps_kv", func(t *testing.T) {
		prefix := fmt.Sprintf("e2e/change-default-%d", time.Now().UnixNano())
		db, err := Open(ctx, bucketURL, DBOptions{Prefix: prefix})
		if err != nil {
			t.Fatalf("open DB: %v", err)
		}
		defer db.Close()

		writerOpts := testChangeWriterOptions()
		writerOpts.OwnerID = "change-default-writer"
		writer, err := db.OpenWriter(ctx, writerOpts)
		if err != nil {
			t.Fatalf("open writer: %v", err)
		}
		if err := writer.Put(ctx, []byte("key"), []byte("value")); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("close writer: %v", err)
		}

		if _, err := db.OpenChangeReader(ctx); !errors.Is(err, ErrChangeFeedDisabled) {
			t.Fatalf("open change reader error=%v want=%v", err, ErrChangeFeedDisabled)
		}
		objects, err := db.store.List(ctx, blobstore.ListOptions{Prefix: "changes/"})
		if err != nil {
			t.Fatalf("list change objects: %v", err)
		}
		if len(objects.Objects) != 0 {
			t.Fatalf("change objects=%d want=0", len(objects.Objects))
		}

		reader, err := db.OpenReader(ctx, ReaderOpenOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("open KV reader: %v", err)
		}
		defer reader.Close()
		value, found, err := reader.Get(ctx, []byte("key"))
		if err != nil || !found || string(value) != "value" {
			t.Fatalf("KV Get value=%q found=%v err=%v", value, found, err)
		}
	})

	t.Run("keys_only_persists_and_omits_values", func(t *testing.T) {
		prefix := fmt.Sprintf("e2e/change-keys-%d", time.Now().UnixNano())
		db, err := Open(ctx, bucketURL, DBOptions{
			Prefix:     prefix,
			ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedKeysOnly},
		})
		if err != nil {
			t.Fatalf("open DB: %v", err)
		}
		defer db.Close()

		writerOpts := testChangeWriterOptions()
		writerOpts.OwnerID = "change-keys-writer-1"
		writer, err := db.OpenWriter(ctx, writerOpts)
		if err != nil {
			t.Fatalf("open writer: %v", err)
		}
		largeValue := benchmarkChangeFeedValues(1, 4<<10, true)[0]
		if err := writer.PutWithTTL(ctx, []byte("stored"), largeValue, time.Hour); err != nil {
			t.Fatalf("put large value: %v", err)
		}
		if err := writer.Put(ctx, []byte("empty"), []byte{}); err != nil {
			t.Fatalf("put empty value: %v", err)
		}
		if err := writer.Delete(ctx, []byte("deleted")); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("close writer: %v", err)
		}

		kv, err := db.OpenReader(ctx, ReaderOpenOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("open KV reader: %v", err)
		}
		value, found, err := kv.Get(ctx, []byte("stored"))
		if err != nil || !found || string(value) != string(largeValue) {
			t.Fatalf("large-value KV Get bytes=%d found=%v err=%v", len(value), found, err)
		}
		if err := kv.Close(); err != nil {
			t.Fatalf("close KV reader: %v", err)
		}

		entries, err := db.manifestStore.ListEntries(ctx)
		if err != nil {
			t.Fatalf("list manifest entries: %v", err)
		}
		var firstBatch *manifest.ChangeBatchMeta
		for _, seq := range entries {
			entry, readErr := db.manifestStore.ReadEntry(ctx, seq)
			if readErr != nil {
				t.Fatalf("read manifest entry %d: %v", seq, readErr)
			}
			if entry.ChangeBatch != nil {
				firstBatch = entry.ChangeBatch
				break
			}
		}
		if firstBatch == nil {
			t.Fatal("missing committed change batch")
		}
		if firstBatch.Payload != manifest.ChangeFeedPayloadKeysOnly {
			t.Fatalf("batch payload=%q want=%q", firstBatch.Payload, manifest.ChangeFeedPayloadKeysOnly)
		}
		if got, want := firstBatch.RawSize, int64(32+len("stored")+32+len("empty")+32+len("deleted")); got != want {
			t.Fatalf("keys-only raw bytes=%d want=%d", got, want)
		}
		encoded, _, err := db.store.Read(ctx, firstBatch.Path)
		if err != nil {
			t.Fatalf("read change object: %v", err)
		}
		decoded, err := decodeChangeBatch(encoded)
		if err != nil {
			t.Fatalf("decode change object: %v", err)
		}
		for i, change := range decoded.Changes {
			if change.Kind == changePut && (!change.ValueOmitted || change.Value != nil) {
				t.Fatalf("decoded change[%d] retained a value: %+v", i, change)
			}
		}

		changes, err := db.OpenChangeReader(ctx)
		if err != nil {
			t.Fatalf("open change reader: %v", err)
		}
		bounds, err := changes.Bounds(ctx)
		if err != nil {
			t.Fatalf("bounds: %v", err)
		}
		if bounds.Payload != ChangeFeedKeysOnly {
			t.Fatalf("bounds payload=%s want=%s", bounds.Payload, ChangeFeedKeysOnly)
		}
		page, err := changes.Read(ctx, bounds.Oldest, ChangeReadOptions{})
		if err != nil {
			t.Fatalf("read change page: %v", err)
		}
		if len(page.Changes) != 3 {
			t.Fatalf("changes=%d want=3", len(page.Changes))
		}
		for i, change := range page.Changes {
			if change.HasValue || change.Value != nil {
				t.Fatalf("public change[%d] exposed a value: %+v", i, change)
			}
		}
		if page.Changes[0].Operation != ChangePut || page.Changes[0].ExpiresAt.IsZero() ||
			page.Changes[1].Operation != ChangePut || page.Changes[2].Operation != ChangeDelete {
			t.Fatalf("unexpected ordered changes: %+v", page.Changes)
		}
		if err := changes.Close(); err != nil {
			t.Fatalf("close change reader: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close first DB: %v", err)
		}

		reopened, err := Open(ctx, bucketURL, DBOptions{Prefix: prefix})
		if err != nil {
			t.Fatalf("reopen without feed options: %v", err)
		}
		defer reopened.Close()
		writerOpts.OwnerID = "change-keys-writer-2"
		writer, err = reopened.OpenWriter(ctx, writerOpts)
		if err != nil {
			t.Fatalf("open reopened writer: %v", err)
		}
		if err := writer.Put(ctx, []byte("after-reopen"), []byte("still-omitted")); err != nil {
			t.Fatalf("put after reopen: %v", err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush after reopen: %v", err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("close reopened writer: %v", err)
		}
		reopenedChanges, err := reopened.OpenChangeReader(ctx)
		if err != nil {
			t.Fatalf("open reopened change reader: %v", err)
		}
		reopenedBounds, err := reopenedChanges.Bounds(ctx)
		if err != nil {
			t.Fatalf("reopened bounds: %v", err)
		}
		if reopenedBounds.Payload != ChangeFeedKeysOnly {
			t.Fatalf("reopened payload=%s want=%s", reopenedBounds.Payload, ChangeFeedKeysOnly)
		}
		if err := reopenedChanges.Close(); err != nil {
			t.Fatalf("close reopened change reader: %v", err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened DB: %v", err)
		}

		if _, err := Open(ctx, bucketURL, DBOptions{
			Prefix:     prefix,
			ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedFullValues},
		}); !errors.Is(err, ErrChangeFeedPayloadMismatch) {
			t.Fatalf("payload mismatch error=%v want=%v", err, ErrChangeFeedPayloadMismatch)
		}
	})
}

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
		Prefix:     fmt.Sprintf("e2e/change-get-budget-%d", time.Now().UnixNano()),
		ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedFullValues},
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
	wantChangeGETs := int64(1 + (records+defaultChangeBatchBlockRecords-1)/defaultChangeBatchBlockRecords)
	if got.current != 1 || got.changeBatches != wantChangeGETs || got.manifestPages != 0 || got.lists != 0 {
		t.Fatalf("object GETs=%+v want current=1 change_batches=%d manifest_pages=0 lists=0", got, wantChangeGETs)
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

	db, err := openDB(ctx, store, dbOpenOptions{changeFeedPayload: manifest.ChangeFeedPayloadFullValues})
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
	if got.changeBatches != 2*flushes {
		t.Fatalf("change-batch GETs=%d want=%d", got.changeBatches, 2*flushes)
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
		Prefix:     fmt.Sprintf("bench/change-reader-%d", time.Now().UnixNano()),
		ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedFullValues},
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
	clear(reader.blockCache)
	reader.blockCache = nil
	reader.blockCacheBytes = 0
	reader.blockCacheClock = 0
	reader.batchMu.Unlock()
}
