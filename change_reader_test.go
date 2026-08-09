package isledb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"gocloud.dev/blob/memblob"
)

func TestChangeReaderReadsAndResumesAcrossFlushes(t *testing.T) {
	ctx := context.Background()
	store, db, writer := openChangeReaderTestDB(t, "change-reader-resume")

	if err := writer.Put(ctx, []byte("a"), []byte("one")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := writer.Delete(ctx, []byte("a")); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	if err := writer.PutWithTTL(ctx, []byte("b"), []byte("two"), time.Hour); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush first batch: %v", err)
	}
	if err := writer.Put(ctx, []byte("c"), []byte("three")); err != nil {
		t.Fatalf("put c: %v", err)
	}
	if err := writer.Put(ctx, []byte("d"), []byte("four")); err != nil {
		t.Fatalf("put d: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush second batch: %v", err)
	}

	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()

	bounds, err := reader.Bounds(ctx)
	if err != nil {
		t.Fatalf("bounds: %v", err)
	}
	if bounds.Oldest.IsZero() || bounds.Head.IsZero() || bounds.Oldest == bounds.Head {
		t.Fatalf("unexpected bounds: %+v", bounds)
	}

	cursor := bounds.Oldest
	var changes []Change
	for calls := 0; calls < 10; calls++ {
		page, err := reader.Read(ctx, cursor, ChangeReadOptions{
			MaxChanges: 2,
			MaxBytes:   1 << 20,
		})
		if err != nil {
			t.Fatalf("read page %d: %v", calls, err)
		}
		changes = append(changes, page.Changes...)

		encoded := page.Next.String()
		cursor, err = ParseChangeCursor(encoded)
		if err != nil {
			t.Fatalf("parse next cursor: %v", err)
		}
		if page.CaughtUp() {
			break
		}
	}

	if got, want := len(changes), 5; got != want {
		t.Fatalf("changes=%d want=%d", got, want)
	}
	want := []struct {
		seq   uint64
		op    ChangeOperation
		key   string
		value string
	}{
		{1, ChangePut, "a", "one"},
		{2, ChangeDelete, "a", ""},
		{3, ChangePut, "b", "two"},
		{4, ChangePut, "c", "three"},
		{5, ChangePut, "d", "four"},
	}
	for i, expected := range want {
		got := changes[i]
		if got.Sequence != expected.seq || got.Operation != expected.op ||
			string(got.Key) != expected.key || string(got.Value) != expected.value {
			t.Fatalf("change[%d]=%+v want=%+v", i, got, expected)
		}
	}
	if changes[2].ExpiresAt.IsZero() {
		t.Fatal("TTL change has no expiry")
	}

	empty, err := reader.Read(ctx, cursor, ChangeReadOptions{})
	if err != nil {
		t.Fatalf("read at head: %v", err)
	}
	if len(empty.Changes) != 0 || !empty.CaughtUp() {
		t.Fatalf("read at head=%+v", empty)
	}

	_ = store
}

func TestChangeReaderReusesObservedCurrentWithinBatch(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "change-reader-observed-current")

	if err := writer.Put(ctx, []byte("a"), []byte("one")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := writer.Put(ctx, []byte("b"), []byte("two")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush first batch: %v", err)
	}

	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()

	reader.viewMu.RLock()
	observedHead := reader.view.Head()
	reader.viewMu.RUnlock()

	opts := ChangeReadOptions{MaxChanges: 1, MaxBytes: 1 << 20}
	first, err := reader.Read(ctx, ChangeCursor{}, opts)
	if err != nil {
		t.Fatalf("read first page: %v", err)
	}
	if got, want := first.Head, changeCursorAt(observedHead, 0); got != want {
		t.Fatalf("first head=%q want observed head=%q", got, want)
	}
	if first.CaughtUp() {
		t.Fatal("first page unexpectedly caught up")
	}

	if err := writer.Put(ctx, []byte("c"), []byte("three")); err != nil {
		t.Fatalf("put c: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush second batch: %v", err)
	}

	second, err := reader.Read(ctx, first.Next, opts)
	if err != nil {
		t.Fatalf("read second page: %v", err)
	}
	if got, want := second.Head, changeCursorAt(observedHead, 0); got != want {
		t.Fatalf("second head=%q want observed head=%q", got, want)
	}
	if !second.CaughtUp() {
		t.Fatal("second page did not reach the observed head")
	}

	third, err := reader.Read(ctx, second.Next, opts)
	if err != nil {
		t.Fatalf("read after observed head: %v", err)
	}
	if len(third.Changes) != 1 || string(third.Changes[0].Key) != "c" {
		t.Fatalf("changes after refresh=%+v want key c", third.Changes)
	}
	if third.Head == second.Head {
		t.Fatalf("head did not advance after refresh: %q", third.Head)
	}
}

func TestChangeReaderZeroCursorStartsAtOldestAndLargeChangeMakesProgress(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "change-reader-zero")
	value := make([]byte, 4096)
	for i := range value {
		value[i] = byte(i)
	}
	if err := writer.Put(ctx, []byte("large"), value); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := writer.Put(ctx, []byte("next"), []byte("value")); err != nil {
		t.Fatalf("put next: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()

	page, err := reader.Read(ctx, ChangeCursor{}, ChangeReadOptions{
		MaxChanges: 10,
		MaxBytes:   32,
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(page.Changes) != 1 || string(page.Changes[0].Key) != "large" {
		t.Fatalf("large change page=%+v", page)
	}
	next, err := reader.Read(ctx, page.Next, ChangeReadOptions{
		MaxChanges: 10,
		MaxBytes:   32,
	})
	if err != nil {
		t.Fatalf("read next: %v", err)
	}
	if len(next.Changes) != 1 || string(next.Changes[0].Key) != "next" {
		t.Fatalf("next page=%+v", next)
	}
}

func TestChangeReaderDetectsExpiredCursor(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "change-reader-expired")
	if err := writer.Put(ctx, []byte("a"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		t.Fatalf("bounds: %v", err)
	}

	token, err := db.manifestStore.ClaimCompactor(ctx, "change-reader-retention")
	if err != nil {
		t.Fatalf("claim compactor: %v", err)
	}
	if _, err := db.manifestStore.AdvanceChangeFeedLogStart(ctx, bounds.Head.entry, token); err != nil {
		t.Fatalf("advance change feed floor: %v", err)
	}

	_, err = reader.Read(ctx, bounds.Oldest, ChangeReadOptions{})
	if !errors.Is(err, ErrChangeCursorExpired) {
		t.Fatalf("read expired cursor error=%v want=%v", err, ErrChangeCursorExpired)
	}
}

func TestChangeReaderRejectsCorruptBatch(t *testing.T) {
	ctx := context.Background()
	store, db, writer := openChangeReaderTestDB(t, "change-reader-corrupt")
	if err := writer.Put(ctx, []byte("a"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	entries, _, _, _, err := db.manifestStore.ReadChangeEntries(ctx, 0, true, 1024)
	if err != nil {
		t.Fatalf("read manifest entries: %v", err)
	}
	var meta *manifest.ChangeBatchMeta
	for _, entry := range entries {
		if entry.ChangeBatch != nil {
			meta = entry.ChangeBatch
			break
		}
	}
	if meta == nil {
		t.Fatal("missing committed change batch")
	}
	data, _, err := store.Read(ctx, meta.Path)
	if err != nil {
		t.Fatalf("read change batch: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if _, err := store.Write(ctx, meta.Path, data); err != nil {
		t.Fatalf("corrupt change batch: %v", err)
	}

	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()
	_, err = reader.Read(ctx, ChangeCursor{}, ChangeReadOptions{})
	if !errors.Is(err, ErrCorruptChangeBatch) {
		t.Fatalf("read corrupt batch error=%v want=%v", err, ErrCorruptChangeBatch)
	}
}

func TestChangeFeedEnablementPersistsAcrossDBInstances(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	first, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix:     "change-feed-persist",
		ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedFullValues},
	})
	if err != nil {
		t.Fatalf("open first DB: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first DB: %v", err)
	}

	second, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix: "change-feed-persist",
	})
	if err != nil {
		t.Fatalf("open second DB: %v", err)
	}
	defer second.Close()
	writer, err := second.OpenWriter(ctx, testChangeWriterOptions())
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close(ctx)
	if err := writer.Put(ctx, []byte("a"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	reader, err := second.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open persisted change reader: %v", err)
	}
	defer reader.Close()
	page, err := reader.Read(ctx, ChangeCursor{}, ChangeReadOptions{})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(page.Changes) != 1 {
		t.Fatalf("changes=%d want=1", len(page.Changes))
	}
	if !page.Changes[0].HasValue || string(page.Changes[0].Value) != "value" {
		t.Fatalf("persisted full-value change=%+v", page.Changes[0])
	}
}

func TestChangeFeedRetentionMaintenanceConfigurationIsPublicAndCopied(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix:     "change-feed-public-retention",
		ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedFullValues},
	})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	defer db.Close()

	retention := ChangeFeedRetentionOptions{}
	opts := DefaultMaintenanceOptions()
	opts.ChangeFeedRetention = &retention
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	defer maintenance.Close(ctx)
	if retention.RetainFor != 0 {
		t.Fatalf("OpenMaintenance mutated caller retention: %+v", retention)
	}
	want := DefaultChangeFeedRetentionOptions()
	if maintenance.opts.changeFeedRetention == nil {
		t.Fatal("public retention was not installed")
	}
	if got := maintenance.opts.changeFeedRetention.retainFor; got != want.RetainFor {
		t.Fatalf("normalized retention=%v want=%v", got, want.RetainFor)
	}
	if maintenance.changeFeed == nil {
		t.Fatal("public retention did not enable the maintenance cleaner")
	}
}

func TestChangeFeedRetentionRejectsNegativeDuration(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix:     "change-feed-invalid-retention",
		ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedFullValues},
	})
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer db.Close()
	retention := ChangeFeedRetentionOptions{RetainFor: -time.Second}
	opts := DefaultMaintenanceOptions()
	opts.ChangeFeedRetention = &retention
	_, err = db.OpenMaintenance(ctx, opts)
	if !errors.Is(err, ErrInvalidMaintenanceOptions) {
		t.Fatalf("OpenMaintenance error=%v want=%v", err, ErrInvalidMaintenanceOptions)
	}
}

func TestChangeFeedRetentionRequiresEnabledFeed(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{Prefix: "change-feed-retention-disabled"})
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer db.Close()

	retention := DefaultChangeFeedRetentionOptions()
	opts := DefaultMaintenanceOptions()
	opts.ChangeFeedRetention = &retention
	if _, err := db.OpenMaintenance(ctx, opts); !errors.Is(err, ErrChangeFeedDisabled) {
		t.Fatalf("OpenMaintenance error=%v want=%v", err, ErrChangeFeedDisabled)
	}

	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance after rejected policy: %v", err)
	}
	defer maintenance.Close(ctx)
}

func TestChangeFeedKeysOnlyPreservesKVAndOmitsFeedValues(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix:     "change-feed-keys-only",
		ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedKeysOnly},
	})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	defer db.Close()

	writerOpts := testChangeWriterOptions()
	writerOpts.Values.InlineValueBytes = 1 // Exercise a blob-backed KV value.
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close(ctx)
	if err := writer.PutWithTTL(ctx, []byte("stored"), []byte("complete-value"), time.Hour); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := writer.Delete(ctx, []byte("deleted")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	changes, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer changes.Close()
	bounds, err := changes.Bounds(ctx)
	if err != nil {
		t.Fatalf("bounds: %v", err)
	}
	if bounds.Payload != ChangeFeedKeysOnly {
		t.Fatalf("bounds payload=%s want=%s", bounds.Payload, ChangeFeedKeysOnly)
	}
	page, err := changes.Read(ctx, bounds.Oldest, ChangeReadOptions{})
	if err != nil {
		t.Fatalf("read changes: %v", err)
	}
	if len(page.Changes) != 2 {
		t.Fatalf("changes=%d want=2", len(page.Changes))
	}
	put := page.Changes[0]
	if put.Operation != ChangePut || put.HasValue || put.Value != nil || string(put.Key) != "stored" || put.ExpiresAt.IsZero() {
		t.Fatalf("keys-only PUT=%+v", put)
	}
	deleted := page.Changes[1]
	if deleted.Operation != ChangeDelete || deleted.HasValue || deleted.Value != nil {
		t.Fatalf("keys-only DELETE=%+v", deleted)
	}

	kv, err := db.OpenReader(ctx, ReaderOpenOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open KV reader: %v", err)
	}
	defer kv.Close()
	value, found, err := kv.Get(ctx, []byte("stored"))
	if err != nil || !found || string(value) != "complete-value" {
		t.Fatalf("KV Get value=%q found=%v err=%v", value, found, err)
	}
}

func TestChangeFeedPayloadConfigurationIsExplicitAndImmutable(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	if _, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix:     "change-feed-invalid-payload",
		ChangeFeed: &ChangeFeedOptions{},
	}); !errors.Is(err, ErrInvalidDBOptions) {
		t.Fatalf("invalid payload error=%v want=%v", err, ErrInvalidDBOptions)
	}

	first, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix:     "change-feed-payload-mismatch",
		ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedKeysOnly},
	})
	if err != nil {
		t.Fatalf("enable keys-only feed: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first DB: %v", err)
	}

	if _, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix:     "change-feed-payload-mismatch",
		ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedFullValues},
	}); !errors.Is(err, ErrChangeFeedPayloadMismatch) {
		t.Fatalf("payload mismatch error=%v want=%v", err, ErrChangeFeedPayloadMismatch)
	}
}

func TestPublicChangeDistinguishesEmptyFromOmittedValue(t *testing.T) {
	var pageData []byte
	full := publicChange(changeRecord{
		Seq: 1, Kind: changePut, Key: []byte("full"), Inline: true, Value: []byte{},
	}, &pageData)
	if !full.HasValue || full.Value == nil || len(full.Value) != 0 {
		t.Fatalf("full empty value=%+v", full)
	}

	omitted := publicChange(changeRecord{
		Seq: 2, Kind: changePut, Key: []byte("omitted"), ValueOmitted: true,
	}, &pageData)
	if omitted.HasValue || omitted.Value != nil {
		t.Fatalf("omitted value=%+v", omitted)
	}
}

func TestChangeReaderSupportsConcurrentConsumers(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "change-reader-concurrent")
	for i := 0; i < 100; i++ {
		if err := writer.Put(ctx, []byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()

	const consumers = 16
	errs := make(chan error, consumers)
	for i := 0; i < consumers; i++ {
		go func() {
			page, err := reader.Read(ctx, ChangeCursor{}, ChangeReadOptions{})
			if err == nil && len(page.Changes) != 100 {
				err = fmt.Errorf("changes=%d want=100", len(page.Changes))
			}
			errs <- err
		}()
	}
	for i := 0; i < consumers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("consumer %d: %v", i, err)
		}
	}
}

func TestChangeReaderPagesDoNotAliasDecodedCache(t *testing.T) {
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "change-reader-page-ownership")
	if err := writer.Put(ctx, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		t.Fatalf("bounds: %v", err)
	}
	first, err := reader.Read(ctx, bounds.Oldest, ChangeReadOptions{})
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	first.Changes[0].Key[0] = 'X'
	first.Changes[0].Value[0] = 'X'

	second, err := reader.Read(ctx, bounds.Oldest, ChangeReadOptions{})
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if got := string(second.Changes[0].Key); got != "key" {
		t.Fatalf("cached key=%q want=key", got)
	}
	if got := string(second.Changes[0].Value); got != "value" {
		t.Fatalf("cached value=%q want=value", got)
	}
}

func TestChangeReaderRangeDecodesOnlyRequestedBlock(t *testing.T) {
	const records = 4096
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "change-reader-bounded-range")
	values := benchmarkChangeFeedValues(records, 256, true)
	for i := 0; i < records; i++ {
		if err := writer.Put(ctx, []byte(fmt.Sprintf("key-%08d", i)), values[i]); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		t.Fatalf("bounds: %v", err)
	}

	reader.work.reset()
	first, err := reader.Read(ctx, bounds.Oldest, ChangeReadOptions{MaxChanges: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("cold range read: %v", err)
	}
	if len(first.Changes) != 1 {
		t.Fatalf("cold range changes=%d want=1", len(first.Changes))
	}
	firstWork := reader.work.snapshot()
	if firstWork.RangeGETs != 2 {
		t.Fatalf("cold range GETs=%d want=2 (index + block)", firstWork.RangeGETs)
	}

	reader.batchMu.Lock()
	index := reader.batch
	meta := reader.batchMeta
	reader.batchMu.Unlock()
	if index == nil || len(index.Blocks) < 4 {
		t.Fatalf("indexed blocks=%v want at least 4", index)
	}
	if got, want := firstWork.DecompressedBytes, uint64(index.Blocks[0].RawSize); got != want {
		t.Fatalf("decompressed bytes=%d want first block=%d", got, want)
	}
	if firstWork.DecompressedBytes >= uint64(meta.RawSize) {
		t.Fatalf("decompressed whole batch: got=%d batch=%d", firstWork.DecompressedBytes, meta.RawSize)
	}
	if firstWork.DownloadedBytes >= uint64(meta.Size) {
		t.Fatalf("downloaded whole batch: got=%d batch=%d", firstWork.DownloadedBytes, meta.Size)
	}

	reader.work.reset()
	second, err := reader.Read(ctx, first.Next, ChangeReadOptions{MaxChanges: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("warm block read: %v", err)
	}
	if len(second.Changes) != 1 || second.Changes[0].Sequence != 2 {
		t.Fatalf("warm block page=%+v", second)
	}
	if got := reader.work.snapshot(); got != (changeReaderWorkSnapshot{}) {
		t.Fatalf("warm block performed I/O: %+v", got)
	}

	clearChangeReaderBatchCache(reader)
	reader.work.reset()
	middle := changeCursorAt(first.Next.entry, 2048)
	middlePage, err := reader.Read(ctx, middle, ChangeReadOptions{MaxChanges: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("cold middle read: %v", err)
	}
	if len(middlePage.Changes) != 1 || middlePage.Changes[0].Sequence != 2049 {
		t.Fatalf("middle page=%+v", middlePage)
	}
	middleWork := reader.work.snapshot()
	middleOrdinal, _, ok := changeBatchBlockForRecord(index, 2048)
	if !ok {
		t.Fatal("middle record is not indexed")
	}
	if middleWork.RangeGETs != 2 || middleWork.DecompressedBytes != uint64(index.Blocks[middleOrdinal].RawSize) {
		t.Fatalf("middle work=%+v want index + block %d", middleWork, middleOrdinal)
	}
}

func TestChangeReaderMaxBytesExactBoundaryDoesNotReadNextBlock(t *testing.T) {
	const (
		records   = 2 * defaultChangeBatchBlockRecords
		valueSize = 256
		keySize   = len("key-00000000")
	)
	ctx := context.Background()
	_, db, writer := openChangeReaderTestDB(t, "change-reader-max-bytes-boundary")
	values := benchmarkChangeFeedValues(records, valueSize, true)
	for i := 0; i < records; i++ {
		if err := writer.Put(ctx, []byte(fmt.Sprintf("key-%08d", i)), values[i]); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()
	bounds, err := reader.Bounds(ctx)
	if err != nil {
		t.Fatalf("bounds: %v", err)
	}

	reader.work.reset()
	maxBytes := int64(defaultChangeBatchBlockRecords * (keySize + valueSize))
	page, err := reader.Read(ctx, bounds.Oldest, ChangeReadOptions{
		MaxChanges: records,
		MaxBytes:   maxBytes,
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := len(page.Changes); got != defaultChangeBatchBlockRecords {
		t.Fatalf("changes=%d want=%d", got, defaultChangeBatchBlockRecords)
	}
	if got := page.Next.index; got != defaultChangeBatchBlockRecords {
		t.Fatalf("next index=%d want=%d", got, defaultChangeBatchBlockRecords)
	}

	reader.batchMu.Lock()
	index := reader.batch
	reader.batchMu.Unlock()
	if index == nil || len(index.Blocks) != 2 {
		t.Fatalf("indexed blocks=%v want=2", index)
	}
	work := reader.work.snapshot()
	if work.RangeGETs != 2 {
		t.Fatalf("range GETs=%d want=2 (index + first block)", work.RangeGETs)
	}
	if got, want := work.DecompressedBytes, uint64(index.Blocks[0].RawSize); got != want {
		t.Fatalf("decompressed bytes=%d want first block=%d", got, want)
	}
}

func TestChangeReaderDecodedBlockCacheIsBounded(t *testing.T) {
	reader := &ChangeReader{}
	meta := &manifest.ChangeBatchMeta{Path: "changes/batch", Size: 1}
	const blockBytes = uint64(512 << 10)
	for ordinal := 0; ordinal < 40; ordinal++ {
		reader.cacheBlock(meta, ordinal, blockBytes, []changeRecord{{Seq: uint64(ordinal + 1)}})
	}
	reader.batchMu.Lock()
	gotBytes := reader.blockCacheBytes
	gotBlocks := len(reader.blockCache)
	reader.batchMu.Unlock()
	if gotBytes > maxChangeReaderBlockCacheBytes {
		t.Fatalf("cache bytes=%d max=%d", gotBytes, maxChangeReaderBlockCacheBytes)
	}
	if gotBlocks == 0 || gotBlocks >= 40 {
		t.Fatalf("cached blocks=%d want a bounded non-empty subset", gotBlocks)
	}
	if changes, err := reader.cachedBlock(meta, 0); err != nil || changes != nil {
		t.Fatalf("oldest block retained: changes=%v err=%v", changes, err)
	}
	if changes, err := reader.cachedBlock(meta, 39); err != nil || len(changes) != 1 {
		t.Fatalf("newest block missing: changes=%v err=%v", changes, err)
	}
}

func TestChangeFeedEnablementFencesCommitWithoutBatch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-feed-required")
	defer store.Close()

	oldDB, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("open old DB: %v", err)
	}
	defer oldDB.Close()
	writer, err := oldDB.OpenWriter(ctx, testChangeWriterOptions())
	if err != nil {
		t.Fatalf("open old writer: %v", err)
	}
	defer writer.Close(ctx)
	if err := writer.Put(ctx, []byte("before-enable"), []byte("value")); err != nil {
		t.Fatalf("put before enable: %v", err)
	}

	newDB, err := openDB(ctx, store, dbOpenOptions{changeFeedPayload: manifest.ChangeFeedPayloadFullValues})
	if err != nil {
		t.Fatalf("enable feed from second DB: %v", err)
	}
	defer newDB.Close()

	err = writer.Flush(ctx)
	if !errors.Is(err, manifest.ErrChangeFeedRequired) {
		t.Fatalf("flush error=%v want=%v", err, manifest.ErrChangeFeedRequired)
	}
}

func TestChangeReaderLifecycleAndValidation(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("change-reader-lifecycle")
	defer store.Close()
	disabled, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("open disabled DB: %v", err)
	}
	if _, err := disabled.OpenChangeReader(ctx); !errors.Is(err, ErrChangeFeedDisabled) {
		t.Fatalf("open disabled reader error=%v want=%v", err, ErrChangeFeedDisabled)
	}
	if err := disabled.Close(); err != nil {
		t.Fatalf("close disabled DB: %v", err)
	}

	enabled, err := openDB(ctx, store, dbOpenOptions{changeFeedPayload: manifest.ChangeFeedPayloadFullValues})
	if err != nil {
		t.Fatalf("open enabled DB: %v", err)
	}
	first, err := enabled.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open first change reader: %v", err)
	}
	second, err := enabled.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open second change reader: %v", err)
	}
	if _, err := ParseChangeCursor("not-a-cursor"); !errors.Is(err, ErrInvalidChangeCursor) {
		t.Fatalf("parse error=%v want=%v", err, ErrInvalidChangeCursor)
	}
	if _, err := first.Read(ctx, changeCursorAt(10, 0), ChangeReadOptions{}); !errors.Is(err, ErrInvalidChangeCursor) {
		t.Fatalf("future cursor error=%v want=%v", err, ErrInvalidChangeCursor)
	}
	if _, err := first.Read(ctx, ChangeCursor{}, ChangeReadOptions{MaxChanges: -1}); !errors.Is(err, ErrInvalidChangeReadOptions) {
		t.Fatalf("invalid options error=%v want=%v", err, ErrInvalidChangeReadOptions)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	if _, err := first.Bounds(ctx); !errors.Is(err, ErrChangeReaderClosed) {
		t.Fatalf("bounds after close error=%v want=%v", err, ErrChangeReaderClosed)
	}
	if err := enabled.Close(); err != nil {
		t.Fatalf("close enabled DB: %v", err)
	}
	if _, err := second.Bounds(ctx); !errors.Is(err, ErrChangeReaderClosed) {
		t.Fatalf("second reader after DB close error=%v want=%v", err, ErrChangeReaderClosed)
	}
}

func openChangeReaderTestDB(t *testing.T, prefix string) (*blobstore.Store, *DB, *Writer) {
	t.Helper()
	ctx := context.Background()
	store := blobstore.NewMemory(prefix)
	db, err := openDB(ctx, store, dbOpenOptions{changeFeedPayload: manifest.ChangeFeedPayloadFullValues})
	if err != nil {
		store.Close()
		t.Fatalf("open DB: %v", err)
	}
	writer, err := db.OpenWriter(ctx, testChangeWriterOptions())
	if err != nil {
		db.Close()
		store.Close()
		t.Fatalf("open writer: %v", err)
	}
	t.Cleanup(func() {
		_ = writer.Close(context.Background())
		_ = db.Close()
		_ = store.Close()
	})
	return store, db, writer
}

func testChangeWriterOptions() WriterOptions {
	opts := DefaultWriterOptions()
	opts.OwnerID = fmt.Sprintf("change-reader-test-%d", time.Now().UnixNano())
	opts.Flush.Interval = 0
	opts.Memtable.TargetBytes = 16 << 20
	return opts
}
