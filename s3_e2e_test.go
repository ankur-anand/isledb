package isledb

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func setupFakeS3StoreWithPrefix(t testing.TB, prefix string) *blobstore.Store {
	t.Helper()
	bucketURL := setupFakeS3BucketURL(t)

	store, err := blobstore.Open(context.Background(), bucketURL, prefix)
	if err != nil {
		t.Fatalf("open s3 store: %v", err)
	}
	return store
}

func setupFakeS3BucketURL(t testing.TB) string {
	return setupFakeS3BucketURLWithObserver(t, nil)
}

func setupFakeS3BucketURLWithObserver(t testing.TB, observe func(*http.Request)) string {
	t.Helper()

	backend := s3mem.New()
	fake := gofakes3.New(backend)
	baseHandler := fake.Server()
	handler := baseHandler
	if observe != nil {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			observe(r)
			baseHandler.ServeHTTP(w, r)
		})
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", fakeS3AccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", fakeS3SecretKey)
	t.Setenv("AWS_REGION", fakeS3Region)
	t.Setenv("AWS_S3_USE_PATH_STYLE", "true")

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(fakeS3Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			fakeS3AccessKey, fakeS3SecretKey, "",
		)),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(fakeS3Bucket),
	})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	return s3BucketURL(server.URL)
}

func TestS3E2E_WriteCompactRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	store := setupFakeS3StoreWithPrefix(t, fmt.Sprintf("e2e-%d", time.Now().UnixNano()))
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{sstOutput: testSSTOutput("none", 1024)})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "s3-e2e-writer"
	writerOpts.Memtable.TargetBytes = 512
	writerOpts.Memtable.MaxPendingMemtables = 4
	writerOpts.Flush.Interval = 0

	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}

	maintenanceOpts := DefaultMaintenanceOptions()
	maintenanceOpts.Interval = 10 * time.Millisecond
	maintenanceOpts.SSTCompaction.L0TriggerSSTs = 4
	maintenanceOpts.SSTCompaction.BaseLevelBytes = 1 << 60
	maintenanceOpts.SSTCompaction.TargetSSTBytes = 2 * 1024

	maintenance, err := db.OpenMaintenance(ctx, maintenanceOpts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	maintenanceDone := make(chan error, 1)
	go func() { maintenanceDone <- maintenance.Run(ctx) }()

	const (
		batches         = 24
		recordsPerBatch = 16
	)
	expected := make(map[string][]byte, batches*recordsPerBatch)
	start := time.Now()
	for batch := 0; batch < batches; batch++ {
		for i := 0; i < recordsPerBatch; i++ {
			n := batch*recordsPerBatch + i
			key := fmt.Sprintf("key:%06d", n)
			value := []byte(fmt.Sprintf("value:%06d:%064d", n, n))
			if err := writer.Put(ctx, []byte(key), value); err != nil {
				t.Fatalf("put %s: %v", key, err)
			}
			expected[key] = append([]byte(nil), value...)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush batch %d: %v", batch, err)
		}
		if batch%4 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	// Make the compaction boundary deterministic. Background maintenance is
	// already running while writes happen; drain any remaining staged work.
	driveMaintenanceToIdle(t, ctx, maintenance, writer)
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("close maintenance: %v", err)
	}
	if err := <-maintenanceDone; err != nil {
		t.Fatalf("maintenance run: %v", err)
	}

	compacted := replayManifestForTest(t, ctx, store)
	if len(compacted.Levels) == 0 {
		t.Fatalf("expected compaction to create a level, got l0=%d levels=0", compacted.L0SSTCount())
	}
	liveSSTs := compacted.AllSSTIDs()
	if len(liveSSTs) < 2 {
		t.Fatalf("expected multiple live SSTs after compaction, got %d", len(liveSSTs))
	}

	current := readCurrentForTest(t, ctx, store)
	if current.LayoutVersion != manifest.LayoutVersion {
		t.Fatalf("current layout_version=%d, want %d", current.LayoutVersion, manifest.LayoutVersion)
	}
	if current.Format != manifest.CurrentFormat {
		t.Fatalf("current format=%q, want %q", current.Format, manifest.CurrentFormat)
	}
	if current.NextSeq == 0 {
		t.Fatalf("current next_seq was not advanced")
	}
	if current.WriterFence == nil {
		t.Fatalf("current missing writer fence")
	}
	if current.CompactorFence != nil {
		t.Fatalf("current contains obsolete compactor fence: %+v", current.CompactorFence)
	}
	if len(current.ActiveEntries) == 0 && len(current.IndexFrontier) == 0 {
		t.Fatalf("current has neither active entries nor index frontier")
	}

	reader := openReaderFromDBForTest(t, ctx, store, ReaderOpenOptions{
		CacheDir:       t.TempDir(),
		BlockCacheSize: 64 << 10,
	})
	defer reader.Close()
	assertReaderHasAll(t, ctx, reader, expected)

	scanned, err := reader.Scan(ctx, []byte("key:"), []byte("key;"))
	if err != nil {
		t.Fatalf("reader scan: %v", err)
	}
	if len(scanned) != len(expected) {
		t.Fatalf("scan count=%d, want %d", len(scanned), len(expected))
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	objects, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list sst files: %v", err)
	}
	t.Logf("fake-s3 e2e records=%d flushes=%d live_ssts=%d physical_sst_objects=%d elapsed=%s",
		len(expected), batches, len(liveSSTs), len(objects), time.Since(start))
}

func TestS3E2E_ChangeFeedRetentionPreservesKVState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := setupFakeS3StoreWithPrefix(t, fmt.Sprintf("change-feed-e2e-%d", time.Now().UnixNano()))
	defer store.Close()
	runChangeFeedRetentionE2E(t, ctx, store)
}

func TestS3E2E_ChangeReaderPaging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := setupFakeS3StoreWithPrefix(t, fmt.Sprintf("change-reader-e2e-%d", time.Now().UnixNano()))
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{changeFeedPayload: manifest.ChangeFeedPayloadFullValues})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	defer db.Close()
	writer, err := db.OpenWriter(ctx, testChangeWriterOptions())
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	for i := 0; i < 257; i++ {
		if err := writer.Put(ctx, []byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("value-%04d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("open change reader: %v", err)
	}
	defer reader.Close()
	cursor := ChangeCursor{}
	total := 0
	for {
		page, err := reader.Read(ctx, cursor, ChangeReadOptions{MaxChanges: 31, MaxBytes: 1 << 20})
		if err != nil {
			t.Fatalf("read page: %v", err)
		}
		total += len(page.Changes)
		cursor = page.Next
		if page.CaughtUp() {
			break
		}
	}
	if total != 257 {
		t.Fatalf("changes=%d want=257", total)
	}
}

func TestS3E2E_KVLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := setupFakeS3StoreWithPrefix(t, fmt.Sprintf("kv-lifecycle-e2e-%d", time.Now().UnixNano()))
	defer store.Close()
	runKVLifecycleE2E(t, ctx, store)
}

func runKVLifecycleE2E(t testing.TB, ctx context.Context, store *blobstore.Store) {
	t.Helper()

	db, err := openDB(ctx, store, dbOpenOptions{sstOutput: testSSTOutput("none", 1024)})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	reader := openReaderFromDBForTest(t, ctx, store, ReaderOpenOptions{
		CacheDir:            t.TempDir(),
		BlockCacheSize:      64 << 10,
		ValidateSSTChecksum: true,
	})
	defer reader.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "kv-lifecycle-writer"
	writerOpts.Flush.Interval = 0

	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}

	for key, value := range map[string]string{
		"stable":  "stable-value",
		"updated": "version-1",
		"deleted": "delete-me",
	} {
		if err := writer.Put(ctx, []byte(key), []byte(value)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	if err := writer.PutWithTTL(ctx, []byte("ttl-expired"), []byte("expires"), 100*time.Millisecond); err != nil {
		t.Fatalf("put ttl-expired: %v", err)
	}
	if err := writer.PutWithTTL(ctx, []byte("ttl-live"), []byte("still-live"), time.Hour); err != nil {
		t.Fatalf("put ttl-live: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush initial state: %v", err)
	}

	assertReaderValue(t, ctx, reader, "updated", "", false)
	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("refresh initial state: %v", err)
	}
	assertReaderValue(t, ctx, reader, "stable", "stable-value", true)
	assertReaderValue(t, ctx, reader, "updated", "version-1", true)
	assertReaderValue(t, ctx, reader, "deleted", "delete-me", true)

	if err := writer.Put(ctx, []byte("updated"), []byte("version-2")); err != nil {
		t.Fatalf("update key: %v", err)
	}
	if err := writer.Delete(ctx, []byte("deleted")); err != nil {
		t.Fatalf("delete key: %v", err)
	}
	if err := writer.Put(ctx, []byte("new"), []byte("new-value")); err != nil {
		t.Fatalf("put new key: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush updated state: %v", err)
	}
	// Reader views are explicit snapshots of manifest state until Refresh.
	assertReaderValue(t, ctx, reader, "updated", "version-1", true)
	assertReaderValue(t, ctx, reader, "deleted", "delete-me", true)
	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("refresh updated state: %v", err)
	}
	assertReaderValue(t, ctx, reader, "updated", "version-2", true)
	assertReaderValue(t, ctx, reader, "deleted", "", false)
	assertReaderValue(t, ctx, reader, "new", "new-value", true)

	time.Sleep(150 * time.Millisecond)
	assertReaderValue(t, ctx, reader, "ttl-expired", "", false)
	assertReaderValue(t, ctx, reader, "ttl-live", "still-live", true)

	before := replayManifestForTest(t, ctx, store)
	if before.L0SSTCount() != 2 {
		t.Fatalf("L0 SST count before compaction=%d, want 2", before.L0SSTCount())
	}
	oldSSTs := make(map[string]struct{}, len(before.L0SSTs))
	for _, sst := range before.L0SSTs {
		oldSSTs[sst.ID] = struct{}{}
	}

	opts := DefaultMaintenanceOptions()
	opts.SSTCompaction.L0TriggerSSTs = 2
	opts.SSTCompaction.BaseLevelBytes = 1 << 60
	opts.SSTCompaction.TargetSSTBytes = 1 << 20
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	stats := driveMaintenanceToIdle(t, ctx, maintenance, writer)
	if stats.SSTCompaction.Jobs == 0 {
		t.Fatalf("compaction did not run: %+v", stats)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("close maintenance: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("refresh compacted state: %v", err)
	}
	assertCurrentKVState(t, ctx, reader)

	compacted := replayManifestForTest(t, ctx, store)
	if compacted.L0SSTCount() != 0 || len(compacted.Levels) == 0 {
		t.Fatalf("unexpected compacted manifest: l0=%d levels=%d", compacted.L0SSTCount(), len(compacted.Levels))
	}
	for _, id := range compacted.AllSSTIDs() {
		if _, stale := oldSSTs[id]; stale {
			t.Fatalf("old L0 SST %q remained visible after compaction", id)
		}
	}

	for id := range oldSSTs {
		if _, _, err := store.Read(ctx, store.SSTPath(id)); err != nil {
			t.Fatalf("old SST %q must remain readable during the pinned-view window: %v", id, err)
		}
	}
	assertCurrentKVState(t, ctx, reader)

	freshReader := openReaderFromDBForTest(t, ctx, store, ReaderOpenOptions{
		CacheDir:            t.TempDir(),
		ValidateSSTChecksum: true,
	})
	defer freshReader.Close()
	assertCurrentKVState(t, ctx, freshReader)

	rows, err := freshReader.Scan(ctx, []byte(""), []byte("zzzz"))
	if err != nil {
		t.Fatalf("scan compacted state: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("compacted scan count=%d, want 4", len(rows))
	}
}

func assertCurrentKVState(t testing.TB, ctx context.Context, reader *Reader) {
	t.Helper()
	assertReaderValue(t, ctx, reader, "stable", "stable-value", true)
	assertReaderValue(t, ctx, reader, "updated", "version-2", true)
	assertReaderValue(t, ctx, reader, "deleted", "", false)
	assertReaderValue(t, ctx, reader, "new", "new-value", true)
	assertReaderValue(t, ctx, reader, "ttl-expired", "", false)
	assertReaderValue(t, ctx, reader, "ttl-live", "still-live", true)
}

func assertReaderValue(t testing.TB, ctx context.Context, reader *Reader, key, want string, wantFound bool) {
	t.Helper()
	got, found, err := reader.Get(ctx, []byte(key))
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	if found != wantFound || (found && string(got) != want) {
		t.Fatalf("get %s=%q found=%v, want %q found=%v", key, got, found, want, wantFound)
	}
}

func runChangeFeedRetentionE2E(t testing.TB, ctx context.Context, store *blobstore.Store) {
	t.Helper()

	db, err := openDB(ctx, store, dbOpenOptions{sstOutput: testSSTOutput("none", 4096)})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := db.manifestStore.EnableChangeFeed(ctx, manifest.ChangeFeedPayloadFullValues); err != nil {
		t.Fatalf("enable change feed: %v", err)
	}

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "change-feed-e2e-writer"
	writerOpts.Flush.Interval = 0

	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	if err := writer.Put(ctx, []byte("key:1"), []byte("value:1")); err != nil {
		t.Fatalf("put key:1: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush key:1: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := writer.Put(ctx, []byte("key:2"), []byte("value:2")); err != nil {
		t.Fatalf("put key:2: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush key:2: %v", err)
	}
	before, err := store.List(ctx, blobstore.ListOptions{Prefix: "changes/"})
	if err != nil {
		t.Fatalf("list change batches before cleanup: %v", err)
	}
	if len(before.Objects) != 2 {
		t.Fatalf("change batches before cleanup=%d, want 2", len(before.Objects))
	}

	time.Sleep(2 * time.Millisecond)
	changeFeed := DefaultChangeFeedRetentionOptions()
	changeFeed.RetainFor = time.Millisecond
	opts := DefaultMaintenanceOptions()
	opts.ChangeFeedRetention = &changeFeed

	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	// Retain the newest writer commit and its change batch while keeping the
	// production safeguards internal to the public API.
	maintenance.changeFeed.opts.KeepAtLeastManifestEntries = 1
	maintenance.changeFeed.opts.SweepGracePeriod = time.Nanosecond
	driveMaintenanceToIdle(t, ctx, maintenance, writer)
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("close maintenance: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	after, err := store.List(ctx, blobstore.ListOptions{Prefix: "changes/"})
	if err != nil {
		t.Fatalf("list change batches after cleanup: %v", err)
	}
	if len(after.Objects) != 1 {
		t.Fatalf("change batches after cleanup=%d, want 1", len(after.Objects))
	}

	replayed := replayManifestForTest(t, ctx, store)
	if replayed.L0SSTCount() != 2 {
		t.Fatalf("replayed L0 SST count=%d, want 2", replayed.L0SSTCount())
	}

	reader := openReaderFromDBForTest(t, ctx, store, ReaderOpenOptions{CacheDir: t.TempDir()})
	defer reader.Close()
	for key, want := range map[string]string{"key:1": "value:1", "key:2": "value:2"} {
		got, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if !found || string(got) != want {
			t.Fatalf("get %s=%q found=%v, want %q true", key, got, found, want)
		}
	}
}

func BenchmarkS3E2E_WriteFlushWithCompactor(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	store := setupFakeS3StoreWithPrefix(b, fmt.Sprintf("bench-%d", time.Now().UnixNano()))
	defer store.Close()

	db, err := openDB(ctx, store, dbOpenOptions{sstOutput: testSSTOutput("none", 4096)})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "s3-bench-writer"
	writerOpts.Memtable.TargetBytes = 4 << 10
	writerOpts.Flush.Interval = 0

	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		b.Fatalf("open writer: %v", err)
	}

	maintenanceOpts := DefaultMaintenanceOptions()
	maintenanceOpts.Interval = 25 * time.Millisecond
	maintenanceOpts.SSTCompaction.L0TriggerSSTs = 8
	maintenanceOpts.SSTCompaction.BaseLevelBytes = 1 << 60
	maintenanceOpts.SSTCompaction.TargetSSTBytes = 64 << 10

	maintenance, err := db.OpenMaintenance(ctx, maintenanceOpts)
	if err != nil {
		b.Fatalf("open maintenance: %v", err)
	}
	maintenanceDone := make(chan error, 1)
	go func() { maintenanceDone <- maintenance.Run(ctx) }()
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		_ = maintenance.Close(closeCtx)
		<-maintenanceDone
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("bench:%09d", i))
		value := []byte(fmt.Sprintf("value:%09d:%064d", i, i))
		if err := writer.Put(ctx, key, value); err != nil {
			b.Fatalf("put: %v", err)
		}
		if i%64 == 63 {
			if err := writer.Flush(ctx); err != nil {
				b.Fatalf("flush: %v", err)
			}
		}
	}
	driveMaintenanceToIdle(b, ctx, maintenance, writer)
	if err := writer.Close(ctx); err != nil {
		b.Fatalf("close writer: %v", err)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "records/s")
}

func replayManifestForTest(t testing.TB, ctx context.Context, store *blobstore.Store) *manifestState {
	t.Helper()

	ms := manifest.NewStore(store)
	m, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	return m
}

func driveMaintenanceToIdle(t testing.TB, ctx context.Context, maintenance *Maintenance, writer *Writer) MaintenanceStats {
	t.Helper()
	var total MaintenanceStats
	for attempt := 0; attempt < compactionMaxIterations*4; attempt++ {
		stats, err := maintenance.RunOnce(ctx)
		if err != nil {
			t.Fatalf("maintenance RunOnce(%d): %v", attempt, err)
		}
		total.SSTCompaction.Jobs += stats.SSTCompaction.Jobs
		total.SSTCompaction.InputSSTs += stats.SSTCompaction.InputSSTs
		total.SSTCompaction.OutputSSTs += stats.SSTCompaction.OutputSSTs
		total.SSTCompaction.OutputBytes += stats.SSTCompaction.OutputBytes
		total.ManifestCheckpoint.Staged = total.ManifestCheckpoint.Staged || stats.ManifestCheckpoint.Staged

		head, _, err := maintenance.manifestLog.ReadMaintenanceHead(ctx)
		if err != nil {
			t.Fatalf("read maintenance HEAD(%d): %v", attempt, err)
		}
		if head != nil && head.Pending != nil {
			if err := writer.Flush(ctx); err != nil {
				t.Fatalf("writer apply maintenance(%d): %v", attempt, err)
			}
			continue
		}
		if stats.State == MaintenanceIdle {
			return total
		}
	}
	t.Fatalf("maintenance did not become idle after %d cycles", compactionMaxIterations*4)
	return MaintenanceStats{}
}

func readCurrentForTest(t testing.TB, ctx context.Context, store *blobstore.Store) *manifest.Current {
	t.Helper()

	data, _, err := store.Read(ctx, store.ManifestPath())
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	current, err := manifest.DecodeCurrent(data)
	if err != nil {
		t.Fatalf("decode current: %v", err)
	}
	return current
}

func assertReaderHasAll(t testing.TB, ctx context.Context, reader *Reader, expected map[string][]byte) {
	t.Helper()

	for key, want := range expected {
		got, ok, err := reader.Get(ctx, []byte(key))
		if err != nil {
			t.Fatalf("reader get %s: %v", key, err)
		}
		if !ok {
			t.Fatalf("reader missing key %s", key)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reader value mismatch for %s: got %q want %q", key, got, want)
		}
	}
}
