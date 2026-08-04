package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func setupFakeS3StoreWithPrefix(t testing.TB, prefix string) *blobstore.Store {
	t.Helper()

	backend := s3mem.New()
	fake := gofakes3.New(backend)
	server := httptest.NewServer(fake.Server())
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

	store, err := blobstore.Open(context.Background(), s3BucketURL(server.URL), prefix)
	if err != nil {
		t.Fatalf("open s3 store: %v", err)
	}
	return store
}

func TestS3E2E_WriteCompactReadRetain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	store := setupFakeS3StoreWithPrefix(t, fmt.Sprintf("e2e-%d", time.Now().UnixNano()))
	defer store.Close()

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "s3-e2e-writer"
	writerOpts.Memtable.TargetBytes = 512
	writerOpts.Memtable.MaxPendingMemtables = 4
	writerOpts.Flush.Interval = 0
	writerOpts.SST.BlockBytes = 1024
	writerOpts.SST.Compression = "none"

	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}

	maintenanceOpts := DefaultMaintenanceOptions()
	maintenanceOpts.OwnerID = "s3-e2e-maintenance"
	maintenanceOpts.Every = 10 * time.Millisecond
	maintenanceOpts.Compaction.L0SSTCount = 4
	maintenanceOpts.Compaction.BaseLevelBytes = 1 << 60
	maintenanceOpts.Compaction.TargetSSTBytes = 2 * 1024
	maintenanceOpts.Compaction.BlockBytes = 1024
	maintenanceOpts.Compaction.Compression = "none"

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
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	// Make the compaction boundary deterministic. Background maintenance is
	// already running while writes happen; RunOnce drains any remaining L0 work.
	if _, err := maintenance.RunOnce(ctx); err != nil {
		t.Fatalf("maintenance run once: %v", err)
	}
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
	liveSSTsBeforeRetention := compacted.AllSSTIDs()
	if len(liveSSTsBeforeRetention) < 2 {
		t.Fatalf("expected multiple live SSTs before retention, got %d", len(liveSSTsBeforeRetention))
	}

	currentBeforeRetention := readCurrentForTest(t, ctx, store)
	if currentBeforeRetention.LayoutVersion != manifest.LayoutVersion {
		t.Fatalf("current layout_version=%d, want %d", currentBeforeRetention.LayoutVersion, manifest.LayoutVersion)
	}
	if currentBeforeRetention.Format != manifest.CurrentFormat {
		t.Fatalf("current format=%q, want %q", currentBeforeRetention.Format, manifest.CurrentFormat)
	}
	if currentBeforeRetention.NextSeq == 0 {
		t.Fatalf("current next_seq was not advanced")
	}
	if currentBeforeRetention.WriterFence == nil {
		t.Fatalf("current missing writer fence")
	}
	if currentBeforeRetention.CompactorFence == nil {
		t.Fatalf("current missing compactor fence")
	}
	if len(currentBeforeRetention.ActiveEntries) == 0 && len(currentBeforeRetention.IndexFrontier) == 0 {
		t.Fatalf("current has neither active entries nor index frontier")
	}

	reader, err := OpenReader(ctx, store, ReaderOpenOptions{
		CacheDir:       t.TempDir(),
		BlockCacheSize: 64 << 10,
	})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	assertReaderHasAll(t, ctx, reader, expected)

	scanned, err := reader.Scan(ctx, []byte("key:"), []byte("key;"))
	if err != nil {
		t.Fatalf("reader scan: %v", err)
	}
	if len(scanned) != len(expected) {
		t.Fatalf("scan count=%d, want %d", len(scanned), len(expected))
	}

	time.Sleep(2 * time.Millisecond)
	var cleanup CleanupStats
	retentionPolicy := DefaultRetentionPolicy()
	retentionPolicy.Mode = RetentionByAge
	retentionPolicy.KeepFor = time.Nanosecond
	retentionPolicy.KeepAtLeastSSTs = 1
	retentionPolicy.OnCleanup = func(stats CleanupStats) {
		cleanup = stats
	}
	retentionMaintenanceOpts := DefaultMaintenanceOptions()
	retentionMaintenanceOpts.Compaction.L0SSTCount = 1 << 20
	retentionMaintenanceOpts.Compaction.BaseLevelBytes = 1 << 60
	retentionMaintenanceOpts.Retention = &retentionPolicy

	retentionMaintenance, err := db.OpenMaintenance(ctx, retentionMaintenanceOpts)
	if err != nil {
		t.Fatalf("open retention maintenance: %v", err)
	}
	if _, err := retentionMaintenance.RunOnce(ctx); err != nil {
		t.Fatalf("retention maintenance run once: %v", err)
	}
	if err := retentionMaintenance.Close(ctx); err != nil {
		t.Fatalf("close retention maintenance: %v", err)
	}
	if cleanup.SSTsDeleted == 0 {
		t.Fatalf("retention deleted no SSTs; live before=%d", len(liveSSTsBeforeRetention))
	}

	afterRetention := replayManifestForTest(t, ctx, store)
	liveSSTsAfterRetention := afterRetention.AllSSTIDs()
	if len(liveSSTsAfterRetention) == 0 {
		t.Fatalf("retention removed every live SST")
	}
	if len(liveSSTsAfterRetention) >= len(liveSSTsBeforeRetention) {
		t.Fatalf("retention did not shrink live set: before=%d after=%d", len(liveSSTsBeforeRetention), len(liveSSTsAfterRetention))
	}

	currentAfterRetention := readCurrentForTest(t, ctx, store)
	if currentAfterRetention.NextSeq <= currentBeforeRetention.NextSeq {
		t.Fatalf("current next_seq did not advance after retention: before=%d after=%d", currentBeforeRetention.NextSeq, currentAfterRetention.NextSeq)
	}

	retainedReader, err := OpenReader(ctx, store, ReaderOpenOptions{
		CacheDir:       t.TempDir(),
		BlockCacheSize: 64 << 10,
	})
	if err != nil {
		t.Fatalf("open retained reader: %v", err)
	}
	defer retainedReader.Close()

	retained, err := retainedReader.Scan(ctx, []byte("key:"), []byte("key;"))
	if err != nil {
		t.Fatalf("retained reader scan: %v", err)
	}
	if len(retained) == 0 {
		t.Fatalf("retained reader returned no keys")
	}
	if len(retained) > len(expected) {
		t.Fatalf("retained reader returned too many keys: got=%d total=%d", len(retained), len(expected))
	}
	for _, kv := range retained {
		want, ok := expected[string(kv.Key)]
		if !ok {
			t.Fatalf("retained reader returned unexpected key %q", kv.Key)
		}
		if !bytes.Equal(kv.Value, want) {
			t.Fatalf("retained value mismatch for %q: got %q want %q", kv.Key, kv.Value, want)
		}
	}

	objects, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list sst files: %v", err)
	}
	t.Logf("fake-s3 e2e records=%d flushes=%d live_ssts_before_retention=%d live_ssts_after_retention=%d physical_sst_objects=%d elapsed=%s",
		len(expected), batches, len(liveSSTsBeforeRetention), len(liveSSTsAfterRetention), len(objects), time.Since(start))
}

func TestS3E2E_ChangeFeedRetentionPreservesKVState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := setupFakeS3StoreWithPrefix(t, fmt.Sprintf("change-feed-e2e-%d", time.Now().UnixNano()))
	defer store.Close()
	runChangeFeedRetentionE2E(t, ctx, store)
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

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	reader, err := OpenReader(ctx, store, ReaderOpenOptions{
		CacheDir:            t.TempDir(),
		BlockCacheSize:      64 << 10,
		ValidateSSTChecksum: true,
	})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "kv-lifecycle-writer"
	writerOpts.Flush.Interval = 0
	writerOpts.SST.BlockBytes = 1024
	writerOpts.SST.Compression = "none"

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
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
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
	opts.OwnerID = "kv-lifecycle-maintenance"
	opts.Compaction.L0SSTCount = 2
	opts.Compaction.BaseLevelBytes = 1 << 60
	opts.Compaction.TargetSSTBytes = 1 << 20
	opts.Compaction.BlockBytes = 1024
	opts.Compaction.Compression = "none"
	opts.GarbageCollection.GracePeriod = time.Nanosecond
	opts.GarbageCollection.DeleteBatchSize = len(oldSSTs)
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	stats, err := maintenance.RunOnce(ctx)
	if err != nil {
		t.Fatalf("maintenance run once: %v", err)
	}
	if stats.CompactionJobs == 0 {
		t.Fatalf("compaction did not run: %+v", stats)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("close maintenance: %v", err)
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
		if _, _, err := store.Read(ctx, store.SSTPath(id)); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("old SST %q read error=%v, want %v", id, err, blobstore.ErrNotFound)
		}
	}
	assertCurrentKVState(t, ctx, reader)

	freshReader, err := OpenReader(ctx, store, ReaderOpenOptions{
		CacheDir:            t.TempDir(),
		ValidateSSTChecksum: true,
	})
	if err != nil {
		t.Fatalf("open fresh reader: %v", err)
	}
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

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "change-feed-e2e-writer"
	writerOpts.Flush.Interval = 0
	writerOpts.ChangeFeed.Enabled = true
	writerOpts.SST.Compression = "none"

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
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	before, err := store.List(ctx, blobstore.ListOptions{Prefix: "changes/"})
	if err != nil {
		t.Fatalf("list change batches before cleanup: %v", err)
	}
	if len(before.Objects) != 2 {
		t.Fatalf("change batches before cleanup=%d, want 2", len(before.Objects))
	}

	time.Sleep(2 * time.Millisecond)
	changeFeed := DefaultChangeFeedRetentionPolicy()
	changeFeed.KeepFor = time.Millisecond
	// The newest retained entries are the maintenance fence claim and the
	// newest add-SST entry, so the newest change batch remains available.
	changeFeed.KeepAtLeastManifestEntries = 2
	changeFeed.DeleteGracePeriod = -1
	opts := DefaultMaintenanceOptions()
	opts.OwnerID = "change-feed-e2e-maintenance"
	opts.ChangeFeedRetention = &changeFeed

	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	stats, err := maintenance.RunOnce(ctx)
	if err != nil {
		t.Fatalf("maintenance run once: %v", err)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("close maintenance: %v", err)
	}
	if stats.ChangeFeed.EntriesRetired == 0 || stats.ChangeFeed.BatchesMarked != 1 || stats.ChangeFeed.BatchesDeleted != 1 {
		t.Fatalf("unexpected change-feed cleanup stats: %+v", stats.ChangeFeed)
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

	reader, err := OpenReader(ctx, store, ReaderOpenOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
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

	db, err := OpenDB(ctx, store, DBOptions{})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "s3-bench-writer"
	writerOpts.Memtable.TargetBytes = 4 << 10
	writerOpts.Flush.Interval = 0
	writerOpts.SST.BlockBytes = 4096
	writerOpts.SST.Compression = "none"

	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		b.Fatalf("open writer: %v", err)
	}

	maintenanceOpts := DefaultMaintenanceOptions()
	maintenanceOpts.OwnerID = "s3-bench-maintenance"
	maintenanceOpts.Every = 25 * time.Millisecond
	maintenanceOpts.Compaction.L0SSTCount = 8
	maintenanceOpts.Compaction.BaseLevelBytes = 1 << 60
	maintenanceOpts.Compaction.TargetSSTBytes = 64 << 10
	maintenanceOpts.Compaction.Compression = "none"

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
	if err := writer.Close(ctx); err != nil {
		b.Fatalf("close writer: %v", err)
	}
	if _, err := maintenance.RunOnce(ctx); err != nil {
		b.Fatalf("maintenance run once: %v", err)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "records/s")
}

func replayManifestForTest(t testing.TB, ctx context.Context, store *blobstore.Store) *Manifest {
	t.Helper()

	ms := manifest.NewStore(store)
	m, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	return m
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
