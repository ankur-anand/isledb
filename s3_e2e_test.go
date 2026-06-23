package isledb

import (
	"bytes"
	"context"
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
	writerOpts.Memtable.MaxFrozen = 4
	writerOpts.Flush.Interval = 0
	writerOpts.SST.BlockBytes = 1024
	writerOpts.SST.Compression = "none"

	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.OwnerID = "s3-e2e-compactor"
	compactorOpts.Trigger.CheckInterval = 10 * time.Millisecond
	compactorOpts.Trigger.L0SSTCount = 4
	compactorOpts.Trigger.MinSources = 1 << 20
	compactorOpts.Trigger.MaxSources = 1 << 20
	compactorOpts.Output.TargetSSTBytes = 2 * 1024
	compactorOpts.Output.BlockBytes = 1024
	compactorOpts.Output.Compression = "none"

	compactor, err := db.OpenCompactor(ctx, compactorOpts)
	if err != nil {
		t.Fatalf("open compactor: %v", err)
	}
	if err := compactor.Start(ctx); err != nil {
		t.Fatalf("start compactor: %v", err)
	}

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

	// Make the compaction boundary deterministic. The background compactor is
	// already running while writes happen; RunOnce drains any remaining L0 work.
	if err := compactor.RunOnce(ctx); err != nil {
		t.Fatalf("compactor run once: %v", err)
	}
	if err := compactor.Close(ctx); err != nil {
		t.Fatalf("close compactor: %v", err)
	}

	compacted := replayManifestForTest(t, ctx, store)
	if compacted.SortedRunCount() == 0 {
		t.Fatalf("expected compaction to create sorted runs, got l0=%d sorted_runs=0", compacted.L0SSTCount())
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
	retentionOpts := DefaultRetentionCompactorOptions()
	retentionOpts.Mode = CompactByAge
	retentionOpts.RetentionPeriod = time.Nanosecond
	retentionOpts.RetentionCount = 1
	retentionOpts.CheckInterval = time.Hour
	retentionOpts.OnCleanup = func(stats CleanupStats) {
		cleanup = stats
	}

	retention, err := db.OpenRetentionCompactor(ctx, retentionOpts)
	if err != nil {
		t.Fatalf("open retention compactor: %v", err)
	}
	if err := retention.RunOnce(ctx); err != nil {
		t.Fatalf("retention run once: %v", err)
	}
	if err := retention.Close(ctx); err != nil {
		t.Fatalf("close retention compactor: %v", err)
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

	compactorOpts := DefaultCompactorOptions()
	compactorOpts.OwnerID = "s3-bench-compactor"
	compactorOpts.Trigger.CheckInterval = 25 * time.Millisecond
	compactorOpts.Trigger.L0SSTCount = 8
	compactorOpts.Trigger.MinSources = 1 << 20
	compactorOpts.Trigger.MaxSources = 1 << 20
	compactorOpts.Output.TargetSSTBytes = 64 << 10
	compactorOpts.Output.Compression = "none"

	compactor, err := db.OpenCompactor(ctx, compactorOpts)
	if err != nil {
		b.Fatalf("open compactor: %v", err)
	}
	if err := compactor.Start(ctx); err != nil {
		b.Fatalf("start compactor: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		_ = compactor.Close(closeCtx)
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
	if err := compactor.RunOnce(ctx); err != nil {
		b.Fatalf("compactor run once: %v", err)
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
