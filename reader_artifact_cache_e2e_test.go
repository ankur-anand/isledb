package isledb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/diskcache"
)

// TestReaderArtifactCacheLifecycle is the in-process end-to-end test for the
// persistent Reader cache. Focused diskcache tests cover failure handling;
// this test verifies the user-visible lifecycle through fake S3 and real
// temporary cache directories without requiring an integration environment.
func TestReaderArtifactCacheLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bucketURL := setupFakeS3BucketURL(t)
	cacheRoot := t.TempDir()

	t.Run("persistence corruption recovery and ownership", func(t *testing.T) {
		cacheDir := filepath.Join(cacheRoot, "persistent")
		db := openArtifactCacheTestDB(t, ctx, bucketURL, "persistent")
		defer db.Close()
		writeArtifactCacheTestBatches(t, ctx, db, []map[string]string{{
			"accounts/001": "Ada",
			"accounts/002": "Grace",
			"accounts/003": "Linus",
		}})

		manifest, err := db.manifestStore.ReplayWithArtifactValidation(ctx)
		if err != nil {
			t.Fatalf("replay manifest: %v", err)
		}
		if len(manifest.L0SSTs) != 1 {
			t.Fatalf("L0 SST count=%d, want 1", len(manifest.L0SSTs))
		}
		meta := manifest.L0SSTs[0]

		reader := openArtifactCacheTestReader(t, ctx, db, cacheDir, 0)
		assertArtifactCacheTestValue(t, ctx, reader, "accounts/001", "Ada")
		if rows, err := reader.ScanLimit(
			ctx, PrefixRange([]byte("accounts/")).Min,
			PrefixRange([]byte("accounts/")).Max, 10,
		); err != nil || len(rows) != 3 {
			t.Fatalf("initial scan rows=%v err=%v", rows, err)
		}
		reader.diskCache.Sync()
		assertArtifactCacheHealthyStats(t, reader, 1, 1)
		if stats := reader.BloomCacheStats(); stats.EntryCount != 1 || stats.Bytes > stats.MaxBytes {
			t.Fatalf("primed decoded Bloom L1 stats=%+v", stats)
		}

		// A second DB using the same local cache directory must fail while the
		// first Reader owns it, then the directory must be reusable after Close.
		contender := openArtifactCacheTestDB(t, ctx, bucketURL, "persistent")
		if _, err := contender.OpenReader(ctx, DefaultReaderOpenOptions(cacheDir)); !errors.Is(err, diskcache.ErrLocked) {
			t.Fatalf("contending Reader error=%v, want %v", err, diskcache.ErrLocked)
		}
		if err := contender.Close(); err != nil {
			t.Fatalf("close contender: %v", err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("close priming Reader: %v", err)
		}

		// Same-size corruption of both tiers must be removed and healed from
		// fake S3 rather than poisoning subsequent reads.
		corruptSingleArtifactFile(
			t, filepath.Join(cacheDir, "artifacts", "v4", "data", "*", "*.whole.*"))
		corruptSingleArtifactFile(
			t, filepath.Join(cacheDir, "artifacts", "v4", "meta", "*", "*.bloom.*"))
		healingReader := openArtifactCacheTestReader(t, ctx, db, cacheDir, 0)
		assertArtifactCacheRecoveredTiers(t, healingReader, 1, 1)
		if stats := healingReader.BloomCacheStats(); stats.EntryCount != 0 || stats.Bytes != 0 {
			t.Fatalf("decoded Bloom L1 survived Reader restart: %+v", stats)
		}
		if !healingReader.bloomMayContain(ctx, meta, []byte("accounts/001")) {
			t.Fatal("recovered Bloom returned definitely absent")
		}
		if stats := healingReader.BloomCacheStats(); stats.EntryCount != 1 || stats.Misses == 0 {
			t.Fatalf("decoded Bloom L1 did not repopulate: %+v", stats)
		}
		assertArtifactCacheTestValue(t, ctx, healingReader, "accounts/001", "Ada")
		if stats := healingReader.DiskCacheStats(); stats.SSTDrops != 1 || stats.Data.Corruptions != 0 {
			t.Fatalf("damaged SST stats=%+v", stats)
		}
		if stats := healingReader.DiskCacheStats().Meta; stats.Corruptions != 1 {
			t.Fatalf("meta tier corruption stats=%+v", stats)
		}
		if err := healingReader.Close(); err != nil {
			t.Fatalf("close healing Reader: %v", err)
		}

		// Remove the authoritative SST only after both local artifacts have
		// healed. Reopening proves completed entries survive Reader lifetime and
		// can serve both tiers without the object store.
		if err := db.store.Delete(ctx, db.store.SSTPath(meta.ID)); err != nil {
			t.Fatalf("delete origin SST: %v", err)
		}
		cacheOnlyReader := openArtifactCacheTestReader(t, ctx, db, cacheDir, 0)
		defer cacheOnlyReader.Close()
		assertArtifactCacheRecoveredTiers(t, cacheOnlyReader, 1, 1)
		if stats := cacheOnlyReader.BloomCacheStats(); stats.EntryCount != 0 || stats.Bytes != 0 {
			t.Fatalf("cache-only decoded Bloom L1 survived Reader restart: %+v", stats)
		}
		if !cacheOnlyReader.bloomMayContain(ctx, meta, []byte("accounts/001")) {
			t.Fatal("persisted Bloom returned definitely absent")
		}
		if stats := cacheOnlyReader.BloomCacheStats(); stats.EntryCount != 1 || stats.Misses == 0 {
			t.Fatalf("cache-only decoded Bloom L1 did not reload from L2: %+v", stats)
		}
		assertArtifactCacheTestValue(t, ctx, cacheOnlyReader, "accounts/001", "Ada")
		if stats := cacheOnlyReader.DiskCacheStats().Data; stats.Hits == 0 {
			t.Fatalf("cache-only data tier stats=%+v", stats)
		}
		if stats := cacheOnlyReader.DiskCacheStats().Meta; stats.Hits == 0 {
			t.Fatalf("cache-only meta tier stats=%+v", stats)
		}
	})

	t.Run("format reset preserves unrelated files", func(t *testing.T) {
		cacheDir := filepath.Join(cacheRoot, "format-reset")
		artifactRoot := filepath.Join(cacheDir, "artifacts")
		legacyPath := filepath.Join(artifactRoot, "v2", "sst", "aa", "legacy.sst")
		if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
			t.Fatalf("create legacy layout: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(artifactRoot, "CACHEMETA"),
			[]byte("isledb-artifact-cache-v1\n"), 0o600,
		); err != nil {
			t.Fatalf("write legacy marker: %v", err)
		}
		if err := os.WriteFile(legacyPath, []byte("legacy"), 0o600); err != nil {
			t.Fatalf("write legacy artifact: %v", err)
		}
		unrelatedPath := filepath.Join(artifactRoot, "operator-note")
		if err := os.WriteFile(unrelatedPath, []byte("keep"), 0o600); err != nil {
			t.Fatalf("write unrelated file: %v", err)
		}

		db := openArtifactCacheTestDB(t, ctx, bucketURL, "format-reset")
		defer db.Close()
		writeArtifactCacheTestBatches(t, ctx, db, []map[string]string{{"key": "value"}})
		reader := openArtifactCacheTestReader(t, ctx, db, cacheDir, 0)
		defer reader.Close()
		assertArtifactCacheTestValue(t, ctx, reader, "key", "value")
		if _, err := os.Stat(legacyPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("legacy artifact survived format reset: %v", err)
		}
		if data, err := os.ReadFile(unrelatedPath); err != nil || string(data) != "keep" {
			t.Fatalf("unrelated file=%q err=%v", data, err)
		}
	})

	t.Run("SST larger than the data tier is read in chunks", func(t *testing.T) {
		cacheDir := filepath.Join(cacheRoot, "oversized")
		db := openArtifactCacheTestDB(t, ctx, bucketURL, "oversized")
		defer db.Close()
		writeArtifactCacheTestBatches(t, ctx, db, []map[string]string{{"key": "value"}})
		manifest, err := db.manifestStore.ReplayWithArtifactValidation(ctx)
		if err != nil || len(manifest.L0SSTs) != 1 {
			t.Fatalf("replay oversized manifest: SSTs=%d err=%v", len(manifest.L0SSTs), err)
		}
		meta := manifest.L0SSTs[0]
		// The data tier, seven eighths of the budget, is smaller than the SST,
		// so it is never cached whole.
		reader := openArtifactCacheTestReader(t, ctx, db, cacheDir, meta.Size)
		defer reader.Close()
		assertArtifactCacheTestValue(t, ctx, reader, "key", "value")
		assertArtifactCacheTestValue(t, ctx, reader, "key", "value")
		reader.diskCache.Sync()
		stats := reader.DiskCacheStats().Data
		if stats.Bytes > stats.MaxBytes || stats.Failures != 0 || stats.Bypasses != 0 {
			t.Fatalf("oversized SST data tier stats=%+v", stats)
		}
		assertArtifactCacheIncomingEmpty(t, cacheDir)
	})

	t.Run("evicted SST is fetched again", func(t *testing.T) {
		cacheDir := filepath.Join(cacheRoot, "evict")
		db := openArtifactCacheTestDB(t, ctx, bucketURL, "evict")
		defer db.Close()
		writeArtifactCacheTestBatches(t, ctx, db, []map[string]string{
			{"a": "first"},
			{"b": "second"},
		})
		manifest, err := db.manifestStore.ReplayWithArtifactValidation(ctx)
		if err != nil {
			t.Fatalf("replay evict manifest: %v", err)
		}
		first := artifactCacheTestSSTForKey(t, manifest, []byte("a"))
		second := artifactCacheTestSSTForKey(t, manifest, []byte("b"))
		// A data tier that holds one SST but not two.
		one := max(first.Size, second.Size)
		reader := openArtifactCacheTestReader(t, ctx, db, cacheDir, (one*8+6)/7+8)
		defer reader.Close()

		assertArtifactCacheTestValue(t, ctx, reader, "a", "first")
		reader.diskCache.Sync()
		assertArtifactCacheTestValue(t, ctx, reader, "b", "second")
		reader.diskCache.Sync()
		stats := reader.DiskCacheStats().Data
		if stats.EntryCount != 1 || stats.Evictions != 1 || stats.Bypasses != 0 {
			t.Fatalf("evict stats=%+v", stats)
		}
		// The first SST is still open in memory; dropping it forces a reopen,
		// which fetches it again.
		reader.openSSTs.clear()
		reader.blockCache.clear()
		assertArtifactCacheTestValue(t, ctx, reader, "a", "first")
		reader.diskCache.Sync()
		assertArtifactCacheIncomingEmpty(t, cacheDir)
	})
}

func openArtifactCacheTestDB(
	t *testing.T,
	ctx context.Context,
	bucketURL string,
	prefix string,
) *DB {
	t.Helper()
	db, err := Open(ctx, bucketURL, DBOptions{Prefix: "reader-cache/" + prefix})
	if err != nil {
		t.Fatalf("open cache test DB %q: %v", prefix, err)
	}
	return db
}

func openArtifactCacheTestReader(
	t *testing.T,
	ctx context.Context,
	db *DB,
	cacheDir string,
	diskBytes int64,
) *Reader {
	t.Helper()
	opts := DefaultReaderOpenOptions(cacheDir)
	if diskBytes > 0 {
		opts.DiskCacheSize = diskBytes
	}
	reader, err := db.OpenReader(ctx, opts)
	if err != nil {
		t.Fatalf("open cache test Reader: %v", err)
	}
	return reader
}

func writeArtifactCacheTestBatches(
	t *testing.T,
	ctx context.Context,
	db *DB,
	batches []map[string]string,
) {
	t.Helper()
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("open cache test Writer: %v", err)
	}
	for _, batch := range batches {
		for key, value := range batch {
			if err := writer.Put(ctx, []byte(key), []byte(value)); err != nil {
				t.Fatalf("put %q: %v", key, err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush cache test batch: %v", err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close cache test Writer: %v", err)
	}
}

func artifactCacheTestSSTForKey(
	t *testing.T,
	manifest *manifestState,
	key []byte,
) sstMetadata {
	t.Helper()
	for _, meta := range manifest.L0SSTs {
		if keyInSSTRange(key, meta.MinKey, meta.MaxKey) {
			return meta
		}
	}
	for _, level := range manifest.Levels {
		for _, meta := range level.SSTs {
			if keyInSSTRange(key, meta.MinKey, meta.MaxKey) {
				return meta
			}
		}
	}
	t.Fatalf("no SST contains key %q", key)
	return sstMetadata{}
}

func assertArtifactCacheTestValue(
	t *testing.T,
	ctx context.Context,
	reader *Reader,
	key string,
	want string,
) {
	t.Helper()
	value, found, err := reader.Get(ctx, []byte(key))
	if err != nil || !found || !bytes.Equal(value, []byte(want)) {
		t.Fatalf("Get(%q) value=%q found=%t err=%v, want %q", key, value, found, err, want)
	}
}

// assertArtifactCacheHealthyStats checks the disk cache holds the expected
// entries: for small SSTs, one whole entry each in the data tier and one Bloom
// entry each in the meta tier.
func assertArtifactCacheHealthyStats(
	t *testing.T,
	reader *Reader,
	wantDataEntries int,
	wantMetaEntries int,
) {
	t.Helper()
	stats := reader.DiskCacheStats()
	if stats.Data.EntryCount != wantDataEntries || stats.Data.Bypasses != 0 ||
		stats.Data.Failures != 0 {
		t.Fatalf("data tier stats=%+v", stats.Data)
	}
	if stats.Meta.EntryCount != wantMetaEntries || stats.Meta.Bypasses != 0 ||
		stats.Meta.Failures != 0 {
		t.Fatalf("meta tier stats=%+v", stats.Meta)
	}
}

func assertArtifactCacheIncomingEmpty(t *testing.T, cacheDir string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(cacheDir, "artifacts", "v4", "incoming"))
	if err != nil {
		t.Fatalf("read incoming cache directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("incoming cache entries=%d, want 0", len(entries))
	}
}
