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
// persistent Reader cache. Focused diskcache tests inject syscall failures;
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
		assertArtifactCacheHealthyStats(t, reader, 1, 1)

		// A second DB using the same local cache directory must fail while the
		// first Reader owns it, then the directory must be reusable after Close.
		contender := openArtifactCacheTestDB(t, ctx, bucketURL, "persistent")
		if _, err := contender.OpenReader(ctx, DefaultReaderOpenOptions(cacheDir)); !errors.Is(err, diskcache.ErrArtifactCacheLocked) {
			t.Fatalf("contending Reader error=%v, want %v", err, diskcache.ErrArtifactCacheLocked)
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
			t, filepath.Join(cacheDir, "artifacts", "v1", "sst", "*", "*.sst"))
		corruptSingleArtifactFile(
			t, filepath.Join(cacheDir, "artifacts", "v1", "bloom", "*", "*.bloom"))
		healingReader := openArtifactCacheTestReader(t, ctx, db, cacheDir, 0)
		if !healingReader.bloomMayContain(ctx, meta, []byte("accounts/001")) {
			t.Fatal("recovered Bloom returned definitely absent")
		}
		assertArtifactCacheTestValue(t, ctx, healingReader, "accounts/001", "Ada")
		if stats := healingReader.SSTCacheStats(); stats.Corruptions != 1 {
			t.Fatalf("SST corruption stats=%+v", stats)
		}
		if stats := healingReader.BloomDiskCacheStats(); stats.Corruptions != 1 {
			t.Fatalf("Bloom corruption stats=%+v", stats)
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
		if !cacheOnlyReader.bloomMayContain(ctx, meta, []byte("accounts/001")) {
			t.Fatal("persisted Bloom returned definitely absent")
		}
		assertArtifactCacheTestValue(t, ctx, cacheOnlyReader, "accounts/001", "Ada")
		if stats := cacheOnlyReader.SSTCacheStats(); stats.Hits == 0 {
			t.Fatalf("cache-only SST stats=%+v", stats)
		}
		if stats := cacheOnlyReader.BloomDiskCacheStats(); stats.Hits == 0 {
			t.Fatalf("cache-only Bloom stats=%+v", stats)
		}
	})

	t.Run("format reset preserves unrelated files", func(t *testing.T) {
		cacheDir := filepath.Join(cacheRoot, "format-reset")
		artifactRoot := filepath.Join(cacheDir, "artifacts")
		legacyPath := filepath.Join(artifactRoot, "v1", "sst", "aa", "legacy.sst")
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

	t.Run("oversized SST bypass", func(t *testing.T) {
		cacheDir := filepath.Join(cacheRoot, "oversized")
		db := openArtifactCacheTestDB(t, ctx, bucketURL, "oversized")
		defer db.Close()
		writeArtifactCacheTestBatches(t, ctx, db, []map[string]string{{"key": "value"}})
		manifest, err := db.manifestStore.ReplayWithArtifactValidation(ctx)
		if err != nil || len(manifest.L0SSTs) != 1 {
			t.Fatalf("replay oversized manifest: SSTs=%d err=%v", len(manifest.L0SSTs), err)
		}
		meta := manifest.L0SSTs[0]
		reader := openArtifactCacheTestReader(t, ctx, db, cacheDir, meta.Size-1)
		defer reader.Close()
		assertArtifactCacheTestValue(t, ctx, reader, "key", "value")
		stats := reader.SSTCacheStats()
		if stats.EntryCount != 0 || stats.Bytes != 0 || stats.AdmissionBypasses != 1 ||
			stats.SyncFailures != 0 || stats.PublicationFailures != 0 {
			t.Fatalf("oversized bypass stats=%+v", stats)
		}
		assertArtifactCacheIncomingEmpty(t, cacheDir)
	})

	t.Run("pinned capacity bypass then admission", func(t *testing.T) {
		cacheDir := filepath.Join(cacheRoot, "pinned-capacity")
		db := openArtifactCacheTestDB(t, ctx, bucketURL, "pinned-capacity")
		defer db.Close()
		writeArtifactCacheTestBatches(t, ctx, db, []map[string]string{
			{"a": "first"},
			{"b": "second"},
		})
		manifest, err := db.manifestStore.ReplayWithArtifactValidation(ctx)
		if err != nil {
			t.Fatalf("replay pinned-capacity manifest: %v", err)
		}
		first := artifactCacheTestSSTForKey(t, manifest, []byte("a"))
		second := artifactCacheTestSSTForKey(t, manifest, []byte("b"))
		reader := openArtifactCacheTestReader(
			t, ctx, db, cacheDir, max(first.Size, second.Size))
		defer reader.Close()

		if err := reader.cacheSST(ctx, &first, db.store.SSTPath(first.ID)); err != nil {
			t.Fatalf("prime first SST: %v", err)
		}
		_, releaseFirst, hit, err := reader.acquireSST(first)
		if err != nil || !hit {
			t.Fatalf("pin first SST: hit=%t err=%v", hit, err)
		}
		defer releaseFirst()

		assertArtifactCacheTestValue(t, ctx, reader, "b", "second")
		stats := reader.SSTCacheStats()
		if stats.EntryCount != 1 || stats.Bytes != first.Size ||
			stats.PinnedBytes != first.Size || stats.AdmissionBypasses != 1 {
			t.Fatalf("pinned-capacity bypass stats=%+v", stats)
		}
		assertArtifactCacheIncomingEmpty(t, cacheDir)

		releaseFirst()
		assertArtifactCacheTestValue(t, ctx, reader, "b", "second")
		stats = reader.SSTCacheStats()
		if stats.EntryCount != 1 || stats.Bytes != second.Size ||
			stats.PinnedBytes != 0 || stats.Evictions != 1 || stats.AdmissionBypasses != 1 {
			t.Fatalf("post-release admission stats=%+v", stats)
		}
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
	sstMaxBytes int64,
) *Reader {
	t.Helper()
	opts := DefaultReaderOpenOptions(cacheDir)
	if sstMaxBytes > 0 {
		opts.SSTCacheSize = sstMaxBytes
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
		if keyInRange(key, meta.MinKey, meta.MaxKey) {
			return meta
		}
	}
	for _, level := range manifest.Levels {
		for _, meta := range level.SSTs {
			if keyInRange(key, meta.MinKey, meta.MaxKey) {
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

func assertArtifactCacheHealthyStats(
	t *testing.T,
	reader *Reader,
	wantSSTEntries int,
	wantBloomEntries int,
) {
	t.Helper()
	sst := reader.SSTCacheStats()
	if sst.EntryCount != wantSSTEntries || sst.AdmissionBypasses != 0 ||
		sst.SyncFailures != 0 || sst.PublicationFailures != 0 {
		t.Fatalf("SST cache stats=%+v", sst)
	}
	bloom := reader.BloomDiskCacheStats()
	if bloom.EntryCount != wantBloomEntries || bloom.AdmissionBypasses != 0 ||
		bloom.SyncFailures != 0 || bloom.PublicationFailures != 0 {
		t.Fatalf("Bloom cache stats=%+v", bloom)
	}
}

func assertArtifactCacheIncomingEmpty(t *testing.T, cacheDir string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(cacheDir, "artifacts", "incoming"))
	if err != nil {
		t.Fatalf("read incoming cache directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("incoming cache entries=%d, want 0", len(entries))
	}
}
