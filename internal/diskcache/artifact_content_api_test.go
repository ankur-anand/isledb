package diskcache

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func openChecksumArtifactCacheForTest(t *testing.T, sstMaxBytes, bloomMaxBytes int64) *ArtifactCache {
	t.Helper()
	cache, err := OpenArtifactCache(ArtifactCacheOptions{
		Dir:           t.TempDir(),
		SSTMaxBytes:   sstMaxBytes,
		BloomMaxBytes: bloomMaxBytes,
	})
	if err != nil {
		t.Fatalf("open content cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

func TestContentCacheUsesChecksumAsPersistentIdentity(t *testing.T) {
	cache := openChecksumArtifactCacheForTest(t, 1<<20, 1<<20)
	data := []byte("shared immutable content")
	first := contentFillDescriptor(ArtifactSST, "database-a-sst", data)
	second := contentFillDescriptor(ArtifactSST, "database-b-sst", data)

	handle, admission, err := cache.AdmitBytes(first, data)
	if err != nil || admission != ArtifactAdmitted {
		t.Fatalf("first admission=%d err=%v", admission, err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("close first handle: %v", err)
	}

	handle, hit, err := cache.Acquire(second)
	if err != nil || !hit {
		t.Fatalf("acquire through second SST ID: hit=%t err=%v", hit, err)
	}
	if !bytes.Equal(handle.Bytes(), data) {
		t.Fatalf("acquired bytes=%q want=%q", handle.Bytes(), data)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("close second handle: %v", err)
	}
	if stats := cache.Stats(ArtifactSST); stats.ResidentEntries != 1 {
		t.Fatalf("resident entries=%d want=1", stats.ResidentEntries)
	}
}

func TestContentCacheDoesNotAliasSameSSTIDWithDifferentChecksums(t *testing.T) {
	cache := openChecksumArtifactCacheForTest(t, 1<<20, 1<<20)
	firstData := []byte("first immutable content")
	secondData := []byte("second immutable content")
	first := contentFillDescriptor(ArtifactSST, "same-sst-id", firstData)
	second := contentFillDescriptor(ArtifactSST, "same-sst-id", secondData)

	firstHandle, _, err := cache.AdmitBytes(first, firstData)
	if err != nil {
		t.Fatalf("admit first content: %v", err)
	}
	if err := firstHandle.Close(); err != nil {
		t.Fatalf("close first handle: %v", err)
	}
	if presence, err := cache.Probe(second); err != nil || presence != ArtifactAbsent {
		t.Fatalf("second checksum presence=%d err=%v, want absent", presence, err)
	}

	secondHandle, _, err := cache.AdmitBytes(second, secondData)
	if err != nil {
		t.Fatalf("admit second content: %v", err)
	}
	if err := secondHandle.Close(); err != nil {
		t.Fatalf("close second handle: %v", err)
	}
	if stats := cache.Stats(ArtifactSST); stats.ResidentEntries != 2 {
		t.Fatalf("resident entries=%d want=2", stats.ResidentEntries)
	}
}

func TestContentCachePurgeDefersPinnedFileDeletion(t *testing.T) {
	cache := openChecksumArtifactCacheForTest(t, 1<<20, 1<<20)
	data := []byte("pinned generation")
	desc := contentFillDescriptor(ArtifactSST, "sst-a", data)
	handle, _, err := cache.AdmitBytes(desc, data)
	if err != nil {
		t.Fatalf("admit content: %v", err)
	}
	address, err := artifactContentAddressFor(desc)
	if err != nil {
		t.Fatalf("content address: %v", err)
	}
	path := cache.inner.path(address)

	if err := cache.Purge(ArtifactSST); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if presence, err := cache.Probe(desc); err != nil || presence != ArtifactAbsent {
		t.Fatalf("post-purge presence=%d err=%v", presence, err)
	}
	if !bytes.Equal(handle.Bytes(), data) {
		t.Fatalf("pinned bytes changed after purge: %q", handle.Bytes())
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("pinned file disappeared before release: %v", err)
	}
	stats := cache.Stats(ArtifactSST)
	if stats.ResidentEntries != 1 || stats.PinnedEntries != 1 {
		t.Fatalf("pinned pending stats=%+v", stats)
	}

	if err := handle.Close(); err != nil {
		t.Fatalf("close pinned handle: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released file stat error=%v want not-exist", err)
	}
	stats = cache.Stats(ArtifactSST)
	if stats.ResidentEntries != 0 || stats.PinnedEntries != 0 {
		t.Fatalf("post-release stats=%+v", stats)
	}
}
