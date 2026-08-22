package diskcache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openArtifactContentCacheForTest(
	t testing.TB,
	dir string,
	sstMaxBytes int64,
	bloomMaxBytes int64,
) *artifactContentCache {
	t.Helper()
	cache, err := openArtifactContentCache(artifactContentCacheOptions{
		dir: dir, sstMaxBytes: sstMaxBytes, bloomMaxBytes: bloomMaxBytes,
	})
	if err != nil {
		t.Fatalf("open content cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.close() })
	return cache
}

func publishIntoArtifactContentCache(
	t *testing.T,
	cache *artifactContentCache,
	kind ArtifactKind,
	id string,
	data []byte,
) (*artifactContentIndexEntry, artifactContentAddress, string) {
	t.Helper()
	desc := contentFillDescriptor(kind, id, data)
	staged := finishArtifactContentFill(t, cache.incomingDir, desc, data)
	path := cache.path(staged.address)
	entry, admission, err := cache.tiers[kind].publishPinned(
		staged.address,
		staged.size,
		func() error { return staged.publish(path) },
		cache.remove,
	)
	if err != nil || admission != artifactContentAdmitted || entry == nil {
		t.Fatalf("publish content: entry=%p admission=%d err=%v", entry, admission, err)
	}
	return entry, staged.address, path
}

func TestArtifactContentCacheExclusivelyLocksDirectory(t *testing.T) {
	dir := t.TempDir()
	cache := openArtifactContentCacheForTest(t, dir, 1<<20, 1<<20)
	if _, err := openArtifactContentCache(artifactContentCacheOptions{
		dir: dir, sstMaxBytes: 1 << 20, bloomMaxBytes: 1 << 20,
	}); !errors.Is(err, ErrArtifactCacheLocked) {
		t.Fatalf("second open error=%v, want %v", err, ErrArtifactCacheLocked)
	}
	if err := cache.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := openArtifactContentCache(artifactContentCacheOptions{
		dir: dir, sstMaxBytes: 1 << 20, bloomMaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	if err := reopened.close(); err != nil {
		t.Fatalf("close reopened cache: %v", err)
	}
}

func TestArtifactContentCacheRecoversChecksumAddressedEntryUnpinned(t *testing.T) {
	dir := t.TempDir()
	data := []byte("persistent cache content")
	cache := openArtifactContentCacheForTest(t, dir, 1<<20, 1<<20)
	entry, address, path := publishIntoArtifactContentCache(t, cache, ArtifactSST, "sst-a", data)
	if err := cache.tiers[ArtifactSST].release(entry, cache.remove); err != nil {
		t.Fatalf("release publication reference: %v", err)
	}
	if err := cache.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openArtifactContentCacheForTest(t, dir, 1<<20, 1<<20)
	recovered, ok := reopened.tiers[ArtifactSST].probe(address)
	if !ok {
		t.Fatal("recovered content is not searchable")
	}
	if recovered.refs != 0 {
		t.Fatalf("recovered references=%d, want 0", recovered.refs)
	}
	handle, ok, err := reopened.tiers[ArtifactSST].acquire(
		address, int64(len(data)), path, reopened.remove)
	if err != nil || !ok {
		t.Fatalf("acquire recovered content: ok=%t err=%v", ok, err)
	}
	if !bytes.Equal(handle.bytes(), data) {
		t.Fatalf("recovered bytes=%q, want %q", handle.bytes(), data)
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close recovered handle: %v", err)
	}
}

func TestArtifactContentCacheRecoveryClearsIncomingFiles(t *testing.T) {
	dir := t.TempDir()
	cache := openArtifactContentCacheForTest(t, dir, 1<<20, 1<<20)
	partialPath := filepath.Join(cache.incomingDir, "sst-interrupted.part")
	if err := os.WriteFile(partialPath, []byte("partial"), 0o600); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	if err := cache.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openArtifactContentCacheForTest(t, dir, 1<<20, 1<<20)
	entries, err := os.ReadDir(reopened.incomingDir)
	if err != nil {
		t.Fatalf("read incoming: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("incoming entries=%d, want 0", len(entries))
	}
}

func TestArtifactContentCacheFormatMismatchRebuildsOnlyOwnedPaths(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(dir, artifactContentCacheMetadataName), []byte("unknown-format\n"), 0o600,
	); err != nil {
		t.Fatalf("write incompatible metadata: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "v99", "sst"), 0o700); err != nil {
		t.Fatalf("create old version: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "v99", "sst", "old"), []byte("old"), 0o600); err != nil {
		t.Fatalf("write old artifact: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "incoming"), 0o700); err != nil {
		t.Fatalf("create incoming: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "incoming", "partial"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	unrelatedPath := filepath.Join(dir, "operator-note")
	if err := os.WriteFile(unrelatedPath, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write unrelated file: %v", err)
	}

	cache := openArtifactContentCacheForTest(t, dir, 1<<20, 1<<20)
	if _, err := os.Stat(filepath.Join(dir, "v99")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incompatible version remains: %v", err)
	}
	incomingEntries, err := os.ReadDir(cache.incomingDir)
	if err != nil {
		t.Fatalf("read rebuilt incoming: %v", err)
	}
	if len(incomingEntries) != 0 {
		t.Fatalf("rebuilt incoming entries=%d, want 0", len(incomingEntries))
	}
	if got, err := os.ReadFile(unrelatedPath); err != nil || string(got) != "keep" {
		t.Fatalf("unrelated file=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, artifactContentCacheMetadataName)); err != nil || string(got) != artifactContentCacheFormat {
		t.Fatalf("metadata=%q err=%v", got, err)
	}
}

func TestArtifactContentCacheRecoveryEnforcesReducedBudget(t *testing.T) {
	dir := t.TempDir()
	firstData := []byte("first-0000")
	secondData := []byte("second-000")
	cache := openArtifactContentCacheForTest(t, dir, 20, 1)
	first, firstAddress, firstPath := publishIntoArtifactContentCache(
		t, cache, ArtifactSST, "sst-a", firstData)
	if err := cache.tiers[ArtifactSST].release(first, cache.remove); err != nil {
		t.Fatalf("release first: %v", err)
	}
	second, secondAddress, secondPath := publishIntoArtifactContentCache(
		t, cache, ArtifactSST, "sst-b", secondData)
	if err := cache.tiers[ArtifactSST].release(second, cache.remove); err != nil {
		t.Fatalf("release second: %v", err)
	}
	older := time.Unix(1_700_000_000, 0)
	newer := older.Add(time.Minute)
	if err := os.Chtimes(firstPath, older, older); err != nil {
		t.Fatalf("set first mtime: %v", err)
	}
	if err := os.Chtimes(secondPath, newer, newer); err != nil {
		t.Fatalf("set second mtime: %v", err)
	}
	if err := cache.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openArtifactContentCacheForTest(t, dir, 10, 1)
	if _, ok := reopened.tiers[ArtifactSST].probe(firstAddress); ok {
		t.Fatal("older recovered entry survived reduced budget")
	}
	if _, ok := reopened.tiers[ArtifactSST].probe(secondAddress); !ok {
		t.Fatal("newer recovered entry was not retained")
	}
	if _, err := os.Stat(firstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("older file remains after recovery trim: %v", err)
	}
	if _, err := os.Stat(secondPath); err != nil {
		t.Fatalf("newer file missing after recovery trim: %v", err)
	}
}
