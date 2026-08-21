package diskcache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func commitThroughArtifactContentCache(
	t *testing.T,
	cache *artifactContentCache,
	desc ArtifactDescriptor,
	data []byte,
) (*artifactContentCacheHandle, artifactContentAdmission) {
	t.Helper()
	fill, err := cache.beginFill(desc)
	if err != nil {
		t.Fatalf("begin fill: %v", err)
	}
	t.Cleanup(func() { _ = fill.abort() })
	if _, err := fill.Write(data); err != nil {
		t.Fatalf("write fill: %v", err)
	}
	handle, admission, err := fill.commit()
	if err != nil {
		t.Fatalf("commit fill: admission=%d err=%v", admission, err)
	}
	if handle == nil {
		t.Fatalf("commit fill: admission=%d returned nil handle", admission)
	}
	t.Cleanup(func() { _ = handle.close() })
	return handle, admission
}

func TestArtifactContentCacheFacadeProbeDoesNotChangeReadCounters(t *testing.T) {
	cache := openArtifactContentCacheForTest(t, t.TempDir(), 1<<20, 1<<20)
	data := []byte("facade SST content")
	desc := contentFillDescriptor(ArtifactSST, "sst-a", data)
	handle, admission := commitThroughArtifactContentCache(t, cache, desc, data)
	if admission != artifactContentAdmitted {
		t.Fatalf("admission=%d, want admitted", admission)
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close publication handle: %v", err)
	}

	before := cache.stats(ArtifactSST)
	for range 3 {
		resident, err := cache.probe(desc)
		if err != nil || !resident {
			t.Fatalf("probe: resident=%t err=%v", resident, err)
		}
	}
	afterProbe := cache.stats(ArtifactSST)
	if afterProbe.Hits != before.Hits || afterProbe.Misses != before.Misses {
		t.Fatalf("probe changed read counters: before=%+v after=%+v", before, afterProbe)
	}

	acquired, hit, err := cache.acquire(desc)
	if err != nil || !hit {
		t.Fatalf("acquire: hit=%t err=%v", hit, err)
	}
	if !bytes.Equal(acquired.bytes(), data) {
		t.Fatalf("acquired bytes=%q, want %q", acquired.bytes(), data)
	}
	if err := acquired.close(); err != nil {
		t.Fatalf("close acquired handle: %v", err)
	}
	afterAcquire := cache.stats(ArtifactSST)
	if afterAcquire.Hits != before.Hits+1 || afterAcquire.Misses != before.Misses {
		t.Fatalf("acquire counters: before=%+v after=%+v", before, afterAcquire)
	}
}

func TestArtifactContentCacheFacadeTracksTransientBypassLifetime(t *testing.T) {
	data := []byte("oversized transient content")
	cache := openArtifactContentCacheForTest(t, t.TempDir(), int64(len(data)-1), 1)
	desc := contentFillDescriptor(ArtifactSST, "sst-a", data)
	fill, err := cache.beginFill(desc)
	if err != nil {
		t.Fatalf("begin fill: %v", err)
	}
	if _, err := fill.Write(data); err != nil {
		t.Fatalf("write fill: %v", err)
	}
	duringFill := cache.stats(ArtifactSST)
	if duringFill.IncomingFiles != 1 || duringFill.IncomingBytes != int64(len(data)) {
		t.Fatalf("incoming stats=%+v", duringFill)
	}

	handle, admission, err := fill.commit()
	if err != nil || admission != artifactContentBypassedOversized {
		t.Fatalf("commit: handle=%p admission=%d err=%v", handle, admission, err)
	}
	if !bytes.Equal(handle.bytes(), data) {
		t.Fatalf("transient bytes=%q, want %q", handle.bytes(), data)
	}
	afterCommit := cache.stats(ArtifactSST)
	if afterCommit.IncomingFiles != 0 || afterCommit.IncomingBytes != 0 ||
		afterCommit.TransientFiles != 1 || afterCommit.TransientBytes != int64(len(data)) ||
		afterCommit.BypassedOversized != 1 {
		t.Fatalf("post-commit stats=%+v", afterCommit)
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close transient: %v", err)
	}
	afterClose := cache.stats(ArtifactSST)
	if afterClose.TransientFiles != 0 || afterClose.TransientBytes != 0 {
		t.Fatalf("post-close transient stats=%+v", afterClose)
	}
	entries, err := os.ReadDir(cache.incomingDir)
	if err != nil {
		t.Fatalf("read incoming: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("incoming entries=%d, want 0", len(entries))
	}
}

func TestArtifactContentCacheFacadePublicationFailureStillReturnsTransient(t *testing.T) {
	cache := openArtifactContentCacheForTest(t, t.TempDir(), 4, 1)
	firstData := []byte("old1")
	firstDesc := contentFillDescriptor(ArtifactSST, "sst-a", firstData)
	firstHandle, _ := commitThroughArtifactContentCache(t, cache, firstDesc, firstData)
	if err := firstHandle.close(); err != nil {
		t.Fatalf("close first handle: %v", err)
	}
	firstAddress, err := artifactContentAddressFor(firstDesc)
	if err != nil {
		t.Fatalf("first address: %v", err)
	}
	firstPath := cache.path(firstAddress)
	if err := os.Remove(firstPath); err != nil {
		t.Fatalf("remove first file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(firstPath, "child"), 0o700); err != nil {
		t.Fatalf("replace victim with non-empty directory: %v", err)
	}

	secondData := []byte("new2")
	secondDesc := contentFillDescriptor(ArtifactSST, "sst-b", secondData)
	secondHandle, admission := commitThroughArtifactContentCache(t, cache, secondDesc, secondData)
	if admission != artifactContentBypassedPublicationFailure {
		t.Fatalf("admission=%d, want publication-failure bypass", admission)
	}
	if !bytes.Equal(secondHandle.bytes(), secondData) {
		t.Fatalf("transient bytes=%q, want %q", secondHandle.bytes(), secondData)
	}
	stats := cache.stats(ArtifactSST)
	if stats.BypassedPublicationFailure != 1 || stats.TransientFiles != 1 {
		t.Fatalf("publication-failure stats=%+v", stats)
	}
}

func TestArtifactContentCacheFacadeCloseRetainsLockUntilActivitiesFinish(t *testing.T) {
	dir := t.TempDir()
	cache := openArtifactContentCacheForTest(t, dir, 1<<20, 1<<20)
	desc := contentFillDescriptor(ArtifactSST, "sst-a", []byte("active"))
	fill, err := cache.beginFill(desc)
	if err != nil {
		t.Fatalf("begin fill: %v", err)
	}
	if err := cache.close(); err != nil {
		t.Fatalf("close with active fill: %v", err)
	}
	if _, err := openArtifactContentCache(artifactContentCacheOptions{
		dir: dir, sstMaxBytes: 1 << 20, bloomMaxBytes: 1 << 20,
	}); !errors.Is(err, ErrArtifactCacheLocked) {
		t.Fatalf("open during active fill error=%v, want %v", err, ErrArtifactCacheLocked)
	}
	if err := fill.abort(); err != nil {
		t.Fatalf("abort fill: %v", err)
	}

	reopened, err := openArtifactContentCache(artifactContentCacheOptions{
		dir: dir, sstMaxBytes: 1 << 20, bloomMaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("reopen after fill abort: %v", err)
	}
	if err := reopened.close(); err != nil {
		t.Fatalf("close reopened cache: %v", err)
	}
}
