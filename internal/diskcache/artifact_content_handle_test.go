package diskcache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func publishArtifactContentForHandleTest(
	t *testing.T,
	kind ArtifactKind,
	data []byte,
) (*artifactContentTier, *artifactContentIndexEntry, artifactContentAddress, string) {
	t.Helper()
	root := t.TempDir()
	incomingDir := filepath.Join(root, "incoming")
	if err := os.MkdirAll(incomingDir, 0o700); err != nil {
		t.Fatalf("create incoming: %v", err)
	}
	desc := contentFillDescriptor(kind, "sst-a", data)
	staged := finishArtifactContentFill(t, incomingDir, desc, data)
	tier, err := newArtifactContentTier(kind, int64(len(data))*2)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	path := filepath.Join(root, staged.address.relativePath())
	removeFile := func(address artifactContentAddress) error {
		return removeArtifactContentFile(filepath.Join(root, address.relativePath()))
	}
	entry, admission, err := tier.publishPinned(
		staged.address, staged.size, func() error { return staged.publish(path) }, removeFile)
	if err != nil || admission != artifactContentAdmitted || entry == nil {
		t.Fatalf("publish: entry=%p admission=%d err=%v", entry, admission, err)
	}
	return tier, entry, staged.address, path
}

func TestArtifactContentPersistentSSTHandleOwnsPublicationReference(t *testing.T) {
	data := []byte("persistent SST bytes")
	tier, entry, address, path := publishArtifactContentForHandleTest(t, ArtifactSST, data)
	removeFile := func(got artifactContentAddress) error {
		if got != address {
			t.Fatalf("remove address=%v, want %v", got, address)
		}
		return removeArtifactContentFile(path)
	}

	handle, ok, err := tier.openPinned(entry, path, removeFile)
	if err != nil || !ok {
		t.Fatalf("open published entry: ok=%t err=%v", ok, err)
	}
	if !bytes.Equal(handle.bytes(), data) {
		t.Fatalf("handle bytes=%q, want %q", handle.bytes(), data)
	}
	if got := entry.refs; got != 1 {
		t.Fatalf("references after open=%d, want publication reference 1", got)
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close handle: %v", err)
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close handle again: %v", err)
	}
	if got := entry.refs; got != 0 {
		t.Fatalf("references after close=%d, want 0", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("resident file removed when ordinary handle closed: %v", err)
	}
}

func TestArtifactContentPersistentHandleDefersRemovalUntilClose(t *testing.T) {
	data := []byte("pinned SST bytes")
	tier, entry, address, path := publishArtifactContentForHandleTest(t, ArtifactSST, data)
	removeFile := func(got artifactContentAddress) error {
		if got != address {
			t.Fatalf("remove address=%v, want %v", got, address)
		}
		return removeArtifactContentFile(path)
	}
	handle, ok, err := tier.openPinned(entry, path, removeFile)
	if err != nil || !ok {
		t.Fatalf("open published entry: ok=%t err=%v", ok, err)
	}

	if detached, err := tier.detach(entry, removeFile); err != nil || !detached {
		t.Fatalf("detach pinned entry: detached=%t err=%v", detached, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("pinned file removed before handle close: %v", err)
	}
	if !bytes.Equal(handle.bytes(), data) {
		t.Fatalf("pinned bytes=%q, want %q", handle.bytes(), data)
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close handle: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("detached file remains after final close: %v", err)
	}
}

func TestArtifactContentPersistentBloomCorruptionIsRemoved(t *testing.T) {
	data := []byte("verified Bloom bytes")
	tier, entry, address, path := publishArtifactContentForHandleTest(t, ArtifactBloom, data)
	removeFile := func(got artifactContentAddress) error {
		if got != address {
			t.Fatalf("remove address=%v, want %v", got, address)
		}
		return removeArtifactContentFile(path)
	}
	if err := tier.release(entry, removeFile); err != nil {
		t.Fatalf("release publication reference: %v", err)
	}
	corrupt := bytes.Repeat([]byte{'x'}, len(data))
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatalf("corrupt Bloom: %v", err)
	}

	handle, ok, err := tier.acquire(address, int64(len(data)), path, removeFile)
	if handle != nil || ok || !errors.Is(err, ErrArtifactChecksumMismatch) {
		t.Fatalf("handle=%p ok=%t err=%v, want corruption miss", handle, ok, err)
	}
	if _, resident := tier.probe(address); resident {
		t.Fatal("corrupt Bloom remains searchable")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt Bloom file remains: %v", err)
	}
}

func TestArtifactContentPersistentSSTAcquireUsesPerHandleMappings(t *testing.T) {
	data := []byte("persistent SST bytes")
	tier, publishedEntry, address, path := publishArtifactContentForHandleTest(t, ArtifactSST, data)
	removeFile := func(artifactContentAddress) error { return removeArtifactContentFile(path) }
	if err := tier.release(publishedEntry, removeFile); err != nil {
		t.Fatalf("release publication reference: %v", err)
	}

	first, ok, err := tier.acquire(address, int64(len(data)), path, removeFile)
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%t err=%v", ok, err)
	}
	second, ok, err := tier.acquire(address, int64(len(data)), path, removeFile)
	if err != nil || !ok {
		t.Fatalf("second acquire: ok=%t err=%v", ok, err)
	}
	if !bytes.Equal(first.bytes(), data) || !bytes.Equal(second.bytes(), data) {
		t.Fatal("acquired mappings do not contain the published bytes")
	}
	if got := publishedEntry.refs; got != 2 {
		t.Fatalf("references=%d, want 2", got)
	}
	if err := first.close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	if err := second.close(); err != nil {
		t.Fatalf("close second: %v", err)
	}
	if got := publishedEntry.refs; got != 0 {
		t.Fatalf("references after closes=%d, want 0", got)
	}
}
