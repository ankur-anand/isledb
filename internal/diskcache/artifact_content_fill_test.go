package diskcache

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func contentFillDescriptor(kind ArtifactKind, id string, data []byte) ArtifactDescriptor {
	checksum := sha256.Sum256(data)
	return ArtifactDescriptor{
		Key:      ArtifactKey{Kind: kind, SSTID: id},
		Size:     int64(len(data)),
		Checksum: fmt.Sprintf("sha256:%x", checksum),
	}
}

func finishArtifactContentFill(
	t *testing.T,
	incomingDir string,
	desc ArtifactDescriptor,
	data []byte,
) *artifactStagedContent {
	t.Helper()
	fill, err := newArtifactContentFill(incomingDir, desc)
	if err != nil {
		t.Fatalf("new fill: %v", err)
	}
	t.Cleanup(func() { _ = fill.abort() })
	if _, err := fill.Write(data); err != nil {
		t.Fatalf("write fill: %v", err)
	}
	staged, err := fill.finish()
	if err != nil {
		t.Fatalf("finish fill: %v", err)
	}
	t.Cleanup(func() { _ = staged.discard() })
	return staged
}

func TestArtifactContentFillRejectsOverflowBeforeWriting(t *testing.T) {
	incomingDir := t.TempDir()
	data := []byte("verified")
	fill, err := newArtifactContentFill(
		incomingDir, contentFillDescriptor(ArtifactSST, "sst-a", data))
	if err != nil {
		t.Fatalf("new fill: %v", err)
	}
	defer fill.abort()

	if written, err := fill.Write(append(data, '!')); written != 0 || !errors.Is(err, ErrArtifactSizeMismatch) {
		t.Fatalf("overflow write: written=%d err=%v", written, err)
	}
	info, err := os.Stat(fill.path)
	if err != nil {
		t.Fatalf("stat incoming file: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("incoming size=%d, want 0", info.Size())
	}
}

func TestArtifactContentFillVerificationFailureRemovesIncomingFile(t *testing.T) {
	incomingDir := t.TempDir()
	data := []byte("verified")
	desc := contentFillDescriptor(ArtifactSST, "sst-a", data)
	desc.Checksum = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fill, err := newArtifactContentFill(incomingDir, desc)
	if err != nil {
		t.Fatalf("new fill: %v", err)
	}
	path := fill.path
	if _, err := fill.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := fill.finish(); !errors.Is(err, ErrArtifactChecksumMismatch) {
		t.Fatalf("finish error=%v, want checksum mismatch", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incoming file still exists: %v", err)
	}
}

func TestArtifactContentFillShortResponseRemovesIncomingFile(t *testing.T) {
	incomingDir := t.TempDir()
	data := []byte("complete response")
	fill, err := newArtifactContentFill(
		incomingDir, contentFillDescriptor(ArtifactSST, "sst-a", data))
	if err != nil {
		t.Fatalf("new fill: %v", err)
	}
	path := fill.path
	if _, err := fill.Write(data[:len(data)-1]); err != nil {
		t.Fatalf("write short response: %v", err)
	}
	if _, err := fill.finish(); !errors.Is(err, ErrArtifactSizeMismatch) {
		t.Fatalf("finish error=%v, want size mismatch", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incoming file still exists: %v", err)
	}
}

func TestArtifactContentOversizedBypassUsesTransientIncomingFile(t *testing.T) {
	root := t.TempDir()
	incomingDir := filepath.Join(root, "incoming")
	if err := os.MkdirAll(incomingDir, 0o700); err != nil {
		t.Fatalf("create incoming: %v", err)
	}
	data := []byte("larger than the resident tier")
	desc := contentFillDescriptor(ArtifactSST, "sst-a", data)
	staged := finishArtifactContentFill(t, incomingDir, desc, data)
	tempPath := staged.path
	tier, err := newArtifactContentTier(ArtifactSST, int64(len(data)-1))
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	finalPath := filepath.Join(root, staged.address.relativePath())

	entry, admission, err := tier.publishPinned(
		staged.address,
		staged.size,
		func() error { return staged.publish(finalPath) },
		func(address artifactContentAddress) error {
			return removeArtifactContentFile(filepath.Join(root, address.relativePath()))
		},
	)
	if err != nil {
		t.Fatalf("admit staged content: %v", err)
	}
	if entry != nil || admission != artifactContentBypassedOversized {
		t.Fatalf("entry=%p admission=%d, want oversized bypass", entry, admission)
	}
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("bypassed temp file missing before transient open: %v", err)
	}
	if _, err := os.Stat(finalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bypassed content unexpectedly published: %v", err)
	}

	prepared, err := staged.prepareHandle()
	if err != nil {
		t.Fatalf("prepare transient: %v", err)
	}
	handle, err := staged.takeTransient(prepared)
	if err != nil {
		_ = prepared.close()
		t.Fatalf("take transient: %v", err)
	}
	if !bytes.Equal(handle.bytes(), data) {
		t.Fatalf("transient bytes=%q, want %q", handle.bytes(), data)
	}
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("transient temp file missing while handle is open: %v", err)
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close transient: %v", err)
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close transient again: %v", err)
	}
	if _, err := os.Stat(tempPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("transient temp file remains after close: %v", err)
	}
}

func TestArtifactContentSyncFailureCanStillServeTransient(t *testing.T) {
	root := t.TempDir()
	incomingDir := filepath.Join(root, "incoming")
	if err := os.MkdirAll(incomingDir, 0o700); err != nil {
		t.Fatalf("create incoming: %v", err)
	}
	data := []byte("verified but not durable")
	desc := contentFillDescriptor(ArtifactBloom, "sst-a", data)
	fill, err := newArtifactContentFill(incomingDir, desc)
	if err != nil {
		t.Fatalf("new fill: %v", err)
	}
	if _, err := fill.Write(data); err != nil {
		t.Fatalf("write fill: %v", err)
	}
	wantErr := errors.New("sync failed")
	fill.syncFile = func(*os.File) error { return wantErr }
	staged, err := fill.finish()
	if staged == nil || !errors.Is(err, wantErr) {
		t.Fatalf("finish: staged=%p err=%v, want staged sync failure", staged, err)
	}
	tempPath := staged.path
	prepared, prepareErr := staged.prepareHandle()
	if prepareErr != nil {
		t.Fatalf("prepare transient after sync failure: %v", prepareErr)
	}
	handle, openErr := staged.takeTransient(prepared)
	if openErr != nil {
		_ = prepared.close()
		t.Fatalf("take transient after sync failure: %v", openErr)
	}
	if !bytes.Equal(handle.bytes(), data) {
		t.Fatalf("transient bytes=%q, want %q", handle.bytes(), data)
	}
	if closeErr := handle.close(); closeErr != nil {
		t.Fatalf("close transient: %v", closeErr)
	}
	if _, statErr := os.Stat(tempPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("transient temp file remains after close: %v", statErr)
	}
}

func TestArtifactContentFillSyncRunsBeforePublicationLock(t *testing.T) {
	root := t.TempDir()
	incomingDir := filepath.Join(root, "incoming")
	if err := os.MkdirAll(incomingDir, 0o700); err != nil {
		t.Fatalf("create incoming: %v", err)
	}
	data := []byte("sync outside publication lock")
	fill, err := newArtifactContentFill(
		incomingDir, contentFillDescriptor(ArtifactSST, "sst-a", data))
	if err != nil {
		t.Fatalf("new fill: %v", err)
	}
	if _, err := fill.Write(data); err != nil {
		t.Fatalf("write fill: %v", err)
	}
	tier, err := newArtifactContentTier(ArtifactSST, int64(len(data)))
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	var syncCalls atomic.Int64
	fill.syncFile = func(*os.File) error {
		syncCalls.Add(1)
		if !tier.publishMu.TryLock() {
			t.Fatal("file sync ran while publication mutex was held")
		}
		tier.publishMu.Unlock()
		return nil
	}
	staged, err := fill.finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	defer staged.discard()

	finalPath := filepath.Join(root, staged.address.relativePath())
	entry, admission, err := tier.publishPinned(
		staged.address,
		staged.size,
		func() error {
			if got := syncCalls.Load(); got != 1 {
				t.Fatalf("sync calls before publication=%d, want 1", got)
			}
			return staged.publish(finalPath)
		},
		func(address artifactContentAddress) error {
			return removeArtifactContentFile(filepath.Join(root, address.relativePath()))
		},
	)
	if err != nil || admission != artifactContentAdmitted || entry == nil {
		t.Fatalf("publish: entry=%p admission=%d err=%v", entry, admission, err)
	}
	if got := syncCalls.Load(); got != 1 {
		t.Fatalf("sync calls=%d, want 1", got)
	}
}

func TestArtifactContentAdmittedFillPublishesChecksumPath(t *testing.T) {
	root := t.TempDir()
	incomingDir := filepath.Join(root, "incoming")
	if err := os.MkdirAll(incomingDir, 0o700); err != nil {
		t.Fatalf("create incoming: %v", err)
	}
	data := []byte("persistent content")
	desc := contentFillDescriptor(ArtifactSST, "sst-a", data)
	staged := finishArtifactContentFill(t, incomingDir, desc, data)
	tempPath := staged.path
	tier, err := newArtifactContentTier(ArtifactSST, int64(len(data)))
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	finalPath := filepath.Join(root, staged.address.relativePath())

	entry, admission, err := tier.publishPinned(
		staged.address,
		staged.size,
		func() error { return staged.publish(finalPath) },
		func(address artifactContentAddress) error {
			return removeArtifactContentFile(filepath.Join(root, address.relativePath()))
		},
	)
	if err != nil || admission != artifactContentAdmitted || entry == nil {
		t.Fatalf("entry=%p admission=%d err=%v", entry, admission, err)
	}
	if _, err := os.Stat(tempPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incoming file remains after publication: %v", err)
	}
	got, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("read published file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("published bytes=%q, want %q", got, data)
	}
}
