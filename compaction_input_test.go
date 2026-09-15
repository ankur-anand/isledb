package isledb

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"gocloud.dev/blob/memblob"
)

func TestStageCompactionSSTStreamsToScratchAndRemovesFile(t *testing.T) {
	ctx := context.Background()
	store := blobstore.New(memblob.OpenBucket(nil), "memory", "stage-compaction")
	data := []byte("complete immutable sst bytes")
	sum := sha256.Sum256(data)
	meta := sstMetadata{
		ID:       "1-1-1-1.sst",
		Size:     int64(len(data)),
		Checksum: fmt.Sprintf("sha256:%x", sum[:]),
	}
	if _, err := store.Write(ctx, store.SSTPath(meta.ID), data); err != nil {
		t.Fatalf("write SST: %v", err)
	}

	staged, err := stageCompactionSST(ctx, store, meta, t.TempDir(), true)
	if err != nil {
		t.Fatalf("stage SST: %v", err)
	}
	path := staged.path
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("scratch file before close: %v", err)
	}
	got := make([]byte, len(data))
	if err := staged.ReadAt(ctx, got, 0); err != nil {
		t.Fatalf("read staged SST: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("staged bytes=%q want=%q", got, data)
	}

	if err := staged.Close(); err != nil {
		t.Fatalf("close staged SST: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("scratch file after close: err=%v, want not exist", err)
	}
}

func TestStageCompactionSSTFailureRemovesPartialFile(t *testing.T) {
	ctx := context.Background()
	store := blobstore.New(memblob.OpenBucket(nil), "memory", "stage-compaction-failure")
	data := []byte("sst bytes")
	meta := sstMetadata{
		ID:       "1-1-1-1.sst",
		Size:     int64(len(data)),
		Checksum: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	}
	if _, err := store.Write(ctx, store.SSTPath(meta.ID), data); err != nil {
		t.Fatalf("write SST: %v", err)
	}
	scratch := t.TempDir()

	if _, err := stageCompactionSST(ctx, store, meta, scratch, true); err == nil {
		t.Fatal("stage SST with invalid checksum succeeded")
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatalf("read scratch directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("partial scratch files survived failure: %v", entries)
	}
}

func TestStageCompactionSSTCancelledBeforeDownloadCreatesNoFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scratch := t.TempDir()
	store := blobstore.New(memblob.OpenBucket(nil), "memory", "stage-compaction-cancel")

	if _, err := stageCompactionSST(ctx, store, sstMetadata{ID: "cancelled.sst", Size: 1}, scratch, false); err == nil {
		t.Fatal("stage SST with cancelled context succeeded")
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatalf("read scratch directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("scratch files created after cancellation: %v", entries)
	}
}

func TestVerifyCompactionSSTStreamsChecksum(t *testing.T) {
	ctx := context.Background()
	store := blobstore.New(memblob.OpenBucket(nil), "memory", "verify-compaction")
	data := []byte("immutable sst bytes verified while streaming")
	sum := sha256.Sum256(data)
	meta := sstMetadata{
		ID:       "1-1-1-1.sst",
		Size:     int64(len(data)),
		Checksum: fmt.Sprintf("sha256:%x", sum[:]),
	}
	if _, err := store.Write(ctx, store.SSTPath(meta.ID), data); err != nil {
		t.Fatalf("write SST: %v", err)
	}

	if err := verifyCompactionSST(ctx, store, meta); err != nil {
		t.Fatalf("verify streamed SST: %v", err)
	}
	meta.Checksum = fmt.Sprintf("sha256:%X", sum[:])
	if err := verifyCompactionSST(ctx, store, meta); err != nil {
		t.Fatalf("verify streamed SST with uppercase checksum: %v", err)
	}
	meta.Checksum = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if err := verifyCompactionSST(ctx, store, meta); err == nil {
		t.Fatal("verify streamed SST with invalid checksum succeeded")
	}
}

func TestVerifyCompactionSSTRejectsMissingManifestSize(t *testing.T) {
	store := blobstore.New(memblob.OpenBucket(nil), "memory", "verify-compaction-size")
	err := verifyCompactionSST(context.Background(), store, sstMetadata{ID: "missing-size.sst"})
	if err == nil {
		t.Fatal("verify streamed SST with missing size succeeded")
	}
}

func TestCompactionScratchWorkspaceHigherEpochRemovesAbandonedSession(t *testing.T) {
	base := t.TempDir()
	old, err := openCompactionScratchWorkspace(base, "database-a", compactionScratchMaintenance, 7)
	if err != nil {
		t.Fatalf("open old workspace: %v", err)
	}
	oldPath := old.Path()
	if err := os.WriteFile(filepath.Join(oldPath, "input.sst"), []byte("abandoned"), 0o600); err != nil {
		t.Fatalf("write abandoned input: %v", err)
	}

	current, err := openCompactionScratchWorkspace(base, "database-a", compactionScratchMaintenance, 8)
	if err != nil {
		t.Fatalf("open current workspace: %v", err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old workspace after higher epoch startup: err=%v, want not exist", err)
	}
	currentPath := current.Path()
	if _, err := os.Stat(currentPath); err != nil {
		t.Fatalf("current workspace: %v", err)
	}
	if err := current.Close(); err != nil {
		t.Fatalf("close current workspace: %v", err)
	}
	if _, err := os.Stat(currentPath); !os.IsNotExist(err) {
		t.Fatalf("current workspace after close: err=%v, want not exist", err)
	}
	if err := old.Close(); err != nil {
		t.Fatalf("close already-removed old workspace: %v", err)
	}
}

func TestCompactionScratchWorkspaceDelayedOlderEpochPreservesNewerSession(t *testing.T) {
	base := t.TempDir()
	newer, err := openCompactionScratchWorkspace(base, "database-a", compactionScratchMaintenance, 8)
	if err != nil {
		t.Fatalf("open newer workspace: %v", err)
	}
	marker := filepath.Join(newer.Path(), "active.sst")
	if err := os.WriteFile(marker, []byte("active"), 0o600); err != nil {
		t.Fatalf("write newer marker: %v", err)
	}

	older, err := openCompactionScratchWorkspace(base, "database-a", compactionScratchMaintenance, 7)
	if err != nil {
		t.Fatalf("open delayed older workspace: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("delayed older epoch removed newer workspace: %v", err)
	}
	if err := older.Close(); err != nil {
		t.Fatalf("close older workspace: %v", err)
	}
	if err := newer.Close(); err != nil {
		t.Fatalf("close newer workspace: %v", err)
	}
}

func TestCompactionScratchWorkspaceSeparatesFenceRoles(t *testing.T) {
	base := t.TempDir()
	maintenance, err := openCompactionScratchWorkspace(
		base, "database-a", compactionScratchMaintenance, 7)
	if err != nil {
		t.Fatalf("open maintenance workspace: %v", err)
	}
	marker := filepath.Join(maintenance.Path(), "active.sst")
	if err := os.WriteFile(marker, []byte("active"), 0o600); err != nil {
		t.Fatalf("write maintenance marker: %v", err)
	}

	standalone, err := openCompactionScratchWorkspace(
		base, "database-a", compactionScratchStandalone, 100)
	if err != nil {
		t.Fatalf("open standalone workspace: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("independent fence role removed maintenance workspace: %v", err)
	}
	if err := standalone.Close(); err != nil {
		t.Fatalf("close standalone workspace: %v", err)
	}
	if err := maintenance.Close(); err != nil {
		t.Fatalf("close maintenance workspace: %v", err)
	}
}

func TestCompactorScratchWorkspaceUsesStoreNamespaceAndCloses(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	store := blobstore.NewMemory("scratch-workspace-database")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	opts := defaultCompactorOptions()
	opts.ScratchDir = base

	c, err := newCompactor(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("new compactor: %v", err)
	}
	workspace := c.opts.ScratchDir
	wantRoot := filepath.Join(base, compactionScratchRootPrefix, compactionScratchStandalone, store.ScratchNamespace())
	if filepath.Dir(workspace) != wantRoot {
		t.Fatalf("workspace=%q want child of %q", workspace, wantRoot)
	}
	if err := os.WriteFile(filepath.Join(workspace, "input.sst"), []byte("staged"), 0o600); err != nil {
		t.Fatalf("write staged input: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close compactor: %v", err)
	}
	if _, err := os.Stat(workspace); !os.IsNotExist(err) {
		t.Fatalf("workspace after compactor close: err=%v, want not exist", err)
	}
}

func TestCompactorFenceTakeoverRemovesOlderScratchWorkspace(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	store := blobstore.NewMemory("scratch-workspace-takeover")
	defer store.Close()
	manifestStore := newManifestStore(store, nil)
	opts := defaultCompactorOptions()
	opts.ScratchDir = base

	old, err := newCompactor(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("new old compactor: %v", err)
	}
	oldPath := old.opts.ScratchDir
	if err := os.WriteFile(filepath.Join(oldPath, "abandoned.sst"), []byte("staged"), 0o600); err != nil {
		t.Fatalf("write abandoned input: %v", err)
	}

	current, err := newCompactor(ctx, store, manifestStore, opts)
	if err != nil {
		t.Fatalf("new current compactor: %v", err)
	}
	if current.fenceToken.Epoch <= old.fenceToken.Epoch {
		t.Fatalf("current epoch=%d, want greater than old epoch=%d",
			current.fenceToken.Epoch, old.fenceToken.Epoch)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old workspace after fence takeover: err=%v, want not exist", err)
	}
	if _, err := os.Stat(current.opts.ScratchDir); err != nil {
		t.Fatalf("current workspace after fence takeover: %v", err)
	}

	if err := old.Close(ctx); err != nil {
		t.Fatalf("close old compactor: %v", err)
	}
	currentPath := current.opts.ScratchDir
	if err := current.Close(ctx); err != nil {
		t.Fatalf("close current compactor: %v", err)
	}
	if _, err := os.Stat(currentPath); !os.IsNotExist(err) {
		t.Fatalf("current workspace after close: err=%v, want not exist", err)
	}
}
