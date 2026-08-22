package diskcache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestArtifactContentFormatResetSyncsBeforePublishingMarker(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, artifactContentCacheMetadataName)
	const previousMarker = "isledb-artifact-cache-v1\n"
	if err := os.WriteFile(metadataPath, []byte(previousMarker), 0o600); err != nil {
		t.Fatalf("write previous marker: %v", err)
	}
	oldPath := filepath.Join(dir, artifactContentCacheVersionDir, "sst", "aa", "old.sst")
	if err := os.MkdirAll(filepath.Dir(oldPath), 0o700); err != nil {
		t.Fatalf("create previous layout: %v", err)
	}
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatalf("write previous artifact: %v", err)
	}

	syncCalls := 0
	cache, err := openArtifactContentCache(artifactContentCacheOptions{
		dir: dir, sstMaxBytes: 1 << 20, bloomMaxBytes: 1 << 20,
		syncDirectory: func(path string) error {
			syncCalls++
			if path != dir {
				t.Fatalf("sync path=%q, want %q", path, dir)
			}
			if _, statErr := os.Stat(filepath.Join(dir, artifactContentCacheVersionDir)); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("old layout still exists before root sync: %v", statErr)
			}
			marker, readErr := os.ReadFile(metadataPath)
			if readErr != nil || string(marker) != previousMarker {
				t.Fatalf("marker changed before root sync: marker=%q err=%v", marker, readErr)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("open rebuilt cache: %v", err)
	}
	defer cache.close()
	if syncCalls != 1 {
		t.Fatalf("directory sync calls=%d, want 1", syncCalls)
	}
	marker, err := os.ReadFile(metadataPath)
	if err != nil || string(marker) != artifactContentCacheFormat {
		t.Fatalf("new marker=%q err=%v", marker, err)
	}
	if stats := cache.stats(ArtifactSST); stats.RecoveredEntries != 0 {
		t.Fatalf("old checksum scheme was adopted: %+v", stats)
	}
}

func TestArtifactContentRecoveryRejectsNoncanonicalArtifacts(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(dir, artifactContentCacheMetadataName),
		[]byte(artifactContentCacheFormat),
		0o600,
	); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	tierDir := filepath.Join(dir, artifactContentCacheVersionDir, ArtifactSST.dirName())
	if err := os.MkdirAll(tierDir, 0o700); err != nil {
		t.Fatalf("create tier: %v", err)
	}

	validData := []byte("valid recovered artifact")
	validDesc := contentFillDescriptor(ArtifactSST, "valid", validData)
	validAddress, err := artifactContentAddressFor(validDesc)
	if err != nil {
		t.Fatalf("valid address: %v", err)
	}
	validPath := filepath.Join(dir, artifactContentCacheVersionDir, validAddress.relativePath())
	writeRecoveryTestFile(t, validPath, validData)

	invalidName := filepath.Join(tierDir, "aa", "not-a-checksum.sst")
	writeRecoveryTestFile(t, invalidName, []byte("invalid"))

	wrongShardDesc := contentFillDescriptor(ArtifactSST, "wrong-shard", []byte("wrong shard"))
	wrongShardAddress, err := artifactContentAddressFor(wrongShardDesc)
	if err != nil {
		t.Fatalf("wrong-shard address: %v", err)
	}
	wrongShardName := filepath.Base(wrongShardAddress.relativePath())
	wrongShard := "00"
	if wrongShardAddress.checksum[0] == 0 {
		wrongShard = "ff"
	}
	wrongShardPath := filepath.Join(tierDir, wrongShard, wrongShardName)
	writeRecoveryTestFile(t, wrongShardPath, []byte("wrong shard"))

	nestedDesc := contentFillDescriptor(ArtifactSST, "nested", []byte("nested"))
	nestedAddress, err := artifactContentAddressFor(nestedDesc)
	if err != nil {
		t.Fatalf("nested address: %v", err)
	}
	nestedCanonicalPath := filepath.Join(
		dir, artifactContentCacheVersionDir, nestedAddress.relativePath())
	nestedPath := filepath.Join(
		filepath.Dir(nestedCanonicalPath), "nested", filepath.Base(nestedCanonicalPath))
	writeRecoveryTestFile(t, nestedPath, []byte("nested"))

	nonRegularDesc := contentFillDescriptor(ArtifactSST, "directory", []byte("directory"))
	nonRegularAddress, err := artifactContentAddressFor(nonRegularDesc)
	if err != nil {
		t.Fatalf("non-regular address: %v", err)
	}
	nonRegularPath := filepath.Join(
		dir, artifactContentCacheVersionDir, nonRegularAddress.relativePath())
	if err := os.MkdirAll(nonRegularPath, 0o700); err != nil {
		t.Fatalf("create non-regular artifact: %v", err)
	}

	symlinkData := []byte("symlink target")
	symlinkDesc := contentFillDescriptor(ArtifactSST, "symlink", symlinkData)
	symlinkAddress, err := artifactContentAddressFor(symlinkDesc)
	if err != nil {
		t.Fatalf("symlink address: %v", err)
	}
	symlinkTarget := filepath.Join(dir, "user-owned-target")
	if err := os.WriteFile(symlinkTarget, symlinkData, 0o600); err != nil {
		t.Fatalf("write symlink target: %v", err)
	}
	symlinkPath := filepath.Join(
		dir, artifactContentCacheVersionDir, symlinkAddress.relativePath())
	if err := os.MkdirAll(filepath.Dir(symlinkPath), 0o700); err != nil {
		t.Fatalf("create symlink shard: %v", err)
	}
	if err := os.Symlink(symlinkTarget, symlinkPath); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	cache, err := openArtifactContentCache(artifactContentCacheOptions{
		dir: dir, sstMaxBytes: 1 << 20, bloomMaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("recover cache: %v", err)
	}
	defer cache.close()
	if stats := cache.stats(ArtifactSST); stats.RecoveredEntries != 1 {
		t.Fatalf("recovery stats=%+v, want one canonical artifact", stats)
	}
	handle, hit, err := cache.acquire(validDesc)
	if err != nil || !hit {
		t.Fatalf("acquire valid recovery: hit=%t err=%v", hit, err)
	}
	if !bytes.Equal(handle.bytes(), validData) {
		t.Fatalf("valid recovered bytes=%q", handle.bytes())
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close valid recovery: %v", err)
	}
	for _, path := range []string{invalidName, wrongShardPath, nestedPath, nonRegularPath, symlinkPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rejected path %q still exists: %v", path, err)
		}
	}
	if data, err := os.ReadFile(symlinkTarget); err != nil || !bytes.Equal(data, symlinkData) {
		t.Fatalf("user-owned symlink target changed: data=%q err=%v", data, err)
	}
}

func writeRecoveryTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create parent for %q: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
}
