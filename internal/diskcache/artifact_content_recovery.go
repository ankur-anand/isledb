package diskcache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type recoveredArtifactContent struct {
	address artifactContentAddress
	path    string
	size    int64
	modTime time.Time
}

func (cache *artifactContentCache) prepare() error {
	if err := cache.prepareMetadata(); err != nil {
		return err
	}
	if err := os.RemoveAll(cache.incomingDir); err != nil {
		return fmt.Errorf("diskcache: clear incoming content: %w", err)
	}
	if err := os.MkdirAll(cache.incomingDir, 0o700); err != nil {
		return fmt.Errorf("diskcache: create incoming content directory: %w", err)
	}
	for kind := range cache.tiers {
		if err := os.MkdirAll(filepath.Join(cache.versionDir, kind.dirName()), 0o700); err != nil {
			return fmt.Errorf("diskcache: create %s content tier: %w", kind.dirName(), err)
		}
	}
	return cache.recover()
}

func (cache *artifactContentCache) prepareMetadata() error {
	metadataPath := filepath.Join(cache.dir, artifactContentCacheMetadataName)
	data, err := os.ReadFile(metadataPath)
	if err == nil && string(data) == artifactContentCacheFormat {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("diskcache: read content cache metadata: %w", err)
	}
	if err := cache.resetOwnedPaths(); err != nil {
		return err
	}
	// Make removal of the old owned layout durable before publishing the new
	// format marker. Steady-state artifact publication remains best-effort and
	// deliberately does not sync directories.
	if err := cache.syncDirectory(cache.dir); err != nil {
		return fmt.Errorf("diskcache: sync reset content cache root: %w", err)
	}
	return writeArtifactContentMetadata(cache.dir, metadataPath)
}

func syncArtifactContentDirectory(path string) error {
	// Windows does not provide a portable directory-fsync equivalent through
	// os.File.Sync. A missing cache entry after a crash remains a safe miss, so
	// format-reset durability is best-effort there.
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (cache *artifactContentCache) resetOwnedPaths() error {
	entries, err := os.ReadDir(cache.dir)
	if err != nil {
		return fmt.Errorf("diskcache: read content cache root: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name != filepath.Base(cache.incomingDir) &&
			!isArtifactContentVersionName(name) &&
			!strings.HasPrefix(name, artifactContentCacheMetadataName+"-") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(cache.dir, name)); err != nil {
			return fmt.Errorf("diskcache: reset owned path %q: %w", name, err)
		}
	}
	return nil
}

func isArtifactContentVersionName(name string) bool {
	if len(name) < 2 || name[0] != 'v' {
		return false
	}
	for index := 1; index < len(name); index++ {
		if name[index] < '0' || name[index] > '9' {
			return false
		}
	}
	return true
}

func writeArtifactContentMetadata(root, metadataPath string) error {
	temp, err := os.CreateTemp(root, artifactContentCacheMetadataName+"-*")
	if err != nil {
		return fmt.Errorf("diskcache: create content cache metadata: %w", err)
	}
	tempPath := temp.Name()
	cleanup := func() {
		_ = temp.Close()
		_ = removeArtifactContentFile(tempPath)
	}
	if _, err := temp.WriteString(artifactContentCacheFormat); err != nil {
		cleanup()
		return fmt.Errorf("diskcache: write content cache metadata: %w", err)
	}
	if err := temp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("diskcache: sync content cache metadata: %w", err)
	}
	if err := temp.Close(); err != nil {
		_ = removeArtifactContentFile(tempPath)
		return fmt.Errorf("diskcache: close content cache metadata: %w", err)
	}
	if err := os.Rename(tempPath, metadataPath); err != nil {
		_ = removeArtifactContentFile(tempPath)
		return fmt.Errorf("diskcache: publish content cache metadata: %w", err)
	}
	return nil
}

func (cache *artifactContentCache) recover() error {
	for kind, tier := range cache.tiers {
		recovered, err := cache.scanRecoveredTier(kind)
		if err != nil {
			return err
		}
		sort.Slice(recovered, func(i, j int) bool {
			if recovered[i].modTime.Equal(recovered[j].modTime) {
				return recovered[i].path < recovered[j].path
			}
			return recovered[i].modTime.Before(recovered[j].modTime)
		})
		for _, item := range recovered {
			removed := 0
			tier.indexMu.Lock()
			victims, admission := tier.index.reserveCapacity(item.size)
			if admission == artifactContentAdmitted {
				_, _, err = tier.index.insertUnpinned(item.address, item.size)
			}
			tier.indexMu.Unlock()

			for _, victim := range victims {
				_ = cache.remove(victim.address)
				removed++
			}
			switch {
			case admission != artifactContentAdmitted:
				_ = removeArtifactContentFile(item.path)
				removed++
			case err != nil:
				_ = removeArtifactContentFile(item.path)
				removed++
			default:
				cache.recordRecovered(kind, item.size)
			}
			cache.recordRecoveryRemoval(kind, removed)
		}
	}
	return nil
}

func (cache *artifactContentCache) scanRecoveredTier(
	kind ArtifactKind,
) ([]recoveredArtifactContent, error) {
	tierDir := filepath.Join(cache.versionDir, kind.dirName())
	var recovered []recoveredArtifactContent
	err := filepath.WalkDir(tierDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path == tierDir {
				return walkErr
			}
			return nil
		}
		if path == tierDir {
			return nil
		}
		if entry.IsDir() {
			relative, relativeErr := filepath.Rel(tierDir, path)
			if relativeErr == nil && isCanonicalArtifactShard(relative) {
				return nil
			}
			_ = os.RemoveAll(path)
			return fs.SkipDir
		}
		if entry.Type()&os.ModeSymlink != 0 {
			_ = removeArtifactContentFile(path)
			return nil
		}
		address, ok := recoveredArtifactContentAddress(kind, path)
		if !ok || filepath.Clean(path) != filepath.Clean(cache.path(address)) {
			_ = removeArtifactContentFile(path)
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil //nolint:nilerr // Recovery deliberately skips unreadable cache entries.
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 {
			_ = removeArtifactContentFile(path)
			return nil
		}
		recovered = append(recovered, recoveredArtifactContent{
			address: address,
			path:    path,
			size:    info.Size(),
			modTime: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("diskcache: scan recovered %s content: %w", kind.dirName(), err)
	}
	return recovered, nil
}

func isCanonicalArtifactShard(relative string) bool {
	if filepath.Base(relative) != relative || len(relative) != 2 {
		return false
	}
	for index := range len(relative) {
		char := relative[index]
		if ('0' > char || char > '9') && ('a' > char || char > 'f') {
			return false
		}
	}
	return true
}

func recoveredArtifactContentAddress(
	kind ArtifactKind,
	path string,
) (artifactContentAddress, bool) {
	name := filepath.Base(path)
	extension := kind.extension()
	if !strings.HasSuffix(name, extension) {
		return artifactContentAddress{}, false
	}
	checksum := strings.TrimSuffix(name, extension)
	if len(checksum) != 64 || filepath.Base(filepath.Dir(path)) != checksum[:2] {
		return artifactContentAddress{}, false
	}
	for _, char := range checksum {
		if ('0' > char || char > '9') && ('a' > char || char > 'f') {
			return artifactContentAddress{}, false
		}
	}
	var decoded [sha256.Size]byte
	if _, err := hex.Decode(decoded[:], []byte(checksum)); err != nil {
		return artifactContentAddress{}, false
	}
	return artifactContentAddress{kind: kind, checksum: decoded}, true
}
