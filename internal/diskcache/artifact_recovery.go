package diskcache

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type recoveredArtifact struct {
	id      artifactID
	path    string
	size    int64
	modTime time.Time
}

func (c *ArtifactCache) prepare() error {
	if err := c.prepareFormat(); err != nil {
		return err
	}
	if err := os.RemoveAll(c.incomingDir); err != nil {
		return fmt.Errorf("diskcache: clear incoming artifacts: %w", err)
	}
	if err := os.MkdirAll(c.incomingDir, 0o700); err != nil {
		return fmt.Errorf("diskcache: create incoming directory: %w", err)
	}
	for kind := range c.tiers {
		if err := os.MkdirAll(filepath.Join(c.versionDir, kind.dirName()), 0o700); err != nil {
			return fmt.Errorf("diskcache: create %s tier: %w", kind.dirName(), err)
		}
	}
	return c.recoverArtifacts()
}

func (c *ArtifactCache) prepareFormat() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return fmt.Errorf("diskcache: read artifact cache root: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "CACHEMETA-") {
			continue
		}
		if err := os.Remove(filepath.Join(c.dir, entry.Name())); err != nil {
			return fmt.Errorf("diskcache: remove interrupted cache metadata: %w", err)
		}
	}

	metaPath := filepath.Join(c.dir, "CACHEMETA")
	data, err := os.ReadFile(metaPath)
	switch {
	case err == nil:
		if string(data) != artifactCacheFormat {
			if resetErr := c.resetIncompatibleFormat(entries, metaPath); resetErr != nil {
				return fmt.Errorf(
					"diskcache: reset incompatible artifact cache format %q: %w",
					strings.TrimSpace(string(data)), resetErr)
			}
			if writeErr := c.writeFormatMetadata(metaPath); writeErr != nil {
				return writeErr
			}
		}
	case errors.Is(err, os.ErrNotExist):
		if writeErr := c.writeFormatMetadata(metaPath); writeErr != nil {
			return writeErr
		}
	default:
		return fmt.Errorf("diskcache: read cache metadata: %w", err)
	}
	if err := os.MkdirAll(c.versionDir, 0o700); err != nil {
		return fmt.Errorf("diskcache: create cache version directory: %w", err)
	}
	return nil
}

func (c *ArtifactCache) resetIncompatibleFormat(entries []os.DirEntry, metaPath string) error {
	for _, entry := range entries {
		name := entry.Name()
		if name != filepath.Base(c.incomingDir) && !isArtifactVersionName(name) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(c.dir, name)); err != nil {
			return fmt.Errorf("remove cache-owned path %q: %w", name, err)
		}
	}
	// Persist removal of all old-format data before publishing metadata that
	// allows the directory to be interpreted as the current format.
	if err := syncArtifactDirectory(c.dir); err != nil {
		return err
	}
	if err := os.Remove(metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove incompatible cache metadata: %w", err)
	}
	return nil
}

func isArtifactVersionName(name string) bool {
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

func (c *ArtifactCache) writeFormatMetadata(metaPath string) error {
	temp, err := os.CreateTemp(c.dir, "CACHEMETA-*")
	if err != nil {
		return fmt.Errorf("diskcache: create cache metadata: %w", err)
	}
	tempPath := temp.Name()
	cleanup := func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}
	if _, err = temp.WriteString(artifactCacheFormat); err != nil {
		cleanup()
		return fmt.Errorf("diskcache: write cache metadata: %w", err)
	}
	if err = temp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("diskcache: sync cache metadata: %w", err)
	}
	if err = temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("diskcache: close cache metadata: %w", err)
	}
	if err = os.Rename(tempPath, metaPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("diskcache: publish cache metadata: %w", err)
	}
	if err = syncArtifactDirectory(c.dir); err != nil {
		return err
	}
	return nil
}

func (c *ArtifactCache) recoverArtifacts() error {
	var recovered []recoveredArtifact
	for kind := range c.tiers {
		tierDir := filepath.Join(c.versionDir, kind.dirName())
		err := filepath.WalkDir(tierDir, func(path string, dirEntry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if path == tierDir || dirEntry.IsDir() {
				return nil
			}
			if dirEntry.Type()&os.ModeSymlink != 0 {
				return os.Remove(path)
			}
			id, ok := recoveredArtifactID(kind, path)
			if !ok {
				return os.Remove(path)
			}
			if filepath.Clean(path) != filepath.Clean(c.artifactPath(id)) {
				return os.Remove(path)
			}
			info, err := dirEntry.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Size() <= 0 {
				return os.Remove(path)
			}
			recovered = append(recovered, recoveredArtifact{
				id: id, path: path, size: info.Size(), modTime: info.ModTime(),
			})
			return nil
		})
		if err != nil {
			return fmt.Errorf("diskcache: recover %s tier: %w", kind.dirName(), err)
		}
	}

	sort.Slice(recovered, func(i, j int) bool {
		return recovered[i].modTime.Before(recovered[j].modTime)
	})
	for _, item := range recovered {
		tier := c.tiers[item.id.kind]
		entry := &artifactEntry{
			id: item.id, path: item.path, size: item.size,
			state: artifactEntryReady, lastAccess: item.modTime, lastTouch: item.modTime,
		}
		entry.elem = tier.lru.PushBack(entry)
		tier.entries[item.id.digest] = entry
		tier.residentBytes += item.size
		tier.stats.RecoveredEntries++
		tier.stats.RecoveredBytes += item.size
	}

	for _, tier := range c.tiers {
		for tier.residentBytes > tier.maxBytes {
			element := tier.lru.Front()
			if element == nil {
				break
			}
			entry := element.Value.(*artifactEntry)
			cleanup := c.detachEntryLocked(tier, entry, ArtifactRemovalRecovery)
			if err := cleanup.run(); err != nil {
				return fmt.Errorf("diskcache: evict recovered artifact: %w", err)
			}
		}
	}
	return nil
}

func recoveredArtifactID(kind ArtifactKind, path string) (artifactID, bool) {
	name := filepath.Base(path)
	extension := kind.extension()
	if !strings.HasSuffix(name, extension) {
		return artifactID{}, false
	}
	digest := strings.TrimSuffix(name, extension)
	if len(digest) != 64 || filepath.Base(filepath.Dir(path)) != digest[:2] {
		return artifactID{}, false
	}
	for _, char := range digest {
		if !('0' <= char && char <= '9') && !('a' <= char && char <= 'f') {
			return artifactID{}, false
		}
	}
	var decoded artifactDigest
	if _, err := hex.Decode(decoded[:], []byte(digest)); err != nil {
		return artifactID{}, false
	}
	return artifactID{kind: kind, digest: decoded}, true
}
