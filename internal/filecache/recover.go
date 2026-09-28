package filecache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// prepare readies dir: it removes earlier cache layouts, clears unfinished
// downloads, and loads each tier's files oldest first, dropping the oldest
// beyond budget. Files it does not recognise as its own are left alone.
func (c *Cache) prepare(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("filecache: read directory: %w", err)
	}
	for _, e := range entries {
		if !isStaleLayout(e.Name()) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("filecache: remove stale %q: %w", e.Name(), err)
		}
	}
	if err := os.RemoveAll(c.incoming); err != nil {
		return fmt.Errorf("filecache: clear unfinished downloads: %w", err)
	}
	if err := os.MkdirAll(c.incoming, 0o700); err != nil {
		return fmt.Errorf("filecache: create %s: %w", incomingName, err)
	}
	for kind := range kindCount {
		if err := c.load(kind); err != nil {
			return err
		}
	}
	return nil
}

type cachedFile struct {
	sum     [sha256.Size]byte
	size    int64
	modTime time.Time
}

// load indexes one tier's files. Anything that is not a regular, non-empty
// file named by its checksum in the matching two-character shard is deleted.
// Contents are not re-read: they were verified before being published.
//
// Recency is not persisted, so files start in download order (modification
// time) and take their place by use as they are read again. A file still
// unread since the restart can therefore be evicted before a colder one; the
// cost is at most one extra download, while persisting recency would add a
// write to every hit.
func (c *Cache) load(kind Kind) error {
	kindDir := filepath.Join(c.root, kind.String())
	if err := os.MkdirAll(kindDir, 0o700); err != nil {
		return fmt.Errorf("filecache: create %s directory: %w", kind, err)
	}
	shards, err := os.ReadDir(kindDir)
	if err != nil {
		return fmt.Errorf("filecache: read %s directory: %w", kind, err)
	}

	var files []cachedFile
	for _, shard := range shards {
		shardDir := filepath.Join(kindDir, shard.Name())
		if !shard.IsDir() || !isLowerHex(shard.Name(), 2) {
			_ = os.RemoveAll(shardDir)
			continue
		}
		names, err := os.ReadDir(shardDir)
		if err != nil {
			continue
		}
		for _, name := range names {
			path := filepath.Join(shardDir, name.Name())
			info, err := name.Info()
			sum, ok := parseName(name.Name(), shard.Name())
			if err != nil || !ok || !info.Mode().IsRegular() || info.Size() <= 0 {
				_ = os.RemoveAll(path)
				continue
			}
			files = append(files, cachedFile{sum: sum, size: info.Size(), modTime: info.ModTime()})
		}
	}

	sort.Slice(files, func(i, j int) bool { return files[i].modTime.Before(files[j].modTime) })
	t := c.tiers[kind]
	for _, file := range files {
		c.insertLocked(kind, file.sum, file.size)
	}
	for t.bytes > t.max {
		c.removeLocked(kind, t.lru.Front())
	}
	return nil
}

// parseName decodes a cache file name: the checksum as 64 lowercase hex
// characters, whose first two match the shard it is stored in.
func parseName(name, shard string) ([sha256.Size]byte, bool) {
	var sum [sha256.Size]byte
	if !isLowerHex(name, 2*sha256.Size) || name[:2] != shard {
		return sum, false
	}
	_, err := hex.Decode(sum[:], []byte(name))
	return sum, err == nil
}

// isStaleLayout reports whether a top-level name belongs to an earlier cache
// layout: another version directory, or the unversioned layout's format
// marker and download directory.
func isStaleLayout(name string) bool {
	if name == incomingName || strings.HasPrefix(name, "CACHEMETA") {
		return true
	}
	if name == versionDir || len(name) < 2 || name[0] != 'v' {
		return false
	}
	for i := 1; i < len(name); i++ {
		if name[i] < '0' || name[i] > '9' {
			return false
		}
	}
	return true
}

func isLowerHex(s string, length int) bool {
	if len(s) != length {
		return false
	}
	for i := 0; i < len(s); i++ {
		if (s[i] < '0' || s[i] > '9') && (s[i] < 'a' || s[i] > 'f') {
			return false
		}
	}
	return true
}
