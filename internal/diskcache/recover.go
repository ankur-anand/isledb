package diskcache

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// prepare readies dir: it removes earlier cache layouts, clears unfinished
// writes, and loads each tier's entries oldest first, dropping the oldest
// beyond budget. Files it does not recognise as its own are left alone.
func (c *Cache) prepare(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("diskcache: read directory: %w", err)
	}
	for _, e := range entries {
		if !isStaleLayout(e.Name()) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("diskcache: remove stale %q: %w", e.Name(), err)
		}
	}
	if err := os.RemoveAll(c.incoming); err != nil {
		return fmt.Errorf("diskcache: clear unfinished writes: %w", err)
	}
	if err := os.MkdirAll(c.incoming, 0o700); err != nil {
		return fmt.Errorf("diskcache: create %s: %w", incomingName, err)
	}
	for t := range tierCount {
		if err := c.load(t); err != nil {
			return err
		}
	}
	return nil
}

type cachedEntry struct {
	key     Key
	size    int64
	modTime time.Time
}

// load indexes one tier's entries. Anything that is not a regular, non-empty
// file named as an entry of this tier, in the matching two-character shard, is
// deleted. Contents are not re-read; readers check them as they use them.
//
// Recency is not persisted, so entries start in write order (modification
// time) and take their place by use as they are read again.
func (c *Cache) load(t Tier) error {
	tierDir := filepath.Join(c.root, t.String())
	if err := os.MkdirAll(tierDir, 0o700); err != nil {
		return fmt.Errorf("diskcache: create %s directory: %w", t, err)
	}
	shards, err := os.ReadDir(tierDir)
	if err != nil {
		return fmt.Errorf("diskcache: read %s directory: %w", t, err)
	}

	var found []cachedEntry
	for _, shard := range shards {
		shardDir := filepath.Join(tierDir, shard.Name())
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
			key, ok := parseName(name.Name())
			if err != nil || !ok || key.Kind.Tier() != t || name.Name()[:2] != shard.Name() ||
				!info.Mode().IsRegular() || info.Size() <= 0 {
				_ = os.RemoveAll(path)
				continue
			}
			found = append(found, cachedEntry{key: key, size: info.Size(), modTime: info.ModTime()})
		}
	}

	sort.Slice(found, func(i, j int) bool { return found[i].modTime.Before(found[j].modTime) })
	tr := c.tiers[t]
	for _, e := range found {
		c.insertLocked(e.key, e.size)
	}
	for tr.bytes > tr.max {
		c.removeLocked(tr.lru.Front())
	}
	return nil
}

// parseName decodes an entry's file name: the object as 64 lowercase hex
// characters, a dot, and the kind, with the chunk number after "c".
func parseName(name string) (Key, bool) {
	var k Key
	object, suffix, ok := strings.Cut(name, ".")
	if !ok || !isLowerHex(object, 2*len(k.Object)) {
		return k, false
	}
	if _, err := hex.Decode(k.Object[:], []byte(object)); err != nil {
		return k, false
	}
	switch suffix {
	case kindSuffix[KindMeta]:
		k.Kind = KindMeta
	case kindSuffix[KindBloom]:
		k.Kind = KindBloom
	case kindSuffix[KindWhole]:
		k.Kind = KindWhole
	default:
		digits, ok := strings.CutPrefix(suffix, kindSuffix[KindChunk])
		if !ok || digits == "" || (len(digits) > 1 && digits[0] == '0') {
			return k, false
		}
		index, err := strconv.ParseUint(digits, 10, 32)
		if err != nil {
			return k, false
		}
		k.Kind, k.Index = KindChunk, uint32(index)
	}
	return k, true
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
