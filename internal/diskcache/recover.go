package diskcache

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// prepare readies dir: it removes earlier cache layouts, clears unfinished
// writes, and loads each tier's entries in directory order, dropping any
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

// load indexes one tier's entries from their file names, which carry each
// entry's size, so it lists directories without a system call per file.
// Anything not named as an entry of this tier, in the matching two-character
// shard, is deleted. Contents are not re-read: a file shorter than its name
// says, as a crash can leave, fails the read that reaches past its end and is
// dropped then.
//
// Recency is not persisted, so entries start in directory order and take
// their place by use as they are read again.
func (c *Cache) load(t Tier) error {
	tierDir := filepath.Join(c.root, t.String())
	if err := os.MkdirAll(tierDir, 0o700); err != nil {
		return fmt.Errorf("diskcache: create %s directory: %w", t, err)
	}
	shards, err := os.ReadDir(tierDir)
	if err != nil {
		return fmt.Errorf("diskcache: read %s directory: %w", t, err)
	}

	tr := c.tiers[t]
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
			key, size, ok := parseName(name.Name())
			_, duplicate := tr.index[key]
			if !ok || duplicate || key.Kind.Tier() != t || name.Name()[:2] != shard.Name() || !name.Type().IsRegular() {
				_ = os.RemoveAll(filepath.Join(shardDir, name.Name()))
				continue
			}
			c.insertLocked(key, size)
		}
	}
	for tr.bytes > tr.max {
		c.removeLocked(tr.lru.Front())
	}
	return nil
}

// parseName decodes an entry's file name: the object as 64 lowercase hex
// characters, a dot, the kind, with the chunk number after "c", a dot, and
// the entry's size in bytes.
func parseName(name string) (Key, int64, bool) {
	var k Key
	base, sizeText, ok := cutLast(name, ".")
	if !ok || !isDecimal(sizeText) {
		return k, 0, false
	}
	size, err := strconv.ParseInt(sizeText, 10, 64)
	if err != nil || size <= 0 {
		return k, 0, false
	}
	object, suffix, ok := strings.Cut(base, ".")
	if !ok || !isLowerHex(object, 2*len(k.Object)) {
		return k, 0, false
	}
	if _, err := hex.Decode(k.Object[:], []byte(object)); err != nil {
		return k, 0, false
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
		if !ok || !isDecimal(digits) {
			return k, 0, false
		}
		index, err := strconv.ParseUint(digits, 10, 32)
		if err != nil {
			return k, 0, false
		}
		k.Kind, k.Index = KindChunk, uint32(index)
	}
	return k, size, true
}

// isDecimal reports whether s is a decimal number without leading zeros.
func isDecimal(s string) bool {
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func cutLast(s, sep string) (string, string, bool) {
	i := strings.LastIndex(s, sep)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+len(sep):], true
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
