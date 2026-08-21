package diskcache

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// ArtifactHandle pins immutable bytes until Close. It is safe to close a
// handle more than once.
type ArtifactHandle struct {
	data    []byte
	cache   *ArtifactCache
	tier    *artifactTier
	entry   *artifactEntry
	closeFn func() error
	once    sync.Once
	err     error
}

func (h *ArtifactHandle) Bytes() []byte {
	if h == nil {
		return nil
	}
	return h.data
}

func (h *ArtifactHandle) Close() error {
	if h == nil {
		return nil
	}
	h.once.Do(func() {
		if h.cache != nil {
			h.err = h.cache.releasePersistentHandle(h.tier, h.entry)
		} else if h.closeFn != nil {
			h.err = h.closeFn()
		}
	})
	return h.err
}

// Acquire returns a verified, pinned artifact. A corrupt resident artifact is
// removed and reported as a miss so the caller can fetch its authoritative
// object-store copy.
func (c *ArtifactCache) Acquire(desc ArtifactDescriptor) (*ArtifactHandle, bool, error) {
	if err := desc.validate(); err != nil {
		return nil, false, err
	}
	if err := c.beginOperation(); err != nil {
		return nil, false, err
	}
	defer c.endOperation()

	id := artifactIDFor(desc.Key)
	tier := c.tiers[id.kind]
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, false, ErrArtifactCacheClosed
		}
		entry := tier.entries[id.digest]
		if entry == nil || entry.pendingRemoval || entry.state == artifactEntryEvicting {
			tier.stats.Misses++
			c.mu.Unlock()
			return nil, false, nil
		}
		if entry.state != artifactEntryReady {
			wait := entry.wait
			c.mu.Unlock()
			if wait != nil {
				<-wait
			}
			continue
		}
		if entry.size != desc.Size {
			var cleanup artifactCleanup
			if entry.refs == 0 {
				cleanup = c.detachEntryLocked(tier, entry, ArtifactRemovalCorrupt)
			} else {
				entry.pendingRemoval = true
				entry.pendingReason = ArtifactRemovalCorrupt
			}
			tier.stats.Misses++
			c.mu.Unlock()
			if err := cleanup.run(); err != nil {
				return nil, false, fmt.Errorf("diskcache: remove invalid artifact: %w", err)
			}
			return nil, false, nil
		}

		if entry.verifiedChecksum != desc.Checksum {
			entry.state = artifactEntryVerifying
			entry.wait = make(chan struct{})
			path := entry.path
			c.mu.Unlock()

			matches, verifyErr := verifyArtifactFile(path, desc)

			c.mu.Lock()
			if current := tier.entries[id.digest]; current != entry {
				c.mu.Unlock()
				if verifyErr != nil {
					return nil, false, verifyErr
				}
				continue
			}
			entry.state = artifactEntryReady
			wait := entry.wait
			entry.wait = nil
			if matches && verifyErr == nil && !entry.pendingRemoval {
				entry.verifiedChecksum = desc.Checksum
				close(wait)
				c.mu.Unlock()
				continue
			}
			close(wait)
			var cleanup artifactCleanup
			if entry.refs == 0 {
				cleanup = c.detachEntryLocked(tier, entry, ArtifactRemovalCorrupt)
			} else {
				entry.pendingRemoval = true
				entry.pendingReason = ArtifactRemovalCorrupt
			}
			tier.stats.Misses++
			c.mu.Unlock()
			cleanupErr := cleanup.run()
			if verifyErr != nil {
				return nil, false, errors.Join(
					fmt.Errorf("diskcache: verify artifact: %w", verifyErr), cleanupErr)
			}
			if cleanupErr != nil {
				return nil, false, fmt.Errorf("diskcache: remove corrupt artifact: %w", cleanupErr)
			}
			return nil, false, nil
		}

		if entry.mmap == nil {
			entry.state = artifactEntryOpening
			entry.wait = make(chan struct{})
			path := entry.path
			c.mu.Unlock()

			file, data, openErr := openMappedArtifact(path)

			c.mu.Lock()
			if current := tier.entries[id.digest]; current != entry {
				c.mu.Unlock()
				_ = artifactCleanup{data: data, file: file}.run()
				if openErr != nil {
					return nil, false, openErr
				}
				continue
			}
			entry.state = artifactEntryReady
			wait := entry.wait
			entry.wait = nil
			if openErr != nil || c.closed || entry.pendingRemoval {
				closed := c.closed
				pending := entry.pendingRemoval
				close(wait)
				var cleanup artifactCleanup
				if entry.refs == 0 && (openErr != nil || entry.pendingRemoval) {
					reason := entry.pendingReason
					if reason == 0 {
						reason = ArtifactRemovalCorrupt
					}
					cleanup = c.detachEntryLocked(tier, entry, reason)
				}
				if openErr != nil || pending {
					tier.stats.Misses++
				}
				c.mu.Unlock()
				cleanupErr := errors.Join(
					artifactCleanup{data: data, file: file}.run(), cleanup.run())
				if openErr != nil {
					return nil, false, errors.Join(
						fmt.Errorf("diskcache: open artifact: %w", openErr), cleanupErr)
				}
				if cleanupErr != nil {
					return nil, false, fmt.Errorf("diskcache: clean unopened artifact: %w", cleanupErr)
				}
				if closed {
					return nil, false, ErrArtifactCacheClosed
				}
				return nil, false, nil
			}
			entry.file = file
			entry.mmap = data
			c.openEntries++
			close(wait)
		}

		wasUnpinned := entry.refs == 0
		entry.refs++
		c.activeHandles++
		if wasUnpinned {
			tier.pinnedBytes += entry.size
		}
		now := time.Now()
		entry.lastAccess = now
		if entry.elem != nil {
			tier.lru.MoveToBack(entry.elem)
		}
		touch := now.Sub(entry.lastTouch) >= c.touchInterval
		if touch {
			entry.lastTouch = now
		}
		tier.stats.Hits++
		data := entry.mmap
		path := entry.path
		c.mu.Unlock()

		if touch {
			_ = os.Chtimes(path, now, now)
		}
		return &ArtifactHandle{data: data, cache: c, tier: tier, entry: entry}, true, nil
	}
}

func verifyArtifactFile(path string, desc ArtifactDescriptor) (bool, error) {
	expected, err := parseSHA256Checksum(desc.Checksum)
	if err != nil {
		return false, err
	}
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	hasher := sha256.New()
	written, err := io.Copy(hasher, file)
	if err != nil {
		return false, err
	}
	if written != desc.Size {
		return false, nil
	}
	actual := hasher.Sum(nil)
	return subtle.ConstantTimeCompare(actual, expected[:]) == 1, nil
}

func openMappedArtifact(path string) (*os.File, []byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	data, err := MmapFile(file)
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	return file, data, nil
}

func (c *ArtifactCache) releasePersistentHandle(tier *artifactTier, entry *artifactEntry) error {
	c.mu.Lock()
	if entry.refs == 0 {
		c.mu.Unlock()
		return nil
	}
	entry.refs--
	c.activeHandles--
	var cleanups []artifactCleanup
	if entry.refs == 0 {
		tier.pinnedBytes -= entry.size
		if entry.pendingRemoval {
			cleanups = append(cleanups, c.detachEntryLocked(tier, entry, entry.pendingReason))
		} else if c.closed && (entry.mmap != nil || entry.file != nil) {
			cleanups = append(cleanups, artifactCleanup{data: entry.mmap, file: entry.file})
			entry.mmap = nil
			entry.file = nil
			c.openEntries--
		}
	}
	if !c.closed {
		cleanups = append(cleanups, c.trimOpenEntriesLocked()...)
	}
	directoryLock := c.takeDirectoryLockIfIdleLocked()
	c.mu.Unlock()

	var err error
	for _, cleanup := range cleanups {
		err = errors.Join(err, cleanup.run())
	}
	if directoryLock != nil {
		err = errors.Join(err, directoryLock.close())
	}
	return err
}

func (c *ArtifactCache) trimOpenEntriesLocked() []artifactCleanup {
	var cleanups []artifactCleanup
	for c.openEntries > c.maxOpenEntries {
		var oldest *artifactEntry
		for _, tier := range c.tiers {
			for element := tier.lru.Front(); element != nil; element = element.Next() {
				candidate := element.Value.(*artifactEntry)
				if candidate.refs != 0 || candidate.state != artifactEntryReady ||
					(candidate.mmap == nil && candidate.file == nil) {
					continue
				}
				if oldest == nil || candidate.lastAccess.Before(oldest.lastAccess) {
					oldest = candidate
				}
				break
			}
		}
		if oldest == nil {
			break
		}
		cleanups = append(cleanups, artifactCleanup{data: oldest.mmap, file: oldest.file})
		oldest.mmap = nil
		oldest.file = nil
		c.openEntries--
	}
	return cleanups
}
