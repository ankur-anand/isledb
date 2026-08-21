package diskcache

import (
	"container/list"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type artifactEntryState uint8

const (
	artifactEntryReady artifactEntryState = iota + 1
	artifactEntryVerifying
	artifactEntryOpening
	artifactEntryEvicting
)

type artifactEntry struct {
	id   artifactID
	path string
	size int64

	state artifactEntryState
	wait  chan struct{}

	verifiedChecksum string

	file *os.File
	mmap []byte
	refs int

	pendingRemoval bool
	pendingReason  ArtifactRemovalReason
	elem           *list.Element

	lastAccess time.Time
	lastTouch  time.Time
}

type artifactTier struct {
	kind     ArtifactKind
	maxBytes int64

	residentBytes int64
	pinnedBytes   int64
	pinnedEntries int
	entries       map[artifactDigest]*artifactEntry
	lru           list.List

	stats ArtifactStats
}

type artifactCleanup struct {
	data   []byte
	file   *os.File
	path   string
	remove bool
}

func (c artifactCleanup) run() error {
	var err error
	if c.data != nil {
		err = errors.Join(err, Munmap(c.data))
	}
	if c.file != nil {
		err = errors.Join(err, c.file.Close())
	}
	if c.remove && c.path != "" {
		if removeErr := os.Remove(c.path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}
	return err
}

func runArtifactCleanups(cleanups []artifactCleanup) error {
	var err error
	for _, cleanup := range cleanups {
		err = errors.Join(err, cleanup.run())
	}
	return err
}

// ArtifactCache is a process-exclusive, persistent cache for immutable SST
// payloads and raw Bloom sidecars.
type ArtifactCache struct {
	mu sync.Mutex

	dir            string
	versionDir     string
	incomingDir    string
	touchInterval  time.Duration
	maxOpenEntries int
	openEntries    int
	syncDirectory  func(string) error
	verifyFile     func(string, ArtifactDescriptor) (bool, error)
	tiers          map[ArtifactKind]*artifactTier
	directoryLock  *artifactDirectoryLock
	closed         bool
	activeOps      int
	activeFills    int
	activeHandles  int
}

// OpenArtifactCache opens or recovers a persistent two-tier artifact cache.
// The returned cache holds an exclusive operating-system lock on opts.Dir.
func OpenArtifactCache(opts ArtifactCacheOptions) (*ArtifactCache, error) {
	normalized, err := opts.normalize()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(normalized.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("diskcache: create artifact cache directory: %w", err)
	}
	directoryLock, err := acquireArtifactDirectoryLock(normalized.Dir)
	if err != nil {
		return nil, err
	}

	cache := &ArtifactCache{
		dir:            normalized.Dir,
		versionDir:     filepath.Join(normalized.Dir, "v1"),
		incomingDir:    filepath.Join(normalized.Dir, "incoming"),
		touchInterval:  normalized.TouchInterval,
		maxOpenEntries: normalized.MaxOpenEntries,
		syncDirectory:  syncArtifactDirectory,
		verifyFile:     verifyArtifactFile,
		directoryLock:  directoryLock,
		tiers: map[ArtifactKind]*artifactTier{
			ArtifactSST: {
				kind:     ArtifactSST,
				maxBytes: normalized.SSTMaxBytes,
				entries:  make(map[artifactDigest]*artifactEntry),
			},
			ArtifactBloom: {
				kind:     ArtifactBloom,
				maxBytes: normalized.BloomMaxBytes,
				entries:  make(map[artifactDigest]*artifactEntry),
			},
		},
	}
	if err := cache.prepare(); err != nil {
		_ = directoryLock.close()
		return nil, err
	}
	return cache, nil
}

func (c *ArtifactCache) CacheDir() string {
	if c == nil {
		return ""
	}
	return c.dir
}

func (c *ArtifactCache) artifactPath(id artifactID) string {
	tierDir := filepath.Join(c.versionDir, id.kind.dirName())
	digest := hex.EncodeToString(id.digest[:])
	return filepath.Join(tierDir, digest[:2], digest+id.kind.extension())
}

func (c *ArtifactCache) beginOperation() error {
	if c == nil {
		return ErrArtifactCacheClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrArtifactCacheClosed
	}
	c.activeOps++
	return nil
}

func (c *ArtifactCache) endOperation() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.activeOps > 0 {
		c.activeOps--
	}
	directoryLock := c.takeDirectoryLockIfIdleLocked()
	c.mu.Unlock()
	if directoryLock != nil {
		_ = directoryLock.close()
	}
}

func (c *ArtifactCache) takeDirectoryLockIfIdleLocked() *artifactDirectoryLock {
	if !c.closed || c.activeOps != 0 || c.activeFills != 0 || c.activeHandles != 0 {
		return nil
	}
	directoryLock := c.directoryLock
	c.directoryLock = nil
	return directoryLock
}

// Probe checks resident state without updating recency, counters, or pins.
func (c *ArtifactCache) Probe(desc ArtifactDescriptor) (ArtifactPresence, error) {
	if err := desc.validate(); err != nil {
		return ArtifactAbsent, err
	}
	if err := c.beginOperation(); err != nil {
		return ArtifactAbsent, err
	}
	defer c.endOperation()

	id := artifactIDFor(desc.Key)
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.tiers[id.kind].entries[id.digest]
	if entry == nil || entry.pendingRemoval || entry.state == artifactEntryEvicting || entry.size != desc.Size {
		return ArtifactAbsent, nil
	}
	if entry.verifiedChecksum == desc.Checksum {
		return ArtifactResidentVerified, nil
	}
	return ArtifactResidentUnverified, nil
}

// Stats returns a consistent snapshot for one persistent tier.
func (c *ArtifactCache) Stats(kind ArtifactKind) ArtifactStats {
	if c == nil || !kind.valid() {
		return ArtifactStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tier := c.tiers[kind]
	stats := tier.stats
	stats.ResidentEntries = len(tier.entries)
	stats.ResidentBytes = tier.residentBytes
	stats.PinnedBytes = tier.pinnedBytes
	stats.PinnedEntries = tier.pinnedEntries
	stats.MaxBytes = tier.maxBytes
	return stats
}

func (c *ArtifactCache) detachEntryLocked(
	tier *artifactTier,
	entry *artifactEntry,
	reason ArtifactRemovalReason,
) artifactCleanup {
	if entry == nil || entry.refs != 0 {
		return artifactCleanup{}
	}
	if current := tier.entries[entry.id.digest]; current != entry {
		return artifactCleanup{}
	}
	delete(tier.entries, entry.id.digest)
	if entry.elem != nil {
		tier.lru.Remove(entry.elem)
		entry.elem = nil
	}
	tier.residentBytes -= entry.size
	if entry.mmap != nil || entry.file != nil {
		c.openEntries--
	}
	entry.state = artifactEntryEvicting
	cleanup := artifactCleanup{
		data: entry.mmap, file: entry.file, path: entry.path, remove: true,
	}
	entry.mmap = nil
	entry.file = nil

	switch reason {
	case ArtifactRemovalCapacity:
		tier.stats.Evictions++
		tier.stats.CapacityEvictions++
	case ArtifactRemovalCorrupt:
		tier.stats.Corruptions++
	case ArtifactRemovalPurge:
		tier.stats.PurgeRemovals++
	case ArtifactRemovalRecovery:
		tier.stats.RecoveryRemovals++
	}
	return cleanup
}

func (c *ArtifactCache) reserveLocked(
	tier *artifactTier,
	required int64,
	exclude artifactDigest,
) ([]artifactCleanup, ArtifactAdmission) {
	if required > tier.maxBytes {
		tier.stats.AdmissionBypasses++
		return nil, ArtifactBypassedOversized
	}
	requiredReclaim := tier.residentBytes + required - tier.maxBytes
	if requiredReclaim <= 0 {
		return nil, ArtifactAdmitted
	}
	var victims []*artifactEntry
	var reclaimable int64
	for element := tier.lru.Front(); element != nil; element = element.Next() {
		candidate := element.Value.(*artifactEntry)
		if candidate.id.digest == exclude || candidate.refs != 0 || candidate.pendingRemoval ||
			candidate.state != artifactEntryReady {
			continue
		}
		victims = append(victims, candidate)
		reclaimable += candidate.size
		if reclaimable >= requiredReclaim {
			break
		}
	}
	if reclaimable < requiredReclaim {
		tier.stats.AdmissionBypasses++
		return nil, ArtifactBypassedPinnedCapacity
	}

	cleanups := make([]artifactCleanup, 0, len(victims))
	for _, victim := range victims {
		cleanups = append(cleanups, c.detachEntryLocked(tier, victim, ArtifactRemovalCapacity))
	}
	return cleanups, ArtifactAdmitted
}

// Remove invalidates one logical artifact. Pinned bytes remain valid until the
// final handle closes.
func (c *ArtifactCache) Remove(key ArtifactKey, reason ArtifactRemovalReason) error {
	if !key.Kind.valid() || key.SSTID == "" {
		return ErrInvalidArtifactDescriptor
	}
	if err := c.beginOperation(); err != nil {
		return err
	}
	defer c.endOperation()

	id := artifactIDFor(key)
	c.mu.Lock()
	tier := c.tiers[id.kind]
	entry := tier.entries[id.digest]
	var cleanup artifactCleanup
	if entry != nil {
		if entry.refs > 0 || entry.state != artifactEntryReady {
			entry.pendingRemoval = true
			entry.pendingReason = reason
		} else {
			cleanup = c.detachEntryLocked(tier, entry, reason)
		}
	}
	c.mu.Unlock()
	return cleanup.run()
}

// Purge removes every artifact in one tier, deferring pinned entries until
// their final handle closes.
func (c *ArtifactCache) Purge(kind ArtifactKind) error {
	if !kind.valid() {
		return ErrInvalidArtifactDescriptor
	}
	if err := c.beginOperation(); err != nil {
		return err
	}
	defer c.endOperation()

	c.mu.Lock()
	tier := c.tiers[kind]
	cleanups := make([]artifactCleanup, 0, len(tier.entries))
	for _, entry := range tier.entries {
		if entry.refs > 0 || entry.state != artifactEntryReady {
			entry.pendingRemoval = true
			entry.pendingReason = ArtifactRemovalPurge
			continue
		}
		cleanups = append(cleanups, c.detachEntryLocked(tier, entry, ArtifactRemovalPurge))
	}
	c.mu.Unlock()

	var err error
	for _, cleanup := range cleanups {
		err = errors.Join(err, cleanup.run())
	}
	return err
}

// Close releases mappings and the directory lock after all outstanding fills
// and handles finish. Committed cache files remain on disk.
func (c *ArtifactCache) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		directoryLock := c.takeDirectoryLockIfIdleLocked()
		c.mu.Unlock()
		if directoryLock != nil {
			return directoryLock.close()
		}
		return nil
	}
	c.closed = true

	var cleanups []artifactCleanup
	for _, tier := range c.tiers {
		for _, entry := range tier.entries {
			if entry.refs != 0 {
				continue
			}
			if entry.mmap != nil || entry.file != nil {
				cleanups = append(cleanups, artifactCleanup{data: entry.mmap, file: entry.file})
				entry.mmap = nil
				entry.file = nil
				c.openEntries--
			}
		}
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
