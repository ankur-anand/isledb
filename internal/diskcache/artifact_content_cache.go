package diskcache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	artifactContentCacheFormat       = "isledb-artifact-cache-checksum-v1\n"
	artifactContentCacheVersionDir   = "v1"
	artifactContentCacheMetadataName = "CACHEMETA"
)

type artifactContentCacheOptions struct {
	dir           string
	sstMaxBytes   int64
	bloomMaxBytes int64
}

// artifactContentCache owns the persistent cache directory and its two
// independently budgeted tiers. It is intentionally private until Reader
// integration replaces the current ArtifactCache in one reviewable change.
type artifactContentCache struct {
	mu sync.Mutex

	dir           string
	versionDir    string
	incomingDir   string
	tiers         map[ArtifactKind]*artifactContentTier
	counters      map[ArtifactKind]*artifactContentCounters
	dirLock       *artifactDirectoryLock
	closed        bool
	activeOps     int
	activeFills   int
	activeHandles int
}

type artifactContentCounters struct {
	hits                       atomic.Int64
	misses                     atomic.Int64
	corruptions                atomic.Int64
	admitted                   atomic.Int64
	alreadyResident            atomic.Int64
	bypassedOversized          atomic.Int64
	bypassedPinnedCapacity     atomic.Int64
	bypassedPublicationFailure atomic.Int64
	removals                   atomic.Int64
	purgeRemovals              atomic.Int64
	recoveryRemovals           atomic.Int64
	recoveredEntries           atomic.Int64
	recoveredBytes             atomic.Int64
	incomingFiles              atomic.Int64
	incomingBytes              atomic.Int64
	transientFiles             atomic.Int64
	transientBytes             atomic.Int64
}

func openArtifactContentCache(opts artifactContentCacheOptions) (*artifactContentCache, error) {
	if opts.dir == "" {
		return nil, errors.New("diskcache: content cache directory is required")
	}
	if opts.sstMaxBytes < 0 || opts.bloomMaxBytes < 0 {
		return nil, errors.New("diskcache: content cache limits cannot be negative")
	}
	if err := os.MkdirAll(opts.dir, 0o700); err != nil {
		return nil, fmt.Errorf("diskcache: create content cache directory: %w", err)
	}
	dirLock, err := acquireArtifactDirectoryLock(opts.dir)
	if err != nil {
		return nil, err
	}

	sstTier, err := newArtifactContentTier(ArtifactSST, opts.sstMaxBytes)
	if err != nil {
		_ = dirLock.close()
		return nil, err
	}
	bloomTier, err := newArtifactContentTier(ArtifactBloom, opts.bloomMaxBytes)
	if err != nil {
		_ = dirLock.close()
		return nil, err
	}
	cache := &artifactContentCache{
		dir:         opts.dir,
		versionDir:  filepath.Join(opts.dir, artifactContentCacheVersionDir),
		incomingDir: filepath.Join(opts.dir, "incoming"),
		tiers: map[ArtifactKind]*artifactContentTier{
			ArtifactSST:   sstTier,
			ArtifactBloom: bloomTier,
		},
		counters: map[ArtifactKind]*artifactContentCounters{
			ArtifactSST:   {},
			ArtifactBloom: {},
		},
		dirLock: dirLock,
	}
	if err := cache.prepare(); err != nil {
		_ = dirLock.close()
		return nil, err
	}
	return cache, nil
}

func (cache *artifactContentCache) path(address artifactContentAddress) string {
	return filepath.Join(cache.versionDir, address.relativePath())
}

func (cache *artifactContentCache) remove(address artifactContentAddress) error {
	return removeArtifactContentFile(cache.path(address))
}

func (cache *artifactContentCache) beginOperation() error {
	if cache == nil {
		return ErrArtifactCacheClosed
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return ErrArtifactCacheClosed
	}
	cache.activeOps++
	return nil
}

func (cache *artifactContentCache) endOperation() {
	cache.mu.Lock()
	cache.activeOps--
	dirLock := cache.takeDirectoryLockIfIdleLocked()
	cache.mu.Unlock()
	if dirLock != nil {
		_ = dirLock.close()
	}
}

func (cache *artifactContentCache) finishFill() {
	cache.mu.Lock()
	cache.activeFills--
	dirLock := cache.takeDirectoryLockIfIdleLocked()
	cache.mu.Unlock()
	if dirLock != nil {
		_ = dirLock.close()
	}
}

func (cache *artifactContentCache) transitionFillToHandleForTier(
	kind ArtifactKind,
	size int64,
	transient bool,
	inner artifactContentHandle,
) *artifactContentCacheHandle {
	cache.mu.Lock()
	cache.activeFills--
	cache.activeHandles++
	cache.mu.Unlock()
	if transient {
		counters := cache.counters[kind]
		counters.transientFiles.Add(1)
		counters.transientBytes.Add(size)
	}
	return &artifactContentCacheHandle{
		cache: cache, inner: inner, kind: kind, size: size, transient: transient,
	}
}

func (cache *artifactContentCache) registerHandle(
	kind ArtifactKind,
	size int64,
	transient bool,
	inner artifactContentHandle,
) *artifactContentCacheHandle {
	cache.mu.Lock()
	cache.activeHandles++
	cache.mu.Unlock()
	if transient {
		counters := cache.counters[kind]
		counters.transientFiles.Add(1)
		counters.transientBytes.Add(size)
	}
	return &artifactContentCacheHandle{
		cache: cache, inner: inner, kind: kind, size: size, transient: transient,
	}
}

func (cache *artifactContentCache) finishHandle() {
	cache.mu.Lock()
	cache.activeHandles--
	dirLock := cache.takeDirectoryLockIfIdleLocked()
	cache.mu.Unlock()
	if dirLock != nil {
		_ = dirLock.close()
	}
}

func (cache *artifactContentCache) takeDirectoryLockIfIdleLocked() *artifactDirectoryLock {
	if !cache.closed || cache.activeOps != 0 || cache.activeFills != 0 || cache.activeHandles != 0 {
		return nil
	}
	dirLock := cache.dirLock
	cache.dirLock = nil
	return dirLock
}

// close releases exclusive directory ownership. The eventual Reader facade
// must drain coalesced fills and close every persistent or transient handle
// before calling it.
func (cache *artifactContentCache) close() error {
	if cache == nil {
		return nil
	}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return nil
	}
	cache.closed = true
	dirLock := cache.takeDirectoryLockIfIdleLocked()
	cache.mu.Unlock()
	if dirLock != nil {
		return dirLock.close()
	}
	return nil
}
