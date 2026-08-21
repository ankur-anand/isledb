package diskcache

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

type artifactContentCacheStats struct {
	Hits                       int64
	Misses                     int64
	Corruptions                int64
	Admitted                   int64
	AlreadyResident            int64
	BypassedOversized          int64
	BypassedPinnedCapacity     int64
	BypassedPublicationFailure int64
	CapacityEvictions          int64
	Removals                   int64
	PurgeRemovals              int64
	RecoveryRemovals           int64
	RecoveredEntries           int64
	RecoveredBytes             int64

	ResidentEntries int
	ResidentBytes   int64
	PinnedEntries   int
	PinnedBytes     int64
	PendingEntries  int
	PendingBytes    int64
	IncomingFiles   int64
	IncomingBytes   int64
	TransientFiles  int64
	TransientBytes  int64
	MaxBytes        int64
}

// probe is counter-free and does not alter LRU order.
func (cache *artifactContentCache) probe(desc ArtifactDescriptor) (bool, error) {
	if err := desc.validate(); err != nil {
		return false, err
	}
	address, err := artifactContentAddressFor(desc)
	if err != nil {
		return false, err
	}
	if err := cache.beginOperation(); err != nil {
		return false, err
	}
	defer cache.endOperation()
	entry, ok := cache.tiers[address.kind].probe(address)
	return ok && entry.size == desc.Size, nil
}

// acquire returns a persistent handle on a cache hit. A non-nil error with
// hit=false is a cache diagnostic; callers must continue to the authoritative
// origin rather than failing the read.
func (cache *artifactContentCache) acquire(
	desc ArtifactDescriptor,
) (*artifactContentCacheHandle, bool, error) {
	if err := desc.validate(); err != nil {
		return nil, false, err
	}
	address, err := artifactContentAddressFor(desc)
	if err != nil {
		return nil, false, err
	}
	if err := cache.beginOperation(); err != nil {
		return nil, false, err
	}
	defer cache.endOperation()

	tier := cache.tiers[address.kind]
	handle, hit, acquireErr := tier.acquire(address, desc.Size, cache.path(address), cache.remove)
	counters := cache.counters[address.kind]
	if !hit {
		counters.misses.Add(1)
		if errors.Is(acquireErr, ErrArtifactChecksumMismatch) ||
			errors.Is(acquireErr, ErrArtifactSizeMismatch) {
			counters.corruptions.Add(1)
		}
		return nil, false, acquireErr
	}
	counters.hits.Add(1)
	return cache.registerHandle(address.kind, desc.Size, false, handle), true, nil
}

func (cache *artifactContentCache) beginFill(
	desc ArtifactDescriptor,
) (*artifactContentCacheFill, error) {
	if err := desc.validate(); err != nil {
		return nil, err
	}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return nil, ErrArtifactCacheClosed
	}
	cache.activeFills++
	cache.mu.Unlock()

	fill, err := newArtifactContentFill(cache.incomingDir, desc)
	if err != nil {
		cache.finishFill()
		return nil, err
	}
	counters := cache.counters[desc.Key.Kind]
	counters.incomingFiles.Add(1)
	return &artifactContentCacheFill{cache: cache, fill: fill, kind: desc.Key.Kind}, nil
}

func (cache *artifactContentCache) removeDescriptor(
	desc ArtifactDescriptor,
	reason ArtifactRemovalReason,
) (bool, error) {
	if err := desc.validate(); err != nil {
		return false, err
	}
	address, err := artifactContentAddressFor(desc)
	if err != nil {
		return false, err
	}
	if err := cache.beginOperation(); err != nil {
		return false, err
	}
	defer cache.endOperation()
	tier := cache.tiers[address.kind]
	entry, ok := tier.probe(address)
	if !ok {
		return false, nil
	}
	detached, err := tier.detach(entry, cache.remove)
	if detached {
		counters := cache.counters[address.kind]
		counters.removals.Add(1)
		if reason == ArtifactRemovalCorrupt {
			counters.corruptions.Add(1)
		}
		if reason == ArtifactRemovalPurge {
			counters.purgeRemovals.Add(1)
		}
	}
	return detached, err
}

func (cache *artifactContentCache) purge(kind ArtifactKind) error {
	if !kind.valid() {
		return fmt.Errorf("diskcache: invalid artifact kind %d", kind)
	}
	if err := cache.beginOperation(); err != nil {
		return err
	}
	defer cache.endOperation()
	removed, err := cache.tiers[kind].purge(cache.remove)
	if removed > 0 {
		cache.counters[kind].removals.Add(int64(removed))
		cache.counters[kind].purgeRemovals.Add(int64(removed))
	}
	return err
}

func (cache *artifactContentCache) stats(kind ArtifactKind) artifactContentCacheStats {
	tier := cache.tiers[kind]
	counters := cache.counters[kind]
	if tier == nil || counters == nil {
		return artifactContentCacheStats{}
	}
	tier.indexMu.Lock()
	stats := artifactContentCacheStats{
		ResidentEntries: len(tier.index.entries),
		ResidentBytes:   tier.index.residentBytes,
		PinnedEntries:   tier.index.pinnedEntries,
		PinnedBytes:     tier.index.pinnedBytes,
		PendingEntries:  tier.index.pendingEntries,
		PendingBytes:    tier.index.pendingBytes,
		MaxBytes:        tier.index.maxBytes,
	}
	tier.indexMu.Unlock()
	stats.Hits = counters.hits.Load()
	stats.Misses = counters.misses.Load()
	stats.Corruptions = counters.corruptions.Load()
	stats.Admitted = counters.admitted.Load()
	stats.AlreadyResident = counters.alreadyResident.Load()
	stats.BypassedOversized = counters.bypassedOversized.Load()
	stats.BypassedPinnedCapacity = counters.bypassedPinnedCapacity.Load()
	stats.BypassedPublicationFailure = counters.bypassedPublicationFailure.Load()
	stats.CapacityEvictions = tier.capacityEvictions.Load()
	stats.Removals = counters.removals.Load()
	stats.PurgeRemovals = counters.purgeRemovals.Load()
	stats.RecoveryRemovals = counters.recoveryRemovals.Load()
	stats.RecoveredEntries = counters.recoveredEntries.Load()
	stats.RecoveredBytes = counters.recoveredBytes.Load()
	stats.IncomingFiles = counters.incomingFiles.Load()
	stats.IncomingBytes = counters.incomingBytes.Load()
	stats.TransientFiles = counters.transientFiles.Load()
	stats.TransientBytes = counters.transientBytes.Load()
	return stats
}

type artifactContentCacheFill struct {
	mu sync.Mutex

	cache   *artifactContentCache
	fill    *artifactContentFill
	kind    ArtifactKind
	written atomic.Int64
	done    bool
}

func (fill *artifactContentCacheFill) Write(data []byte) (int, error) {
	if fill == nil {
		return 0, os.ErrInvalid
	}
	written, err := fill.fill.Write(data)
	if written > 0 {
		fill.written.Add(int64(written))
		fill.cache.counters[fill.kind].incomingBytes.Add(int64(written))
	}
	return written, err
}

func (fill *artifactContentCacheFill) commit() (
	*artifactContentCacheHandle,
	artifactContentAdmission,
	error,
) {
	if fill == nil {
		return nil, 0, os.ErrInvalid
	}
	fill.mu.Lock()
	if fill.done {
		fill.mu.Unlock()
		return nil, 0, os.ErrClosed
	}
	fill.done = true
	fill.mu.Unlock()

	staged, err := fill.fill.finish()
	if err != nil {
		fill.completeWithoutHandle()
		return nil, 0, err
	}
	tier := fill.cache.tiers[fill.kind]
	entry, admission, publishErr := tier.publishPinned(
		staged.address,
		staged.size,
		func() error { return staged.publish(fill.cache.path(staged.address)) },
		fill.cache.remove,
	)
	fill.recordAdmission(admission)

	switch admission {
	case artifactContentAdmitted:
		if publishErr != nil {
			return fill.transientOrError(staged, admission, publishErr)
		}
		persistent, hit, openErr := tier.openPinned(
			entry, fill.cache.path(staged.address), fill.cache.remove)
		if openErr != nil || !hit {
			_ = staged.discard()
			fill.completeWithoutHandle()
			return nil, admission, openErr
		}
		_ = staged.discard()
		fill.completeIncomingAccounting()
		return fill.cache.transitionFillToHandleForTier(
			fill.kind, staged.size, false, persistent), admission, nil

	case artifactContentAlreadyResident:
		if publishErr == nil {
			persistent, hit, openErr := tier.openPinned(
				entry, fill.cache.path(staged.address), fill.cache.remove)
			if openErr == nil && hit {
				_ = staged.discard()
				fill.completeIncomingAccounting()
				return fill.cache.transitionFillToHandleForTier(
					fill.kind, staged.size, false, persistent), admission, nil
			}
			publishErr = openErr
		}
		fill.cache.counters[fill.kind].bypassedPublicationFailure.Add(1)
		return fill.transientOrError(staged, artifactContentBypassedPublicationFailure, publishErr)

	case artifactContentBypassedOversized,
		artifactContentBypassedPinnedCapacity,
		artifactContentBypassedPublicationFailure:
		return fill.transientOrError(staged, admission, publishErr)

	default:
		_ = staged.discard()
		fill.completeWithoutHandle()
		return nil, admission, errors.New("diskcache: invalid content admission outcome")
	}
}

func (fill *artifactContentCacheFill) transientOrError(
	staged *artifactStagedContent,
	admission artifactContentAdmission,
	diagnostic error,
) (*artifactContentCacheHandle, artifactContentAdmission, error) {
	transient, err := staged.openTransient()
	if err != nil {
		_ = staged.discard()
		fill.completeWithoutHandle()
		return nil, admission, errors.Join(diagnostic, err)
	}
	fill.completeIncomingAccounting()
	return fill.cache.transitionFillToHandleForTier(
		fill.kind, staged.size, true, transient), admission, nil
}

func (fill *artifactContentCacheFill) abort() error {
	if fill == nil {
		return nil
	}
	fill.mu.Lock()
	if fill.done {
		fill.mu.Unlock()
		return nil
	}
	fill.done = true
	fill.mu.Unlock()
	err := fill.fill.abort()
	fill.completeWithoutHandle()
	return err
}

func (fill *artifactContentCacheFill) recordAdmission(admission artifactContentAdmission) {
	counters := fill.cache.counters[fill.kind]
	switch admission {
	case artifactContentAdmitted:
		counters.admitted.Add(1)
	case artifactContentAlreadyResident:
		counters.alreadyResident.Add(1)
	case artifactContentBypassedOversized:
		counters.bypassedOversized.Add(1)
	case artifactContentBypassedPinnedCapacity:
		counters.bypassedPinnedCapacity.Add(1)
	case artifactContentBypassedPublicationFailure:
		counters.bypassedPublicationFailure.Add(1)
	}
}

func (fill *artifactContentCacheFill) completeIncomingAccounting() {
	counters := fill.cache.counters[fill.kind]
	counters.incomingFiles.Add(-1)
	counters.incomingBytes.Add(-fill.written.Load())
}

func (fill *artifactContentCacheFill) completeWithoutHandle() {
	fill.completeIncomingAccounting()
	fill.cache.finishFill()
}

type artifactContentCacheHandle struct {
	cache     *artifactContentCache
	inner     artifactContentHandle
	kind      ArtifactKind
	size      int64
	transient bool
	once      sync.Once
	err       error
}

func (handle *artifactContentCacheHandle) bytes() []byte {
	if handle == nil || handle.inner == nil {
		return nil
	}
	return handle.inner.bytes()
}

func (handle *artifactContentCacheHandle) close() error {
	if handle == nil {
		return nil
	}
	handle.once.Do(func() {
		handle.err = handle.inner.close()
		if handle.transient {
			counters := handle.cache.counters[handle.kind]
			counters.transientFiles.Add(-1)
			counters.transientBytes.Add(-handle.size)
		}
		handle.cache.finishHandle()
	})
	return handle.err
}

func (cache *artifactContentCache) recordRecoveryRemoval(kind ArtifactKind, count int) {
	if count > 0 {
		cache.counters[kind].recoveryRemovals.Add(int64(count))
	}
}

func (cache *artifactContentCache) recordRecovered(kind ArtifactKind, size int64) {
	cache.counters[kind].recoveredEntries.Add(1)
	cache.counters[kind].recoveredBytes.Add(size)
}
