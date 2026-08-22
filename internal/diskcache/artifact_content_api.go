package diskcache

import "os"

// ArtifactCache is the Reader-facing facade for the checksum-addressed
// persistent SST and Bloom cache.
type ArtifactCache struct {
	inner *artifactContentCache
}

// ArtifactHandle pins immutable artifact bytes until Close.
type ArtifactHandle struct {
	inner *artifactContentCacheHandle
}

// ArtifactFill streams one bounded artifact into required local staging.
// Failure to create that staging file is terminal for the fill. After all
// bytes have been staged and verified, Commit returns usable bytes through a
// transient handle when persistent publication is unavailable.
type ArtifactFill struct {
	inner *artifactContentCacheFill
}

// OpenArtifactCache opens or recovers the checksum-addressed cache.
func OpenArtifactCache(opts ArtifactCacheOptions) (*ArtifactCache, error) {
	normalized, err := opts.normalize()
	if err != nil {
		return nil, err
	}
	inner, err := openArtifactContentCache(artifactContentCacheOptions{
		dir:           normalized.Dir,
		sstMaxBytes:   normalized.SSTMaxBytes,
		bloomMaxBytes: normalized.BloomMaxBytes,
	})
	if err != nil {
		return nil, err
	}
	return &ArtifactCache{inner: inner}, nil
}

func (cache *ArtifactCache) Probe(desc ArtifactDescriptor) (ArtifactPresence, error) {
	if cache == nil || cache.inner == nil {
		return ArtifactAbsent, ErrArtifactCacheClosed
	}
	resident, err := cache.inner.probe(desc)
	if err != nil || !resident {
		return ArtifactAbsent, err
	}
	// Recovered SSTs are intentionally not rehashed. Bloom integrity is checked
	// when its bytes are acquired, so presence alone never claims verification.
	return ArtifactResidentUnverified, nil
}

func (cache *ArtifactCache) Acquire(
	desc ArtifactDescriptor,
) (*ArtifactHandle, bool, error) {
	if cache == nil || cache.inner == nil {
		return nil, false, ErrArtifactCacheClosed
	}
	handle, hit, err := cache.inner.acquire(desc)
	if !hit {
		return nil, false, err
	}
	return &ArtifactHandle{inner: handle}, true, nil
}

func (cache *ArtifactCache) BeginFill(desc ArtifactDescriptor) (*ArtifactFill, error) {
	if cache == nil || cache.inner == nil {
		return nil, ErrArtifactCacheClosed
	}
	fill, err := cache.inner.beginFill(desc)
	if err != nil {
		return nil, err
	}
	return &ArtifactFill{inner: fill}, nil
}

func (cache *ArtifactCache) AdmitBytes(
	desc ArtifactDescriptor,
	data []byte,
) (*ArtifactHandle, ArtifactAdmission, error) {
	fill, err := cache.BeginFill(desc)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fill.Write(data); err != nil {
		_ = fill.Abort()
		return nil, 0, err
	}
	return fill.Commit()
}

func (cache *ArtifactCache) Remove(
	desc ArtifactDescriptor,
	reason ArtifactRemovalReason,
) error {
	if cache == nil || cache.inner == nil {
		return ErrArtifactCacheClosed
	}
	_, err := cache.inner.removeDescriptor(desc, reason)
	return err
}

func (cache *ArtifactCache) Purge(kind ArtifactKind) error {
	if cache == nil || cache.inner == nil {
		return ErrArtifactCacheClosed
	}
	return cache.inner.purge(kind)
}

func (cache *ArtifactCache) Stats(kind ArtifactKind) ArtifactStats {
	if cache == nil || cache.inner == nil {
		return ArtifactStats{}
	}
	stats := cache.inner.stats(kind)
	return ArtifactStats{
		Hits:                stats.Hits,
		Misses:              stats.Misses,
		Corruptions:         stats.Corruptions,
		Evictions:           stats.CapacityEvictions,
		CapacityEvictions:   stats.CapacityEvictions,
		PurgeRemovals:       stats.PurgeRemovals,
		RecoveryRemovals:    stats.RecoveryRemovals,
		SyncFailures:        stats.SyncFailures,
		PublicationFailures: stats.PublicationFailures,
		AdmissionBypasses: stats.BypassedOversized +
			stats.BypassedPinnedCapacity + stats.BypassedPublicationFailure,
		RecoveredEntries: stats.RecoveredEntries,
		RecoveredBytes:   stats.RecoveredBytes,
		// Preserve the Reader-facing occupancy meaning: a detached generation
		// still pinned by a handle occupies a file even though it no longer
		// participates in the searchable resident-byte budget.
		ResidentEntries: stats.ResidentEntries + stats.PendingEntries,
		ResidentBytes:   stats.ResidentBytes + stats.PendingBytes,
		PinnedEntries:   stats.PinnedEntries,
		PinnedBytes:     stats.PinnedBytes,
		MaxBytes:        stats.MaxBytes,
	}
}

// Close releases the cache's local resources and directory lock. A
// caller-supplied ArtifactCache must remain open until all Readers using it
// have closed; closing it early makes local SST staging unavailable and may
// cause subsequent reads to fail.
func (cache *ArtifactCache) Close() error {
	if cache == nil || cache.inner == nil {
		return nil
	}
	return cache.inner.close()
}

func (handle *ArtifactHandle) Bytes() []byte {
	if handle == nil || handle.inner == nil {
		return nil
	}
	return handle.inner.bytes()
}

func (handle *ArtifactHandle) Close() error {
	if handle == nil || handle.inner == nil {
		return nil
	}
	return handle.inner.close()
}

func (fill *ArtifactFill) Write(data []byte) (int, error) {
	if fill == nil || fill.inner == nil {
		return 0, os.ErrInvalid
	}
	return fill.inner.Write(data)
}

func (fill *ArtifactFill) Commit() (*ArtifactHandle, ArtifactAdmission, error) {
	if fill == nil || fill.inner == nil {
		return nil, 0, os.ErrInvalid
	}
	handle, admission, err := fill.inner.commit()
	if err != nil {
		return nil, contentAdmission(admission), err
	}
	return &ArtifactHandle{inner: handle}, contentAdmission(admission), nil
}

func (fill *ArtifactFill) Abort() error {
	if fill == nil || fill.inner == nil {
		return nil
	}
	return fill.inner.abort()
}

func contentAdmission(admission artifactContentAdmission) ArtifactAdmission {
	switch admission {
	case artifactContentAdmitted:
		return ArtifactAdmitted
	case artifactContentAlreadyResident:
		return ArtifactAlreadyResident
	case artifactContentBypassedOversized:
		return ArtifactBypassedOversized
	case artifactContentBypassedPinnedCapacity:
		return ArtifactBypassedPinnedCapacity
	case artifactContentBypassedPublicationFailure:
		return ArtifactBypassedPublicationFailure
	default:
		return 0
	}
}
