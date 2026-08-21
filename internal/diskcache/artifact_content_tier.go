package diskcache

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type artifactContentAdmission uint8

const (
	artifactContentAdmitted artifactContentAdmission = iota + 1
	artifactContentAlreadyResident
	artifactContentBypassedOversized
	artifactContentBypassedPinnedCapacity
	artifactContentBypassedPublicationFailure
)

// artifactContentTier serializes one independently budgeted artifact index.
//
// Lock order is always publishMu followed by indexMu. Operations that begin
// with indexMu must release it before attempting to acquire publishMu.
type artifactContentTier struct {
	publishMu         sync.Mutex
	indexMu           sync.Mutex
	index             *artifactContentIndex
	capacityEvictions atomic.Int64
}

func newArtifactContentTier(kind ArtifactKind, maxBytes int64) (*artifactContentTier, error) {
	index, err := newArtifactContentIndex(kind, maxBytes)
	if err != nil {
		return nil, err
	}
	return &artifactContentTier{index: index}, nil
}

// probe checks residency without pinning or changing recency.
func (tier *artifactContentTier) probe(
	address artifactContentAddress,
) (*artifactContentIndexEntry, bool) {
	tier.indexMu.Lock()
	defer tier.indexMu.Unlock()
	return tier.index.probe(address)
}

// pin acquires a resident entry for a real read.
func (tier *artifactContentTier) pin(
	address artifactContentAddress,
	size int64,
) (*artifactContentIndexEntry, bool, error) {
	tier.indexMu.Lock()
	defer tier.indexMu.Unlock()
	return tier.index.pin(address, size)
}

// publishPinned serializes publication and inserts the resulting entry with
// the reference owned by the publishing handle. publishFile must make the
// final derived path visible before it returns.
func (tier *artifactContentTier) publishPinned(
	address artifactContentAddress,
	size int64,
	publishFile func() error,
	removeFile func(artifactContentAddress) error,
) (*artifactContentIndexEntry, artifactContentAdmission, error) {
	if address.kind != tier.index.kind {
		return nil, artifactContentBypassedPublicationFailure, fmt.Errorf(
			"diskcache: artifact kind %d does not belong in tier %d",
			address.kind, tier.index.kind)
	}
	if size <= 0 {
		return nil, artifactContentBypassedPublicationFailure,
			fmt.Errorf("diskcache: invalid artifact size %d", size)
	}
	if publishFile == nil || removeFile == nil {
		return nil, artifactContentBypassedPublicationFailure,
			errors.New("diskcache: artifact publication callbacks are required")
	}
	tier.publishMu.Lock()
	defer tier.publishMu.Unlock()

	tier.indexMu.Lock()
	existing, ok, err := tier.index.pin(address, size)
	if err != nil {
		tier.indexMu.Unlock()
		return nil, artifactContentBypassedPublicationFailure, err
	}
	if ok {
		tier.indexMu.Unlock()
		return existing, artifactContentAlreadyResident, nil
	}
	victims, admission := tier.index.reserveCapacity(size)
	tier.indexMu.Unlock()
	if admission != artifactContentAdmitted {
		return nil, admission, nil
	}
	tier.capacityEvictions.Add(int64(len(victims)))

	var cleanupErr error
	for _, victim := range victims {
		cleanupErr = errors.Join(cleanupErr, removeFile(victim.address))
	}
	if cleanupErr != nil {
		return nil, artifactContentBypassedPublicationFailure,
			fmt.Errorf("diskcache: remove capacity victims: %w", cleanupErr)
	}

	if err := publishFile(); err != nil {
		return nil, artifactContentBypassedPublicationFailure, err
	}

	tier.indexMu.Lock()
	entry, inserted, err := tier.index.insertPinned(address, size)
	tier.indexMu.Unlock()
	if err != nil || !inserted {
		if err == nil {
			err = errors.New("diskcache: published content was already indexed")
		}
		return nil, artifactContentBypassedPublicationFailure, err
	}
	return entry, artifactContentAdmitted, nil
}

// detach removes an entry from searchable residency. removeFile runs while
// publication is excluded, but only when no handle still references the
// detached generation.
func (tier *artifactContentTier) detach(
	entry *artifactContentIndexEntry,
	removeFile func(artifactContentAddress) error,
) (bool, error) {
	tier.publishMu.Lock()
	defer tier.publishMu.Unlock()

	tier.indexMu.Lock()
	deleteNow, detached := tier.index.detach(entry)
	tier.indexMu.Unlock()
	if !detached || !deleteNow {
		return detached, nil
	}
	return true, removeFile(entry.address)
}

// purge detaches every searchable entry. Pinned generations remain usable by
// their existing handles and remove their file after the final release.
func (tier *artifactContentTier) purge(
	removeFile func(artifactContentAddress) error,
) (int, error) {
	if removeFile == nil {
		return 0, errors.New("diskcache: artifact removal callback is required")
	}
	tier.publishMu.Lock()
	defer tier.publishMu.Unlock()

	tier.indexMu.Lock()
	entries := make([]*artifactContentIndexEntry, 0, len(tier.index.entries))
	for _, entry := range tier.index.entries {
		entries = append(entries, entry)
	}
	deleteNow := make([]artifactContentAddress, 0, len(entries))
	for _, entry := range entries {
		removeNow, detached := tier.index.detach(entry)
		if detached && removeNow {
			deleteNow = append(deleteNow, entry.address)
		}
	}
	tier.indexMu.Unlock()

	var cleanupErr error
	for _, address := range deleteNow {
		cleanupErr = errors.Join(cleanupErr, removeFile(address))
	}
	return len(entries), cleanupErr
}

// release drops a handle reference. It uses two disjoint lock phases so it
// never attempts publishMu while holding indexMu. If a replacement was
// admitted in between, the old generation no longer owns the derived path and
// removal is skipped.
func (tier *artifactContentTier) release(
	entry *artifactContentIndexEntry,
	removeFile func(artifactContentAddress) error,
) error {
	tier.indexMu.Lock()
	deletionOwed, err := tier.index.release(entry)
	tier.indexMu.Unlock()
	if err != nil || !deletionOwed {
		return err
	}

	tier.publishMu.Lock()
	defer tier.publishMu.Unlock()
	tier.indexMu.Lock()
	_, replacementPresent := tier.index.probe(entry.address)
	tier.indexMu.Unlock()
	if replacementPresent {
		return nil
	}
	return removeFile(entry.address)
}
