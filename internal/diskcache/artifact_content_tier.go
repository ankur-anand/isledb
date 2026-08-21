package diskcache

import "sync"

// artifactContentTier serializes one independently budgeted artifact index.
//
// Lock order is always publishMu followed by indexMu. Operations that begin
// with indexMu must release it before attempting to acquire publishMu.
type artifactContentTier struct {
	publishMu sync.Mutex
	indexMu   sync.Mutex
	index     *artifactContentIndex
}

func newArtifactContentTier(kind ArtifactKind) (*artifactContentTier, error) {
	index, err := newArtifactContentIndex(kind)
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
) (*artifactContentIndexEntry, bool, error) {
	tier.publishMu.Lock()
	defer tier.publishMu.Unlock()

	tier.indexMu.Lock()
	existing, ok, err := tier.index.pin(address, size)
	tier.indexMu.Unlock()
	if err != nil || ok {
		return existing, false, err
	}

	if err := publishFile(); err != nil {
		return nil, false, err
	}

	tier.indexMu.Lock()
	entry, inserted, err := tier.index.insertPinned(address, size)
	tier.indexMu.Unlock()
	return entry, inserted, err
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
