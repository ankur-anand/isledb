package diskcache

import (
	"container/list"
	"fmt"
)

// artifactContentIndexEntry is one generation of a cached artifact. An entry
// may outlive its searchable index membership while an existing handle keeps
// it pinned.
type artifactContentIndexEntry struct {
	address         artifactContentAddress
	size            int64
	refs            int
	deleteOnRelease bool
	element         *list.Element
}

// artifactContentIndex maintains byte accounting and LRU order for one
// artifact kind. It provides no synchronization; the cache tier that owns the
// index must serialize access.
type artifactContentIndex struct {
	kind           ArtifactKind
	maxBytes       int64
	entries        map[artifactContentAddress]*artifactContentIndexEntry
	lru            list.List
	residentBytes  int64
	pinnedEntries  int
	pinnedBytes    int64
	pendingEntries int
	pendingBytes   int64
}

func newArtifactContentIndex(kind ArtifactKind, maxBytes int64) (*artifactContentIndex, error) {
	if !kind.valid() {
		return nil, fmt.Errorf("diskcache: invalid artifact index kind %d", kind)
	}
	if maxBytes < 0 {
		return nil, fmt.Errorf("diskcache: invalid artifact index capacity %d", maxBytes)
	}
	return &artifactContentIndex{
		kind:     kind,
		maxBytes: maxBytes,
		entries:  make(map[artifactContentAddress]*artifactContentIndexEntry),
	}, nil
}

// insertPinned adds an artifact as the most recently used entry with the
// reference owned by its publishing handle. Existing content is pinned and
// touched instead of being accounted twice.
func (index *artifactContentIndex) insertPinned(
	address artifactContentAddress,
	size int64,
) (*artifactContentIndexEntry, bool, error) {
	return index.insert(address, size, true)
}

func (index *artifactContentIndex) insertUnpinned(
	address artifactContentAddress,
	size int64,
) (*artifactContentIndexEntry, bool, error) {
	return index.insert(address, size, false)
}

func (index *artifactContentIndex) insert(
	address artifactContentAddress,
	size int64,
	pin bool,
) (*artifactContentIndexEntry, bool, error) {
	if address.kind != index.kind {
		return nil, false, fmt.Errorf(
			"diskcache: artifact kind %d does not belong in tier %d",
			address.kind, index.kind)
	}
	if size <= 0 {
		return nil, false, fmt.Errorf("diskcache: invalid artifact size %d", size)
	}
	if existing := index.entries[address]; existing != nil {
		if existing.size != size {
			return nil, false, fmt.Errorf(
				"diskcache: content size changed for one checksum: got=%d want=%d",
				size, existing.size)
		}
		if pin {
			if existing.refs == 0 {
				index.pinnedEntries++
				index.pinnedBytes += existing.size
			}
			existing.refs++
		}
		index.lru.MoveToBack(existing.element)
		return existing, false, nil
	}

	entry := &artifactContentIndexEntry{address: address, size: size}
	if pin {
		entry.refs = 1
		index.pinnedEntries++
		index.pinnedBytes += entry.size
	}
	entry.element = index.lru.PushBack(entry)
	index.entries[address] = entry
	index.residentBytes += size
	return entry, true, nil
}

// probe checks searchable residency without changing references or LRU order.
func (index *artifactContentIndex) probe(
	address artifactContentAddress,
) (*artifactContentIndexEntry, bool) {
	entry := index.entries[address]
	if entry == nil {
		return nil, false
	}
	return entry, true
}

// pin acquires a reference and records real use in the LRU.
func (index *artifactContentIndex) pin(
	address artifactContentAddress,
	size int64,
) (*artifactContentIndexEntry, bool, error) {
	entry := index.entries[address]
	if entry == nil {
		return nil, false, nil
	}
	if entry.size != size {
		return nil, false, fmt.Errorf(
			"diskcache: content size changed for one checksum: got=%d want=%d",
			size, entry.size)
	}
	if entry.refs == 0 {
		index.pinnedEntries++
		index.pinnedBytes += entry.size
	}
	entry.refs++
	index.lru.MoveToBack(entry.element)
	return entry, true, nil
}

// detach removes entry from searchable residency only if it still owns its
// content address. The first result reports whether its path may be deleted
// immediately; pinned entries defer that work until their last release.
func (index *artifactContentIndex) detach(
	entry *artifactContentIndexEntry,
) (deleteNow bool, detached bool) {
	if entry == nil || index.entries[entry.address] != entry {
		return false, false
	}
	delete(index.entries, entry.address)
	index.lru.Remove(entry.element)
	index.residentBytes -= entry.size
	entry.element = nil
	if entry.refs > 0 {
		entry.deleteOnRelease = true
		index.pendingEntries++
		index.pendingBytes += entry.size
		return false, true
	}
	return true, true
}

// release drops one handle reference. A true result means a detached entry's
// final handle closed and its derived path may now be considered for deletion.
// The caller must still verify that a replacement does not own that path.
func (index *artifactContentIndex) release(
	entry *artifactContentIndexEntry,
) (deletionOwed bool, err error) {
	if entry == nil || entry.refs == 0 {
		return false, fmt.Errorf("diskcache: release of unpinned artifact entry")
	}
	entry.refs--
	if entry.refs == 0 {
		index.pinnedEntries--
		index.pinnedBytes -= entry.size
		if entry.deleteOnRelease {
			index.pendingEntries--
			index.pendingBytes -= entry.size
		}
	}
	return entry.refs == 0 && entry.deleteOnRelease, nil
}

func (index *artifactContentIndex) oldest() *artifactContentIndexEntry {
	element := index.lru.Front()
	if element == nil {
		return nil
	}
	return element.Value.(*artifactContentIndexEntry)
}

// reserveCapacity atomically plans and detaches enough unpinned LRU entries
// for incomingSize. If insufficient reclaimable capacity exists, it leaves
// every entry searchable and returns a bypass decision.
func (index *artifactContentIndex) reserveCapacity(
	incomingSize int64,
) ([]*artifactContentIndexEntry, artifactContentAdmission) {
	if incomingSize > index.maxBytes {
		return nil, artifactContentBypassedOversized
	}
	required := index.residentBytes + incomingSize - index.maxBytes
	if required <= 0 {
		return nil, artifactContentAdmitted
	}

	var reclaimable int64
	var victims []*artifactContentIndexEntry
	for element := index.lru.Front(); element != nil && reclaimable < required; element = element.Next() {
		entry := element.Value.(*artifactContentIndexEntry)
		if entry.refs != 0 {
			continue
		}
		victims = append(victims, entry)
		reclaimable += entry.size
	}
	if reclaimable < required {
		return nil, artifactContentBypassedPinnedCapacity
	}

	for _, victim := range victims {
		delete(index.entries, victim.address)
		index.lru.Remove(victim.element)
		index.residentBytes -= victim.size
		victim.element = nil
	}
	return victims, artifactContentAdmitted
}
