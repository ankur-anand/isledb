package diskcache

import (
	"container/list"
	"fmt"
)

// artifactContentIndexEntry is one searchable resident artifact. Filesystem
// lifetime and handle references are deliberately outside this first index
// primitive.
type artifactContentIndexEntry struct {
	address artifactContentAddress
	size    int64
	element *list.Element
}

// artifactContentIndex maintains byte accounting and LRU order for one
// artifact kind. It provides no synchronization; the cache tier that owns the
// index must serialize access.
type artifactContentIndex struct {
	kind          ArtifactKind
	entries       map[artifactContentAddress]*artifactContentIndexEntry
	lru           list.List
	residentBytes int64
}

func newArtifactContentIndex(kind ArtifactKind) (*artifactContentIndex, error) {
	if !kind.valid() {
		return nil, fmt.Errorf("diskcache: invalid artifact index kind %d", kind)
	}
	return &artifactContentIndex{
		kind:    kind,
		entries: make(map[artifactContentAddress]*artifactContentIndexEntry),
	}, nil
}

// insert adds an artifact as the most recently used entry. Existing content
// is left in place and touched instead of being accounted twice.
func (index *artifactContentIndex) insert(
	address artifactContentAddress,
	size int64,
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
		index.lru.MoveToBack(existing.element)
		return existing, false, nil
	}

	entry := &artifactContentIndexEntry{address: address, size: size}
	entry.element = index.lru.PushBack(entry)
	index.entries[address] = entry
	index.residentBytes += size
	return entry, true, nil
}

// find returns an entry and optionally records its use. Probe-like callers can
// pass touch=false to avoid changing eviction order.
func (index *artifactContentIndex) find(
	address artifactContentAddress,
	touch bool,
) (*artifactContentIndexEntry, bool) {
	entry := index.entries[address]
	if entry == nil {
		return nil, false
	}
	if touch {
		index.lru.MoveToBack(entry.element)
	}
	return entry, true
}

// remove deletes entry only if it is still the indexed owner of its content
// address. This prevents stale entry pointers from removing a replacement.
func (index *artifactContentIndex) remove(entry *artifactContentIndexEntry) bool {
	if entry == nil || index.entries[entry.address] != entry {
		return false
	}
	delete(index.entries, entry.address)
	index.lru.Remove(entry.element)
	index.residentBytes -= entry.size
	entry.element = nil
	return true
}

func (index *artifactContentIndex) oldest() *artifactContentIndexEntry {
	element := index.lru.Front()
	if element == nil {
		return nil
	}
	return element.Value.(*artifactContentIndexEntry)
}
