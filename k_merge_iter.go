package isledb

import (
	"bytes"
	"container/heap"

	"github.com/cockroachdb/pebble/sstable"
)

// KMergeIterator merges multiple SSTable iterators, yielding entries in sorted order.
type KMergeIterator struct {
	iters   []*sstIterWrapper
	heap    mergeHeap
	current *mergeEntry
	lastKey []byte
	err     error
}

// mergeEntry represents an entry from one of the source iterators.
type mergeEntry struct {
	key     []byte
	seq     uint64
	trailer uint64
	kind    sstable.InternalKeyKind
	value   []byte
	source  int
}

type sstIterWrapper struct {
	iter   sstable.Iterator
	source int
}

// mergeHeap implements heap.Interface for merge entries.
type mergeHeap []*mergeEntry

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	// first lexicographically small and then
	// we compare version wise.
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].trailer > h[j].trailer
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(*mergeEntry))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// NewMergeIterator creates a merge iterator from SSTable iterators.
func NewMergeIterator(iters []sstable.Iterator) *KMergeIterator {
	mi := &KMergeIterator{
		iters: make([]*sstIterWrapper, len(iters)),
		heap:  make(mergeHeap, 0, len(iters)),
	}

	for i, iter := range iters {
		mi.iters[i] = &sstIterWrapper{iter: iter, source: i}
		mi.advanceIter(i)
	}

	heap.Init(&mi.heap)
	return mi
}

// advanceIter moves iterator i to the first entry and pushes to heap.
func (mi *KMergeIterator) advanceIter(i int) {
	wrapper := mi.iters[i]
	ikey, val := wrapper.iter.First()
	if ikey != nil {
		mi.pushEntry(ikey, &val, i)
	}
}

// advanceIterNext calls Next() and pushes if valid.
func (mi *KMergeIterator) advanceIterNext(i int) {
	wrapper := mi.iters[i]
	ikey, val := wrapper.iter.Next()
	if ikey != nil {
		mi.pushEntry(ikey, &val, i)
	}
}

// lazyValueGetter is a minimal interface for getting the in-place value.
type lazyValueGetter interface {
	InPlaceValue() []byte
}

func (mi *KMergeIterator) pushEntry(ikey *sstable.InternalKey, val lazyValueGetter, source int) {
	trailer := ikey.Trailer
	seq := trailer >> 8
	kind := ikey.Kind()

	keyCopy := make([]byte, len(ikey.UserKey))
	copy(keyCopy, ikey.UserKey)

	valBytes := val.InPlaceValue()
	valCopy := make([]byte, len(valBytes))
	copy(valCopy, valBytes)

	entry := &mergeEntry{
		key:     keyCopy,
		seq:     seq,
		trailer: trailer,
		kind:    kind,
		value:   valCopy,
		source:  source,
	}

	heap.Push(&mi.heap, entry)
}

// Next advances to the next unique key, skipping older versions.
func (mi *KMergeIterator) Next() bool {
	for mi.heap.Len() > 0 {
		entry := heap.Pop(&mi.heap).(*mergeEntry)

		mi.advanceIterNext(entry.source)

		if mi.lastKey != nil && bytes.Equal(entry.key, mi.lastKey) {
			continue
		}

		mi.current = entry
		mi.lastKey = append(mi.lastKey[:0], entry.key...)
		return true
	}
	return false
}

func (mi *KMergeIterator) Key() []byte {
	if mi.current == nil {
		return nil
	}
	return mi.current.key
}

func (mi *KMergeIterator) Seq() uint64 {
	if mi.current == nil {
		return 0
	}
	return mi.current.seq
}

func (mi *KMergeIterator) Kind() sstable.InternalKeyKind {
	if mi.current == nil {
		return sstable.InternalKeyKindInvalid
	}
	return mi.current.kind
}

func (mi *KMergeIterator) Value() []byte {
	if mi.current == nil {
		return nil
	}
	return mi.current.value
}

func (mi *KMergeIterator) Entry() (CompactionEntry, error) {
	if mi.current == nil {
		return CompactionEntry{}, nil
	}

	entry := CompactionEntry{
		Key: mi.current.key,
		Seq: mi.current.seq,
	}

	if mi.current.kind == sstable.InternalKeyKindDelete {
		entry.Kind = OpDelete
		return entry, nil
	}

	keyEntry, err := DecodeKeyEntry(mi.current.key, mi.current.value)
	if err != nil {
		return CompactionEntry{}, err
	}

	entry.Kind = keyEntry.Kind
	entry.Inline = keyEntry.Inline
	entry.Value = keyEntry.Value
	entry.VLogID = keyEntry.VLogID
	entry.VOffset = keyEntry.VOffset
	entry.VLength = keyEntry.VLength
	entry.VChecksum = keyEntry.VChecksum

	return entry, nil
}

func (mi *KMergeIterator) Err() error {
	if mi.err != nil {
		return mi.err
	}
	for _, w := range mi.iters {
		if err := w.iter.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (mi *KMergeIterator) Close() error {
	var firstErr error
	for _, w := range mi.iters {
		if err := w.iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
