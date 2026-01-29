package isledb

import (
	"bytes"
	"container/heap"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

type KMergeIterator struct {
	iters   []*sstIterWrapper
	heap    mergeHeap
	current *mergeEntry
	lastKey []byte
	err     error
}

type mergeEntry struct {
	key     []byte
	seq     uint64
	trailer uint64
	kind    pebble.InternalKeyKind
	value   []byte
	source  int
}

type sstIterWrapper struct {
	iter   sstable.Iterator
	source int
}

type mergeHeap []*mergeEntry

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {

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

func (mi *KMergeIterator) advanceIter(i int) {
	wrapper := mi.iters[i]
	kv := wrapper.iter.First()
	if kv != nil {
		mi.pushEntry(&kv.K, kv.InPlaceValue(), i)
	}
}

func (mi *KMergeIterator) advanceIterNext(i int) {
	wrapper := mi.iters[i]
	kv := wrapper.iter.Next()
	if kv != nil {
		mi.pushEntry(&kv.K, kv.InPlaceValue(), i)
	}
}

func (mi *KMergeIterator) pushEntry(ikey *sstable.InternalKey, val []byte, source int) {
	trailer := ikey.Trailer
	seq := uint64(trailer >> 8)
	kind := ikey.Kind()

	keyCopy := make([]byte, len(ikey.UserKey))
	copy(keyCopy, ikey.UserKey)

	valCopy := make([]byte, len(val))
	copy(valCopy, val)

	entry := &mergeEntry{
		key:     keyCopy,
		seq:     seq,
		trailer: uint64(trailer),
		kind:    kind,
		value:   valCopy,
		source:  source,
	}

	heap.Push(&mi.heap, entry)
}

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

func (mi *KMergeIterator) Kind() pebble.InternalKeyKind {
	if mi.current == nil {
		return pebble.InternalKeyKindInvalid
	}
	return mi.current.kind
}

func (mi *KMergeIterator) Value() []byte {
	if mi.current == nil {
		return nil
	}
	return mi.current.value
}

func (mi *KMergeIterator) Entry() (internal.CompactionEntry, error) {
	if mi.current == nil {
		return internal.CompactionEntry{}, nil
	}

	entry := internal.CompactionEntry{
		Key: mi.current.key,
		Seq: mi.current.seq,
	}

	if mi.current.kind == sstable.InternalKeyKindDelete {
		entry.Kind = internal.OpDelete

		if len(mi.current.value) > 0 {
			keyEntry, err := internal.DecodeKeyEntry(mi.current.key, mi.current.value)
			if err == nil {
				entry.ExpireAt = keyEntry.ExpireAt
			}
		}
		return entry, nil
	}

	keyEntry, err := internal.DecodeKeyEntry(mi.current.key, mi.current.value)
	if err != nil {
		return internal.CompactionEntry{}, err
	}

	entry.Kind = keyEntry.Kind
	entry.Inline = keyEntry.Inline
	entry.Value = keyEntry.Value
	entry.BlobID = keyEntry.BlobID
	entry.ExpireAt = keyEntry.ExpireAt

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
