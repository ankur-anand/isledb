package isledb

import (
	"bytes"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

type kMergeIterator struct {
	iters   []sstable.Iterator
	states  []mergeState
	tree    []int
	size    int
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

type mergeState struct {
	key     []byte
	value   []byte
	trailer uint64
	seq     uint64
	kind    pebble.InternalKeyKind
	valid   bool
}

func newMergeIterator(iters []sstable.Iterator) *kMergeIterator {
	mi := &kMergeIterator{
		iters:  iters,
		states: make([]mergeState, len(iters)),
	}

	n := len(iters)
	if n == 0 {
		return mi
	}

	size := 1
	for size < n {
		size *= 2
	}
	mi.size = size
	mi.tree = make([]int, size*2)
	for i := range mi.tree {
		mi.tree[i] = -1
	}

	for i, iter := range iters {
		kv := iter.First()
		if kv != nil {
			mi.loadState(i, &kv.K, kv.InPlaceValue())
			mi.tree[size+i] = i
		}
	}

	for i := size - 1; i >= 1; i-- {
		mi.tree[i] = mi.winner(mi.tree[i*2], mi.tree[i*2+1])
	}

	return mi
}

func (mi *kMergeIterator) loadState(i int, ikey *sstable.InternalKey, val []byte) {
	state := &mi.states[i]
	state.key = append(state.key[:0], ikey.UserKey...)
	state.value = append(state.value[:0], val...)

	trailer := uint64(ikey.Trailer)
	state.trailer = trailer
	state.seq = trailer >> 8
	state.kind = ikey.Kind()
	state.valid = true
}

func (mi *kMergeIterator) winner(a, b int) int {
	if a == -1 {
		return b
	}
	if b == -1 {
		return a
	}

	cmp := bytes.Compare(mi.states[a].key, mi.states[b].key)
	if cmp < 0 {
		return a
	}
	if cmp > 0 {
		return b
	}

	if mi.states[a].trailer > mi.states[b].trailer {
		return a
	}
	if mi.states[a].trailer < mi.states[b].trailer {
		return b
	}

	if a < b {
		return a
	}
	return b
}

func (mi *kMergeIterator) advance(i int) {
	kv := mi.iters[i].Next()
	if kv != nil {
		mi.loadState(i, &kv.K, kv.InPlaceValue())
		mi.tree[mi.size+i] = i
	} else {
		mi.states[i].valid = false
		mi.states[i].key = nil
		mi.states[i].value = nil
		mi.tree[mi.size+i] = -1
	}

	for pos := mi.size + i; pos > 1; {
		parent := pos / 2
		sibling := pos ^ 1
		mi.tree[parent] = mi.winner(mi.tree[pos], mi.tree[sibling])
		pos = parent
	}
}

func (mi *kMergeIterator) Next() bool {
	for mi.tree != nil && mi.tree[1] != -1 {
		index := mi.tree[1]
		state := &mi.states[index]

		if mi.current == nil {
			mi.current = &mergeEntry{}
		}
		mi.current.key = append(mi.current.key[:0], state.key...)
		mi.current.value = append(mi.current.value[:0], state.value...)
		mi.current.trailer = state.trailer
		mi.current.seq = state.seq
		mi.current.kind = state.kind
		mi.current.source = index

		mi.advance(index)

		if mi.lastKey != nil && bytes.Equal(mi.current.key, mi.lastKey) {
			continue
		}

		mi.lastKey = append(mi.lastKey[:0], mi.current.key...)
		return true
	}

	return false
}

func (mi *kMergeIterator) key() []byte {
	if mi.current == nil {
		return nil
	}
	return mi.current.key
}

func (mi *kMergeIterator) seq() uint64 {
	if mi.current == nil {
		return 0
	}
	return mi.current.seq
}

func (mi *kMergeIterator) kind() pebble.InternalKeyKind {
	if mi.current == nil {
		return pebble.InternalKeyKindInvalid
	}
	return mi.current.kind
}

func (mi *kMergeIterator) value() []byte {
	if mi.current == nil {
		return nil
	}
	return mi.current.value
}

func (mi *kMergeIterator) entry() (internal.CompactionEntry, error) {
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
			if err != nil {
				return internal.CompactionEntry{}, err
			}
			entry.ExpireAt = keyEntry.ExpireAt
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

func (mi *kMergeIterator) Err() error {
	if mi.err != nil {
		return mi.err
	}
	for _, iter := range mi.iters {
		if err := iter.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (mi *kMergeIterator) close() error {
	var firstErr error
	for _, iter := range mi.iters {
		if err := iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
