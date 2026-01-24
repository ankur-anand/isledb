package isledb

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/segmentio/ksuid"
)

const (
	memEntryInline  byte = 1
	memEntryPending byte = 0
	memEntryPointer byte = 2
)

const (
	pointerVlogSize = 2 + KSUIDSize + 8 + 4 + 4
)

type Memtable struct {
	sl *skl.Skiplist

	inlineThreshold int

	mu           sync.RWMutex
	pendingSize  int64
	pendingCount int64
	pointerCount int64
}

// NewMemtable creates a memtable with the given arena size and inline threshold.
func NewMemtable(arenaBytes int64, inlineThreshold int) *Memtable {
	if inlineThreshold <= 0 {
		inlineThreshold = DefaultInlineThreshold
	}
	return &Memtable{
		sl:              skl.NewSkiplist(arenaBytes),
		inlineThreshold: inlineThreshold,
	}
}

// Put inserts or overwrites a value with the provided sequence number.
// If the value is smaller than or equal to inlineThreshold we store inline,
// else larger values are marked as pending for VLog storage at flush time.
func (m *Memtable) Put(key, value []byte, seq uint64) {
	ikey := y.KeyWithTs(key, seq)

	var encoded []byte
	if len(value) <= m.inlineThreshold {
		encoded = make([]byte, 2+len(value))
		encoded[0] = byte(OpPut)
		encoded[1] = memEntryInline
		copy(encoded[2:], value)
	} else {
		encoded = make([]byte, 2+4+len(value))
		encoded[0] = byte(OpPut)
		encoded[1] = memEntryPending
		binary.LittleEndian.PutUint32(encoded[2:6], uint32(len(value)))
		copy(encoded[6:], value)

		m.mu.Lock()
		m.pendingSize += int64(len(value))
		m.pendingCount++
		m.mu.Unlock()
	}

	m.sl.Put(ikey, y.ValueStruct{
		Value: encoded,
	})
}

// PutPointer stores a pointer to a large value already written to Vlog file.
func (m *Memtable) PutPointer(key []byte, ptr *VLogPointer, seq uint64) {
	iKey := y.KeyWithTs(key, seq)
	encoded := make([]byte, pointerVlogSize)
	encoded[0] = byte(OpPut)
	encoded[1] = memEntryPointer
	copy(encoded[2:22], ptr.VLogID[:])
	binary.LittleEndian.PutUint64(encoded[22:30], ptr.Offset)
	binary.LittleEndian.PutUint32(encoded[30:34], ptr.Length)
	binary.LittleEndian.PutUint32(encoded[34:38], ptr.Checksum)
	m.mu.Lock()
	m.pointerCount++
	m.mu.Unlock()
	m.sl.Put(iKey, y.ValueStruct{
		Value: encoded,
	})
}

// Delete adds a tombstone with the provided sequence number.
func (m *Memtable) Delete(key []byte, seq uint64) {
	ikey := y.KeyWithTs(key, seq)
	encoded := []byte{byte(OpDelete), 0}
	m.sl.Put(ikey, y.ValueStruct{
		Value: encoded,
	})
}

func (m *Memtable) ApproxSize() int64 {
	return m.sl.MemSize()
}

func (m *Memtable) PendingValueSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pendingSize
}

func (m *Memtable) TotalSize() int64 {
	return m.ApproxSize() + m.PendingValueSize()
}

// Iterator returns an ordered iterator over the memtable.
func (m *Memtable) Iterator() *MemtableIterator {
	return &MemtableIterator{it: m.sl.NewUniIterator(false)}
}

// MemtableIterator iterates over memtable entries.
type MemtableIterator struct {
	it     *skl.UniIterator
	inited bool
	cur    MemEntry
	err    error
}

// Next moves to the next entry in sorted order.
func (it *MemtableIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.inited {
		it.it.Rewind()
		it.inited = true
	} else {
		it.it.Next()
	}
	if !it.it.Valid() {
		return false
	}
	it.cur, it.err = decodeMemEntry(it.it.Key(), it.it.Value())
	return it.err == nil
}

func (it *MemtableIterator) Entry() MemEntry {
	return it.cur
}

func (it *MemtableIterator) Err() error {
	return it.err
}

func (it *MemtableIterator) Close() error {
	return it.it.Close()
}

func decodeMemEntry(key []byte, vs y.ValueStruct) (MemEntry, error) {
	if len(key) < 8 {
		return MemEntry{}, errors.New("memtable key missing timestamp suffix")
	}
	if len(vs.Value) < 2 {
		return MemEntry{}, errors.New("memtable value too short")
	}

	seq := y.ParseTs(key)
	userKey := append([]byte(nil), y.ParseKey(key)...)

	kind := OpKind(vs.Value[0])
	entryType := vs.Value[1]

	entry := MemEntry{
		Key:  userKey,
		Seq:  seq,
		Kind: kind,
	}

	if kind == OpDelete {
		return entry, nil
	}

	switch entryType {
	case memEntryInline:
		entry.Inline = true
		entry.Value = append([]byte(nil), vs.Value[2:]...)
	case memEntryPending:
		if len(vs.Value) < 6 {
			return MemEntry{}, errors.New("pending value header too short")
		}
		length := binary.LittleEndian.Uint32(vs.Value[2:6])
		if len(vs.Value) < int(6+length) {
			return MemEntry{}, errors.New("pending value truncated")
		}
		entry.PendingValue = append([]byte(nil), vs.Value[6:int(6+length)]...)
	case memEntryPointer:
		if len(vs.Value) < pointerVlogSize {
			return MemEntry{}, errors.New("pointer entry too sort")
		}

		var vLogID ksuid.KSUID
		copy(vLogID[:], vs.Value[2:22])
		entry.VLogPtr = &VLogPointer{
			VLogID:   vLogID,
			Offset:   binary.LittleEndian.Uint64(vs.Value[22:30]),
			Length:   binary.LittleEndian.Uint32(vs.Value[30:34]),
			Checksum: binary.LittleEndian.Uint32(vs.Value[34:38]),
		}
	}

	return entry, nil
}
