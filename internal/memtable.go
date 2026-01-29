package internal

import (
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
)

const (
	memEntryInline  byte = 1
	memEntryBlob    byte = 3
	memEntryTTLFlag byte = 0x80
)

const (
	blobRefSize    = 2 + 32
	blobRefSizeTTL = 2 + 8 + 32
)

type Memtable struct {
	sl *skl.Skiplist
}

func NewMemtable(arenaBytes int64, inlineThreshold int) *Memtable {
	return &Memtable{
		sl: skl.NewSkiplist(arenaBytes),
	}
}

func (m *Memtable) Put(key, value []byte, seq uint64) {
	m.PutWithTTL(key, value, seq, 0)
}

func (m *Memtable) PutWithTTL(key, value []byte, seq uint64, expireAt int64) {
	ikey := y.KeyWithTs(key, seq)

	if expireAt > 0 {
		encoded := make([]byte, 2+8+len(value))
		encoded[0] = byte(OpPut)
		encoded[1] = memEntryInline | memEntryTTLFlag
		binary.BigEndian.PutUint64(encoded[2:10], uint64(expireAt))
		copy(encoded[10:], value)
		m.sl.Put(ikey, y.ValueStruct{Value: encoded})
	} else {
		encoded := make([]byte, 2+len(value))
		encoded[0] = byte(OpPut)
		encoded[1] = memEntryInline
		copy(encoded[2:], value)
		m.sl.Put(ikey, y.ValueStruct{Value: encoded})
	}
}

func (m *Memtable) PutBlobRef(key []byte, blobID [32]byte, seq uint64) {
	m.PutBlobRefWithTTL(key, blobID, seq, 0)
}

func (m *Memtable) PutBlobRefWithTTL(key []byte, blobID [32]byte, seq uint64, expireAt int64) {
	iKey := y.KeyWithTs(key, seq)

	if expireAt > 0 {
		encoded := make([]byte, blobRefSizeTTL)
		encoded[0] = byte(OpPut)
		encoded[1] = memEntryBlob | memEntryTTLFlag
		binary.BigEndian.PutUint64(encoded[2:10], uint64(expireAt))
		copy(encoded[10:42], blobID[:])
		m.sl.Put(iKey, y.ValueStruct{Value: encoded})
	} else {
		encoded := make([]byte, blobRefSize)
		encoded[0] = byte(OpPut)
		encoded[1] = memEntryBlob
		copy(encoded[2:34], blobID[:])
		m.sl.Put(iKey, y.ValueStruct{Value: encoded})
	}
}

func (m *Memtable) Delete(key []byte, seq uint64) {
	m.DeleteWithTTL(key, seq, 0)
}

func (m *Memtable) DeleteWithTTL(key []byte, seq uint64, expireAt int64) {
	ikey := y.KeyWithTs(key, seq)

	if expireAt > 0 {
		encoded := make([]byte, 2+8)
		encoded[0] = byte(OpDelete)
		encoded[1] = memEntryTTLFlag
		binary.BigEndian.PutUint64(encoded[2:10], uint64(expireAt))
		m.sl.Put(ikey, y.ValueStruct{Value: encoded})
	} else {
		encoded := []byte{byte(OpDelete), 0}
		m.sl.Put(ikey, y.ValueStruct{Value: encoded})
	}
}

func (m *Memtable) ApproxSize() int64 {
	return m.sl.MemSize()
}

func (m *Memtable) TotalSize() int64 {
	return m.ApproxSize()
}

func (m *Memtable) Iterator() *MemtableIterator {
	return &MemtableIterator{it: m.sl.NewUniIterator(false)}
}

type MemtableIterator struct {
	it     *skl.UniIterator
	inited bool
	cur    MemEntry
	err    error
}

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
	flags := vs.Value[1]
	hasTTL := (flags & memEntryTTLFlag) != 0
	entryType := flags & ^memEntryTTLFlag

	entry := MemEntry{
		Key:  userKey,
		Seq:  seq,
		Kind: kind,
	}

	offset := 2
	if hasTTL {
		if len(vs.Value) < 10 {
			return MemEntry{}, errors.New("memtable TTL entry too short")
		}
		entry.ExpireAt = int64(binary.BigEndian.Uint64(vs.Value[2:10]))
		offset = 10
	}

	if kind == OpDelete {
		return entry, nil
	}

	switch entryType {
	case memEntryInline:
		entry.Inline = true
		entry.Value = append([]byte(nil), vs.Value[offset:]...)
	case memEntryBlob:
		if len(vs.Value) < offset+32 {
			return MemEntry{}, errors.New("blob entry too short")
		}
		copy(entry.BlobID[:], vs.Value[offset:offset+32])
	default:
		return MemEntry{}, errors.New("unknown entry type")
	}

	return entry, nil
}
