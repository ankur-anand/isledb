package isledb

import "time"

// Batch is a list of mutations for Writer.Apply, which buffers them so that
// they commit together. The zero value is an empty batch. Adding copies the
// key and value, so callers may reuse their buffers at once; adding never
// fails, and Apply checks every mutation. A Batch is not safe for concurrent
// use.
type Batch struct {
	data    []byte
	entries []batchEntry
}

type batchEntry struct {
	start    int // the key at data[start:], then the value
	keyLen   int
	valueLen int
	delete   bool
	ttl      time.Duration
}

// NewBatch returns an empty batch.
func NewBatch() *Batch { return &Batch{} }

// Put adds a write of value to key.
func (b *Batch) Put(key, value []byte) { b.add(key, value, false, 0) }

// PutWithTTL adds a write of value to key that expires ttl after Apply; zero
// means no expiration.
func (b *Batch) PutWithTTL(key, value []byte, ttl time.Duration) { b.add(key, value, false, ttl) }

// Delete adds a deletion of key.
func (b *Batch) Delete(key []byte) { b.add(key, nil, true, 0) }

// Len is the number of mutations added.
func (b *Batch) Len() int { return len(b.entries) }

// Reset empties the batch, keeping its memory for reuse.
func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.entries = b.entries[:0]
}

func (b *Batch) add(key, value []byte, delete bool, ttl time.Duration) {
	start := len(b.data)
	b.data = append(b.data, key...)
	b.data = append(b.data, value...)
	b.entries = append(b.entries, batchEntry{start: start, keyLen: len(key), valueLen: len(value), delete: delete, ttl: ttl})
}

func (b *Batch) key(e batchEntry) []byte {
	return b.data[e.start : e.start+e.keyLen : e.start+e.keyLen]
}

func (b *Batch) value(e batchEntry) []byte {
	from := e.start + e.keyLen
	return b.data[from : from+e.valueLen : from+e.valueLen]
}
