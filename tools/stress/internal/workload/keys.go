package workload

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// A Shape maps the indexes [0, N) to keys. Every shape can enumerate the
// indexes whose keys fall in a key range, in key order, so a scan's answer
// can be checked for missing keys and not only for wrong ones.
type Shape interface {
	Name() string
	// AppendKey appends the key of idx.
	AppendKey(dst []byte, idx uint64) []byte
	// Index returns the index a key encodes, and false for a key no index
	// has.
	Index(key []byte) (uint64, bool)
	// Walk calls yield with the indexes whose keys are in [lo, hi), in key
	// order, until yield returns false. A nil lo is the start of the
	// keyspace and a nil hi its end.
	Walk(lo, hi []byte, yield func(idx uint64) bool)
	// Place returns where idx's key falls in the keyspace, from 0 for the
	// first key to below 1 for the last, so a share of it is a key range.
	Place(idx uint64) float64
}

// MaxKeys bounds a workload's keys: 2^40, far beyond any run.
const MaxKeys = 1 << 40

// NewShape returns the shape name over n keys, seeded by seed.
func NewShape(name string, n, seed uint64) (Shape, error) {
	if n == 0 || n > MaxKeys {
		return nil, fmt.Errorf("shape %s: %d keys, want 1 to %d", name, n, uint64(MaxKeys))
	}
	switch name {
	case "random":
		b := domainBits(n)
		return &randomShape{n: n, bits: b, f: newFeistel(b, seed^0x510e527fade682d1)}, nil
	case "sequential":
		return &sequentialShape{n: n, seed: seed}, nil
	default:
		return nil, fmt.Errorf("shape %q: want random or sequential", name)
	}
}

// prefix8 returns the first 8 bytes of b as a number, padded with zeros.
func prefix8(b []byte) uint64 {
	var buf [8]byte
	copy(buf[:], b)
	return binary.BigEndian.Uint64(buf[:])
}

// randomShape spreads keys uniformly: a key is a permutation of its index
// over a domain of 2^bits at least n, in the high bits of 8 bytes, then the
// index. Inserting in index order lands every key at a random place, as
// hashing would, but the permutation can be inverted, so the indexes in any
// range are its domain values in order, at a density of at least a quarter.
type randomShape struct {
	n    uint64
	bits uint
	f    feistel
}

func (s *randomShape) Name() string { return "random" }

func (s *randomShape) high(p uint64) uint64 { return p << (64 - s.bits) }

func (s *randomShape) AppendKey(dst []byte, idx uint64) []byte {
	dst = binary.BigEndian.AppendUint64(dst, s.high(s.f.encrypt(idx)))
	return binary.BigEndian.AppendUint64(dst, idx)
}

func (s *randomShape) Index(key []byte) (uint64, bool) {
	if len(key) != 16 {
		return 0, false
	}
	idx := binary.BigEndian.Uint64(key[8:])
	return idx, idx < s.n && binary.BigEndian.Uint64(key) == s.high(s.f.encrypt(idx))
}

func (s *randomShape) Place(idx uint64) float64 {
	return float64(s.f.encrypt(idx)) / float64(uint64(1)<<s.bits)
}

func (s *randomShape) Walk(lo, hi []byte, yield func(uint64) bool) {
	var key []byte
	for p := prefix8(lo) >> (64 - s.bits); p < 1<<s.bits; p++ {
		idx := s.f.decrypt(p)
		if idx >= s.n {
			continue
		}
		key = s.AppendKey(key[:0], idx)
		if bytes.Compare(key, lo) < 0 {
			continue
		}
		if hi != nil && bytes.Compare(key, hi) >= 0 {
			return
		}
		if !yield(idx) {
			return
		}
	}
}

// sequentialShape orders keys by index: the index, then a check hash.
type sequentialShape struct {
	n, seed uint64
}

func (s *sequentialShape) Name() string { return "sequential" }

func (s *sequentialShape) AppendKey(dst []byte, idx uint64) []byte {
	dst = binary.BigEndian.AppendUint64(dst, idx)
	return binary.BigEndian.AppendUint64(dst, mix64(idx^s.seed))
}

func (s *sequentialShape) Index(key []byte) (uint64, bool) {
	if len(key) != 16 {
		return 0, false
	}
	idx := binary.BigEndian.Uint64(key)
	return idx, idx < s.n && binary.BigEndian.Uint64(key[8:]) == mix64(idx^s.seed)
}

func (s *sequentialShape) Place(idx uint64) float64 { return float64(idx) / float64(s.n) }

func (s *sequentialShape) Walk(lo, hi []byte, yield func(uint64) bool) {
	var key []byte
	for idx := prefix8(lo); idx < s.n; idx++ {
		key = s.AppendKey(key[:0], idx)
		if bytes.Compare(key, lo) < 0 {
			continue
		}
		if hi != nil && bytes.Compare(key, hi) >= 0 {
			return
		}
		if !yield(idx) {
			return
		}
	}
}
