package workload

import "math/bits"

// mix64 is the splitmix64 finalizer: a fast bijection whose output bits all
// depend on every input bit.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// feistel is a seeded bijection on [0, 2^bits), bits even: four rounds of a
// balanced Feistel network.
type feistel struct {
	halfBits uint
	halfMask uint64
	keys     [4]uint64
}

func newFeistel(bits uint, seed uint64) feistel {
	f := feistel{halfBits: bits / 2, halfMask: 1<<(bits/2) - 1}
	for i := range f.keys {
		f.keys[i] = mix64(seed*4 + uint64(i) + 0x6a09e667f3bcc909)
	}
	return f
}

func (f feistel) round(i int, x uint64) uint64 { return mix64(x^f.keys[i]) & f.halfMask }

func (f feistel) encrypt(x uint64) uint64 {
	l, r := x>>f.halfBits, x&f.halfMask
	for i := range 4 {
		l, r = r, l^f.round(i, r)
	}
	return l<<f.halfBits | r
}

func (f feistel) decrypt(y uint64) uint64 {
	l, r := y>>f.halfBits, y&f.halfMask
	for i := 3; i >= 0; i-- {
		l, r = r^f.round(i, l), l
	}
	return l<<f.halfBits | r
}

// domainBits returns the smallest even bit count, at least 2, whose domain
// holds n values.
func domainBits(n uint64) uint {
	b := uint(2)
	if n > 1 {
		b = max(b, uint(bits.Len64(n-1)))
	}
	return b + b&1
}

// Perm is a seeded bijection on [0, n): a Feistel network over the smallest
// even power of two holding n, walked until it lands below n. Both
// directions are cheap, so a position's index and an index's position are
// both computable. The domain is at most four times n, so a walk takes four
// steps on average.
type Perm struct {
	n uint64
	f feistel
}

// NewPerm returns the permutation of [0, n) seeded by seed; n must be
// positive.
func NewPerm(n, seed uint64) Perm {
	if n == 0 {
		panic("workload: permutation of no values")
	}
	return Perm{n: n, f: newFeistel(domainBits(n), seed)}
}

// At returns the index at position pos.
func (p Perm) At(pos uint64) uint64 {
	x := p.f.encrypt(pos)
	for x >= p.n {
		x = p.f.encrypt(x)
	}
	return x
}

// Position returns the position of idx.
func (p Perm) Position(idx uint64) uint64 {
	x := p.f.decrypt(idx)
	for x >= p.n {
		x = p.f.decrypt(x)
	}
	return x
}
