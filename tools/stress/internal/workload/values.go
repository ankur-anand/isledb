package workload

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
)

// ValueHeaderLen is the header every value starts with: its index, version,
// total length, and the CRC-32C of the bytes after the header.
const ValueHeaderLen = 8 + 4 + 4 + 4

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// sizeBand is pct percent of values, sized uniformly in [min, max).
type sizeBand struct{ pct, min, max int }

// SizeDist picks each value's size from its index and version, so a reader
// can check a value's length without being told it.
type SizeDist struct {
	spec  string
	bands []sizeBand
}

// ParseSizeDist accepts "mixed" (50% 100B-512B, 35% 512B-4KiB, 15%
// 4KiB-16KiB) or "fixed:N".
func ParseSizeDist(spec string) (SizeDist, error) {
	switch {
	case spec == "mixed":
		return SizeDist{spec: spec, bands: []sizeBand{{50, 100, 512}, {35, 512, 4096}, {15, 4096, 16384}}}, nil
	case strings.HasPrefix(spec, "fixed:"):
		n, err := strconv.Atoi(strings.TrimPrefix(spec, "fixed:"))
		if err != nil || n < ValueHeaderLen || n > 16<<20 {
			return SizeDist{}, fmt.Errorf("value size %q: want fixed:N with %d <= N <= 16MiB", spec, ValueHeaderLen)
		}
		return SizeDist{spec: spec, bands: []sizeBand{{100, n, n + 1}}}, nil
	default:
		return SizeDist{}, fmt.Errorf("value size %q: want mixed or fixed:N", spec)
	}
}

func (d SizeDist) String() string { return d.spec }

// Size returns the length of the value of idx at version.
func (d SizeDist) Size(idx uint64, version uint32) int {
	h := mix64(idx*0x9e3779b97f4a7c15 ^ uint64(version)<<32 ^ 0x3c6ef372fe94f82b)
	p := int(h % 100)
	for _, b := range d.bands {
		if p < b.pct {
			return b.min + int((h>>8)%uint64(b.max-b.min))
		}
		p -= b.pct
	}
	return d.bands[len(d.bands)-1].min
}

// Mean is the expected value size in bytes.
func (d SizeDist) Mean() float64 {
	var m float64
	for _, b := range d.bands {
		m += float64(b.pct) / 100 * float64(b.min+b.max-1) / 2
	}
	return m
}

// AppendValue appends the value of idx at version: the header, then
// incompressible bytes seeded by seed, idx and version.
func AppendValue(dst []byte, seed, idx uint64, version uint32, size int) []byte {
	start := len(dst)
	dst = binary.BigEndian.AppendUint64(dst, idx)
	dst = binary.BigEndian.AppendUint32(dst, version)
	dst = binary.BigEndian.AppendUint32(dst, uint32(size))
	dst = binary.BigEndian.AppendUint32(dst, 0)
	payload := len(dst)
	dst = slices.Grow(dst, size-ValueHeaderLen)[:start+size]
	var s [32]byte
	binary.LittleEndian.PutUint64(s[0:], idx)
	binary.LittleEndian.PutUint64(s[8:], uint64(version))
	binary.LittleEndian.PutUint64(s[16:], seed)
	_, _ = rand.NewChaCha8(s).Read(dst[payload:])
	binary.BigEndian.PutUint32(dst[start+16:], crc32.Checksum(dst[payload:], castagnoli))
	return dst
}

var (
	ErrWrongKey    = errors.New("value belongs to another key")
	ErrBadLength   = errors.New("value length is wrong")
	ErrBadChecksum = errors.New("value checksum mismatch")
)

// CheckValue verifies that value is intact and belongs to idx, and returns
// its version.
func CheckValue(d SizeDist, idx uint64, value []byte) (uint32, error) {
	if len(value) < ValueHeaderLen {
		return 0, ErrBadLength
	}
	if binary.BigEndian.Uint64(value) != idx {
		return 0, ErrWrongKey
	}
	version := binary.BigEndian.Uint32(value[8:])
	if n := binary.BigEndian.Uint32(value[12:]); int(n) != len(value) || int(n) != d.Size(idx, version) {
		return version, ErrBadLength
	}
	if crc32.Checksum(value[ValueHeaderLen:], castagnoli) != binary.BigEndian.Uint32(value[16:]) {
		return version, ErrBadChecksum
	}
	return version, nil
}
