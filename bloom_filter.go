package isledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"

	"github.com/ankur-anand/isledb/internal/manifest"
)

// sstBloomFilter is a Bloom filter sized to exactly keys × bitsPerKey bits,
// rounded up to a whole 64-bit word, and queried directly on its encoded
// bytes.
//
// Encoding (little-endian):
//
//	[0:8)   magic "ISLEBLF1"
//	[8:12)  probes: bits tested per key, in [1, maxSSTBloomProbes]
//	[12:16) words: number of 64-bit words in the bit array, at least 1
//	[16:)   bit array, words × 8 bytes
//
// Probe i of a key hash tests bit (h1 + i·h2) mapped onto [0, bits) by a
// multiply-high reduction, with h1 the hash and h2 the hash rotated by 32 bits
// and forced odd. Because no power-of-two mask is involved, the bit count can
// follow the configured bits per key exactly.
type sstBloomFilter struct {
	bits   []byte
	probes uint64
	nbits  uint64
}

const (
	sstBloomMagic     = "ISLEBLF1"
	sstBloomHeaderLen = 16

	// maxSSTBloomProbes bounds the bits tested per lookup. The optimal count
	// for 43 bits per key already exceeds it, far beyond any useful setting.
	maxSSTBloomProbes = 30
)

var errSSTBloomNoKeys = errors.New("bloom filter has no keys")

// sstBloomProbes returns the probe count that minimizes the false-positive
// rate for bitsPerKey: bitsPerKey × ln 2, rounded, within [1, 30].
func sstBloomProbes(bitsPerKey int) int {
	probes := int(math.Round(float64(bitsPerKey) * math.Ln2))
	return min(max(probes, 1), maxSSTBloomProbes)
}

// buildSSTBloomFilter encodes a filter over hashes, as produced by
// bloomHashKey. Duplicate hashes set the same bits but still size the filter,
// so callers should pass each distinct key once.
func buildSSTBloomFilter(hashes []uint64, bitsPerKey int) ([]byte, error) {
	if bitsPerKey <= 0 {
		return nil, fmt.Errorf("bloom filter bits_per_key=%d", bitsPerKey)
	}
	if len(hashes) == 0 {
		return nil, errSSTBloomNoKeys
	}
	keys := uint64(len(hashes))
	maxBits := uint64(maxBloomSidecarBytes-sstBloomHeaderLen) * 8
	if uint64(bitsPerKey) > maxBits/keys {
		return nil, fmt.Errorf("bloom filter keys=%d bits_per_key=%d exceeds %d bytes",
			len(hashes), bitsPerKey, maxBloomSidecarBytes)
	}
	words := (keys*uint64(bitsPerKey) + 63) / 64

	encoded := make([]byte, sstBloomHeaderLen+words*8)
	copy(encoded, sstBloomMagic)
	probes := sstBloomProbes(bitsPerKey)
	binary.LittleEndian.PutUint32(encoded[8:], uint32(probes))
	binary.LittleEndian.PutUint32(encoded[12:], uint32(words))

	filter := sstBloomFilter{
		bits:   encoded[sstBloomHeaderLen:],
		probes: uint64(probes),
		nbits:  words * 64,
	}
	for _, hash := range hashes {
		h1, h2 := sstBloomProbeHashes(hash)
		for i := uint64(0); i < filter.probes; i++ {
			bit := filter.bitIndex(h1 + i*h2)
			filter.bits[bit>>3] |= 1 << (bit & 7)
		}
	}
	return encoded, nil
}

// parseSSTBloomFilter validates an encoded filter and returns a view over it.
// The filter aliases data, which must stay unmodified while the filter is in
// use. Every field is checked against the data length, so a lookup can never
// index outside the bit array whatever bytes were supplied.
func parseSSTBloomFilter(data []byte) (sstBloomFilter, error) {
	if len(data) > maxBloomSidecarBytes {
		return sstBloomFilter{}, fmt.Errorf("bloom filter bytes=%d max=%d", len(data), maxBloomSidecarBytes)
	}
	if len(data) < sstBloomHeaderLen || string(data[:len(sstBloomMagic)]) != sstBloomMagic {
		return sstBloomFilter{}, errors.New("bloom filter header missing")
	}
	probes := binary.LittleEndian.Uint32(data[8:])
	if probes < 1 || probes > maxSSTBloomProbes {
		return sstBloomFilter{}, fmt.Errorf("bloom filter probes=%d outside [1,%d]", probes, maxSSTBloomProbes)
	}
	words := uint64(binary.LittleEndian.Uint32(data[12:]))
	body := data[sstBloomHeaderLen:]
	if words == 0 || uint64(len(body)) != words*8 {
		return sstBloomFilter{}, fmt.Errorf("bloom filter words=%d body_bytes=%d", words, len(body))
	}
	return sstBloomFilter{bits: body, probes: uint64(probes), nbits: words * 64}, nil
}

// mayContain reports whether a key with this bloomHashKey hash may be in the
// SST. False means the key is certainly absent.
func (f sstBloomFilter) mayContain(hash uint64) bool {
	h1, h2 := sstBloomProbeHashes(hash)
	for i := uint64(0); i < f.probes; i++ {
		bit := f.bitIndex(h1 + i*h2)
		if f.bits[bit>>3]&(1<<(bit&7)) == 0 {
			return false
		}
	}
	return true
}

// sizeBytes is the encoded bit array size, for cache accounting.
func (f sstBloomFilter) sizeBytes() int {
	return len(f.bits)
}

func sstBloomProbeHashes(hash uint64) (h1, h2 uint64) {
	return hash, bits.RotateLeft64(hash, 32) | 1
}

// bitIndex maps a 64-bit value uniformly onto [0, nbits).
func (f sstBloomFilter) bitIndex(x uint64) uint64 {
	hi, _ := bits.Mul64(x, f.nbits)
	return hi
}

// sstBloomKeys collects the key hashes for one SST's filter while the SST is
// written. Keys arrive sorted, so the versions of one key are adjacent and each
// distinct key is hashed once.
type sstBloomKeys struct {
	bitsPerKey int
	hashes     []uint64
	last       []byte
}

// newSSTBloomKeys returns a collector for bitsPerKey; zero or less disables the
// filter.
func newSSTBloomKeys(bitsPerKey int) *sstBloomKeys {
	return &sstBloomKeys{bitsPerKey: bitsPerKey}
}

// add records key. The collector retains key, so the caller must not modify it.
func (k *sstBloomKeys) add(key []byte) {
	if k.bitsPerKey <= 0 || (len(k.hashes) > 0 && bytes.Equal(key, k.last)) {
		return
	}
	k.hashes = append(k.hashes, bloomHashKey(key))
	k.last = key
}

// reset prepares the collector for the next SST.
func (k *sstBloomKeys) reset() {
	k.hashes = k.hashes[:0]
	k.last = nil
}

// build encodes the filter for the collected keys and describes it for an SST
// payload of sstSize bytes. With the filter disabled or no keys, it returns no
// bytes and metadata recording no sidecar.
func (k *sstBloomKeys) build(sstSize int64) ([]byte, manifest.BloomMeta, error) {
	meta := manifest.BloomMeta{Offset: sstSize}
	if k.bitsPerKey <= 0 || len(k.hashes) == 0 {
		return nil, meta, nil
	}
	filter, err := buildSSTBloomFilter(k.hashes, k.bitsPerKey)
	if err != nil {
		return nil, meta, err
	}
	meta.Format = manifest.BloomFormatExactV1
	meta.BitsPerKey = k.bitsPerKey
	meta.K = sstBloomProbes(k.bitsPerKey)
	meta.Length = int64(len(filter))
	meta.Checksum = bloomChecksum(filter)
	return filter, meta, nil
}

// hasUsableBloom reports whether the SST carries a filter this reader can use.
// Filters in any other format are ignored without being fetched, so lookups
// simply open those SSTs.
func hasUsableBloom(meta manifest.SSTMeta) bool {
	return meta.Bloom.Length > 0 && meta.Bloom.Format == manifest.BloomFormatExactV1
}
