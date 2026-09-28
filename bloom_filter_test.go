package isledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func sstBloomTestHashes(prefix string, n int) []uint64 {
	hashes := make([]uint64, n)
	for i := range hashes {
		hashes[i] = bloomHashKey([]byte(fmt.Sprintf("%s-%08d", prefix, i)))
	}
	return hashes
}

func mustBuildSSTBloomFilter(t *testing.T, hashes []uint64, bitsPerKey int) ([]byte, sstBloomFilter) {
	t.Helper()
	encoded, err := buildSSTBloomFilter(hashes, bitsPerKey)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	filter, err := parseSSTBloomFilter(encoded)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return encoded, filter
}

func TestSSTBloomProbes(t *testing.T) {
	cases := map[int]int{1: 1, 2: 1, 10: 7, 12: 8, 14: 10, 16: 11, 20: 14, 43: 30, 100: 30}
	for bitsPerKey, want := range cases {
		if got := sstBloomProbes(bitsPerKey); got != want {
			t.Errorf("sstBloomProbes(%d)=%d, want %d", bitsPerKey, got, want)
		}
	}
}

func TestSSTBloomFilter_NoFalseNegatives(t *testing.T) {
	for _, n := range []int{1, 2, 63, 64, 65, 1_000, 100_000} {
		for _, bitsPerKey := range []int{1, 10, 12, 16} {
			t.Run(fmt.Sprintf("keys=%d/bits=%d", n, bitsPerKey), func(t *testing.T) {
				hashes := sstBloomTestHashes("key", n)
				_, filter := mustBuildSSTBloomFilter(t, hashes, bitsPerKey)
				for i, hash := range hashes {
					if !filter.mayContain(hash) {
						t.Fatalf("key %d reported absent", i)
					}
				}
			})
		}
	}
}

// TestSSTBloomFilter_FalsePositiveRate checks the measured rate against the
// standard Bloom estimate (1 - e^(-k/b))^k for b bits per key and k probes.
func TestSSTBloomFilter_FalsePositiveRate(t *testing.T) {
	if testing.Short() {
		t.Skip("probes one million absent keys per case")
	}
	const keys, absent = 100_000, 1_000_000
	hashes := sstBloomTestHashes("key", keys)
	misses := sstBloomTestHashes("miss", absent)

	for _, bitsPerKey := range []int{8, 10, 12, 14, 16} {
		t.Run(fmt.Sprintf("bits=%d", bitsPerKey), func(t *testing.T) {
			_, filter := mustBuildSSTBloomFilter(t, hashes, bitsPerKey)
			falsePositives := 0
			for _, hash := range misses {
				if filter.mayContain(hash) {
					falsePositives++
				}
			}
			k := float64(sstBloomProbes(bitsPerKey))
			expected := math.Pow(1-math.Exp(-k/float64(bitsPerKey)), k)
			measured := float64(falsePositives) / absent
			// A 30% margin plus a small absolute floor absorbs sampling noise
			// at the low rates of 14 and 16 bits per key.
			if measured > expected*1.3+0.0002 {
				t.Fatalf("false-positive rate %.4f%%, expected about %.4f%%", 100*measured, 100*expected)
			}
			t.Logf("false-positive rate %.4f%% (expected %.4f%%)", 100*measured, 100*expected)
		})
	}
}

func TestSSTBloomFilter_ExactSize(t *testing.T) {
	cases := []struct {
		keys, bitsPerKey int
		wantWords        int
	}{
		{keys: 1, bitsPerKey: 10, wantWords: 1},
		{keys: 6, bitsPerKey: 10, wantWords: 1},
		{keys: 7, bitsPerKey: 10, wantWords: 2},
		{keys: 300_000, bitsPerKey: 12, wantWords: 56_250},
		{keys: 400_000, bitsPerKey: 10, wantWords: 62_500},
		{keys: 420_000, bitsPerKey: 10, wantWords: 65_625},
	}
	for _, tc := range cases {
		encoded, filter := mustBuildSSTBloomFilter(t, sstBloomTestHashes("key", tc.keys), tc.bitsPerKey)
		if want := sstBloomHeaderLen + tc.wantWords*8; len(encoded) != want {
			t.Errorf("keys=%d bits=%d: encoded %d bytes, want %d", tc.keys, tc.bitsPerKey, len(encoded), want)
		}
		if filter.sizeBytes() != tc.wantWords*8 {
			t.Errorf("keys=%d bits=%d: sizeBytes=%d, want %d", tc.keys, tc.bitsPerKey, filter.sizeBytes(), tc.wantWords*8)
		}
	}
}

func TestSSTBloomFilter_Header(t *testing.T) {
	encoded, filter := mustBuildSSTBloomFilter(t, sstBloomTestHashes("key", 1_000), 12)
	if got := string(encoded[:8]); got != sstBloomMagic {
		t.Fatalf("magic=%q", got)
	}
	if got := binary.LittleEndian.Uint32(encoded[8:]); got != 8 {
		t.Fatalf("probes=%d, want 8", got)
	}
	if got := binary.LittleEndian.Uint32(encoded[12:]); got != 188 {
		t.Fatalf("words=%d, want 188", got)
	}
	if filter.probes != 8 || filter.nbits != 188*64 {
		t.Fatalf("parsed probes=%d nbits=%d", filter.probes, filter.nbits)
	}
}

func TestSSTBloomFilter_Deterministic(t *testing.T) {
	hashes := sstBloomTestHashes("key", 10_000)
	first, err := buildSSTBloomFilter(hashes, 12)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	second, err := buildSSTBloomFilter(hashes, 12)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("building the same hashes twice produced different bytes")
	}
}

func TestSSTBloomFilter_DuplicateHashes(t *testing.T) {
	hashes := sstBloomTestHashes("key", 100)
	doubled := append(append([]uint64(nil), hashes...), hashes...)
	_, filter := mustBuildSSTBloomFilter(t, doubled, 12)
	for i, hash := range hashes {
		if !filter.mayContain(hash) {
			t.Fatalf("key %d reported absent", i)
		}
	}
}

func TestSSTBloomFilter_BuildRejectsInvalidInput(t *testing.T) {
	if _, err := buildSSTBloomFilter(nil, 12); !errors.Is(err, errSSTBloomNoKeys) {
		t.Fatalf("no keys: err=%v, want errSSTBloomNoKeys", err)
	}
	for _, bitsPerKey := range []int{0, -1} {
		if _, err := buildSSTBloomFilter([]uint64{1}, bitsPerKey); err == nil {
			t.Fatalf("bits_per_key=%d: expected error", bitsPerKey)
		}
	}
	if _, err := buildSSTBloomFilter(make([]uint64, 1<<20), 1<<10); err == nil ||
		!strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized filter: err=%v", err)
	}
}

func TestParseSSTBloomFilter_RejectsInvalidData(t *testing.T) {
	valid, err := buildSSTBloomFilter(sstBloomTestHashes("key", 100), 12)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// The JSON sidecar shape written before the exact-v1 format.
	legacy := []byte(`{"FilterSet":"AAAAAAAAAAA=","SetLocs":6}`)
	edit := func(mutate func([]byte) []byte) []byte {
		return mutate(append([]byte(nil), valid...))
	}
	cases := map[string][]byte{
		"empty":        nil,
		"short header": valid[:sstBloomHeaderLen-1],
		"header only":  valid[:sstBloomHeaderLen],
		"legacy JSON":  legacy,
		"wrong magic":  edit(func(b []byte) []byte { b[0] = 'X'; return b }),
		"zero probes":  edit(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[8:], 0); return b }),
		"too many probes": edit(func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[8:], maxSSTBloomProbes+1)
			return b
		}),
		"zero words":     edit(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[12:], 0); return b }),
		"words too many": edit(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[12:], 1<<31); return b }),
		"truncated body": valid[:len(valid)-1],
		"trailing bytes": append(append([]byte(nil), valid...), 0),
		"oversized":      make([]byte, maxBloomSidecarBytes+1),
	}
	for name, data := range cases {
		if _, err := parseSSTBloomFilter(data); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// FuzzParseSSTBloomFilter checks that no input makes parsing or a lookup on
// the parsed filter panic, since sidecar bytes come from object storage and
// the disk cache.
func FuzzParseSSTBloomFilter(f *testing.F) {
	for _, n := range []int{1, 10, 1_000} {
		encoded, err := buildSSTBloomFilter(sstBloomTestHashes("key", n), 12)
		if err != nil {
			f.Fatalf("build seed: %v", err)
		}
		f.Add(encoded, uint64(0))
	}
	f.Add([]byte(sstBloomMagic), uint64(1))
	f.Add([]byte(`{"FilterSet":"AA==","SetLocs":6}`), uint64(2))
	f.Fuzz(func(t *testing.T, data []byte, hash uint64) {
		filter, err := parseSSTBloomFilter(data)
		if err != nil {
			return
		}
		filter.mayContain(hash)
		filter.mayContain(^hash)
	})
}
