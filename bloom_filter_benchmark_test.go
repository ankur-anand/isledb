package isledb

import (
	"fmt"
	"testing"
)

// BenchmarkSSTBloomFilter measures the SST key filter at several bits-per-key
// settings on the same key sets. The layout leaf reports the encoded size and
// the false-positive rate measured over one million absent keys; the other
// leaves time building the filter from an SST's keys, parsing it from its
// stored bytes, and probing present and absent keys. Build and probe include
// hashing the key, as the writer and reader do.
func BenchmarkSSTBloomFilter(b *testing.B) {
	const negatives = 1_000_000
	absent := make([][]byte, negatives)
	for i := range absent {
		absent[i] = []byte(fmt.Sprintf("miss-%08d", i))
	}

	for _, n := range []int{10_000, 300_000} {
		keys := make([][]byte, n)
		for i := range keys {
			keys[i] = kvLeveledBenchmarkKey(i)
		}

		for _, bitsPerKey := range []int{10, 12, 14, 16} {
			b.Run(fmt.Sprintf("keys=%d/bits=%d", n, bitsPerKey), func(b *testing.B) {
				build := func() []byte {
					hashes := make([]uint64, len(keys))
					for i, key := range keys {
						hashes[i] = bloomHashKey(key)
					}
					encoded, err := buildSSTBloomFilter(hashes, bitsPerKey)
					if err != nil {
						b.Fatalf("build: %v", err)
					}
					return encoded
				}
				encoded := build()
				filter, err := parseSSTBloomFilter(encoded)
				if err != nil {
					b.Fatalf("parse: %v", err)
				}
				falsePositives := 0
				for _, key := range absent {
					if filter.mayContain(bloomHashKey(key)) {
						falsePositives++
					}
				}

				b.Run("layout", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
					}
					b.ReportMetric(float64(len(encoded))/(1<<10), "stored_KiB")
					b.ReportMetric(float64(len(encoded)*8)/float64(n), "bits/key")
					b.ReportMetric(100*float64(falsePositives)/negatives, "fpr_%")
				})
				b.Run("build", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						build()
					}
				})
				b.Run("parse", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, err := parseSSTBloomFilter(encoded); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("probe-present", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						if !filter.mayContain(bloomHashKey(keys[i%n])) {
							b.Fatal("false negative")
						}
					}
				})
				b.Run("probe-absent", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						filter.mayContain(bloomHashKey(absent[i%negatives]))
					}
				})
			})
		}
	}
}
