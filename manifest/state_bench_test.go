package manifest

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

func BenchmarkLevelUpdate(b *testing.B) {
	cases := []struct {
		name      string
		existing  []SSTMeta
		additions []SSTMeta
	}{
		{
			name:      "append-at-end",
			existing:  benchmarkSSTRange(1, 0, 7168),
			additions: benchmarkSSTRange(1, 7168, 128),
		},
		{
			name: "insert-in-middle",
			existing: append(
				benchmarkSSTRange(1, 0, 3520),
				benchmarkSSTRange(1, 3648, 3648)...,
			),
			additions: benchmarkSSTRange(1, 3520, 128),
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("insert-bounded-additions", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if got := insertLevelSSTs(tc.existing, tc.additions); len(got) != len(tc.existing)+len(tc.additions) {
						b.Fatal(len(got))
					}
				}
			})

			b.Run("append-and-sort-whole-level", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					all := make([]SSTMeta, 0, len(tc.existing)+len(tc.additions))
					all = append(all, tc.existing...)
					all = append(all, tc.additions...)
					sort.Slice(all, func(i, j int) bool {
						return bytes.Compare(all[i].MinKey, all[j].MinKey) < 0
					})
					if len(all) != len(tc.existing)+len(tc.additions) {
						b.Fatal(len(all))
					}
				}
			})
		})
	}
}

func BenchmarkCompactionInputRemoval(b *testing.B) {
	base := &Manifest{L0SSTs: benchmarkSSTRange(0, 0, 1024)}
	for level := uint32(1); level <= 8; level++ {
		base.Levels = append(base.Levels, Level{
			Number: level,
			SSTs:   benchmarkSSTRange(level, int(level)*10000, 1024),
		})
	}
	ids := make([]string, 0, 128)
	for _, level := range []uint32{4, 5} {
		for _, sst := range base.Level(level).SSTs[:64] {
			ids = append(ids, sst.ID)
		}
	}

	b.Run("targeted-adjacent-levels", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			m := base.Clone()
			m.RemoveCompactionInputs(4, 5, ids)
		}
	})

	b.Run("scan-all-levels", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			m := base.Clone()
			m.RemoveSSTables(ids)
		}
	})
}

func benchmarkSSTRange(level uint32, start, count int) []SSTMeta {
	ssts := make([]SSTMeta, count)
	for i := range ssts {
		key := []byte(fmt.Sprintf("key-%08d", start+i))
		ssts[i] = SSTMeta{
			ID:     fmt.Sprintf("l%d-%08d", level, start+i),
			Level:  level,
			MinKey: key,
			MaxKey: append([]byte(nil), key...),
		}
	}
	return ssts
}
