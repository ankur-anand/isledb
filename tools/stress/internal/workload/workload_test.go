package workload

import (
	"bytes"
	"errors"
	"slices"
	"sort"
	"testing"
)

func TestPermIsABijection(t *testing.T) {
	for _, n := range []uint64{1, 2, 3, 5, 16, 17, 1000, 4097} {
		p := NewPerm(n, 7)
		seen := make([]bool, n)
		for pos := range n {
			idx := p.At(pos)
			if idx >= n || seen[idx] {
				t.Fatalf("n=%d: position %d maps to %d, out of range or repeated", n, pos, idx)
			}
			seen[idx] = true
			if got := p.Position(idx); got != pos {
				t.Fatalf("n=%d: Position(At(%d)) = %d", n, pos, got)
			}
		}
	}
}

func TestPermOfNothingPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewPerm(0) did not panic")
		}
	}()
	NewPerm(0, 1)
}

func TestValuesRoundTripAndCatchDamage(t *testing.T) {
	for _, spec := range []string{"mixed", "fixed:20", "fixed:1024"} {
		d, err := ParseSizeDist(spec)
		if err != nil {
			t.Fatal(err)
		}
		for idx := range uint64(200) {
			v := AppendValue(nil, 9, idx, 3, d.Size(idx, 3))
			if got, err := CheckValue(d, idx, v); err != nil || got != 3 {
				t.Fatalf("%s idx %d: version %d, %v", spec, idx, got, err)
			}
			if _, err := CheckValue(d, idx+1, v); !errors.Is(err, ErrWrongKey) {
				t.Fatalf("%s: another key's value accepted: %v", spec, err)
			}
			if len(v) > ValueHeaderLen {
				bad := bytes.Clone(v)
				bad[len(bad)-1] ^= 1
				if _, err := CheckValue(d, idx, bad); !errors.Is(err, ErrBadChecksum) {
					t.Fatalf("%s: flipped byte accepted: %v", spec, err)
				}
			}
			if _, err := CheckValue(d, idx, v[:len(v)-1]); !errors.Is(err, ErrBadLength) {
				t.Fatalf("%s: truncated value accepted: %v", spec, err)
			}
		}
	}
	for _, bad := range []string{"fixed:19", "fixed:x", "huge"} {
		if _, err := ParseSizeDist(bad); err == nil {
			t.Errorf("ParseSizeDist(%q) accepted", bad)
		}
	}
}

func TestMixedSizesMatchTheirMean(t *testing.T) {
	d, _ := ParseSizeDist("mixed")
	var sum float64
	const n = 200000
	for idx := range uint64(n) {
		sum += float64(d.Size(idx, 0))
	}
	if got, want := sum/n, d.Mean(); got < want*0.98 || got > want*1.02 {
		t.Fatalf("mean size %.0f, want about %.0f", got, want)
	}
}

// keysInOrder returns every index of s, sorted by key.
func keysInOrder(s Shape, n uint64) []uint64 {
	idx := make([]uint64, n)
	keys := make([][]byte, n)
	for i := range n {
		idx[i], keys[i] = i, s.AppendKey(nil, i)
	}
	sort.Slice(idx, func(a, b int) bool { return bytes.Compare(keys[idx[a]], keys[idx[b]]) < 0 })
	return idx
}

func walk(s Shape, lo, hi []byte) []uint64 {
	var out []uint64
	s.Walk(lo, hi, func(i uint64) bool { out = append(out, i); return true })
	return out
}

func TestShapesEnumerateRangesInKeyOrder(t *testing.T) {
	for _, name := range []string{"random", "sequential"} {
		const n = 3000
		s, err := NewShape(name, n, 11)
		if err != nil {
			t.Fatal(err)
		}
		order := keysInOrder(s, n)
		for i, idx := range order {
			key := s.AppendKey(nil, idx)
			if got, ok := s.Index(key); !ok || got != idx {
				t.Fatalf("%s: Index of key %d is %d, %v", name, idx, got, ok)
			}
			if i > 0 && bytes.Equal(key, s.AppendKey(nil, order[i-1])) {
				t.Fatalf("%s: two indexes share a key", name)
			}
		}
		if got := walk(s, nil, nil); !slices.Equal(got, order) {
			t.Fatalf("%s: a walk of everything gives %d indexes, not the %d in key order", name, len(got), len(order))
		}
		// Bounds that are keys, and bounds between keys.
		for _, c := range []struct{ a, b int }{{0, 10}, {100, 2500}, {2990, n}, {7, 7}} {
			lo := s.AppendKey(nil, order[c.a])
			var hi []byte
			if c.b < n {
				hi = s.AppendKey(nil, order[c.b])
			}
			if got := walk(s, lo, hi); !slices.Equal(got, order[c.a:c.b]) {
				t.Fatalf("%s: [%d, %d) gives %v", name, c.a, c.b, got)
			}
			between := append(bytes.Clone(lo), 0)
			if got := walk(s, between, hi); c.a < c.b && !slices.Equal(got, order[c.a+1:c.b]) {
				t.Fatalf("%s: just past key %d gives %d indexes", name, c.a, len(got))
			}
		}
		for _, bad := range [][]byte{nil, []byte("~stress"), make([]byte, 16)} {
			if idx, ok := s.Index(bad); ok && !bytes.Equal(s.AppendKey(nil, idx), bad) {
				t.Fatalf("%s: Index accepted %x", name, bad)
			}
		}
	}
}

func TestRandomShapeSpreadsInserts(t *testing.T) {
	s, _ := NewShape("random", 1<<16, 3)
	var buckets [16]int
	for idx := range uint64(1 << 12) {
		buckets[s.AppendKey(nil, idx)[0]>>4]++
	}
	for i, c := range buckets {
		if c < 128 || c > 384 {
			t.Fatalf("the first 4096 inserts put %d keys in sixteenth %d of the keyspace, want about 256", c, i)
		}
	}
}

func TestScheduleBatchesCoverEveryKeyOncePerRound(t *testing.T) {
	d, _ := ParseSizeDist("fixed:64")
	s, err := NewSchedule(1001, 16, 5, 100, d)
	if err != nil {
		t.Fatal(err)
	}
	if s.BatchesPerRound() != 63 {
		t.Fatalf("%d batches a round, want 63", s.BatchesPerRound())
	}
	deleted := 0
	for round := uint32(0); round < 3; round++ {
		seen := make([]bool, s.N)
		for j := range s.BatchesPerRound() {
			b := s.Batch(uint64(round)*s.BatchesPerRound() + j)
			if b.Round != round {
				t.Fatalf("batch %d is in round %d, want %d", b.No, b.Round, round)
			}
			for pos := b.Start; pos < b.Start+b.Count; pos++ {
				idx := s.Index(round, pos)
				if seen[idx] {
					t.Fatalf("round %d writes %d twice", round, idx)
				}
				seen[idx] = true
				if got := s.BatchOf(round, idx); got != b.No {
					t.Fatalf("BatchOf(%d, %d) = %d, want %d", round, idx, got, b.No)
				}
				if st := s.After(round, idx); st.Present != !s.Deletes(round, idx) || (st.Present && st.Version != round) {
					t.Fatalf("After(%d, %d) = %v", round, idx, st)
				}
				if s.Deletes(round, idx) {
					deleted++
				}
			}
		}
		if slices.Contains(seen, false) {
			t.Fatalf("round %d misses a key", round)
		}
	}
	// Round 0 deletes nothing; rounds 1 and 2 each about 10%.
	if deleted < 150 || deleted > 250 {
		t.Fatalf("%d deletes in two rounds of 1001 keys at 100‰, want about 200", deleted)
	}
}

func TestKeysForBytes(t *testing.T) {
	d, _ := ParseSizeDist("fixed:100")
	if got := KeysForBytes(d, 16, 1160); got != 10 {
		t.Fatalf("%d keys for 1160 bytes of 116-byte entries, want 10", got)
	}
	if got := KeysForBytes(d, 16, 1161); got != 11 {
		t.Fatalf("%d keys for 1161 bytes, want 11", got)
	}
}

// The schedule as it was before rounds could write part of the keys, so the
// default can be checked to be unchanged: old ledgers stay valid.
type fullSchedule struct{ n, chunk, seed, del uint64 }

func (f fullSchedule) per() uint64 { return (f.n + f.chunk - 1) / f.chunk }
func (f fullSchedule) index(round uint32, pos uint64) uint64 {
	if round == 0 {
		return pos
	}
	return NewPerm(f.n, f.seed^uint64(round)).At(pos)
}
func (f fullSchedule) deletes(round uint32, idx uint64) bool {
	return round > 0 && f.del > 0 && mix64(idx^uint64(round)*0x9e3779b97f4a7c15^f.seed)%1000 < f.del
}

func TestDefaultScheduleIsUnchanged(t *testing.T) {
	d, _ := ParseSizeDist("mixed")
	f := fullSchedule{n: 1003, chunk: 16, seed: 9, del: 150}
	s, err := NewSchedule(f.n, f.chunk, f.seed, f.del, d)
	if err != nil {
		t.Fatal(err)
	}
	if s.BatchesPerRound() != f.per() {
		t.Fatalf("%d batches a round, want %d", s.BatchesPerRound(), f.per())
	}
	for no := range 4 * f.per() {
		round, j := uint32(no/f.per()), no%f.per()
		b := s.Batch(no)
		if b.Round != round || b.Start != j*f.chunk || b.Count != min(f.chunk, f.n-j*f.chunk) {
			t.Fatalf("batch %d = %+v", no, b)
		}
		for pos := b.Start; pos < b.Start+b.Count; pos++ {
			idx := s.Index(round, pos)
			if idx != f.index(round, pos) || !s.Writes(round, idx) || s.Deletes(round, idx) != f.deletes(round, idx) ||
				s.BatchOf(round, idx) != no {
				t.Fatalf("round %d position %d: index %d, deletes %v, batch %d", round, pos, idx, s.Deletes(round, idx), s.BatchOf(round, idx))
			}
		}
	}
}

func TestPartialRoundsWriteTheirShareOnce(t *testing.T) {
	d, _ := ParseSizeDist("fixed:32")
	const n, rounds = 4000, 6
	hot := func(idx uint64) bool { return idx%10 == 0 } // a tenth of the keys
	s, err := NewScheduleWith(n, 8, 3, 200, d, Options{UpdatePermille: 50, HotPermille: 600, Hot: hot, HotShare: 100,
		InsertsPerRound: 300, Rounds: rounds})
	if err != nil {
		t.Fatal(err)
	}
	if s.MaxKeys() != n+rounds*300 {
		t.Fatalf("%d keys at most, want %d", s.MaxKeys(), n+rounds*300)
	}
	var hotWrites, coldWrites, hotKeys, coldKeys, deleted, keys int
	for round := uint32(0); round <= rounds; round++ {
		first, next := s.FirstBatch(round), s.FirstBatch(round+1)
		seen := map[uint64]bool{}
		for no := first; no < next; no++ {
			b := s.Batch(no)
			if b.No != no || b.Round != round {
				t.Fatalf("batch %d = %+v, want round %d", no, b, round)
			}
			var written uint64
			for pos := b.Start; pos < b.Start+b.Count; pos++ {
				idx := s.Index(round, pos)
				if !s.Writes(round, idx) {
					continue
				}
				written++
				if seen[idx] {
					t.Fatalf("round %d writes %d twice", round, idx)
				}
				seen[idx] = true
				if got := s.BatchOf(round, idx); got != no {
					t.Fatalf("BatchOf(%d, %d) = %d, want %d", round, idx, got, no)
				}
				if s.Deletes(round, idx) {
					deleted++
					if idx >= s.Keys(round-1) {
						t.Fatalf("round %d deletes %d, a key it inserts", round, idx)
					}
				}
			}
			if written != s.Written(b) {
				t.Fatalf("batch %d writes %d keys, Written says %d", no, written, s.Written(b))
			}
		}
		keys += len(seen)
		// Every key Writes names is in some batch of the round, and no other.
		for idx := range s.MaxKeys() {
			if s.Writes(round, idx) != seen[idx] {
				t.Fatalf("round %d: Writes(%d) = %v, but a batch writes it: %v", round, idx, s.Writes(round, idx), seen[idx])
			}
		}
		if round == 0 {
			continue
		}
		// The keys the round inserts, and only those, are new.
		for idx := s.Keys(round - 1); idx < s.Keys(round); idx++ {
			if !seen[idx] {
				t.Fatalf("round %d does not insert %d", round, idx)
			}
		}
		for idx := range s.Keys(round - 1) {
			if hot(idx) {
				hotKeys++
				if seen[idx] {
					hotWrites++
				}
			} else {
				coldKeys++
				if seen[idx] {
					coldWrites++
				}
			}
		}
	}
	if share := float64(hotWrites) / float64(hotKeys); share < 0.55 || share > 0.65 {
		t.Fatalf("hot keys written %.3f of the time, want about 0.6", share)
	}
	if share := float64(coldWrites) / float64(coldKeys); share < 0.04 || share > 0.06 {
		t.Fatalf("other keys written %.3f of the time, want about 0.05", share)
	}
	if share := float64(deleted) / float64(hotWrites+coldWrites); share < 0.17 || share > 0.23 {
		t.Fatalf("%.3f of updates deleted, want about 0.2", share)
	}
	// Batches of updates write about 8 keys each, as inserts do.
	if mean := float64(keys) / float64(s.FirstBatch(rounds+1)); mean < 6 || mean > 9 {
		t.Fatalf("%.1f keys a batch, want about 8", mean)
	}
}

func TestLastFindsTheRoundThatLastWroteAKey(t *testing.T) {
	d, _ := ParseSizeDist("fixed:8")
	s, err := NewScheduleWith(500, 4, 1, 0, d, Options{UpdatePermille: 100, InsertsPerRound: 50, Rounds: 8})
	if err != nil {
		t.Fatal(err)
	}
	for idx := range s.MaxKeys() {
		for round := uint32(0); round <= 8; round++ {
			got, ok := s.Last(round, idx)
			want, found := uint32(0), false
			for r := uint32(0); r <= round; r++ {
				if s.Writes(r, idx) {
					want, found = r, true
				}
			}
			if ok != found || got != want {
				t.Fatalf("Last(%d, %d) = %d %v, want %d %v", round, idx, got, ok, want, found)
			}
		}
	}
}

func TestShapesPlaceKeysInKeyOrder(t *testing.T) {
	for _, name := range []string{"random", "sequential"} {
		shape, _ := NewShape(name, 3000, 4)
		prev := -1.0
		shape.Walk(nil, nil, func(idx uint64) bool {
			p := shape.Place(idx)
			if p < 0 || p >= 1 || p <= prev {
				t.Fatalf("%s: key %d placed at %v after %v", name, idx, p, prev)
			}
			prev = p
			return true
		})
	}
}
