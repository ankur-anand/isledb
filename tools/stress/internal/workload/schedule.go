package workload

import (
	"fmt"
	"sort"
)

// A Schedule is the whole workload as a pure function of batch numbers.
// Round 0 inserts keys 0..N-1 in index order. Each later round r writes a
// seeded share of the keys that exist, UpdatePermille of them, or more of
// those in the hot range, visiting them in the order of a permutation
// seeded by r, and deletes a seeded share of those it writes. It then
// inserts InsertsPerRound new keys, in index order. Batches cover runs of
// positions of one round and write the keys among them the round writes, so
// which batch wrote any key in any round, and what it wrote, can be
// computed. With UpdatePermille 1000 and no inserts, every round writes
// every key, as the schedule always has.
type Schedule struct {
	N     uint64 // keys round 0 inserts
	Chunk uint64
	Seed  uint64
	// DeletePermille is the share, in thousandths, of the keys each round
	// after the first writes that it deletes instead.
	DeletePermille uint64
	// UpdatePermille is the share, in thousandths, of the keys that exist
	// that each round after the first writes; HotPermille the share of
	// those in the hot range.
	UpdatePermille, HotPermille uint64
	// Hot reports whether a key is in the hot range; nil for none.
	Hot func(idx uint64) bool
	// InsertsPerRound is the new keys each round after the first inserts.
	InsertsPerRound uint64
	// Rounds bounds the rounds a schedule with inserts runs, and so its
	// keys.
	Rounds uint32
	Dist   SizeDist

	// span is the positions a batch of updates covers, so that it writes
	// about Chunk keys.
	span uint64
	// first[r] is round r's first batch, for a schedule with inserts, whose
	// rounds grow.
	first []uint64
}

// Options are a schedule's settings beyond NewSchedule's.
type Options struct {
	// UpdatePermille is the share of keys each round after the first
	// writes, 1 to 1000; 0 is 1000. HotPermille is that share for keys in
	// the hot range, which Hot reports; 0 is UpdatePermille.
	UpdatePermille, HotPermille uint64
	Hot                         func(idx uint64) bool
	// HotShare is the share, in thousandths, of keys Hot reports, to size
	// batches by.
	HotShare        uint64
	InsertsPerRound uint64
	// Rounds is the most rounds after the first the schedule runs; needed
	// with inserts.
	Rounds uint32
}

// NewSchedule returns the schedule that writes every key every round.
func NewSchedule(n, chunk, seed, deletePermille uint64, dist SizeDist) (*Schedule, error) {
	return NewScheduleWith(n, chunk, seed, deletePermille, dist, Options{})
}

func NewScheduleWith(n, chunk, seed, deletePermille uint64, dist SizeDist, o Options) (*Schedule, error) {
	if n == 0 || n > MaxKeys {
		return nil, fmt.Errorf("schedule: %d keys, want 1 to %d", n, uint64(MaxKeys))
	}
	if chunk == 0 {
		return nil, fmt.Errorf("schedule: batches must hold at least one key")
	}
	if deletePermille > 1000 {
		return nil, fmt.Errorf("schedule: delete share %d‰ is above 1000‰", deletePermille)
	}
	if o.UpdatePermille == 0 {
		o.UpdatePermille = 1000
	}
	if o.HotPermille == 0 || o.Hot == nil {
		o.HotPermille = o.UpdatePermille
	}
	if o.UpdatePermille > 1000 || o.HotPermille > 1000 || o.HotShare > 1000 {
		return nil, fmt.Errorf("schedule: shares are thousandths, at most 1000")
	}
	if o.InsertsPerRound > 0 && o.Rounds == 0 {
		return nil, fmt.Errorf("schedule: inserts need the number of rounds, which bounds the keys")
	}
	if total := n + uint64(o.Rounds)*o.InsertsPerRound; total > MaxKeys || total < n {
		return nil, fmt.Errorf("schedule: %d keys with inserts, want at most %d", total, uint64(MaxKeys))
	}
	s := &Schedule{N: n, Chunk: chunk, Seed: seed, DeletePermille: deletePermille, Dist: dist,
		UpdatePermille: o.UpdatePermille, HotPermille: o.HotPermille, Hot: o.Hot,
		InsertsPerRound: o.InsertsPerRound, Rounds: o.Rounds}
	// A batch of updates covers enough positions to write about Chunk keys.
	share := o.UpdatePermille
	if o.Hot != nil {
		share = (o.UpdatePermille*(1000-o.HotShare) + o.HotPermille*o.HotShare) / 1000
	}
	s.span = (chunk*1000 + max(share, 1) - 1) / max(share, 1)
	if o.InsertsPerRound > 0 {
		s.first = make([]uint64, o.Rounds+2)
		for r := uint32(0); r <= o.Rounds; r++ {
			s.first[r+1] = s.first[r] + s.batchesIn(r)
		}
	}
	return s, nil
}

// KeysForBytes returns how many keys the insert round needs to write at
// least target bytes of keys and values, keyLen bytes a key.
func KeysForBytes(d SizeDist, keyLen int, target int64) uint64 {
	var n uint64
	for total := int64(0); total < target && n < MaxKeys; n++ {
		total += int64(keyLen + d.Size(n, 0))
	}
	return n
}

// Keys returns how many keys exist once round has run: N, and the inserts
// of every round after the first up to it.
func (s *Schedule) Keys(round uint32) uint64 { return s.N + uint64(round)*s.InsertsPerRound }

// MaxKeys returns the keys the schedule ever has.
func (s *Schedule) MaxKeys() uint64 { return s.Keys(s.Rounds) }

// existing returns how many keys exist when round starts: those it may
// update; its inserts follow them.
func (s *Schedule) existing(round uint32) uint64 {
	if round == 0 {
		return 0
	}
	return s.Keys(round - 1)
}

func ceilDiv(a, b uint64) uint64 { return (a + b - 1) / b }

// updateBatches is the batches round spends on updates; its inserts follow.
func (s *Schedule) updateBatches(round uint32) uint64 {
	return ceilDiv(s.existing(round), s.span)
}

func (s *Schedule) batchesIn(round uint32) uint64 {
	if round == 0 {
		return ceilDiv(s.N, s.Chunk)
	}
	return s.updateBatches(round) + ceilDiv(s.InsertsPerRound, s.Chunk)
}

// BatchesPerRound is the number of batches the first round after the
// insert round takes; every one does without inserts.
func (s *Schedule) BatchesPerRound() uint64 { return s.batchesIn(1) }

// FirstBatch returns round's first batch.
func (s *Schedule) FirstBatch(round uint32) uint64 {
	if s.first != nil {
		if int(round) < len(s.first) {
			return s.first[round]
		}
		return s.first[len(s.first)-1]
	}
	if round == 0 {
		return 0
	}
	return s.batchesIn(0) + uint64(round-1)*s.batchesIn(1)
}

// Batch is one write request: positions [Start, Start+Count) of Round, of
// which it writes the keys the round writes.
type Batch struct {
	No           uint64
	Round        uint32
	Start, Count uint64
}

func (s *Schedule) Batch(no uint64) Batch {
	var round uint32
	switch {
	case s.first != nil:
		round = uint32(sort.Search(len(s.first), func(i int) bool { return s.first[i] > no }) - 1)
	case no >= s.batchesIn(0):
		round = uint32(1 + (no-s.batchesIn(0))/s.batchesIn(1))
	}
	j := no - s.FirstBatch(round)
	if round == 0 {
		start := j * s.Chunk
		return Batch{No: no, Round: 0, Start: start, Count: min(s.Chunk, s.N-start)}
	}
	e, u := s.existing(round), s.updateBatches(round)
	if j < u {
		start := j * s.span
		return Batch{No: no, Round: round, Start: start, Count: min(s.span, e-start)}
	}
	start := e + (j-u)*s.Chunk
	return Batch{No: no, Round: round, Start: start, Count: min(s.Chunk, s.Keys(round)-start)}
}

// Index returns the key round visits at position pos: a key that exists,
// in the round's order, or one it inserts.
func (s *Schedule) Index(round uint32, pos uint64) uint64 {
	if e := s.existing(round); pos < e {
		return NewPerm(e, s.Seed^uint64(round)).At(pos)
	}
	return pos
}

// BatchOf returns the batch that writes idx in round, if the round writes
// it.
func (s *Schedule) BatchOf(round uint32, idx uint64) uint64 {
	first := s.FirstBatch(round)
	if round == 0 {
		return first + idx/s.Chunk
	}
	e := s.existing(round)
	if idx >= e {
		return first + s.updateBatches(round) + (idx-e)/s.Chunk
	}
	return first + NewPerm(e, s.Seed^uint64(round)).Position(idx)/s.span
}

// Writes reports whether round writes idx: inserts it, updates it or
// deletes it.
func (s *Schedule) Writes(round uint32, idx uint64) bool {
	e := s.existing(round)
	switch {
	case idx >= s.Keys(round):
		return false
	case round == 0 || idx >= e:
		return true
	}
	share := s.UpdatePermille
	if s.Hot != nil && s.Hot(idx) {
		share = s.HotPermille
	}
	return share >= 1000 || mix64(idx*0xd1b54a32d192ed03^uint64(round)<<40^s.Seed^0x8cb92ba72f3d8dd7)%1000 < share
}

// Deletes reports whether round deletes idx instead of writing it. Only a
// key that existed before the round can be deleted.
func (s *Schedule) Deletes(round uint32, idx uint64) bool {
	return round > 0 && s.DeletePermille > 0 && idx < s.existing(round) && s.Writes(round, idx) &&
		mix64(idx^uint64(round)*0x9e3779b97f4a7c15^s.Seed)%1000 < s.DeletePermille
}

// Last returns the last round at or before round that writes idx.
func (s *Schedule) Last(round uint32, idx uint64) (uint32, bool) {
	for r := int64(round); r >= 0; r-- {
		if s.Writes(uint32(r), idx) {
			return uint32(r), true
		}
	}
	return 0, false
}

// State is what a key holds: absent, or the value written at Version.
type State struct {
	Present bool
	Version uint32
}

func (st State) String() string {
	if !st.Present {
		return "absent"
	}
	return fmt.Sprintf("v%d", st.Version)
}

// After returns the state round leaves idx in, if the round writes it.
func (s *Schedule) After(round uint32, idx uint64) State {
	if s.Deletes(round, idx) {
		return State{}
	}
	return State{Present: true, Version: round}
}

// Written returns how many keys batch b writes.
func (s *Schedule) Written(b Batch) uint64 {
	var n uint64
	for pos := b.Start; pos < b.Start+b.Count; pos++ {
		if s.Writes(b.Round, s.Index(b.Round, pos)) {
			n++
		}
	}
	return n
}

// Bytes returns the key and value bytes batch b writes, keyLen bytes a key.
func (s *Schedule) Bytes(b Batch, keyLen int) int64 {
	var n int64
	for pos := b.Start; pos < b.Start+b.Count; pos++ {
		idx := s.Index(b.Round, pos)
		if !s.Writes(b.Round, idx) {
			continue
		}
		n += int64(keyLen)
		if !s.Deletes(b.Round, idx) {
			n += int64(s.Dist.Size(idx, b.Round))
		}
	}
	return n
}

// KeysBy returns how many keys exist once batch b has been sent: those of
// the rounds before it, and the inserts of b's round up to b.
func (s *Schedule) KeysBy(b Batch) uint64 {
	if e := s.existing(b.Round); b.Start < e {
		return e
	}
	return b.Start + b.Count
}
