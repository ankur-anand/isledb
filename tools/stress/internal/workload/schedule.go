package workload

import "fmt"

// A Schedule is the whole workload as a pure function of batch numbers.
// Round 0 inserts keys 0..N-1 in index order. Each later round r visits all
// N keys once, in the order of a permutation seeded by r, and writes each at
// version r, except a seeded share it deletes. Batch b holds Chunk
// consecutive positions of one round, so which batch wrote any key in any
// round, and what it wrote, can be computed.
type Schedule struct {
	N     uint64
	Chunk uint64
	Seed  uint64
	// DeletePermille is the share of keys, in thousandths, each round after
	// the first deletes instead of rewriting.
	DeletePermille uint64
	Dist           SizeDist

	perRound uint64
}

func NewSchedule(n, chunk, seed, deletePermille uint64, dist SizeDist) (*Schedule, error) {
	if n == 0 || n > MaxKeys {
		return nil, fmt.Errorf("schedule: %d keys, want 1 to %d", n, uint64(MaxKeys))
	}
	if chunk == 0 {
		return nil, fmt.Errorf("schedule: batches must hold at least one key")
	}
	if deletePermille > 1000 {
		return nil, fmt.Errorf("schedule: delete share %d‰ is above 1000‰", deletePermille)
	}
	return &Schedule{N: n, Chunk: chunk, Seed: seed, DeletePermille: deletePermille, Dist: dist,
		perRound: (n + chunk - 1) / chunk}, nil
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

// BatchesPerRound is the number of batches each round takes.
func (s *Schedule) BatchesPerRound() uint64 { return s.perRound }

// Batch is one write request: Count positions of Round from Start.
type Batch struct {
	No           uint64
	Round        uint32
	Start, Count uint64
}

func (s *Schedule) Batch(no uint64) Batch {
	round, j := no/s.perRound, no%s.perRound
	start := j * s.Chunk
	return Batch{No: no, Round: uint32(round), Start: start, Count: min(s.Chunk, s.N-start)}
}

// Index returns the key round visits at position pos.
func (s *Schedule) Index(round uint32, pos uint64) uint64 {
	if round == 0 {
		return pos
	}
	return NewPerm(s.N, s.Seed^uint64(round)).At(pos)
}

// BatchOf returns the batch that writes idx in round.
func (s *Schedule) BatchOf(round uint32, idx uint64) uint64 {
	pos := idx
	if round > 0 {
		pos = NewPerm(s.N, s.Seed^uint64(round)).Position(idx)
	}
	return uint64(round)*s.perRound + pos/s.Chunk
}

// Deletes reports whether round deletes idx instead of writing it.
func (s *Schedule) Deletes(round uint32, idx uint64) bool {
	return round > 0 && s.DeletePermille > 0 &&
		mix64(idx^uint64(round)*0x9e3779b97f4a7c15^s.Seed)%1000 < s.DeletePermille
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

// After returns the state round leaves idx in.
func (s *Schedule) After(round uint32, idx uint64) State {
	if s.Deletes(round, idx) {
		return State{}
	}
	return State{Present: true, Version: round}
}

// Bytes returns the key and value bytes batch b writes, keyLen bytes a key.
func (s *Schedule) Bytes(b Batch, keyLen int) int64 {
	var n int64
	for pos := b.Start; pos < b.Start+b.Count; pos++ {
		idx := s.Index(b.Round, pos)
		n += int64(keyLen)
		if !s.Deletes(b.Round, idx) {
			n += int64(s.Dist.Size(idx, b.Round))
		}
	}
	return n
}
