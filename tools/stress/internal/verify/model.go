// Package verify checks what readers see against what the load wrote. The
// schedule says what every batch writes and the ledger at which revision it
// committed, so the state of any key at any settled revision is known
// exactly.
package verify

import (
	"slices"

	"github.com/ankur-anand/isledb/tools/stress/internal/ledger"
	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

// Model answers what a key may hold at a revision.
type Model struct {
	Schedule *workload.Schedule
	Ledger   *ledger.Ledger
}

// expect returns the states idx may hold in a read answered at rev, which
// the ledger must have settled. Most keys have exactly one; a batch whose
// earlier attempt may have committed unseen adds its state as another.
func (m *Model) expect(idx, rev uint64, dst []workload.State) []workload.State {
	s := m.Schedule
	maxSent := m.Ledger.MaxSent()
	if maxSent < 0 {
		return append(dst, workload.State{})
	}
	top := s.Batch(uint64(maxSent)).Round
	add := func(st workload.State) {
		if !slices.Contains(dst, st) {
			dst = append(dst, st)
		}
	}
	for r := int64(top); r >= 0; r-- {
		round := uint32(r)
		if !s.Writes(round, idx) {
			continue // the round left it as the one before left it
		}
		e := m.Ledger.Get(s.BatchOf(round, idx))
		switch {
		case e.Revision != 0 && e.Revision <= rev:
			add(s.After(round, idx))
			return dst
		case e.MaybeEarlier:
			add(s.After(round, idx))
		}
	}
	add(workload.State{})
	return dst
}

// Batches returns the batches that write idx up to the newest round sent,
// and what the ledger knows of each, for failure reports. Rounds that don't
// write idx have none.
func (m *Model) Batches(idx uint64) []BatchInfo {
	maxSent := m.Ledger.MaxSent()
	if maxSent < 0 {
		return nil
	}
	top := m.Schedule.Batch(uint64(maxSent)).Round
	out := make([]BatchInfo, 0, top+1)
	for r := uint32(0); r <= top; r++ {
		if !m.Schedule.Writes(r, idx) {
			continue
		}
		no := m.Schedule.BatchOf(r, idx)
		e := m.Ledger.Get(no)
		out = append(out, BatchInfo{Round: r, No: no, Revision: e.Revision, MaybeEarlier: e.MaybeEarlier})
	}
	return out
}

type BatchInfo struct {
	Round        uint32 `json:"round"`
	No           uint64 `json:"batch"`
	Revision     uint64 `json:"revision"`
	MaybeEarlier bool   `json:"maybe_earlier,omitempty"`
}
