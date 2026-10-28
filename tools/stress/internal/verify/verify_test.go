package verify

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ankur-anand/isledb/tools/stress/internal/ledger"
	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

// fakeDB applies batches in commit order, one revision each, and answers
// reads at any revision it has reached.
type fakeDB struct {
	t       *testing.T
	s       *workload.Schedule
	shape   workload.Shape
	commits [][]uint64 // revision-1 -> the batches it committed
}

func (db *fakeDB) commit(batches ...uint64) uint64 {
	db.commits = append(db.commits, batches)
	return uint64(len(db.commits))
}

// state returns idx at rev by replaying the commits.
func (db *fakeDB) state(idx, rev uint64) workload.State {
	var st workload.State
	for _, batches := range db.commits[:rev] {
		for _, no := range batches {
			b := db.s.Batch(no)
			if db.s.Writes(b.Round, idx) && db.s.BatchOf(b.Round, idx) == no {
				st = db.s.After(b.Round, idx)
			}
		}
	}
	return st
}

func (db *fakeDB) value(idx uint64, st workload.State) []byte {
	return workload.AppendValue(nil, db.s.Seed, idx, st.Version, db.s.Dist.Size(idx, st.Version))
}

// scan returns up to limit items from lo at rev.
func (db *fakeDB) scan(lo []byte, rev uint64, limit int) []Item {
	var out []Item
	db.shape.Walk(lo, nil, func(idx uint64) bool {
		if st := db.state(idx, rev); st.Present {
			out = append(out, Item{Key: db.shape.AppendKey(nil, idx), Value: db.value(idx, st)})
		}
		return len(out) < limit
	})
	return out
}

func setup(t *testing.T, n, chunk, deletes uint64) (*fakeDB, *ledger.Ledger, *Verifier, *bytes.Buffer) {
	t.Helper()
	return setupWith(t, n, chunk, deletes, workload.Options{})
}

func setupWith(t *testing.T, n, chunk, deletes uint64, o workload.Options) (*fakeDB, *ledger.Ledger, *Verifier, *bytes.Buffer) {
	t.Helper()
	d, _ := workload.ParseSizeDist("fixed:40")
	s, err := workload.NewScheduleWith(n, chunk, 3, deletes, d, o)
	if err != nil {
		t.Fatal(err)
	}
	shape, err := workload.NewShape("random", s.MaxKeys(), 3)
	if err != nil {
		t.Fatal(err)
	}
	l := ledger.New()
	var log bytes.Buffer
	v := New(&Model{Schedule: s, Ledger: l}, shape, &log)
	return &fakeDB{t: t, s: s, shape: shape}, l, v, &log
}

// send begins and commits each batch in its own commit, in the given order.
func (db *fakeDB) send(t *testing.T, l *ledger.Ledger, batches ...uint64) {
	t.Helper()
	for _, no := range batches {
		a, err := l.Begin(no)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Committed(db.commit(no)); err != nil {
			t.Fatal(err)
		}
	}
}

func requireClean(t *testing.T, v *Verifier, log *bytes.Buffer) {
	t.Helper()
	if s := v.Stats(); s.Failed() != 0 {
		t.Fatalf("%s\n%s", s.Summary(), log)
	}
}

func TestCorrectAnswersPass(t *testing.T) {
	db, l, v, log := setup(t, 200, 8, 150)
	per := db.s.BatchesPerRound()
	// Round 0 in a scrambled order, then two rounds of overwrites and
	// deletes.
	for j := range per {
		db.send(t, l, (j*7)%per)
	}
	for no := per; no < 3*per; no++ {
		db.send(t, l, no)
	}
	top := uint64(len(db.commits))
	for rev := uint64(0); rev <= top; rev += 5 {
		for idx := range db.s.N {
			st := db.state(idx, rev)
			var value []byte
			if st.Present {
				value = db.value(idx, st)
			}
			v.Get(idx, rev, value, st.Present)
		}
		var lo []byte
		for range 4 {
			items := db.scan(lo, rev, 17)
			more := len(items) == 17
			v.Page(lo, nil, rev, items, more)
			if !more {
				break
			}
			lo = append(bytes.Clone(items[len(items)-1].Key), 0)
		}
	}
	requireClean(t, v, log)
	if s := v.Stats(); s.Checked == 0 || s.Queued != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestWrongAnswersAreCaught(t *testing.T) {
	db, l, v, log := setup(t, 100, 10, 1000) // round 1 deletes every key
	per := db.s.BatchesPerRound()
	for no := range per {
		db.send(t, l, no)
	}
	inserted := uint64(len(db.commits))
	for no := per; no < 2*per; no++ {
		db.send(t, l, no)
	}
	deleted := uint64(len(db.commits))
	idx := uint64(42)
	insertRev := l.Get(db.s.BatchOf(0, idx)).Revision
	v0 := db.value(idx, workload.State{Present: true})

	cases := []struct {
		kind  string
		check func()
	}{
		{Missing, func() { v.Get(idx, inserted, nil, false) }},
		{Resurrected, func() { v.Get(idx, deleted, v0, true) }},
		{Future, func() { v.Get(idx, insertRev-1, v0, true) }},
		{Corrupt, func() { bad := bytes.Clone(v0); bad[len(bad)-1] ^= 1; v.Get(idx, inserted, bad, true) }},
		{Missing, func() {
			items := db.scan(nil, inserted, 20)
			v.Page(nil, nil, inserted, append(items[:5:5], items[6:]...), true) // one key skipped
		}},
		{Order, func() {
			items := db.scan(nil, inserted, 20)
			items[3], items[4] = items[4], items[3]
			v.Page(nil, nil, inserted, items, true)
		}},
		{Unexpected, func() {
			v.Page(nil, nil, inserted, []Item{{Key: []byte("~stress/meta"), Value: []byte("x")}}, true)
		}},
	}
	for _, c := range cases {
		before := v.Stats().Failures[c.kind]
		c.check()
		if got := v.Stats().Failures[c.kind]; got != before+1 {
			t.Errorf("%s: %d failures, want %d\n%s", c.kind, got, before+1, log)
		}
	}
	if !strings.Contains(log.String(), `"kind":"resurrected"`) || !strings.Contains(log.String(), `"batches":[`) {
		t.Fatalf("the failure log lacks detail:\n%s", log)
	}

	// A stale read: a version older than one already overwritten.
	db2, l2, v2, log2 := setup(t, 50, 5, 0)
	per2 := db2.s.BatchesPerRound()
	for no := range 2 * per2 {
		db2.send(t, l2, no)
	}
	v2.Get(7, uint64(len(db2.commits)), db2.value(7, workload.State{Present: true}), true)
	if v2.Stats().Failures[Stale] != 1 {
		t.Fatalf("stale read not caught: %s\n%s", v2.Stats().Summary(), log2)
	}
}

func TestReadsAboveTheSettledRevisionWait(t *testing.T) {
	db, l, v, log := setup(t, 40, 4, 0)
	db.send(t, l, 0, 1)
	slow, err := l.Begin(2) // in flight while later batches commit
	if err != nil {
		t.Fatal(err)
	}
	db.send(t, l, 3)
	rev := db.commit(2)     // batch 2 commits, but its answer is not back yet
	idx := db.s.Index(0, 8) // in batch 2
	st := db.state(idx, rev)
	v.Get(idx, rev, db.value(idx, st), true)
	if s := v.Stats(); s.Pending != 1 || s.Checked != 0 {
		t.Fatalf("a read past the settled revision was decided: %+v", s)
	}
	if err := slow.Committed(rev); err != nil {
		t.Fatal(err)
	}
	if left := v.Drain(); left != 0 {
		t.Fatalf("%d still queued after its batch settled", left)
	}
	requireClean(t, v, log)

	// A read that never settles fails at the end.
	if _, err := l.Begin(5); err != nil {
		t.Fatal(err)
	}
	v.Get(0, rev+1, db.value(0, workload.State{Present: true}), true)
	v.Finish()
	if v.Stats().Failures[Unsettled] != 1 {
		t.Fatalf("an unsettled read at the end: %s", v.Stats().Summary())
	}
}

func TestUnknownOutcomesAllowEitherState(t *testing.T) {
	db, l, v, log := setup(t, 40, 4, 0)
	per := db.s.BatchesPerRound()
	for no := range per {
		db.send(t, l, no)
	}
	// Round 1's first batch: its first attempt's answer is lost, but it
	// committed; the resend commits again later.
	no := per
	a, _ := l.Begin(no)
	unseen := db.commit(no)
	a.Unknown()
	db.send(t, l, no)
	idx := db.s.Index(1, 0)
	v1 := db.value(idx, workload.State{Present: true, Version: 1})
	v0 := db.value(idx, workload.State{Present: true})
	// Below the resend's revision the key may hold either version.
	v.Get(idx, unseen, v1, true)
	v.Get(idx, unseen-1, v0, true)
	requireClean(t, v, log)
}

// Publishing maintenance's work moves the revision without a write, so a
// read can be answered above every batch's revision. With nothing in flight
// it is checked at once, not left to fail as unsettled.
func TestReadsPastTheLastWriteSettle(t *testing.T) {
	db, l, v, log := setup(t, 40, 4, 0)
	for no := range db.s.BatchesPerRound() {
		db.send(t, l, no)
	}
	past := uint64(len(db.commits)) + 2 // two compactions published since
	idx := uint64(7)
	v.Get(idx, past, db.value(idx, workload.State{Present: true}), true)
	v.Page(nil, nil, past, db.scan(nil, uint64(len(db.commits)), 1000), false)
	v.Finish()
	requireClean(t, v, log)
	if s := v.Stats(); s.Checked != 2 || s.Queued != 0 {
		t.Fatalf("stats %+v: want both reads checked at once", s)
	}
}

// checkAll reads every key the schedule may ever have, and scans
// everything in pages, at each revision up to the newest.
func checkAll(t *testing.T, db *fakeDB, v *Verifier, step uint64) {
	t.Helper()
	top := uint64(len(db.commits))
	for rev := uint64(0); rev <= top; rev += step {
		for idx := range db.s.MaxKeys() {
			st := db.state(idx, rev)
			var value []byte
			if st.Present {
				value = db.value(idx, st)
			}
			v.Get(idx, rev, value, st.Present)
		}
		var lo []byte
		for {
			items := db.scan(lo, rev, 23)
			more := len(items) == 23
			v.Page(lo, nil, rev, items, more)
			if !more {
				break
			}
			lo = append(bytes.Clone(items[len(items)-1].Key), 0)
		}
	}
}

// Rounds that write part of the keys, a hot range among them, delete some
// and insert new ones: right answers pass at every revision, and a key keeps
// what the last round to write it left.
func TestPartialRoundsAreCheckedExactly(t *testing.T) {
	hot := func(idx uint64) bool { return idx < 30 }
	db, l, v, log := setupWith(t, 300, 4, 250, workload.Options{UpdatePermille: 100, HotPermille: 700, Hot: hot, HotShare: 100,
		InsertsPerRound: 40, Rounds: 4})
	for no := range db.s.FirstBatch(5) {
		if db.s.Written(db.s.Batch(no)) > 0 {
			db.send(t, l, no)
		}
	}
	checkAll(t, db, v, 3)
	requireClean(t, v, log)

	// A wrong answer for a key the last round left alone is caught: it must
	// still hold what an earlier round wrote.
	top := uint64(len(db.commits))
	kept, found := uint64(0), false
	for idx := range db.s.N {
		if r, ok := db.s.Last(4, idx); ok && r < 4 && r > 0 && !db.s.Deletes(r, idx) {
			kept, found = idx, true
			break
		}
	}
	if !found {
		t.Fatal("no key whose last write was a middle round")
	}
	st := db.state(kept, top)
	stale := workload.State{Present: true, Version: st.Version - 1}
	v.Get(kept, top, db.value(kept, stale), true)
	if s := v.Stats(); s.Failures[Stale] != 1 {
		t.Fatalf("a stale read of key %d (%v, holds %v) was not caught: %s", kept, stale, st, s.Summary())
	}
	// A key round 4 inserts is absent before its batch committed.
	idx := db.s.Keys(3)
	rev := l.Get(db.s.BatchOf(4, idx)).Revision - 1
	v.Get(idx, rev, db.value(idx, workload.State{Present: true, Version: 4}), true)
	if s := v.Stats(); s.Failures[Future] != 1 {
		t.Fatalf("key %d read before round 4 inserted it was not caught: %s", idx, s.Summary())
	}
}
