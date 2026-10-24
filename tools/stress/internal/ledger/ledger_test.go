package ledger

import (
	"os"
	"path/filepath"
	"testing"
)

func begin(t *testing.T, l *Ledger, no uint64) *Attempt {
	t.Helper()
	a, err := l.Begin(no)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestSettledStaysBelowAttemptsInFlight(t *testing.T) {
	l := New()
	if l.Settled() != 0 || l.MaxSent() != -1 {
		t.Fatalf("empty ledger: settled %d, max sent %d", l.Settled(), l.MaxSent())
	}
	a := begin(t, l, 0)
	b := begin(t, l, 1)
	if err := b.Committed(10); err != nil {
		t.Fatal(err)
	}
	// a began when nothing had committed, so it may still commit at any
	// revision above 0, below b's.
	if l.Settled() != 0 {
		t.Fatalf("settled %d while batch 0 is in flight, want 0", l.Settled())
	}
	c := begin(t, l, 2) // began after revision 10 was seen
	if err := a.Committed(12); err != nil {
		t.Fatal(err)
	}
	if l.Settled() != 10 {
		t.Fatalf("settled %d with batch 2 in flight since revision 10, want 10", l.Settled())
	}
	c.NotCommitted()
	if l.Settled() != 12 || l.MaxSent() != 2 {
		t.Fatalf("settled %d, max sent %d; want 12 and 2", l.Settled(), l.MaxSent())
	}
	if e := l.Get(0); e.Revision != 12 || e.MaybeEarlier {
		t.Fatalf("batch 0: %+v", e)
	}
	if e := l.Get(2); e != (Entry{}) {
		t.Fatalf("batch 2 never committed: %+v", e)
	}
	if e := l.Get(5 << segBits); e != (Entry{}) {
		t.Fatalf("an unseen segment: %+v", e)
	}
}

func TestUnknownOutcomeMarksTheBatch(t *testing.T) {
	l := New()
	a := begin(t, l, 3)
	a.Unknown()
	if e := l.Get(3); e.Revision != 0 || !e.MaybeEarlier {
		t.Fatalf("after an unknown outcome: %+v", e)
	}
	b := begin(t, l, 3)
	if err := b.Committed(7); err != nil {
		t.Fatal(err)
	}
	if e := l.Get(3); e.Revision != 7 || !e.MaybeEarlier {
		t.Fatalf("resent after an unknown outcome: %+v, want revision 7, maybe earlier", e)
	}
	if err := begin(t, l, 4).Committed(0); err == nil {
		t.Fatal("revision 0 accepted")
	}
}

func TestReopenKeepsCommitsAndMarksTheUnfinished(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := begin(t, l, 0).Committed(5); err != nil {
		t.Fatal(err)
	}
	begin(t, l, 1) // never finished: the process dies
	if err := begin(t, l, 2).Committed(9); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// A torn record at the end is dropped.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.Write([]byte{recCommit, 0, 0})
	_ = f.Close()

	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if e := l.Get(0); e.Revision != 5 || e.MaybeEarlier {
		t.Fatalf("batch 0: %+v", e)
	}
	if e := l.Get(1); e.Revision != 0 || !e.MaybeEarlier {
		t.Fatalf("batch 1, begun and never finished: %+v, want maybe earlier", e)
	}
	if e := l.Get(2); e.Revision != 9 {
		t.Fatalf("batch 2: %+v", e)
	}
	if l.MaxRevision() != 9 || l.MaxSent() != 2 || l.Settled() != 9 {
		t.Fatalf("max revision %d, max sent %d, settled %d", l.MaxRevision(), l.MaxSent(), l.Settled())
	}
	if err := begin(t, l, 1).Committed(11); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if l, err = Open(path); err != nil {
		t.Fatal(err)
	}
	if e := l.Get(1); e.Revision != 11 || !e.MaybeEarlier {
		t.Fatalf("batch 1 after its resend committed: %+v", e)
	}
	_ = l.Close()
}
