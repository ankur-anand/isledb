// Package ledger records what became of every batch the load sent: the
// revision it committed at, or that its outcome is unknown. A read answered
// at a revision the ledger has settled can then be checked exactly: every
// batch at or below that revision is in the ledger, and none above it is
// visible.
package ledger

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

const (
	segBits = 20
	segSize = 1 << segBits

	// maybeEarlier marks a batch whose earlier attempt may have committed at
	// a revision nobody saw: a read below its recorded revision may hold it
	// too.
	maybeEarlier = 1 << 63
	revMask      = maybeEarlier - 1
)

// Entry is what the ledger knows of one batch.
type Entry struct {
	// Revision is the revision the batch committed at; 0 if it has not, as
	// far as anyone saw.
	Revision uint64
	// MaybeEarlier is set when an attempt whose outcome was never learned
	// came first: the batch may also be visible below Revision, or, with
	// Revision 0, at any revision.
	MaybeEarlier bool
}

// Ledger is safe for concurrent use.
type Ledger struct {
	mu   sync.RWMutex
	segs []*[segSize]atomic.Uint64

	maxRev  atomic.Uint64
	maxSent atomic.Int64 // highest batch number begun, -1 for none

	// In-flight attempts by begin order, each with the highest revision
	// seen when it began: it can only commit above that.
	flightMu sync.Mutex
	flight   map[uint64]uint64 // token -> floor
	nextTok  uint64
	oldest   uint64

	log    *os.File
	logBuf *bufio.Writer
}

// New returns an empty ledger kept in memory only.
func New() *Ledger {
	l := &Ledger{flight: map[uint64]uint64{}}
	l.maxSent.Store(-1)
	return l
}

const (
	recBegin  = 1
	recCommit = 2
)

// Open returns the ledger recorded at path, creating it if it does not
// exist, and keeps recording there. Batches a previous process began but
// never saw an outcome for are marked MaybeEarlier: they may have committed.
func Open(path string) (*Ledger, error) {
	l := New()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	begun := map[uint64]bool{}
	r := bufio.NewReader(f)
	var rec [17]byte
	var good int64
	for {
		if _, err := io.ReadFull(r, rec[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // a torn last record is dropped
			}
			_ = f.Close()
			return nil, err
		}
		no, v := binary.BigEndian.Uint64(rec[1:]), binary.BigEndian.Uint64(rec[9:])
		switch rec[0] {
		case recBegin:
			begun[no] = true
			l.noteSent(no)
		case recCommit:
			delete(begun, no)
			l.set(no, v)
			l.noteRev(v & revMask)
		default:
			_ = f.Close()
			return nil, fmt.Errorf("ledger %s: bad record at byte %d", path, good)
		}
		good += int64(len(rec))
	}
	for no := range begun {
		if l.Get(no).Revision == 0 {
			l.set(no, maybeEarlier)
		}
	}
	if err := f.Truncate(good); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	l.log, l.logBuf = f, bufio.NewWriterSize(f, 1<<16)
	return l, nil
}

// Close flushes and closes the record.
func (l *Ledger) Close() error {
	if l.log == nil {
		return nil
	}
	l.mu.Lock()
	err := l.logBuf.Flush()
	l.mu.Unlock()
	return errors.Join(err, l.log.Close())
}

func (l *Ledger) cell(no uint64, grow bool) *atomic.Uint64 {
	s, i := no>>segBits, no&(segSize-1)
	l.mu.RLock()
	if s < uint64(len(l.segs)) && l.segs[s] != nil {
		c := &l.segs[s][i]
		l.mu.RUnlock()
		return c
	}
	l.mu.RUnlock()
	if !grow {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for uint64(len(l.segs)) <= s {
		l.segs = append(l.segs, nil)
	}
	if l.segs[s] == nil {
		l.segs[s] = new([segSize]atomic.Uint64)
	}
	return &l.segs[s][i]
}

func (l *Ledger) set(no, v uint64) { l.cell(no, true).Store(v) }

func (l *Ledger) noteRev(rev uint64) {
	for {
		cur := l.maxRev.Load()
		if rev <= cur || l.maxRev.CompareAndSwap(cur, rev) {
			return
		}
	}
}

func (l *Ledger) noteSent(no uint64) {
	for {
		cur := l.maxSent.Load()
		if int64(no) <= cur || l.maxSent.CompareAndSwap(cur, int64(no)) {
			return
		}
	}
}

func (l *Ledger) record(kind byte, no, v uint64) error {
	if l.log == nil {
		return nil
	}
	var rec [17]byte
	rec[0] = kind
	binary.BigEndian.PutUint64(rec[1:], no)
	binary.BigEndian.PutUint64(rec[9:], v)
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.logBuf.Write(rec[:])
	return err
}

// Flush writes buffered records to the file.
func (l *Ledger) Flush() error {
	if l.log == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.logBuf.Flush()
}

// Get returns what is known of batch no.
func (l *Ledger) Get(no uint64) Entry {
	c := l.cell(no, false)
	if c == nil {
		return Entry{}
	}
	v := c.Load()
	return Entry{Revision: v & revMask, MaybeEarlier: v&maybeEarlier != 0}
}

// Attempt is one send of a batch, from Begin to its outcome.
type Attempt struct {
	l   *Ledger
	no  uint64
	tok uint64
}

// Begin records that batch no is about to be sent.
func (l *Ledger) Begin(no uint64) (*Attempt, error) {
	if err := l.record(recBegin, no, 0); err != nil {
		return nil, err
	}
	l.noteSent(no)
	l.flightMu.Lock()
	tok := l.nextTok
	l.nextTok++
	l.flight[tok] = l.maxRev.Load()
	l.flightMu.Unlock()
	return &Attempt{l: l, no: no, tok: tok}, nil
}

func (a *Attempt) end() {
	l := a.l
	l.flightMu.Lock()
	delete(l.flight, a.tok)
	for l.oldest < l.nextTok {
		if _, ok := l.flight[l.oldest]; ok {
			break
		}
		l.oldest++
	}
	l.flightMu.Unlock()
}

// Committed records that the attempt committed at rev.
func (a *Attempt) Committed(rev uint64) error {
	if rev == 0 || rev&maybeEarlier != 0 {
		return fmt.Errorf("ledger: batch %d committed at impossible revision %d", a.no, rev)
	}
	v := rev
	if a.l.Get(a.no).MaybeEarlier {
		v |= maybeEarlier
	}
	a.l.set(a.no, v)
	a.l.noteRev(rev)
	a.end()
	return a.l.record(recCommit, a.no, v)
}

// NotCommitted records that the attempt had no effect: the writer refused
// it, or said it will never be saved.
func (a *Attempt) NotCommitted() { a.end() }

// Unknown records that the attempt's outcome will never be learned: it may
// have committed at a revision nobody saw.
func (a *Attempt) Unknown() {
	c := a.l.cell(a.no, true)
	for {
		v := c.Load()
		if v&maybeEarlier != 0 || c.CompareAndSwap(v, v|maybeEarlier) {
			break
		}
	}
	a.end()
}

// Settled returns the highest revision at which every batch visible is in
// the ledger: no attempt in flight can commit at or below it.
func (l *Ledger) Settled() uint64 {
	l.flightMu.Lock()
	defer l.flightMu.Unlock()
	if floor, ok := l.flight[l.oldest]; ok {
		// Attempts begin in order, so the oldest has the lowest floor.
		return floor
	}
	return l.maxRev.Load()
}

// MaxSent returns the highest batch number ever begun, or -1.
func (l *Ledger) MaxSent() int64 { return l.maxSent.Load() }

// MaxRevision returns the highest revision any batch committed at.
func (l *Ledger) MaxRevision() uint64 { return l.maxRev.Load() }
