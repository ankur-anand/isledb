package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
)

type batchFixture struct {
	ctx    context.Context
	store  *blobstore.Store
	writer *Writer
}

// newBatchFixture opens a writer with small memtables and no flush interval.
func newBatchFixture(t *testing.T, payload manifest.ChangeFeedPayload, target int64, maxPending int, adjust ...func(*WriterOptions)) *batchFixture {
	t.Helper()
	f := &batchFixture{ctx: context.Background(), store: blobstore.NewMemory("batch")}
	t.Cleanup(func() { _ = f.store.Close() })
	db, err := openDB(f.ctx, f.store, dbOpenOptions{changeFeedPayload: payload})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	opts.Memtable.TargetBytes = target
	opts.Memtable.MaxPendingMemtables = maxPending
	for _, fn := range adjust {
		fn(&opts)
	}
	if f.writer, err = db.OpenWriter(f.ctx, opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.writer.Close(f.ctx) })
	return f
}

func (f *batchFixture) reader(t *testing.T) *Reader {
	t.Helper()
	db, err := openDB(f.ctx, f.store, dbOpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.Views.RefreshAfter = time.Hour
	reader, err := db.OpenReader(f.ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

// sstHolding returns the committed L0 SST whose sequences include seq.
func (f *batchFixture) sstHolding(t *testing.T, seq uint64) manifest.SSTMeta {
	t.Helper()
	m, err := manifest.NewStore(f.store).Replay(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, sst := range m.L0SSTs {
		if sst.SeqLo <= seq && seq <= sst.SeqHi {
			return sst
		}
	}
	t.Fatalf("no SST holds sequence %d", seq)
	return manifest.SSTMeta{}
}

func TestApplyCommitsAsOneSST(t *testing.T) {
	f := newBatchFixture(t, "", 64<<10, 8)
	value := bytes.Repeat([]byte("v"), 1<<10)
	for i := range 50 { // most of a memtable
		if _, err := f.writer.Put(f.ctx, fmt.Appendf(nil, "fill-%02d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	filled := f.writer.AcceptedSequence()
	b := NewBatch()
	for i := range 20 { // does not fit what is left
		b.Put(fmt.Appendf(nil, "batch-%02d", i), value)
	}
	last, err := f.writer.Apply(f.ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	first := last - uint64(b.Len()) + 1
	if first != filled+1 || b.Len() != 20 {
		t.Fatalf("sequences %d-%d after %d, and %d mutations left in the batch; want %d on and 20", first, last, filled, b.Len(), filled+1)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	sst := f.sstHolding(t, first)
	if sst.SeqLo != first || sst.SeqHi != last {
		t.Fatalf("batch %d-%d is in an SST holding %d-%d, want it alone in one", first, last, sst.SeqLo, sst.SeqHi)
	}
	if _, err := f.writer.WaitCommitted(f.ctx, last); err != nil {
		t.Fatal(err)
	}
	if got, err := f.writer.CommitStatus(f.writer.Epoch(), last); err != nil || got != CommitDone {
		t.Fatalf("CommitStatus of the batch: %v, %v; want done", got, err)
	}
}

func TestApplyIsWholeOrNothing(t *testing.T) {
	f := newBatchFixture(t, manifest.ChangeFeedPayloadFullValues, 4<<10, 1)
	w := f.writer.w
	state := func() (uint64, int64, int64) {
		w.mu.Lock()
		defer w.mu.Unlock()
		var change int64
		if w.changeBuffer != nil {
			change = w.changeBuffer.bodySize
		}
		return w.seq, w.memtable.ApproxSize(), change
	}

	// An invalid mutation at the end applies none of those before it.
	b := NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Delete([]byte("b"))
	b.Put(nil, []byte("3"))
	before1, before2, before3 := state()
	if _, err := f.writer.Apply(f.ctx, b); !errors.Is(err, ErrInvalidMutation) || !strings.Contains(err.Error(), "mutation 2") {
		t.Fatalf("invalid last mutation: %v, want ErrInvalidMutation naming mutation 2", err)
	}
	if a1, a2, a3 := state(); a1 != before1 || a2 != before2 || a3 != before3 {
		t.Fatal("a refused batch changed the writer")
	}
	if _, err := f.writer.Apply(f.ctx, NewBatch()); !errors.Is(err, ErrInvalidMutation) {
		t.Fatalf("empty batch: %v, want ErrInvalidMutation", err)
	}

	// Backpressure: one memtable is already waiting, and the active one is
	// full; a batch that needs room is refused whole.
	value := bytes.Repeat([]byte("v"), 512)
	for i := 0; ; i++ {
		_, err := f.writer.Put(f.ctx, fmt.Appendf(nil, "fill-%03d", i), value)
		if errors.Is(err, ErrBackpressure) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	big := NewBatch()
	for i := range 3 {
		big.Put(fmt.Appendf(nil, "big-%d", i), value)
	}
	before1, before2, before3 = state()
	if _, err := f.writer.Apply(f.ctx, big); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("no room: %v, want ErrBackpressure", err)
	}
	if a1, a2, a3 := state(); a1 != before1 || a2 != before2 || a3 != before3 {
		t.Fatal("a batch refused for backpressure changed the writer")
	}
	// Once commits free room, the same batch goes in.
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.writer.Apply(f.ctx, big); err != nil {
		t.Fatalf("retried batch: %v", err)
	}
}

func TestApplyRefusesABatchPastTheArena(t *testing.T) {
	f := newBatchFixture(t, "", 64<<10, 8, func(o *WriterOptions) { o.Values.MaxValueBytes = 1 << 10 })
	limit, _ := f.writer.w.batchLimits()
	per := internal.EntryArenaBytes(8, 0, false)
	fits := int(limit / per)
	b := NewBatch()
	for i := range fits { // many tiny entries: their nodes, not their data, fill the arena
		b.Put(fmt.Appendf(nil, "k%07d", i), nil)
	}
	if _, err := f.writer.Apply(f.ctx, b); err != nil {
		t.Fatalf("a batch at the cap: %v", err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	b.Put([]byte("k-one-more"), nil)
	_, err := f.writer.Apply(f.ctx, b)
	if !errors.Is(err, ErrBatchTooLarge) || errors.Is(err, ErrInvalidMutation) {
		t.Fatalf("a batch past the cap: %v, want ErrBatchTooLarge alone", err)
	}
}

func TestApplyFeedsOneChangeBatch(t *testing.T) {
	f := newBatchFixture(t, manifest.ChangeFeedPayloadFullValues, 16<<20, 4)
	b := NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Delete([]byte("b"))
	b.PutWithTTL([]byte("c"), []byte("3"), time.Hour)
	last, err := f.writer.Apply(f.ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	db, err := openDB(f.ctx, f.store, dbOpenOptions{changeFeedPayload: manifest.ChangeFeedPayloadFullValues})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	feed, err := db.OpenChangeReader(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = feed.Close() })
	bounds, err := feed.Bounds(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	page, err := feed.Read(f.ctx, bounds.Oldest, ChangeReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Changes) != 3 {
		t.Fatalf("%d changes, want 3", len(page.Changes))
	}
	for i, c := range page.Changes {
		if want := last - 2 + uint64(i); c.Sequence != want {
			t.Fatalf("change %d has sequence %d, want %d", i, c.Sequence, want)
		}
	}
	if page.Changes[1].Operation != ChangeDelete || !bytes.Equal(page.Changes[2].Value, []byte("3")) {
		t.Fatalf("changes %+v, want put, delete, put", page.Changes)
	}
}

func TestApplyResolvesLikePutsInOrder(t *testing.T) {
	f := newBatchFixture(t, "", 16<<20, 4)
	b := NewBatch()
	b.Put([]byte("k"), []byte("first"))
	b.Put([]byte("k"), []byte("second"))
	b.PutWithTTL([]byte("short"), []byte("v"), 20*time.Millisecond)
	if _, err := f.writer.Apply(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	reader := f.reader(t)
	if got, found, err := reader.Get(f.ctx, []byte("k")); err != nil || !found || string(got) != "second" {
		t.Fatalf("k = %q, %t, %v; want the batch's last write", got, found, err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, found, err := reader.Get(f.ctx, []byte("short")); err != nil || found {
		t.Fatalf("expired key: found %t, %v; want hidden", found, err)
	}

	f.writer.StopWrites()
	if _, err := f.writer.Apply(f.ctx, b); !errors.Is(err, ErrWritesStopped) {
		t.Fatalf("after StopWrites: %v, want ErrWritesStopped", err)
	}
}

// TestApplyIsAtomicForReaders applies batches that each set every key to one
// generation while commits cut memtables as often as they can; a reader
// never sees keys from two generations.
func TestApplyIsAtomicForReaders(t *testing.T) {
	f := newBatchFixture(t, "", 8<<10, 64)
	const keys = 10
	value := bytes.Repeat([]byte("x"), 200)
	// write applies one generation, waiting out backpressure as a caller would.
	write := func(gen int) error {
		b := NewBatch()
		for k := range keys {
			b.Put(fmt.Appendf(nil, "key-%d", k), fmt.Appendf(value[:0:0], "%06d-%s", gen, value))
		}
		for {
			_, err := f.writer.Apply(f.ctx, b)
			if !errors.Is(err, ErrBackpressure) {
				return err
			}
			time.Sleep(time.Millisecond)
		}
	}
	if err := write(0); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	reader := f.reader(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // commit as often as possible, cutting memtables between batches
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = f.writer.Flush(f.ctx)
			}
		}
	}()
	done := make(chan struct{})
	var writeErr error
	go func() {
		defer close(done)
		for gen := 1; gen <= 300 && writeErr == nil; gen++ {
			writeErr = write(gen)
		}
	}()
	checks := 0
	for running := true; running; {
		select {
		case <-done:
			running = false
		default:
		}
		if err := reader.Refresh(f.ctx); err != nil {
			t.Fatal(err)
		}
		snap, err := reader.Snapshot(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		var gen string
		for k := range keys {
			got, found, err := snap.Get(f.ctx, fmt.Appendf(nil, "key-%d", k))
			if err != nil || !found {
				t.Fatalf("key-%d: found %t, %v", k, found, err)
			}
			if g := string(got[:6]); gen == "" {
				gen = g
			} else if g != gen {
				t.Fatalf("one view holds generations %s and %s: a batch was split", gen, g)
			}
		}
		_ = snap.Close()
		checks++
	}
	close(stop)
	wg.Wait()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if checks < 10 {
		t.Fatalf("only %d views checked", checks)
	}
}

func TestApplyTakesAnyWritePutTakes(t *testing.T) {
	const target = 64 << 10
	f := newBatchFixture(t, manifest.ChangeFeedPayloadFullValues, target, 8,
		func(o *WriterOptions) { o.Values.MaxValueBytes = target })
	value := bytes.Repeat([]byte("v"), target)
	if _, err := f.writer.Put(f.ctx, []byte("put"), value); err != nil {
		t.Fatalf("Put of a value at MaxValueBytes: %v", err)
	}
	b := NewBatch()
	b.PutWithTTL([]byte("batch"), value, time.Hour)
	if _, err := f.writer.Apply(f.ctx, b); err != nil {
		t.Fatalf("a one-entry batch Put would take: %v", err)
	}
	b.Put([]byte("batch-2"), value)
	if _, err := f.writer.Apply(f.ctx, b); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("two such writes in one batch: %v, want ErrBatchTooLarge", err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// TestEntryArenaBytesIsAnUpperBound puts entries into a real memtable and
// compares what each took with the bound Apply sizes batches by.
func TestEntryArenaBytesIsAnUpperBound(t *testing.T) {
	m := internal.NewMemtable(64 << 20)
	seq := uint64(0)
	for _, keyLen := range []int{1, 16, 1 << 10, maxMemtableUserKeyBytes} {
		for _, valueLen := range []int{0, 1, 100, 1 << 10, 1 << 20} {
			for _, ttl := range []bool{false, true} {
				seq++
				key := bytes.Repeat([]byte("k"), keyLen)
				expire := int64(0)
				if ttl {
					expire = time.Now().Add(time.Hour).UnixMilli()
				}
				before := m.ApproxSize()
				m.PutWithTTL(key, make([]byte, valueLen), seq, expire)
				if took, bound := m.ApproxSize()-before, internal.EntryArenaBytes(keyLen, valueLen, ttl); took > bound {
					t.Fatalf("key %d, value %d, ttl %t: took %d arena bytes, bound %d", keyLen, valueLen, ttl, took, bound)
				}
			}
		}
		seq++
		before := m.ApproxSize()
		m.Delete(bytes.Repeat([]byte("d"), keyLen), seq)
		if took, bound := m.ApproxSize()-before, internal.EntryArenaBytes(keyLen, 0, false); took > bound {
			t.Fatalf("delete, key %d: took %d arena bytes, bound %d", keyLen, took, bound)
		}
	}
}

// TestBatchCapFitsAFreshArena checks that a batch at Apply's cap fits a new
// memtable, whose arena already holds the skiplist's head node.
func TestBatchCapFitsAFreshArena(t *testing.T) {
	empty := internal.NewMemtable(1 << 16).ApproxSize()
	for _, target := range []int64{4 << 10, 64 << 10, 1 << 20, 16 << 20, 256 << 20} {
		for _, maxValue := range []int64{0, 1 << 10, 64 << 10, 16 << 20, 64 << 20} {
			values := ValueOptions{MaxKeyBytes: maxMemtableUserKeyBytes, MaxValueBytes: maxValue}
			w := &writer{opts: WriterOptions{Memtable: WriterMemtableOptions{TargetBytes: target}, Values: values}}
			memory, _ := w.batchLimits()
			if capacity := defaultMemtableArenaBytes(target, values); empty+memory > capacity {
				t.Fatalf("target %d, max value %d: cap %d plus empty %d exceeds the arena's %d",
					target, maxValue, memory, empty, capacity)
			}
		}
	}
}
