package isledb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// applyRetrying applies b, waiting out backpressure as a caller would.
func applyRetrying(ctx context.Context, w *Writer, b *Batch) (uint64, error) {
	for {
		last, err := w.Apply(ctx, b)
		if !errors.Is(err, ErrBackpressure) {
			return last, err
		}
		time.Sleep(time.Millisecond)
	}
}

// readWholeFeed returns every change in the feed by sequence.
func (f *batchFixture) readWholeFeed(t *testing.T, payload manifest.ChangeFeedPayload) map[uint64]Change {
	t.Helper()
	db, err := openDB(f.ctx, f.store, dbOpenOptions{changeFeedPayload: payload})
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
	changes := map[uint64]Change{}
	for cursor := bounds.Oldest; cursor != bounds.Head; {
		page, err := feed.Read(f.ctx, cursor, ChangeReadOptions{MaxChanges: 500})
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range page.Changes {
			changes[c.Sequence] = c
		}
		cursor = page.Next
	}
	return changes
}

// TestApplyConcurrentBatchesStayContiguous applies batches from several
// goroutines, with Puts between them and commits cutting memtables: each
// batch's sequences, and its feed records, are one unbroken run.
func TestApplyConcurrentBatchesStayContiguous(t *testing.T) {
	f := newBatchFixture(t, manifest.ChangeFeedPayloadFullValues, 32<<10, 64)
	const workers, batches, size = 4, 40, 5
	type applied struct {
		last   uint64
		worker int
		batch  int
	}
	results := make(chan applied, workers*batches)
	errs := make(chan error, workers+2)
	stop := make(chan struct{})
	var wg, background sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := NewBatch()
			for i := range batches {
				b.Reset()
				for j := range size {
					b.Put(fmt.Appendf(nil, "w%d-b%03d-%d", w, i, j), []byte("value"))
				}
				last, err := applyRetrying(f.ctx, f.writer, b)
				if err != nil {
					errs <- err
					return
				}
				results <- applied{last, w, i}
			}
		}()
	}
	background.Add(2)
	go func() { // single Puts between the batches
		defer background.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := f.writer.Put(f.ctx, fmt.Appendf(nil, "single-%05d", i), []byte("v")); err != nil && !errors.Is(err, ErrBackpressure) {
				errs <- err
				return
			}
		}
	}()
	go func() { // commits as often as possible
		defer background.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = f.writer.Flush(f.ctx)
			}
		}
	}()
	wg.Wait()
	close(stop)
	background.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}

	changes := f.readWholeFeed(t, manifest.ChangeFeedPayloadFullValues)
	close(results)
	n := 0
	for r := range results {
		n++
		for j := range size {
			seq := r.last - size + 1 + uint64(j)
			want := fmt.Sprintf("w%d-b%03d-%d", r.worker, r.batch, j)
			if got := string(changes[seq].Key); got != want {
				t.Fatalf("sequence %d holds %q, want %q: batch w%d-b%03d is not one run", seq, got, want, r.worker, r.batch)
			}
		}
	}
	if n != workers*batches {
		t.Fatalf("%d batches applied, want %d", n, workers*batches)
	}
}

func TestBatchCopiesWhatIsAdded(t *testing.T) {
	f := newBatchFixture(t, "", 16<<20, 4)
	key, value := []byte("key-1"), []byte("value-1")
	b := NewBatch()
	b.Put(key, value)
	copy(key, "XXXXX")
	copy(value, "YYYYYYY")
	if _, err := f.writer.Apply(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	reader := f.reader(t)
	if got, found, err := reader.Get(f.ctx, []byte("key-1")); err != nil || !found || string(got) != "value-1" {
		t.Fatalf("key-1 = %q, %t, %v; want the value as it was added", got, found, err)
	}
	if _, found, _ := reader.Get(f.ctx, []byte("XXXXX")); found {
		t.Fatal("a key changed after adding was written")
	}
}

func TestApplyKeysOnlyFeed(t *testing.T) {
	f := newBatchFixture(t, manifest.ChangeFeedPayloadKeysOnly, 16<<20, 4)
	b := NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Delete([]byte("b"))
	last, err := f.writer.Apply(f.ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	changes := f.readWholeFeed(t, manifest.ChangeFeedPayloadKeysOnly)
	put, del := changes[last-1], changes[last]
	if string(put.Key) != "a" || put.HasValue || put.Value != nil || put.Operation == ChangeDelete {
		t.Fatalf("put change %+v, want key a with no value", put)
	}
	if string(del.Key) != "b" || del.Operation != ChangeDelete {
		t.Fatalf("delete change %+v, want a delete of b", del)
	}
}

func TestApplyBatchOfAFencedWriterNeverCommits(t *testing.T) {
	f := newBatchFixture(t, "", 16<<20, 4)
	b := NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	last, err := f.writer.Apply(f.ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manifest.NewStore(f.store).ClaimWriter(f.ctx, "successor"); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("flush after takeover: %v, want ErrFenced", err)
	}
	if _, err := f.writer.WaitCommitted(f.ctx, last); !errors.Is(err, ErrFenced) {
		t.Fatalf("WaitCommitted on the batch: %v, want ErrFenced", err)
	}
	if got, err := f.writer.CommitStatus(f.writer.Epoch(), last); err != nil || got != CommitNever {
		t.Fatalf("CommitStatus of the batch: %v, %v; want never", got, err)
	}
	if _, err := f.writer.Apply(f.ctx, b); !errors.Is(err, ErrFenced) {
		t.Fatalf("Apply on a fenced writer: %v, want ErrFenced", err)
	}
}

func TestApplyRefusesEachInvalidMutation(t *testing.T) {
	f := newBatchFixture(t, "", 16<<20, 4, func(o *WriterOptions) {
		o.Values.MaxKeyBytes = 16
		o.Values.MaxValueBytes = 32
	})
	for _, c := range []struct {
		name string
		add  func(*Batch)
		want string
	}{
		{"oversize key", func(b *Batch) { b.Put([]byte(strings.Repeat("k", 17)), nil) }, "key size"},
		{"oversize value", func(b *Batch) { b.Put([]byte("k"), []byte(strings.Repeat("v", 33))) }, "value size"},
		{"negative TTL", func(b *Batch) { b.PutWithTTL([]byte("k"), []byte("v"), -time.Second) }, "negative TTL"},
		{"empty key in a delete", func(b *Batch) { b.Delete(nil) }, "empty key"},
	} {
		b := NewBatch()
		b.Put([]byte("ok"), []byte("v"))
		c.add(b)
		before := f.writer.AcceptedSequence()
		_, err := f.writer.Apply(f.ctx, b)
		if !errors.Is(err, ErrInvalidMutation) || !strings.Contains(err.Error(), "mutation 1") || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want ErrInvalidMutation naming mutation 1 and %q", c.name, err, c.want)
		}
		if f.writer.AcceptedSequence() != before {
			t.Errorf("%s: the refused batch took sequences", c.name)
		}
	}
}

func TestApplyEarlyExits(t *testing.T) {
	f := newBatchFixture(t, "", 16<<20, 4)
	b := NewBatch()
	b.Put([]byte("k"), []byte("v"))
	if _, err := f.writer.Apply(f.ctx, nil); !errors.Is(err, ErrInvalidMutation) {
		t.Errorf("nil batch: %v, want ErrInvalidMutation", err)
	}
	cancelled, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := f.writer.Apply(cancelled, b); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled context: %v, want context.Canceled", err)
	}
	if f.writer.AcceptedSequence() != 0 {
		t.Fatal("an early exit took a sequence")
	}
	if err := f.writer.Close(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.writer.Apply(f.ctx, b); !errors.Is(err, ErrWriterClosed) {
		t.Errorf("closed writer: %v, want ErrWriterClosed", err)
	}
}

func TestBatchReuseAndShapes(t *testing.T) {
	f := newBatchFixture(t, "", 16<<20, 4)
	var b Batch // the zero value is an empty batch
	b.Put([]byte("k"), []byte("first"))
	first, err := f.writer.Apply(f.ctx, &b)
	if err != nil {
		t.Fatal(err)
	}
	// Applying again writes again, under new sequences.
	again, err := f.writer.Apply(f.ctx, &b)
	if err != nil || again != first+1 || b.Len() != 1 {
		t.Fatalf("second apply: last %d, %v, len %d; want %d and the batch unchanged", again, err, b.Len(), first+1)
	}
	b.Reset()
	if b.Len() != 0 {
		t.Fatalf("after Reset, %d mutations", b.Len())
	}
	b.Put([]byte("k"), []byte("second"))
	b.Put([]byte("nil-value"), nil)
	b.Put([]byte("empty-value"), []byte{})
	if _, err := f.writer.Apply(f.ctx, &b); err != nil {
		t.Fatal(err)
	}
	deletes := NewBatch() // a batch of only deletes
	deletes.Delete([]byte("gone-1"))
	deletes.Delete([]byte("empty-value"))
	if _, err := f.writer.Apply(f.ctx, deletes); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	reader := f.reader(t)
	for key, want := range map[string]string{"k": "second", "nil-value": ""} {
		if got, found, err := reader.Get(f.ctx, []byte(key)); err != nil || !found || string(got) != want {
			t.Fatalf("%s = %q, %t, %v; want %q", key, got, found, err, want)
		}
	}
	if _, found, err := reader.Get(f.ctx, []byte("empty-value")); err != nil || found {
		t.Fatalf("deleted key: found %t, %v", found, err)
	}
}

// TestApplyBatchAtTheCapReadsBack commits a batch of tiny entries at the cap
// and reads every key back.
func TestApplyBatchAtTheCapReadsBack(t *testing.T) {
	f := newBatchFixture(t, "", 64<<10, 8, func(o *WriterOptions) { o.Values.MaxValueBytes = 1 << 10 })
	limit, _ := f.writer.w.batchLimits()
	n := int(limit / internal.EntryArenaBytes(8, 1, false))
	b := NewBatch()
	for i := range n {
		b.Put(fmt.Appendf(nil, "k%07d", i), []byte("v"))
	}
	if _, err := f.writer.Apply(f.ctx, b); err != nil {
		t.Fatalf("a batch at the cap: %v", err)
	}
	if err := f.writer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	reader := f.reader(t)
	for i := range n {
		if _, found, err := reader.Get(f.ctx, fmt.Appendf(nil, "k%07d", i)); err != nil || !found {
			t.Fatalf("k%07d of %d: found %t, %v", i, n, found, err)
		}
	}
}
