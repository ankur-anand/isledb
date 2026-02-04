package isledb

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func buildTestIter(t *testing.T, entries []internal.MemEntry) (*sstable.Reader, sstable.Iterator) {
	t.Helper()

	it := &sliceSSTIter{entries: entries}
	res, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(t, res.Meta, res.SSTData)), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		_ = reader.Close()
		t.Fatalf("NewIter: %v", err)
	}
	return reader, iter
}

type heapMergeIterator struct {
	iters   []sstable.Iterator
	heap    mergeHeap
	current *mergeEntry
	lastKey []byte
	err     error
}

type mergeHeap []*mergeEntry

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].trailer > h[j].trailer
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(*mergeEntry))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func newHeapMergeIterator(iters []sstable.Iterator) *heapMergeIterator {
	mi := &heapMergeIterator{
		iters: iters,
		heap:  make(mergeHeap, 0, len(iters)),
	}

	for i, iter := range iters {
		kv := iter.First()
		if kv != nil {
			mi.pushEntry(&kv.K, kv.InPlaceValue(), i)
		}
	}

	heap.Init(&mi.heap)
	return mi
}

func (mi *heapMergeIterator) pushEntry(ikey *sstable.InternalKey, val []byte, source int) {
	trailer := ikey.Trailer
	seq := uint64(trailer >> 8)
	kind := ikey.Kind()

	keyCopy := make([]byte, len(ikey.UserKey))
	copy(keyCopy, ikey.UserKey)

	valCopy := make([]byte, len(val))
	copy(valCopy, val)

	entry := &mergeEntry{
		key:     keyCopy,
		seq:     seq,
		trailer: uint64(trailer),
		kind:    kind,
		value:   valCopy,
		source:  source,
	}

	heap.Push(&mi.heap, entry)
}

func (mi *heapMergeIterator) Next() bool {
	for mi.heap.Len() > 0 {
		entry := heap.Pop(&mi.heap).(*mergeEntry)

		kv := mi.iters[entry.source].Next()
		if kv != nil {
			mi.pushEntry(&kv.K, kv.InPlaceValue(), entry.source)
		}

		if mi.lastKey != nil && bytes.Equal(entry.key, mi.lastKey) {
			continue
		}

		mi.current = entry
		mi.lastKey = append(mi.lastKey[:0], entry.key...)
		return true
	}
	return false
}

func (mi *heapMergeIterator) key() []byte {
	if mi.current == nil {
		return nil
	}
	return mi.current.key
}

func (mi *heapMergeIterator) entry() (internal.CompactionEntry, error) {
	if mi.current == nil {
		return internal.CompactionEntry{}, nil
	}

	entry := internal.CompactionEntry{
		Key: mi.current.key,
		Seq: mi.current.seq,
	}

	if mi.current.kind == sstable.InternalKeyKindDelete {
		entry.Kind = internal.OpDelete

		if len(mi.current.value) > 0 {
			keyEntry, err := internal.DecodeKeyEntry(mi.current.key, mi.current.value)
			if err != nil {
				return internal.CompactionEntry{}, err
			}
			entry.ExpireAt = keyEntry.ExpireAt
		}
		return entry, nil
	}

	keyEntry, err := internal.DecodeKeyEntry(mi.current.key, mi.current.value)
	if err != nil {
		return internal.CompactionEntry{}, err
	}

	entry.Kind = keyEntry.Kind
	entry.Inline = keyEntry.Inline
	entry.Value = keyEntry.Value
	entry.BlobID = keyEntry.BlobID
	entry.ExpireAt = keyEntry.ExpireAt

	return entry, nil
}

func (mi *heapMergeIterator) Err() error {
	if mi.err != nil {
		return mi.err
	}
	for _, iter := range mi.iters {
		if err := iter.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (mi *heapMergeIterator) close() error {
	var firstErr error
	for _, iter := range mi.iters {
		if err := iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func TestKMergeIterator_PrefersHigherSeq(t *testing.T) {
	readerOld, iterOld := buildTestIter(t, []internal.MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("old")},
	})
	defer readerOld.Close()

	readerNew, iterNew := buildTestIter(t, []internal.MemEntry{
		{Key: []byte("k"), Seq: 5, Kind: internal.OpPut, Inline: true, Value: []byte("new")},
	})
	defer readerNew.Close()

	merge := newMergeIterator([]sstable.Iterator{iterOld, iterNew})
	defer merge.close()

	if !merge.Next() {
		t.Fatalf("expected entry")
	}
	entry, err := merge.entry()
	if err != nil {
		t.Fatalf("entry: %v", err)
	}
	if !bytes.Equal(entry.Key, []byte("k")) {
		t.Fatalf("key: got %q want %q", entry.Key, []byte("k"))
	}
	if entry.Seq != 5 {
		t.Fatalf("seq: got %d want 5", entry.Seq)
	}
	if !entry.Inline || !bytes.Equal(entry.Value, []byte("new")) {
		t.Fatalf("value: got %q want %q", entry.Value, []byte("new"))
	}
	if merge.Next() {
		t.Fatalf("expected single entry")
	}
}

func TestKMergeIterator_PrefersHigherTrailerKind(t *testing.T) {
	seq := uint64(7)
	readerSet, iterSet := buildTestIter(t, []internal.MemEntry{
		{Key: []byte("k"), Seq: seq, Kind: internal.OpPut, Inline: true, Value: []byte("set")},
	})
	defer readerSet.Close()

	readerDel, iterDel := buildTestIter(t, []internal.MemEntry{
		{Key: []byte("k"), Seq: seq, Kind: internal.OpDelete},
	})
	defer readerDel.Close()

	kvSet := iterSet.First()
	kvDel := iterDel.First()
	if kvSet == nil || kvDel == nil {
		t.Fatalf("expected keys from iterators")
	}
	trailerSet := kvSet.K.Trailer
	trailerDel := kvDel.K.Trailer

	iterSet.First()
	iterDel.First()

	merge := newMergeIterator([]sstable.Iterator{iterSet, iterDel})
	defer merge.close()

	if !merge.Next() {
		t.Fatalf("expected entry")
	}
	entry, err := merge.entry()
	if err != nil {
		t.Fatalf("entry: %v", err)
	}

	if trailerSet > trailerDel {
		if entry.Kind != internal.OpPut || !bytes.Equal(entry.Value, []byte("set")) {
			t.Fatalf("expected set to win, got kind=%v value=%q", entry.Kind, entry.Value)
		}
	} else {
		if entry.Kind != internal.OpDelete {
			t.Fatalf("expected delete to win, got kind=%v", entry.Kind)
		}
	}
}

func TestTournamentMergeIterator_MatchesHeap(t *testing.T) {
	entries := [][]internal.MemEntry{
		{
			{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("a1")},
			{Key: []byte("b"), Seq: 3, Kind: internal.OpDelete, ExpireAt: 12345},
			{Key: []byte("c"), Seq: 5, Kind: internal.OpPut, Inline: true, Value: []byte("c5")},
		},
		{
			{Key: []byte("a"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("a2")},
			{Key: []byte("b"), Seq: 4, Kind: internal.OpPut, Inline: true, Value: []byte("b4")},
			{Key: []byte("d"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("d1")},
		},
		{
			{Key: []byte("a"), Seq: 3, Kind: internal.OpDelete},
			{Key: []byte("c"), Seq: 6, Kind: internal.OpPut, Inline: true, Value: []byte("c6")},
			{Key: []byte("e"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("e2")},
		},
	}

	readers := make([]*sstable.Reader, len(entries))
	for i := range entries {
		it := &sliceSSTIter{entries: entries[i]}
		res, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
		if err != nil {
			t.Fatalf("writeSST: %v", err)
		}
		reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(t, res.Meta, res.SSTData)), sstable.ReaderOptions{})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		readers[i] = reader
	}
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	makeIters := func() []sstable.Iterator {
		iters := make([]sstable.Iterator, len(readers))
		for i, r := range readers {
			iter, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
			if err != nil {
				t.Fatalf("NewIter: %v", err)
			}
			iters[i] = iter
		}
		return iters
	}

	heapMerge := newHeapMergeIterator(makeIters())
	tournamentMerge := newMergeIterator(makeIters())

	collect := func(it interface {
		Next() bool
		entry() (internal.CompactionEntry, error)
		Err() error
		close() error
	}) []internal.CompactionEntry {
		var out []internal.CompactionEntry
		for it.Next() {
			entry, err := it.entry()
			if err != nil {
				t.Fatalf("entry: %v", err)
			}
			entry.Key = append([]byte(nil), entry.Key...)
			if entry.Value != nil {
				entry.Value = append([]byte(nil), entry.Value...)
			}
			out = append(out, entry)
		}
		if err := it.Err(); err != nil {
			t.Fatalf("iter err: %v", err)
		}
		_ = it.close()
		return out
	}

	gotHeap := collect(heapMerge)
	gotTournament := collect(tournamentMerge)

	if len(gotHeap) != len(gotTournament) {
		t.Fatalf("entry count mismatch: heap=%d tournament=%d", len(gotHeap), len(gotTournament))
	}
	for i := range gotHeap {
		h := gotHeap[i]
		tm := gotTournament[i]
		if !bytes.Equal(h.Key, tm.Key) || h.Seq != tm.Seq || h.Kind != tm.Kind || h.Inline != tm.Inline ||
			!bytes.Equal(h.Value, tm.Value) || h.BlobID != tm.BlobID || h.ExpireAt != tm.ExpireAt {
			t.Fatalf("entry %d mismatch: heap=%+v tournament=%+v", i, h, tm)
		}
	}
}

func TestKMergeIterator_DeleteCorruptValue(t *testing.T) {
	buf := new(bytes.Buffer)
	writable := newHashingWritable(buf)
	writer := sstable.NewWriter(writable, sstable.WriterOptions{
		BlockSize:   4096,
		Compression: sstable.NoCompression,
	})

	ikey := pebble.MakeInternalKey([]byte("k"), pebble.SeqNum(1), pebble.InternalKeyKindDelete)
	badValue := []byte{internal.MarkerBlob}
	if err := writer.Raw().Add(ikey, badValue, false); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(buf.Bytes()), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer reader.Close()

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		t.Fatalf("new iter: %v", err)
	}

	merge := newMergeIterator([]sstable.Iterator{iter})
	defer merge.close()

	if !merge.Next() {
		t.Fatalf("expected entry")
	}
	if _, err := merge.entry(); err == nil {
		t.Fatalf("expected decode error for corrupt delete value")
	}
}

func buildBenchIter(b *testing.B, n int, keyOffset int, valueSize int) (*sstable.Reader, sstable.Iterator) {
	b.Helper()

	entries := make([]internal.MemEntry, n)
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%010d", keyOffset+i)
		entries[i] = internal.MemEntry{
			Key:    []byte(key),
			Seq:    uint64(keyOffset + i),
			Kind:   internal.OpPut,
			Inline: true,
			Value:  value,
		}
	}

	it := &sliceSSTIter{entries: entries}
	res, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "snappy"}, 1)
	if err != nil {
		b.Fatalf("writeSST: %v", err)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(b, res.Meta, res.SSTData)), sstable.ReaderOptions{})
	if err != nil {
		b.Fatalf("newReader: %v", err)
	}
	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		_ = reader.Close()
		b.Fatalf("NewIter: %v", err)
	}
	return reader, iter
}

func BenchmarkKMergeIterator_DisjointKeys(b *testing.B) {
	numItersCases := []int{2, 4, 8, 16}
	entriesPerIter := 1000
	valueSize := 100

	for _, numIters := range numItersCases {
		b.Run(fmt.Sprintf("iters=%d", numIters), func(b *testing.B) {

			readers := make([]*sstable.Reader, numIters)
			for i := 0; i < numIters; i++ {
				reader, _ := buildBenchIter(b, entriesPerIter, i*entriesPerIter, valueSize)
				readers[i] = reader
			}
			defer func() {
				for _, r := range readers {
					r.Close()
				}
			}()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {

				iters := make([]sstable.Iterator, numIters)
				for j := 0; j < numIters; j++ {
					iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
					iters[j] = iter
				}

				merge := newMergeIterator(iters)
				count := 0
				for merge.Next() {
					_ = merge.key()
					_ = merge.value()
					count++
				}
				merge.close()

				if count != numIters*entriesPerIter {
					b.Fatalf("expected %d entries, got %d", numIters*entriesPerIter, count)
				}
			}

			totalEntries := numIters * entriesPerIter
			b.ReportMetric(float64(totalEntries), "entries/op")
		})
	}
}

func BenchmarkKMergeIterator_OverlappingKeys(b *testing.B) {
	numItersCases := []int{2, 4, 8}
	entriesPerIter := 1000
	valueSize := 100

	for _, numIters := range numItersCases {
		b.Run(fmt.Sprintf("iters=%d", numIters), func(b *testing.B) {

			readers := make([]*sstable.Reader, numIters)
			for i := 0; i < numIters; i++ {

				reader, _ := buildBenchIterWithSeqOffset(b, entriesPerIter, 0, valueSize, uint64(i*entriesPerIter))
				readers[i] = reader
			}
			defer func() {
				for _, r := range readers {
					r.Close()
				}
			}()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iters := make([]sstable.Iterator, numIters)
				for j := 0; j < numIters; j++ {
					iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
					iters[j] = iter
				}

				merge := newMergeIterator(iters)
				count := 0
				for merge.Next() {
					_ = merge.key()
					count++
				}
				merge.close()

				if count != entriesPerIter {
					b.Fatalf("expected %d unique entries, got %d", entriesPerIter, count)
				}
			}

			b.ReportMetric(float64(entriesPerIter), "unique_keys/op")
			b.ReportMetric(float64(numIters*entriesPerIter), "total_entries/op")
		})
	}
}

func buildBenchIterWithSeqOffset(b *testing.B, n int, keyOffset int, valueSize int, seqOffset uint64) (*sstable.Reader, sstable.Iterator) {
	b.Helper()

	entries := make([]internal.MemEntry, n)
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%010d", keyOffset+i)
		entries[i] = internal.MemEntry{
			Key:    []byte(key),
			Seq:    seqOffset + uint64(i),
			Kind:   internal.OpPut,
			Inline: true,
			Value:  value,
		}
	}

	it := &sliceSSTIter{entries: entries}
	res, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "snappy"}, 1)
	if err != nil {
		b.Fatalf("writeSST: %v", err)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(sstPayload(b, res.Meta, res.SSTData)), sstable.ReaderOptions{})
	if err != nil {
		b.Fatalf("newReader: %v", err)
	}
	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		_ = reader.Close()
		b.Fatalf("NewIter: %v", err)
	}
	return reader, iter
}

func BenchmarkKMergeIterator_EntryCounts(b *testing.B) {
	entryCounts := []int{100, 1000, 10000}
	numIters := 4
	valueSize := 100

	for _, entryCount := range entryCounts {
		b.Run(fmt.Sprintf("entries=%d", entryCount), func(b *testing.B) {
			readers := make([]*sstable.Reader, numIters)
			for i := 0; i < numIters; i++ {
				reader, _ := buildBenchIter(b, entryCount, i*entryCount, valueSize)
				readers[i] = reader
			}
			defer func() {
				for _, r := range readers {
					r.Close()
				}
			}()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iters := make([]sstable.Iterator, numIters)
				for j := 0; j < numIters; j++ {
					iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
					iters[j] = iter
				}

				merge := newMergeIterator(iters)
				count := 0
				for merge.Next() {
					count++
				}
				merge.close()

				if count != numIters*entryCount {
					b.Fatalf("expected %d entries, got %d", numIters*entryCount, count)
				}
			}

			b.ReportMetric(float64(numIters*entryCount), "entries/op")
		})
	}
}

func BenchmarkKMergeIterator_ValueSizes(b *testing.B) {
	valueSizes := []int{10, 100, 1000, 10000}
	numIters := 4
	entriesPerIter := 500

	for _, valueSize := range valueSizes {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			readers := make([]*sstable.Reader, numIters)
			for i := 0; i < numIters; i++ {
				reader, _ := buildBenchIter(b, entriesPerIter, i*entriesPerIter, valueSize)
				readers[i] = reader
			}
			defer func() {
				for _, r := range readers {
					r.Close()
				}
			}()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iters := make([]sstable.Iterator, numIters)
				for j := 0; j < numIters; j++ {
					iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
					iters[j] = iter
				}

				merge := newMergeIterator(iters)
				count := 0
				for merge.Next() {
					_ = merge.value()
					count++
				}
				merge.close()
			}

			b.ReportMetric(float64(valueSize), "bytes/value")
		})
	}
}

func BenchmarkKMergeIterator_WithEntryDecode(b *testing.B) {
	numIters := 4
	entriesPerIter := 1000
	valueSize := 100

	readers := make([]*sstable.Reader, numIters)
	for i := 0; i < numIters; i++ {
		reader, _ := buildBenchIter(b, entriesPerIter, i*entriesPerIter, valueSize)
		readers[i] = reader
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	b.Run("KeyValueOnly", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			iters := make([]sstable.Iterator, numIters)
			for j := 0; j < numIters; j++ {
				iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
				iters[j] = iter
			}

			merge := newMergeIterator(iters)
			for merge.Next() {
				_ = merge.key()
				_ = merge.value()
			}
			merge.close()
		}
	})

	b.Run("WithEntryDecode", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			iters := make([]sstable.Iterator, numIters)
			for j := 0; j < numIters; j++ {
				iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
				iters[j] = iter
			}

			merge := newMergeIterator(iters)
			for merge.Next() {
				_, _ = merge.entry()
			}
			merge.close()
		}
	})
}

func BenchmarkMergeIterator_Comparison(b *testing.B) {
	numItersCases := []int{2, 4, 8, 16}
	entriesPerIter := 1000
	valueSize := 100

	for _, numIters := range numItersCases {

		readers := make([]*sstable.Reader, numIters)
		for i := 0; i < numIters; i++ {
			reader, _ := buildBenchIter(b, entriesPerIter, i*entriesPerIter, valueSize)
			readers[i] = reader
		}

		b.Run(fmt.Sprintf("Heap/iters=%d", numIters), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iters := make([]sstable.Iterator, numIters)
				for j := 0; j < numIters; j++ {
					iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
					iters[j] = iter
				}

				merge := newHeapMergeIterator(iters)
				count := 0
				for merge.Next() {
					_ = merge.key()
					count++
				}
				merge.close()
			}
		})

		b.Run(fmt.Sprintf("Tournament/iters=%d", numIters), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iters := make([]sstable.Iterator, numIters)
				for j := 0; j < numIters; j++ {
					iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
					iters[j] = iter
				}

				merge := newMergeIterator(iters)
				count := 0
				for merge.Next() {
					_ = merge.key()
					count++
				}
				merge.close()
			}
		})

		for _, r := range readers {
			r.Close()
		}
	}
}

func BenchmarkKMergeIterator_HeapOperations(b *testing.B) {
	numItersCases := []int{2, 4, 8, 16, 32}

	for _, numIters := range numItersCases {
		b.Run(fmt.Sprintf("iters=%d", numIters), func(b *testing.B) {

			readers := make([]*sstable.Reader, numIters)
			for i := 0; i < numIters; i++ {

				reader, _ := buildBenchIter(b, 100, i*100, 50)
				readers[i] = reader
			}
			defer func() {
				for _, r := range readers {
					r.Close()
				}
			}()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iters := make([]sstable.Iterator, numIters)
				for j := 0; j < numIters; j++ {
					iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
					iters[j] = iter
				}

				merge := newMergeIterator(iters)
				for merge.Next() {
				}
				merge.close()
			}

			b.ReportMetric(float64(numIters), "heap_size")
		})
	}
}
