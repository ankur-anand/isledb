package isledb

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func buildTestIter(t *testing.T, entries []internal.MemEntry) (*sstable.Reader, sstable.Iterator) {
	t.Helper()

	it := &sliceSSTIter{entries: entries}
	res, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("WriteSST: %v", err)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(res.SSTData), sstable.ReaderOptions{})
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
	res, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "snappy"}, 1)
	if err != nil {
		b.Fatalf("WriteSST: %v", err)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(res.SSTData), sstable.ReaderOptions{})
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
	res, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "snappy"}, 1)
	if err != nil {
		b.Fatalf("WriteSST: %v", err)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(res.SSTData), sstable.ReaderOptions{})
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

type binaryMergeIter struct {
	iters   []sstable.Iterator
	tree    []int
	current int
	lastKey []byte
	keys    [][]byte
	vals    [][]byte
	valid   []bool
}

func newBinaryMergeIter(iters []sstable.Iterator) *binaryMergeIter {
	n := len(iters)
	if n == 0 {
		return &binaryMergeIter{current: -1}
	}

	size := 1
	for size < n {
		size *= 2
	}

	bmi := &binaryMergeIter{
		iters:   iters,
		tree:    make([]int, size*2),
		keys:    make([][]byte, n),
		vals:    make([][]byte, n),
		valid:   make([]bool, n),
		current: -1,
	}

	for i := range bmi.tree {
		bmi.tree[i] = -1
	}

	for i := 0; i < n; i++ {
		kv := iters[i].First()
		if kv != nil {
			bmi.keys[i] = append([]byte(nil), kv.K.UserKey...)
			bmi.vals[i] = kv.InPlaceValue()
			bmi.valid[i] = true
			bmi.tree[size+i] = i
		}
	}

	for i := size - 1; i >= 1; i-- {
		bmi.tree[i] = bmi.winner(bmi.tree[i*2], bmi.tree[i*2+1])
	}

	return bmi
}

func (bmi *binaryMergeIter) winner(a, b int) int {
	if a == -1 {
		return b
	}
	if b == -1 {
		return a
	}
	cmp := bytes.Compare(bmi.keys[a], bmi.keys[b])
	if cmp <= 0 {
		return a
	}
	return b
}

func (bmi *binaryMergeIter) Next() bool {
	for {

		bmi.current = bmi.tree[1]
		if bmi.current == -1 {
			return false
		}

		currentKey := bmi.keys[bmi.current]

		kv := bmi.iters[bmi.current].Next()
		if kv != nil {
			bmi.keys[bmi.current] = append(bmi.keys[bmi.current][:0], kv.K.UserKey...)
			bmi.vals[bmi.current] = kv.InPlaceValue()
		} else {
			bmi.valid[bmi.current] = false
			bmi.keys[bmi.current] = nil
		}

		n := len(bmi.iters)
		size := 1
		for size < n {
			size *= 2
		}

		pos := size + bmi.current
		if !bmi.valid[bmi.current] {
			bmi.tree[pos] = -1
		}

		for pos > 1 {
			parent := pos / 2
			sibling := pos ^ 1
			bmi.tree[parent] = bmi.winner(bmi.tree[pos], bmi.tree[sibling])
			pos = parent
		}

		if bmi.lastKey != nil && bytes.Equal(currentKey, bmi.lastKey) {
			continue
		}

		bmi.lastKey = append(bmi.lastKey[:0], currentKey...)
		return true
	}
}

func (bmi *binaryMergeIter) Key() []byte {
	if bmi.current == -1 {
		return nil
	}
	return bmi.lastKey
}

func (bmi *binaryMergeIter) Value() []byte {
	if bmi.current == -1 {
		return nil
	}
	return bmi.vals[bmi.current]
}

func (bmi *binaryMergeIter) Close() error {
	var firstErr error
	for _, iter := range bmi.iters {
		if err := iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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

				merge := newMergeIterator(iters)
				count := 0
				for merge.Next() {
					_ = merge.key()
					count++
				}
				merge.close()
			}
		})

		b.Run(fmt.Sprintf("Binary/iters=%d", numIters), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iters := make([]sstable.Iterator, numIters)
				for j := 0; j < numIters; j++ {
					iter, _ := readers[j].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
					iters[j] = iter
				}

				merge := newBinaryMergeIter(iters)
				count := 0
				for merge.Next() {
					_ = merge.Key()
					count++
				}
				merge.Close()
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
