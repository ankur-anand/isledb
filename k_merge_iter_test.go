package isledb

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2/sstable"
)

func buildTestIter(t *testing.T, entries []MemEntry) (*sstable.Reader, sstable.Iterator) {
	t.Helper()

	it := &sliceSSTIter{entries: entries}
	res, err := WriteSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none"}, 1)
	if err != nil {
		t.Fatalf("WriteSST: %v", err)
	}

	reader, err := sstable.NewReader(context.Background(), newMemReadable(res.SSTData), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		_ = reader.Close()
		t.Fatalf("NewIter: %v", err)
	}
	return reader, iter
}

func TestKMergeIterator_PrefersHigherSeq(t *testing.T) {
	readerOld, iterOld := buildTestIter(t, []MemEntry{
		{Key: []byte("k"), Seq: 1, Kind: OpPut, Inline: true, Value: []byte("old")},
	})
	defer readerOld.Close()

	readerNew, iterNew := buildTestIter(t, []MemEntry{
		{Key: []byte("k"), Seq: 5, Kind: OpPut, Inline: true, Value: []byte("new")},
	})
	defer readerNew.Close()

	merge := NewMergeIterator([]sstable.Iterator{iterOld, iterNew})
	defer merge.Close()

	if !merge.Next() {
		t.Fatalf("expected entry")
	}
	entry, err := merge.Entry()
	if err != nil {
		t.Fatalf("Entry: %v", err)
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
	readerSet, iterSet := buildTestIter(t, []MemEntry{
		{Key: []byte("k"), Seq: seq, Kind: OpPut, Inline: true, Value: []byte("set")},
	})
	defer readerSet.Close()

	readerDel, iterDel := buildTestIter(t, []MemEntry{
		{Key: []byte("k"), Seq: seq, Kind: OpDelete},
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

	merge := NewMergeIterator([]sstable.Iterator{iterSet, iterDel})
	defer merge.Close()

	if !merge.Next() {
		t.Fatalf("expected entry")
	}
	entry, err := merge.Entry()
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}

	if trailerSet > trailerDel {
		if entry.Kind != OpPut || !bytes.Equal(entry.Value, []byte("set")) {
			t.Fatalf("expected set to win, got kind=%v value=%q", entry.Kind, entry.Value)
		}
	} else {
		if entry.Kind != OpDelete {
			t.Fatalf("expected delete to win, got kind=%v", entry.Kind)
		}
	}
}
