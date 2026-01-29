package internal

import (
	"bytes"
	"context"
	"sort"
	
	"github.com/ankur-anand/isledb/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func NewSortedRun(id uint32, ssts []manifest.SSTMeta) *manifest.SortedRun {
	sr := &manifest.SortedRun{
		ID:   id,
		SSTs: make([]manifest.SSTMeta, len(ssts)),
	}
	copy(sr.SSTs, ssts)

	sort.Slice(sr.SSTs, func(i, j int) bool {
		return bytes.Compare(sr.SSTs[i].MinKey, sr.SSTs[j].MinKey) < 0
	})

	return sr
}

type SSTOpener func(ctx context.Context, sst manifest.SSTMeta) (*sstable.Reader, sstable.Iterator, error)

type SSTBoundedOpener func(ctx context.Context, sst manifest.SSTMeta, lower, upper []byte) (*sstable.Reader, sstable.Iterator, error)

type SortedRunIterator struct {
	ctx           context.Context
	run           *manifest.SortedRun
	opener        SSTOpener
	boundedOpener SSTBoundedOpener

	idx     int
	current sstable.Iterator
	reader  *sstable.Reader
	entry   *CompactionEntry
	err     error
	started bool

	readers []*sstable.Reader
}

func NewSortedRunIterator(ctx context.Context, run *manifest.SortedRun, opener SSTOpener) *SortedRunIterator {
	return &SortedRunIterator{
		ctx:     ctx,
		run:     run,
		opener:  opener,
		idx:     -1,
		readers: make([]*sstable.Reader, 0),
	}
}

func NewSortedRunIteratorWithBounds(ctx context.Context, run *manifest.SortedRun, opener SSTOpener, boundedOpener SSTBoundedOpener) *SortedRunIterator {
	return &SortedRunIterator{
		ctx:           ctx,
		run:           run,
		opener:        opener,
		boundedOpener: boundedOpener,
		idx:           -1,
		readers:       make([]*sstable.Reader, 0),
	}
}

func (it *SortedRunIterator) Next() bool {
	if it.err != nil {
		return false
	}

	if !it.started {
		it.started = true
		if err := it.advanceToSST(0); err != nil {
			it.err = err
			return false
		}
		if it.current == nil {
			return false
		}
		return it.advanceWithinSST(true)
	}

	if it.advanceWithinSST(false) {
		return true
	}

	for {
		if err := it.advanceToSST(it.idx + 1); err != nil {
			it.err = err
			return false
		}
		if it.current == nil {
			return false
		}
		if it.advanceWithinSST(true) {
			return true
		}

	}
}

func (it *SortedRunIterator) advanceToSST(idx int) error {

	if it.current != nil {
		_ = it.current.Close()
		it.current = nil
	}

	it.idx = idx
	if idx >= len(it.run.SSTs) {
		return nil
	}

	reader, iter, err := it.opener(it.ctx, it.run.SSTs[idx])
	if err != nil {
		return err
	}

	it.current = iter
	it.reader = reader
	it.readers = append(it.readers, reader)
	return nil
}

func (it *SortedRunIterator) advanceWithinSST(first bool) bool {
	if it.current == nil {
		return false
	}

	var kv = it.current.Next()
	if first {
		kv = it.current.First()
	}

	if kv == nil {
		if err := it.current.Error(); err != nil {
			it.err = err
		}
		return false
	}

	raw := kv.InPlaceValue()

	decoded, err := DecodeKeyEntry(kv.K.UserKey, raw)
	if err != nil {
		it.err = err
		return false
	}

	it.entry = &CompactionEntry{
		Key:    append([]byte(nil), kv.K.UserKey...),
		Seq:    uint64(kv.K.SeqNum()),
		Kind:   decoded.Kind,
		Inline: decoded.Inline,
		Value:  decoded.Value,
		BlobID: decoded.BlobID,
	}

	return true
}

func (it *SortedRunIterator) Seek(key []byte) error {
	if it.err != nil {
		return it.err
	}

	it.started = true

	startIdx := sort.Search(len(it.run.SSTs), func(i int) bool {
		return bytes.Compare(it.run.SSTs[i].MaxKey, key) >= 0
	})

	if startIdx >= len(it.run.SSTs) {

		it.current = nil
		return nil
	}

	if it.boundedOpener != nil {
		if err := it.advanceToSSTBounded(startIdx, key, nil); err != nil {
			return err
		}
	} else {

		if err := it.advanceToSST(startIdx); err != nil {
			return err
		}
	}

	if it.current == nil {
		return nil
	}

	kv := it.current.First()
	if kv != nil {

		if it.boundedOpener == nil {

			for kv != nil && bytes.Compare(kv.K.UserKey, key) < 0 {
				kv = it.current.Next()
			}
		}

		if kv != nil {
			raw := kv.InPlaceValue()
			decoded, err := DecodeKeyEntry(kv.K.UserKey, raw)
			if err != nil {
				return err
			}
			it.entry = &CompactionEntry{
				Key:    append([]byte(nil), kv.K.UserKey...),
				Seq:    uint64(kv.K.SeqNum()),
				Kind:   decoded.Kind,
				Inline: decoded.Inline,
				Value:  decoded.Value,
				BlobID: decoded.BlobID,
			}
			return nil
		}
	}

	if err := it.current.Error(); err != nil {
		return err
	}

	return it.seekNext()
}

func (it *SortedRunIterator) advanceToSSTBounded(idx int, lower, upper []byte) error {

	if it.current != nil {
		_ = it.current.Close()
		it.current = nil
	}

	it.idx = idx
	if idx >= len(it.run.SSTs) {
		return nil
	}

	reader, iter, err := it.boundedOpener(it.ctx, it.run.SSTs[idx], lower, upper)
	if err != nil {
		return err
	}

	it.current = iter
	it.reader = reader
	it.readers = append(it.readers, reader)
	return nil
}

func (it *SortedRunIterator) seekNext() error {
	for {
		if err := it.advanceToSST(it.idx + 1); err != nil {
			return err
		}
		if it.current == nil {
			return nil
		}
		if it.advanceWithinSST(true) {
			return nil
		}
	}
}

func (it *SortedRunIterator) Entry() (*CompactionEntry, error) {
	if it.err != nil {
		return nil, it.err
	}
	return it.entry, nil
}

func (it *SortedRunIterator) Err() error {
	return it.err
}

func (it *SortedRunIterator) Close() error {
	var firstErr error

	if it.current != nil {
		if err := it.current.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		it.current = nil
	}

	for _, reader := range it.readers {
		if reader != nil {
			if err := reader.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	it.readers = nil

	return firstErr
}
