package isledb

import (
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/pebble/v2/sstable"
)

// levelMergeIteratorSource presents an ordered, non-overlapping SST sequence
// as one merge source. It opens only the SST containing the source's current
// position. Once that SST is exhausted, it closes it and opens the next one.
// L0 ranges may overlap, so each overlapping L0 SST uses its own one-SST source
// rather than concatenating the whole level.
type levelMergeIteratorSource struct {
	reader *Reader
	ctx    context.Context
	ssts   []sstMetadata
	lower  []byte
	upper  []byte

	index    int
	current  *sstableMergeIteratorSource
	errValue error
	closed   bool
}

func newLevelMergeIteratorSource(
	reader *Reader,
	ctx context.Context,
	ssts []sstMetadata,
	lower, upper []byte,
) *levelMergeIteratorSource {
	return newLevelMergeIteratorSourceWithMetadata(reader, ctx,
		append([]sstMetadata(nil), ssts...), lower, upper)
}

func newBorrowedLevelMergeIteratorSource(
	reader *Reader,
	ctx context.Context,
	ssts []sstMetadata,
	lower, upper []byte,
) *levelMergeIteratorSource {
	return newLevelMergeIteratorSourceWithMetadata(reader, ctx, ssts, lower, upper)
}

func newLevelMergeIteratorSourceWithMetadata(
	reader *Reader,
	ctx context.Context,
	ssts []sstMetadata,
	lower, upper []byte,
) *levelMergeIteratorSource {
	return &levelMergeIteratorSource{
		reader: reader,
		ctx:    ctx,
		ssts:   ssts,
		lower:  append([]byte(nil), lower...),
		upper:  append([]byte(nil), upper...),
		index:  -1,
	}
}

func (s *levelMergeIteratorSource) first() (*sstable.InternalKey, []byte) {
	if !s.reset() || len(s.ssts) == 0 {
		return nil, nil
	}
	s.index = 0
	return s.openAndFirst()
}

func (s *levelMergeIteratorSource) next() (*sstable.InternalKey, []byte) {
	if s.closed || s.errValue != nil || s.current == nil {
		return nil, nil
	}
	key, value := s.current.next()
	if key != nil {
		return key, value
	}
	if err := s.current.err(); err != nil {
		s.errValue = err
		return nil, nil
	}
	if !s.closeCurrent() {
		return nil, nil
	}
	s.index++
	return s.openAndFirst()
}

func (s *levelMergeIteratorSource) seekGE(target []byte) (*sstable.InternalKey, []byte) {
	if !s.reset() || len(s.ssts) == 0 {
		return nil, nil
	}
	if len(s.lower) > 0 && bytes.Compare(target, s.lower) < 0 {
		target = s.lower
	}
	if len(s.upper) > 0 && bytes.Compare(target, s.upper) >= 0 {
		return nil, nil
	}

	s.index = sort.Search(len(s.ssts), func(i int) bool {
		return bytes.Compare(s.ssts[i].MaxKey, target) >= 0
	})
	if s.index == len(s.ssts) {
		return nil, nil
	}
	if !s.openCurrent() {
		return nil, nil
	}
	key, value := s.current.seekGE(target)
	if key != nil {
		return key, value
	}
	if err := s.current.err(); err != nil {
		s.errValue = err
		return nil, nil
	}
	if !s.closeCurrent() {
		return nil, nil
	}
	s.index++
	return s.openAndFirst()
}

func (s *levelMergeIteratorSource) openAndFirst() (*sstable.InternalKey, []byte) {
	for s.index >= 0 && s.index < len(s.ssts) {
		if !s.openCurrent() {
			return nil, nil
		}
		key, value := s.current.first()
		if key != nil {
			return key, value
		}
		if err := s.current.err(); err != nil {
			s.errValue = err
			return nil, nil
		}
		if !s.closeCurrent() {
			return nil, nil
		}
		s.index++
	}
	return nil, nil
}

func (s *levelMergeIteratorSource) openCurrent() bool {
	if s.closed || s.errValue != nil || s.index < 0 || s.index >= len(s.ssts) {
		return false
	}
	_, iter, err := s.reader.openSSTIterBounded(
		s.ctx, s.ssts[s.index], s.lower, s.upper)
	if err != nil {
		s.errValue = err
		return false
	}
	s.current = &sstableMergeIteratorSource{iter: iter}
	return true
}

func (s *levelMergeIteratorSource) reset() bool {
	if s.closed {
		return false
	}
	if !s.closeCurrent() {
		return false
	}
	s.errValue = nil
	s.index = -1
	return true
}

func (s *levelMergeIteratorSource) closeCurrent() bool {
	if s.current == nil {
		return true
	}
	err := s.current.close()
	s.current = nil
	if err != nil {
		s.errValue = err
		return false
	}
	return true
}

func (s *levelMergeIteratorSource) err() error {
	if s.errValue != nil {
		return s.errValue
	}
	if s.current != nil {
		return s.current.err()
	}
	return nil
}

func (s *levelMergeIteratorSource) close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.current != nil {
		err = s.current.close()
		s.current = nil
	}
	s.ssts = nil
	return err
}
