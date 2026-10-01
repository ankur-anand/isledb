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
	// ctx carries the parent read/iterator lifetime into SST opens that happen
	// lazily, after this source is constructed.
	ctx   context.Context
	ssts  []sstMetadata
	lower []byte
	upper []byte

	index    int
	current  *scanSSTSource
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
	if s.closed || s.errValue != nil || len(s.ssts) == 0 {
		return nil, nil
	}
	if len(s.lower) > 0 && bytes.Compare(target, s.lower) < 0 {
		target = s.lower
	}
	if len(s.upper) > 0 && bytes.Compare(target, s.upper) >= 0 {
		_ = s.reset()
		return nil, nil
	}

	index := sort.Search(len(s.ssts), func(i int) bool {
		return bytes.Compare(s.ssts[i].MaxKey, target) >= 0
	})
	if index == len(s.ssts) {
		_ = s.reset()
		return nil, nil
	}

	// Pebble iterators support repositioning in either direction. Keep the
	// current SST open when the target remains inside it; rebuilding the reader
	// and iterator on every seek is unnecessary cache and metadata work.
	if s.current != nil && s.index == index {
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

	if !s.reset() {
		return nil, nil
	}
	s.index = index
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
	current, err := openScanSSTSource(
		s.reader, s.ctx, s.ssts[s.index], s.lower, s.upper, scanCacheFillBytes)
	if err != nil {
		s.errValue = err
		return false
	}
	s.current = current
	return true
}

func (s *levelMergeIteratorSource) reset() bool {
	if s.closed || s.errValue != nil {
		return false
	}
	if !s.closeCurrent() {
		return false
	}
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
	if err != nil && s.errValue == nil {
		s.errValue = err
	}
	s.ssts = nil
	return err
}

// scanCacheFillBytes is how much of each SST a scan, or each seek, reads
// through the block cache, adding the blocks it reads, before it switches to
// reading into buffers of its own. About four 16 KiB blocks: a short read,
// such as a page, a prefix read or a seek, is cached like a lookup and is
// warm when repeated, while a long scan adds at most this much per SST and
// never evicts the blocks lookups reuse.
const scanCacheFillBytes = 64 << 10

// scanSSTSource reads one SST for a scan. Each read, from the start or from
// a seek, begins with an iterator that fills the block cache, so a seek and
// the short read after it are cached as a lookup would be. Once the keys and
// values a read has returned exceed its budget, the source reopens the SST
// with an iterator that reads into its own buffers, positioned just after the
// last entry returned; the next seek starts a new read, filling again. Scans
// only move forward, so a switch is invisible to the merge, which, as with
// any Pebble iterator, does not keep an entry past the next call.
type scanSSTSource struct {
	reader       *Reader
	ctx          context.Context
	meta         sstMetadata
	lower, upper []byte

	// iter is nil once a reopen has failed, with the failure in errValue.
	iter     sstable.Iterator
	errValue error
	private  bool
	// budget is how many more bytes the filling iterator may return before
	// the source switches to its own buffers; each seek resets it to
	// fillBudget. A fillBudget of zero or less keeps the source private.
	budget     int64
	fillBudget int64
	// last is the entry most recently returned, valid until the iterator
	// moves.
	last *sstable.InternalKey
}

func openScanSSTSource(
	reader *Reader, ctx context.Context, meta sstMetadata, lower, upper []byte, budget int64,
) (*scanSSTSource, error) {
	s := &scanSSTSource{
		reader: reader, ctx: ctx, meta: meta, lower: lower, upper: upper,
		budget: budget, fillBudget: budget,
	}
	private := budget <= 0
	_, iter, err := reader.openSSTIterBounded(ctx, meta, lower, upper, private)
	if err != nil {
		return nil, err
	}
	s.iter, s.private = iter, private
	return s, nil
}

func (s *scanSSTSource) first() (*sstable.InternalKey, []byte) {
	if !s.restart() {
		return nil, nil
	}
	if kv := s.iter.First(); kv != nil {
		return s.track(&kv.K, kv.InPlaceValue())
	}
	return s.track(nil, nil)
}

func (s *scanSSTSource) seekGE(target []byte) (*sstable.InternalKey, []byte) {
	if !s.restart() {
		return nil, nil
	}
	// 0 is base.SeekGEFlagNone.
	if kv := s.iter.SeekGE(target, 0); kv != nil {
		return s.track(&kv.K, kv.InPlaceValue())
	}
	return s.track(nil, nil)
}

func (s *scanSSTSource) next() (*sstable.InternalKey, []byte) {
	if s.iter == nil {
		return nil, nil
	}
	if !s.private && s.budget <= 0 && s.last != nil {
		return s.track(s.switchToPrivate())
	}
	if kv := s.iter.Next(); kv != nil {
		return s.track(&kv.K, kv.InPlaceValue())
	}
	return s.track(nil, nil)
}

// restart begins a new read from a seek: the read gets a fresh budget,
// so a seek caches what it reads as a lookup does, and a source that had
// switched to its own buffers reopens filling the cache. It reports false
// once the source has failed.
func (s *scanSSTSource) restart() bool {
	if s.iter == nil {
		return false
	}
	if s.private && s.fillBudget > 0 && !s.reopen(false) {
		return false
	}
	s.budget = s.fillBudget
	return true
}

// switchToPrivate replaces the filling iterator with one reading into its own
// buffers and returns the entry after the last one returned. Internal keys
// order by user key, then newest version first, so it seeks to the last user
// key and skips that key's versions up to and including the last returned.
func (s *scanSSTSource) switchToPrivate() (*sstable.InternalKey, []byte) {
	lastKey := append([]byte(nil), s.last.UserKey...)
	lastTrailer := s.last.Trailer
	if !s.reopen(true) {
		return nil, nil
	}
	kv := s.iter.SeekGE(lastKey, 0)
	for kv != nil && bytes.Equal(kv.K.UserKey, lastKey) && kv.K.Trailer >= lastTrailer {
		kv = s.iter.Next()
	}
	if kv == nil {
		return nil, nil
	}
	return &kv.K, kv.InPlaceValue()
}

// reopen replaces the iterator with an unpositioned one that fills the cache,
// or, if private, reads into its own buffers. On failure the source keeps the
// error and has no iterator.
func (s *scanSSTSource) reopen(private bool) bool {
	s.last = nil
	err := s.iter.Close()
	s.iter = nil
	if err == nil {
		_, s.iter, err = s.reader.openSSTIterBounded(s.ctx, s.meta, s.lower, s.upper, private)
	}
	if err != nil {
		s.errValue = err
		return false
	}
	s.private = private
	return true
}

// track records the entry about to be returned, charging it to the budget
// while the filling iterator is in use.
func (s *scanSSTSource) track(key *sstable.InternalKey, value []byte) (*sstable.InternalKey, []byte) {
	s.last = key
	if key != nil && !s.private {
		s.budget -= int64(len(key.UserKey) + len(value))
	}
	return key, value
}

func (s *scanSSTSource) err() error {
	if s.iter == nil {
		return s.errValue
	}
	return s.iter.Error()
}

func (s *scanSSTSource) close() error {
	if s.iter == nil {
		return nil
	}
	err := s.iter.Close()
	s.iter = nil
	return err
}
