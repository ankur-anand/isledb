package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ankur-anand/isledb/manifest"
)

var ErrSnapshotClosed = errors.New("snapshot closed")

// Version is an opaque identifier for one loaded visible state.
type Version struct {
	value string
}

func (v Version) String() string {
	return v.value
}

func (v Version) IsZero() bool {
	return v.value == ""
}

// Snapshot is an immutable read handle over one loaded manifest state.
//
// A Snapshot does not refresh. It keeps reading the same visible state even if
// its parent Reader is refreshed later.
type Snapshot struct {
	reader          *Reader
	manifest        *Manifest
	version         Version
	maxCommittedLSN uint64
	hasCommittedLSN bool
	lowWatermarkLSN uint64
	hasLowWatermark bool
	closed          atomic.Bool
}

func newSnapshot(reader *Reader, m *Manifest, current *manifest.Current) *Snapshot {
	maxCommittedLSN, hasCommittedLSN := currentCommittedLSN(current)
	lowWatermarkLSN, hasLowWatermark := currentLowWatermarkLSN(current)
	return &Snapshot{
		reader:          reader,
		manifest:        m,
		version:         versionFromCurrent(current),
		maxCommittedLSN: maxCommittedLSN,
		hasCommittedLSN: hasCommittedLSN,
		lowWatermarkLSN: lowWatermarkLSN,
		hasLowWatermark: hasLowWatermark,
	}
}

func (s *Snapshot) Version() Version {
	if s == nil {
		return Version{}
	}
	return s.version
}

func (s *Snapshot) MaxCommittedLSN() (uint64, bool) {
	if s == nil {
		return 0, false
	}
	return s.maxCommittedLSN, s.hasCommittedLSN
}

func (s *Snapshot) LowWatermarkLSN() (uint64, bool) {
	if s == nil {
		return 0, false
	}
	return s.lowWatermarkLSN, s.hasLowWatermark
}

func (s *Snapshot) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, false, err
	}
	return s.reader.getWithManifest(ctx, s.manifest, key)
}

func (s *Snapshot) NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	return s.reader.newIteratorWithManifest(ctx, s.manifest, opts)
}

func (s *Snapshot) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	return s.reader.scanInternalWithManifest(ctx, s.manifest, minKey, maxKey, limit)
}

func (s *Snapshot) CatchUp(ctx context.Context, opts CatchUpOptions, handler func(KV) error) (CatchUpResult, error) {
	if err := s.ensureOpen(); err != nil {
		return CatchUpResult{}, err
	}
	return catchUpWithManifest(ctx, s.reader, s.manifest, opts, handler)
}

func (s *Snapshot) Close() error {
	if s == nil {
		return nil
	}
	s.closed.Store(true)
	return nil
}

func (s *Snapshot) ensureOpen() error {
	if s == nil || s.reader == nil || s.manifest == nil {
		return ErrSnapshotClosed
	}
	if s.closed.Load() {
		return ErrSnapshotClosed
	}
	return nil
}

func currentCommittedLSN(current *manifest.Current) (uint64, bool) {
	if current == nil || current.MaxCommittedLSN == nil {
		return 0, false
	}
	return *current.MaxCommittedLSN, true
}

func currentLowWatermarkLSN(current *manifest.Current) (uint64, bool) {
	if current == nil || current.LowWatermarkLSN == nil {
		return 0, false
	}
	return *current.LowWatermarkLSN, true
}

func versionFromCurrent(current *manifest.Current) Version {
	if current == nil {
		return Version{}
	}

	maxCommittedLSN, hasCommittedLSN := currentCommittedLSN(current)
	lowWatermarkLSN, hasLowWatermark := currentLowWatermarkLSN(current)
	return Version{
		value: fmt.Sprintf("%s:%d:%d:%t:%d:%t:%d",
			current.Snapshot,
			current.LogSeqStart,
			current.NextSeq,
			hasCommittedLSN,
			maxCommittedLSN,
			hasLowWatermark,
			lowWatermarkLSN,
		),
	}
}

func catchUpWithManifest(ctx context.Context, reader *Reader, m *Manifest, opts CatchUpOptions, handler func(KV) error) (CatchUpResult, error) {
	iter, err := reader.newIteratorWithManifest(ctx, m, IteratorOptions{
		MinKey: catchUpMinKey(opts.MinKey, opts.StartAfterKey),
		MaxKey: opts.MaxKey,
	})
	if err != nil {
		return CatchUpResult{}, err
	}
	defer iter.Close()

	var result CatchUpResult

	for iter.Next() {
		kv := KV{
			Key:   iter.Key(),
			Value: iter.Value(),
		}

		if err := handler(kv); err != nil {
			return result, err
		}

		result.LastKey = append(result.LastKey[:0], kv.Key...)
		result.Count++

		if opts.Limit > 0 && result.Count >= opts.Limit {
			result.Truncated = true
			return result, nil
		}
	}

	if err := iter.Err(); err != nil {
		return result, err
	}

	return result, nil
}
