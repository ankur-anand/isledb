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
	maxPosition     uint64
	hasMaxPosition  bool
	lowPosition     uint64
	hasLowWatermark bool
	closed          atomic.Bool
}

func newSnapshot(reader *Reader, m *Manifest, current *manifest.Current) *Snapshot {
	maxPosition, hasMaxPosition := currentMaxPosition(current)
	lowPosition, hasLowWatermark := currentLowWatermarkPosition(current)
	return &Snapshot{
		reader:          reader,
		manifest:        m,
		version:         versionFromCurrent(current),
		maxPosition:     maxPosition,
		hasMaxPosition:  hasMaxPosition,
		lowPosition:     lowPosition,
		hasLowWatermark: hasLowWatermark,
	}
}

func (s *Snapshot) Version() Version {
	if s == nil {
		return Version{}
	}
	return s.version
}

func (s *Snapshot) MaxCommittedPosition() (uint64, bool) {
	if s == nil {
		return 0, false
	}
	return s.maxPosition, s.hasMaxPosition
}

func (s *Snapshot) LowWatermarkPosition() (uint64, bool) {
	if s == nil {
		return 0, false
	}
	return s.lowPosition, s.hasLowWatermark
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

func currentMaxPosition(current *manifest.Current) (uint64, bool) {
	if current == nil || current.MaxCommittedPosition == nil {
		return 0, false
	}
	return *current.MaxCommittedPosition, true
}

func currentLowWatermarkPosition(current *manifest.Current) (uint64, bool) {
	if current == nil || current.LowWatermarkPosition == nil {
		return 0, false
	}
	return *current.LowWatermarkPosition, true
}

func versionFromCurrent(current *manifest.Current) Version {
	if current == nil {
		return Version{}
	}

	maxPosition, hasMaxPosition := currentMaxPosition(current)
	lowPosition, hasLowWatermark := currentLowWatermarkPosition(current)
	return Version{
		value: fmt.Sprintf("%s:%d:%d:%t:%d:%t:%d",
			current.Snapshot,
			current.LogSeqStart,
			current.NextSeq,
			hasMaxPosition,
			maxPosition,
			hasLowWatermark,
			lowPosition,
		),
	}
}
