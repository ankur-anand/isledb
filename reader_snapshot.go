package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ankur-anand/isledb/manifest"
)

var ErrSnapshotClosed = errors.New("snapshot closed")
var ErrReaderClosed = errors.New("reader closed")

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
	reader   *Reader
	manifest *Manifest
	version  Version
	closed   atomic.Bool
}

func newSnapshot(reader *Reader, m *Manifest, current *manifest.Current) *Snapshot {
	return &Snapshot{
		reader:   reader,
		manifest: m,
		version:  versionFromCurrent(current),
	}
}

func (s *Snapshot) Version() Version {
	if s == nil {
		return Version{}
	}
	return s.version
}

func (s *Snapshot) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	done, err := s.beginRead()
	if err != nil {
		return nil, false, err
	}
	defer done()

	return s.reader.getWithManifest(ctx, s.manifest, key)
}

func (s *Snapshot) NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error) {
	done, err := s.beginRead()
	if err != nil {
		return nil, err
	}
	defer done()

	it, err := s.reader.newIteratorWithManifest(ctx, s.manifest, opts)
	if err != nil {
		return nil, err
	}
	return it, nil
}

func (s *Snapshot) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error) {
	done, err := s.beginRead()
	if err != nil {
		return nil, err
	}
	defer done()

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

func (s *Snapshot) beginRead() (func(), error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	done, err := s.reader.beginRead()
	if err != nil {
		return nil, err
	}
	if err := s.ensureOpen(); err != nil {
		done()
		return nil, err
	}
	return done, nil
}

func versionFromCurrent(current *manifest.Current) Version {
	if current == nil {
		return Version{}
	}

	return Version{
		value: fmt.Sprintf("%s:%d:%d",
			current.Snapshot,
			current.LogSeqStart,
			current.NextSeq,
		),
	}
}
