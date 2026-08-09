package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/internal/manifest"
)

var ErrSnapshotClosed = errors.New("snapshot closed")
var ErrSnapshotExpired = errors.New("snapshot expired")
var ErrIteratorExpired = errors.New("iterator expired")
var ErrReadViewExpired = errors.New("read view expired")
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
	reader    *Reader
	manifest  *manifestState
	version   Version
	expiresAt time.Time
	closed    atomic.Bool
}

func newSnapshot(reader *Reader, m *manifestState, version Version, expiresAt time.Time) *Snapshot {
	return &Snapshot{
		reader:    reader,
		manifest:  m,
		version:   version,
		expiresAt: expiresAt,
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

	readCtx, cancel := context.WithDeadlineCause(ctx, s.expiresAt, ErrSnapshotExpired)
	defer cancel()
	value, found, err := s.reader.getWithManifest(readCtx, s.manifest, key)
	return value, found, readViewError(readCtx, err)
}

func (s *Snapshot) NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error) {
	done, err := s.beginRead()
	if err != nil {
		return nil, err
	}
	defer done()

	it, err := s.reader.newIteratorWithManifest(ctx, s.manifest, opts, s.expiresAt)
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

	readCtx, cancel := context.WithDeadlineCause(ctx, s.expiresAt, ErrSnapshotExpired)
	defer cancel()
	values, err := s.reader.scanInternalWithManifest(readCtx, s.manifest, minKey, maxKey, limit)
	return values, readViewError(readCtx, err)
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
	if !time.Now().Before(s.expiresAt) {
		return ErrSnapshotExpired
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

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

func readViewError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil &&
		(errors.Is(cause, ErrReadViewExpired) || errors.Is(cause, ErrSnapshotExpired) || errors.Is(cause, ErrIteratorExpired)) {
		return cause
	}
	return err
}

func versionFromCurrent(current *manifest.Current) Version {
	if current == nil {
		return Version{}
	}
	snapshot := ""
	if current.Snapshot != nil {
		snapshot = current.Snapshot.Path + ":" + current.Snapshot.Checksum
	}

	return Version{
		value: fmt.Sprintf("%s:%d:%d",
			snapshot,
			current.LogSeqStart,
			current.NextSeq,
		),
	}
}
