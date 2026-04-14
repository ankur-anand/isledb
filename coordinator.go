package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ankur-anand/isledb/manifest"
)

var (
	ErrCoordinatorClosed = errors.New("coordinator closed")
	ErrViewClosed        = errors.New("view closed")
)

// Version is an opaque identifier for one immutable visible manifest state.
type Version struct {
	value string
}

func (v Version) String() string {
	return v.value
}

func (v Version) IsZero() bool {
	return v.value == ""
}

// View is an immutable read handle over one loaded manifest snapshot.
type View interface {
	Version() Version
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
	NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error)
	ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error)
	MaxCommittedLSN() (uint64, bool)
	CatchUp(ctx context.Context, opts CatchUpOptions, handler func(KV) error) (CatchUpResult, error)
	Close() error
}

// Coordinator owns refresh and publishes immutable views over loaded manifest state.
type Coordinator struct {
	reader *Reader

	mu      sync.Mutex
	cond    *sync.Cond
	current *viewState
	refs    int
	closing bool
	closed  bool
}

type viewState struct {
	manifest        *Manifest
	version         Version
	maxCommittedLSN uint64
	hasCommittedLSN bool
}

type coordinatorView struct {
	coord  *Coordinator
	state  *viewState
	closed atomic.Bool
}

func newCoordinator(reader *Reader) *Coordinator {
	coord := &Coordinator{reader: reader}
	coord.cond = sync.NewCond(&coord.mu)
	return coord
}

func (c *Coordinator) initCurrentView() error {
	state := newViewState(c.reader.manifestSnapshot(), c.reader.manifestStore.CurrentData())

	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = state
	return nil
}

// Refresh reloads storage state when CURRENT advanced and returns the current
// immutable view after the refresh attempt.
func (c *Coordinator) Refresh(ctx context.Context) (View, bool, error) {
	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		return nil, false, ErrCoordinatorClosed
	}
	currentState := c.current
	c.mu.Unlock()

	current, err := c.reader.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		return nil, false, err
	}
	targetVersion := versionFromCurrent(current)
	if currentState != nil && currentState.version == targetVersion {
		return c.acquireView(currentState), false, nil
	}

	if err := c.reader.Refresh(ctx); err != nil {
		return nil, false, err
	}

	state := newViewState(c.reader.manifestSnapshot(), c.reader.manifestStore.CurrentData())

	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		return nil, false, ErrCoordinatorClosed
	}
	c.current = state
	c.refs++
	c.mu.Unlock()

	return &coordinatorView{coord: c, state: state}, true, nil
}

// Current returns the most recently published immutable view without
// performing I/O. It returns nil after the coordinator is closed.
func (c *Coordinator) Current() View {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed || c.current == nil {
		return nil
	}
	c.refs++
	return &coordinatorView{coord: c, state: c.current}
}

// Close closes the shared reader runtime after all outstanding views have
// been released.
func (c *Coordinator) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closing = true
	for c.refs > 0 {
		c.cond.Wait()
	}
	c.closed = true
	c.mu.Unlock()

	return c.reader.Close()
}

func (c *Coordinator) acquireView(state *viewState) View {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed || state == nil {
		return nil
	}
	c.refs++
	return &coordinatorView{coord: c, state: state}
}

func (c *Coordinator) releaseView() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refs > 0 {
		c.refs--
	}
	if c.closing && c.refs == 0 {
		c.cond.Broadcast()
	}
}

func (v *coordinatorView) Version() Version {
	if v == nil || v.state == nil {
		return Version{}
	}
	return v.state.version
}

func (v *coordinatorView) MaxCommittedLSN() (uint64, bool) {
	if v == nil || v.state == nil {
		return 0, false
	}
	return v.state.maxCommittedLSN, v.state.hasCommittedLSN
}

func (v *coordinatorView) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if err := v.ensureOpen(); err != nil {
		return nil, false, err
	}
	return v.coord.reader.getWithManifest(ctx, v.state.manifest, key)
}

func (v *coordinatorView) NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error) {
	if err := v.ensureOpen(); err != nil {
		return nil, err
	}
	return v.coord.reader.newIteratorWithManifest(ctx, v.state.manifest, opts)
}

func (v *coordinatorView) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error) {
	if err := v.ensureOpen(); err != nil {
		return nil, err
	}
	return v.coord.reader.scanInternalWithManifest(ctx, v.state.manifest, minKey, maxKey, limit)
}

func (v *coordinatorView) CatchUp(ctx context.Context, opts CatchUpOptions, handler func(KV) error) (CatchUpResult, error) {
	if err := v.ensureOpen(); err != nil {
		return CatchUpResult{}, err
	}
	return catchUpWithManifest(ctx, v.coord.reader, v.state.manifest, opts, handler)
}

func (v *coordinatorView) Close() error {
	if v == nil {
		return nil
	}
	if v.closed.CompareAndSwap(false, true) {
		v.coord.releaseView()
	}
	return nil
}

func (v *coordinatorView) ensureOpen() error {
	if v == nil || v.state == nil {
		return ErrViewClosed
	}
	if v.closed.Load() {
		return ErrViewClosed
	}
	return nil
}

func newViewState(m *Manifest, current *manifest.Current) *viewState {
	maxCommittedLSN, hasCommittedLSN := currentCommittedLSN(current)
	return &viewState{
		manifest:        m,
		version:         versionFromCurrent(current),
		maxCommittedLSN: maxCommittedLSN,
		hasCommittedLSN: hasCommittedLSN,
	}
}

func currentCommittedLSN(current *manifest.Current) (uint64, bool) {
	if current == nil || current.MaxCommittedLSN == nil {
		return 0, false
	}
	return *current.MaxCommittedLSN, true
}

func versionFromCurrent(current *manifest.Current) Version {
	if current == nil {
		return Version{}
	}

	maxCommittedLSN, hasCommittedLSN := currentCommittedLSN(current)
	return Version{
		value: fmt.Sprintf("%s:%d:%d:%t:%d",
			current.Snapshot,
			current.LogSeqStart,
			current.NextSeq,
			hasCommittedLSN,
			maxCommittedLSN,
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
