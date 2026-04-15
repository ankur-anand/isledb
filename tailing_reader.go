package isledb

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

var ErrTailingReaderStopped = errors.New("tailing reader stopped")

// TailingReader is a read-only handle that refreshes manifests periodically
// and can tail new keys as they appear in object storage.
type TailingReader struct {
	reader *Reader
	opts   TailingReaderOptions

	mu          sync.RWMutex
	lastSeq     uint64
	lastRefresh time.Time

	stopCh chan struct{}
	wg     sync.WaitGroup

	running atomic.Bool
	stopped atomic.Bool
	closed  atomic.Bool
}

func newTailingReader(ctx context.Context, store *blobstore.Store, opts TailingReaderOptions) (*TailingReader, error) {
	if opts.RefreshInterval == 0 {
		opts.RefreshInterval = 100 * time.Millisecond
	}

	reader, err := newReader(ctx, store, opts.ReaderOptions)
	if err != nil {
		return nil, err
	}

	tr := &TailingReader{
		reader: reader,
		opts:   opts,
		stopCh: make(chan struct{}),
	}

	return tr, nil
}

func (tr *TailingReader) Start() error {
	if tr.closed.Load() || tr.stopped.Load() {
		return ErrTailingReaderStopped
	}
	if tr.running.Swap(true) {
		return nil
	}

	tr.wg.Add(1)
	go tr.refreshLoop()
	return nil
}

// Stop terminates the background refresh loop.
func (tr *TailingReader) Stop() {
	if tr.stopped.Swap(true) {
		return
	}

	if tr.running.Swap(false) {
		close(tr.stopCh)
		tr.wg.Wait()
	}
}

// Close stops background refresh and closes the underlying reader.
func (tr *TailingReader) Close() error {
	if !tr.closed.CompareAndSwap(false, true) {
		return nil
	}

	tr.Stop()
	return tr.reader.Close()
}

func (tr *TailingReader) refreshLoop() {
	defer tr.wg.Done()

	ticker := time.NewTicker(tr.opts.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), tr.opts.RefreshInterval*2)
			err := tr.reader.Refresh(ctx)
			cancel()

			if err != nil {
				if tr.opts.OnRefreshError != nil {
					tr.opts.OnRefreshError(err)
				}
				continue
			}

			tr.mu.Lock()
			tr.lastRefresh = time.Now()
			tr.mu.Unlock()

			if tr.opts.OnRefresh != nil {
				tr.opts.OnRefresh()
			}

		case <-tr.stopCh:
			return
		}
	}
}

func (tr *TailingReader) Refresh(ctx context.Context) error {
	err := tr.reader.Refresh(ctx)
	if err == nil {
		tr.mu.Lock()
		tr.lastRefresh = time.Now()
		tr.mu.Unlock()
	}
	return err
}

func (tr *TailingReader) LastRefresh() time.Time {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return tr.lastRefresh
}

func (tr *TailingReader) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	return tr.reader.Get(ctx, key)
}

// MaxCommittedLSN returns the latest committed application LSN recorded in CURRENT.
// See Reader.MaxCommittedLSN for usage constraints.
func (tr *TailingReader) MaxCommittedLSN(ctx context.Context) (uint64, bool, error) {
	return tr.reader.MaxCommittedLSN(ctx)
}

// LowWatermarkLSN returns the earliest still-visible application LSN recorded
// in CURRENT. See Reader.LowWatermarkLSN for usage constraints.
func (tr *TailingReader) LowWatermarkLSN(ctx context.Context) (uint64, bool, error) {
	return tr.reader.LowWatermarkLSN(ctx)
}

// Scan returns all key-value pairs in the given range.
func (tr *TailingReader) Scan(ctx context.Context, minKey, maxKey []byte) ([]KV, error) {
	return tr.reader.Scan(ctx, minKey, maxKey)
}

// ScanLimit returns up to limit key-value pairs in the given range.
func (tr *TailingReader) ScanLimit(ctx context.Context, minKey, maxKey []byte, limit int) ([]KV, error) {
	return tr.reader.ScanLimit(ctx, minKey, maxKey, limit)
}

// NewIterator returns an iterator over the requested key range.
func (tr *TailingReader) NewIterator(ctx context.Context, opts IteratorOptions) (*Iterator, error) {
	return tr.reader.NewIterator(ctx, opts)
}

// Manifest returns a snapshot of the current manifest.
func (tr *TailingReader) Manifest() *Manifest {
	return tr.reader.Manifest()
}

// Reader exposes the underlying Reader.
func (tr *TailingReader) Reader() *Reader {
	return tr.reader
}

// CatchUp refreshes the manifest once, emits the currently visible records in
// the requested range, and then returns progress suitable for checkpointing.
func (tr *TailingReader) CatchUp(ctx context.Context, opts CatchUpOptions, handler func(KV) error) (CatchUpResult, error) {
	if err := tr.Refresh(ctx); err != nil {
		return CatchUpResult{}, err
	}
	return tr.catchUpNoRefresh(ctx, opts, handler)
}

// CatchUpCurrent emits the currently visible records from the reader's current
// manifest snapshot without performing a refresh first.
func (tr *TailingReader) CatchUpCurrent(ctx context.Context, opts CatchUpOptions, handler func(KV) error) (CatchUpResult, error) {
	return tr.catchUpNoRefresh(ctx, opts, handler)
}

// Tail continuously scans for new keys and calls handler for each result.
func (tr *TailingReader) Tail(ctx context.Context, opts TailOptions, handler func(KV) error) error {
	if opts.PollInterval == 0 {
		opts.PollInterval = tr.opts.RefreshInterval
	}

	var lastKey []byte
	if opts.StartAfterKey != nil {
		lastKey = opts.StartAfterKey
	}

	timer := time.NewTimer(opts.PollInterval)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		if !tr.running.Load() {
			if err := tr.Refresh(ctx); err != nil {
				if tr.opts.OnRefreshError != nil {
					tr.opts.OnRefreshError(err)
				}
			}
		}

		catchUpOpts := CatchUpOptions{
			MaxKey: opts.MaxKey,
		}
		if len(lastKey) > 0 {
			catchUpOpts.StartAfterKey = lastKey
		} else {
			catchUpOpts.MinKey = opts.MinKey
		}

		result, err := tr.catchUpNoRefresh(ctx, catchUpOpts, handler)
		if err != nil {
			return err
		}
		if len(result.LastKey) > 0 {
			lastKey = append(lastKey[:0], result.LastKey...)
		}

		if err := waitForTailPoll(ctx, timer, opts.PollInterval); err != nil {
			return err
		}
	}
}

// TailChannel returns a channel of KV updates and an error channel.
func (tr *TailingReader) TailChannel(ctx context.Context, opts TailOptions) (<-chan KV, <-chan error) {
	ch := make(chan KV, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)

		err := tr.Tail(ctx, opts, func(kv KV) error {
			select {
			case ch <- kv:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		if err != nil {
			errCh <- err
		}
	}()

	return ch, errCh
}

func (tr *TailingReader) catchUpNoRefresh(ctx context.Context, opts CatchUpOptions, handler func(KV) error) (CatchUpResult, error) {
	return catchUpWithManifest(ctx, tr.reader, tr.reader.currentManifest(), opts, handler)
}

func catchUpMinKey(minKey, startAfterKey []byte) []byte {
	if len(startAfterKey) == 0 {
		return minKey
	}

	nextKey := incrementKey(startAfterKey)
	if len(minKey) == 0 || bytes.Compare(nextKey, minKey) > 0 {
		return nextKey
	}
	return minKey
}

func waitForTailPoll(ctx context.Context, timer *time.Timer, pollInterval time.Duration) error {
	timer.Reset(pollInterval)

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
