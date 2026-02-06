package isledb

import (
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

		if err := tr.Refresh(ctx); err != nil {
			if tr.opts.OnRefreshError != nil {
				tr.opts.OnRefreshError(err)
			}

		}

		minKey := opts.MinKey
		if lastKey != nil {

			minKey = incrementKey(lastKey)
		}

		iter, err := tr.reader.NewIterator(ctx, IteratorOptions{
			MinKey: minKey,
			MaxKey: opts.MaxKey,
		})
		if err != nil {
			return err
		}

		for iter.Next() {
			kv := KV{
				Key:   iter.Key(),
				Value: iter.Value(),
			}

			if err := handler(kv); err != nil {
				iter.Close()
				return err
			}

			lastKey = append(lastKey[:0], kv.Key...)
		}

		if err := iter.Err(); err != nil {
			iter.Close()
			return err
		}
		iter.Close()

		timer.Stop()
		timer.Reset(opts.PollInterval)

		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
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
