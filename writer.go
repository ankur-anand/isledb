package isledb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

type Writer struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        WriterOptions
	valueConfig ValueStorageConfig

	mu                      sync.Mutex
	memtable                *Memtable
	memtableInlineThreshold int
	seq                     uint64
	epoch                   uint64
	blobStorage             *BlobStorage

	flushMu     sync.Mutex
	flushTicker *time.Ticker
	stopCh      chan struct{}
	wg          sync.WaitGroup

	fenced     atomic.Bool
	fenceToken *manifest.FenceToken

	closed atomic.Bool
}

func NewWriter(ctx context.Context, store *blobstore.Store, opts WriterOptions) (*Writer, error) {

	defaults := DefaultWriterOptions()
	if opts.MemtableSize == 0 {
		opts.MemtableSize = defaults.MemtableSize
	}
	if opts.FlushInterval == 0 {
		opts.FlushInterval = defaults.FlushInterval
	}
	if opts.BloomBitsPerKey == 0 {
		opts.BloomBitsPerKey = defaults.BloomBitsPerKey
	}
	if opts.BlockSize == 0 {
		opts.BlockSize = defaults.BlockSize
	}
	if opts.Compression == "" {
		opts.Compression = defaults.Compression
	}

	valueConfig := opts.ValueStorage
	if valueConfig.BlobThreshold == 0 {
		valueConfig = DefaultValueStorageConfig()
	}
	if valueConfig.MaxKeySize == 0 {
		valueConfig.MaxKeySize = 64 * 1024
	}
	if valueConfig.MaxValueSize == 0 {
		valueConfig.MaxValueSize = 256 * 1024 * 1024
	}

	manifestLog := manifest.NewStore(store)
	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	memtableInlineThreshold := valueConfig.BlobThreshold

	w := &Writer{
		store:                   store,
		manifestLog:             manifestLog,
		opts:                    opts,
		valueConfig:             valueConfig,
		memtable:                NewMemtable(opts.MemtableSize*2, memtableInlineThreshold),
		memtableInlineThreshold: memtableInlineThreshold,
		seq:                     m.MaxSeqNum(),
		epoch:                   m.NextEpoch,
		blobStorage:             NewBlobStorage(store, valueConfig),
		stopCh:                  make(chan struct{}),
	}

	if opts.EnableFencing {
		ownerID := opts.OwnerID
		if ownerID == "" {
			ownerID = fmt.Sprintf("writer-%d-%d", time.Now().UnixNano(), m.NextEpoch)
		}
		token, err := manifestLog.ClaimWriter(ctx, ownerID)
		if err != nil {
			return nil, fmt.Errorf("claim writer fence: %w", err)
		}
		w.fenceToken = token
	}

	if opts.FlushInterval > 0 {
		w.flushTicker = time.NewTicker(opts.FlushInterval)
		w.wg.Add(1)
		go w.flushLoop()
	}

	return w, nil
}

func (w *Writer) Put(key, value []byte) error {
	return w.PutWithTTL(key, value, 0)
}

func (w *Writer) PutWithTTL(key, value []byte, ttl time.Duration) error {
	if w.closed.Load() {
		return errors.New("writer closed")
	}
	if w.fenced.Load() {
		return manifest.ErrFenced
	}

	if len(key) == 0 {
		return errors.New("empty key")
	}
	if len(key) > w.valueConfig.MaxKeySize {
		return fmt.Errorf("key size %d exceeds max %d", len(key), w.valueConfig.MaxKeySize)
	}
	if int64(len(value)) > w.valueConfig.MaxValueSize {
		return fmt.Errorf("value size %d exceeds max %d", len(value), w.valueConfig.MaxValueSize)
	}

	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixMilli()
	}

	if len(value) >= w.valueConfig.BlobThreshold {
		return w.putBlob(key, value, expireAt)
	}
	return w.putInline(key, value, expireAt)
}

func (w *Writer) putInline(key, value []byte, expireAt int64) error {
	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.memtable.PutWithTTL(key, value, seq, expireAt)
	size := w.memtable.TotalSize()
	w.mu.Unlock()

	if size >= w.opts.MemtableSize {
		return w.Flush(context.Background())
	}
	return nil
}

func (w *Writer) putBlob(key, value []byte, expireAt int64) error {
	ctx := context.Background()

	blobID, err := w.blobStorage.Write(ctx, value)
	if err != nil {
		return fmt.Errorf("write blob: %w", err)
	}

	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.memtable.PutBlobRefWithTTL(key, blobID, seq, expireAt)
	size := w.memtable.TotalSize()
	w.mu.Unlock()

	if size >= w.opts.MemtableSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *Writer) Delete(key []byte) error {
	return w.DeleteWithTTL(key, 0)
}

func (w *Writer) DeleteWithTTL(key []byte, ttl time.Duration) error {
	if w.closed.Load() {
		return errors.New("writer closed")
	}
	if w.fenced.Load() {
		return manifest.ErrFenced
	}

	if len(key) == 0 {
		return errors.New("empty key")
	}
	if len(key) > w.valueConfig.MaxKeySize {
		return fmt.Errorf("key size %d exceeds max %d", len(key), w.valueConfig.MaxKeySize)
	}

	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixMilli()
	}

	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.memtable.DeleteWithTTL(key, seq, expireAt)
	w.mu.Unlock()

	return nil
}

func (w *Writer) Flush(ctx context.Context) error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if w.fenced.Load() {
		return manifest.ErrFenced
	}

	w.mu.Lock()
	if w.memtable.ApproxSize() == 0 {
		w.mu.Unlock()
		return nil
	}

	oldMemtable := w.memtable
	w.memtable = NewMemtable(w.opts.MemtableSize*2, w.memtableInlineThreshold)
	epoch := w.epoch
	w.epoch++
	w.mu.Unlock()

	sstOpts := SSTWriterOptions{
		BloomBitsPerKey: w.opts.BloomBitsPerKey,
		BlockSize:       w.opts.BlockSize,
		Compression:     w.opts.Compression,
	}

	result, err := WriteSST(ctx, oldMemtable.Iterator(), sstOpts, epoch)
	if err != nil {
		if errors.Is(err, ErrEmptyIterator) {
			return nil
		}
		return fmt.Errorf("build sst: %w", err)
	}

	sstPath := w.store.SSTPath(result.Meta.ID)
	if _, err := w.store.Write(ctx, sstPath, result.SSTData); err != nil {
		return fmt.Errorf("upload sst: %w", err)
	}

	var appendErr error
	if w.opts.EnableFencing {
		_, appendErr = w.manifestLog.AppendAddSSTableWithFence(ctx, result.Meta)
	} else {
		_, appendErr = w.manifestLog.AppendAddSSTable(ctx, result.Meta)
	}

	if appendErr != nil {
		if errors.Is(appendErr, manifest.ErrFenced) {
			w.fenced.Store(true)
		}
		return fmt.Errorf("update manifest: %w", appendErr)
	}

	return nil
}

func (w *Writer) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.flushTicker.C:
			if err := w.Flush(context.Background()); err != nil {

				if errors.Is(err, manifest.ErrFenced) {
					slog.Error("isledb: writer fenced, stopping background flush")
					return
				}
				if w.opts.OnFlushError != nil {
					w.opts.OnFlushError(err)
				} else {
					slog.Error("isledb: background flush failed", "error", err)
				}
			}
		case <-w.stopCh:
			return
		}
	}
}

func (w *Writer) IsFenced() bool {
	return w.fenced.Load()
}

func (w *Writer) FenceToken() *manifest.FenceToken {
	return w.fenceToken
}

func (w *Writer) Close() error {

	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(w.stopCh)
	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}
	w.wg.Wait()

	return w.Flush(context.Background())
}

func (w *Writer) Seq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}
