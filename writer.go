package isledb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/segmentio/ksuid"
)

// WriterOptions configures the writer.
type WriterOptions struct {
	MemtableSize    int64
	FlushInterval   time.Duration
	InlineThreshold int

	BloomBitsPerKey int
	BlockSize       int
	Compression     string

	OnFlushError func(error)
}

// DefaultWriterOptions returns sensible defaults.
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		MemtableSize:    4 * 1024 * 1024,
		FlushInterval:   time.Second,
		InlineThreshold: DefaultInlineThreshold,
		BloomBitsPerKey: 10,
		BlockSize:       4096,
		Compression:     "snappy",
	}
}

// Writer is a writer for a single tenant with WiscKey-style key-value separation.
type Writer struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        WriterOptions

	mu           sync.Mutex
	memtable     *Memtable
	seq          uint64
	epoch        uint64
	streamVLog   *StreamingVLogWriter
	pendingVLogs map[ksuid.KSUID]VLogMeta

	flushMu     sync.Mutex
	flushTicker *time.Ticker
	stopCh      chan struct{}
	wg          sync.WaitGroup

	closed atomic.Bool
}

// NewWriter creates a new writer for a tenant.
func NewWriter(ctx context.Context, store *blobstore.Store, opts WriterOptions) (*Writer, error) {
	if opts.MemtableSize == 0 {
		opts = DefaultWriterOptions()
	}

	manifestLog := manifest.NewStore(store)
	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	var maxSeq uint64
	for _, sst := range m.SSTables {
		if sst.SeqHi > maxSeq {
			maxSeq = sst.SeqHi
		}
	}

	w := &Writer{
		store:        store,
		manifestLog:  manifestLog,
		opts:         opts,
		memtable:     NewMemtable(opts.MemtableSize*2, opts.InlineThreshold),
		seq:          maxSeq,
		epoch:        m.NextEpoch,
		streamVLog:   NewStreamingVLogWriter(store),
		pendingVLogs: make(map[ksuid.KSUID]VLogMeta),
		stopCh:       make(chan struct{}),
	}

	if opts.FlushInterval > 0 {
		w.flushTicker = time.NewTicker(opts.FlushInterval)
		w.wg.Add(1)
		go w.flushLoop()
	}

	return w, nil
}

// Put inserts or updates a key-value pair.
func (w *Writer) Put(key, value []byte) error {
	if w.closed.Load() {
		return errors.New("writer closed")
	}

	if len(key) == 0 {
		return errors.New("empty key")
	}
	if len(key) > MaxKeySize {
		return fmt.Errorf("key size %d exceeds max %d", len(key), MaxKeySize)
	}

	if len(value) > DirectWriteThreshold {
		return w.putLarge(key, value)
	}
	return w.putSmallOrMedium(key, value)
}

func (w *Writer) putSmallOrMedium(key, value []byte) error {
	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.memtable.Put(key, value, seq)
	size := w.memtable.TotalSize()
	w.mu.Unlock()

	if size >= w.opts.MemtableSize {
		return w.Flush(context.Background())
	}
	return nil
}

func (w *Writer) putLarge(key, value []byte) error {

	ptr, err := w.streamVLog.WriteValue(context.Background(), value)
	if err != nil {
		return fmt.Errorf("stream large value to vlog: %w", err)
	}

	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.memtable.PutPointer(key, &ptr, seq)
	w.pendingVLogs[ptr.VLogID] = VLogMeta{
		ID:   ptr.VLogID,
		Size: int64(VLogEntryHeaderSize + len(value)),
	}
	size := w.memtable.TotalSize()
	w.mu.Unlock()

	if size >= w.opts.MemtableSize {
		return w.Flush(context.Background())
	}
	return nil
}

// Delete removes a key.
func (w *Writer) Delete(key []byte) error {
	if w.closed.Load() {
		return errors.New("writer closed")
	}

	if len(key) == 0 {
		return errors.New("empty key")
	}
	if len(key) > MaxKeySize {
		return fmt.Errorf("key size %d exceeds max %d", len(key), MaxKeySize)
	}

	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.memtable.Delete(key, seq)
	w.mu.Unlock()

	return nil
}

// Flush writes the current memtable to object storage.
func (w *Writer) Flush(ctx context.Context) error {
	if w.closed.Load() {
		return errors.New("writer closed")
	}

	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	w.mu.Lock()
	if w.memtable.ApproxSize() == 0 {
		w.mu.Unlock()
		return nil
	}

	oldMemtable := w.memtable
	w.memtable = NewMemtable(w.opts.MemtableSize*2, w.opts.InlineThreshold)
	oldPendingVLogs := w.pendingVLogs
	w.pendingVLogs = make(map[ksuid.KSUID]VLogMeta)
	epoch := w.epoch
	w.epoch++
	w.mu.Unlock()
	w.streamVLog.Reset()

	sstOpts := SSTWriterOptions{
		BloomBitsPerKey: w.opts.BloomBitsPerKey,
		BlockSize:       w.opts.BlockSize,
		Compression:     w.opts.Compression,
	}

	result, err := WriteSST(ctx, oldMemtable.Iterator(), sstOpts, epoch)
	if err != nil {
		if errors.Is(err, ErrEmptyIterator) {
			return nil // Nothing to write
		}
		return fmt.Errorf("build sst: %w", err)
	}

	if len(result.VLogData) > 0 {
		vlogPath := w.store.VLogPath(result.VLogID.String())
		if _, err := w.store.Write(ctx, vlogPath, result.VLogData); err != nil {
			return fmt.Errorf("upload vlog: %w", err)
		}
	}

	sstPath := w.store.SSTPath(result.Meta.ID)
	if _, err := w.store.Write(ctx, sstPath, result.SSTData); err != nil {
		return fmt.Errorf("upload sst: %w", err)
	}

	var addVLogs []VLogMeta
	if len(oldPendingVLogs) > 0 {
		addVLogs = make([]VLogMeta, 0, len(oldPendingVLogs))
		for _, vlog := range oldPendingVLogs {
			vlog.ReferencedBy = []string{result.Meta.ID}
			addVLogs = append(addVLogs, vlog)
		}
	}
	if _, err := w.manifestLog.AppendAddSSTableWithVLogs(ctx, result.Meta, addVLogs); err != nil {
		return fmt.Errorf("update manifest: %w", err)
	}

	return nil
}

// flushLoop runs periodic flushes.
func (w *Writer) flushLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.flushTicker.C:
			if err := w.Flush(context.Background()); err != nil {
				if w.opts.OnFlushError != nil {
					w.opts.OnFlushError(err)
				} else {
					log.Printf("segmentdb: background flush failed: %v", err)
				}
			}
		case <-w.stopCh:
			return
		}
	}
}

// Close flushes any remaining data and closes the writer.
func (w *Writer) Close() error {
	if w.closed.Load() {
		return nil // Already closed
	}

	close(w.stopCh)
	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}
	w.wg.Wait()

	err := w.Flush(context.Background())

	w.closed.Store(true)

	return err
}

func (w *Writer) Seq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}
