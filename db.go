package isledb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// ErrWriterAlreadyOpen is returned when a DB already owns an active writer.
var ErrWriterAlreadyOpen = errors.New("writer already open")

// ErrReaderAlreadyOpen is returned when a DB already owns an active reader.
var ErrReaderAlreadyOpen = errors.New("reader already open")

// ErrChangeFeedDisabled is returned when a change reader is opened for a
// database whose change feed has not been enabled.
var ErrChangeFeedDisabled = errors.New("change feed disabled")

// Writer provides write access to the database.
//
// A Writer owns one fenced write session for a DB bucket/prefix. It buffers
// writes in memory, flushes full memtables into immutable SST files, and then
// commits those SSTs through the manifest. Only manifest-committed SSTs are
// visible to readers.
//
// Writer uses internal locks to protect its memtable, sequence assignment, and
// background flush loop. Those locks are an implementation guard, not a
// concurrent API contract: concurrent public calls do not have documented
// ordering or Close/Flush semantics. Callers should serialize Put, Delete,
// Flush, and Close for one Writer.
//
// If WriterOptions.Flush.Interval is greater than zero, the Writer also runs a
// background flush loop. An unobserved background flush failure makes the
// Writer terminal. The callback, later mutations, Flush, and Close all observe
// the same ErrWriterFailed value wrapping the original cause.
type Writer struct {
	w           *writer
	releaseOnce sync.Once
	release     func()
}

// Put writes a key-value pair to the active memtable.
//
// Put returns after the mutation is buffered locally. The mutation becomes
// durable and visible to readers only after a successful Flush, background
// flush, or Close.
func (w *Writer) Put(ctx context.Context, key, value []byte) error {
	return w.w.put(ctx, key, value)
}

// PutWithTTL writes a key-value pair with a time-to-live duration.
//
// ttl <= 0 means no expiration. Expired values are filtered by readers.
func (w *Writer) PutWithTTL(ctx context.Context, key, value []byte, ttl time.Duration) error {
	return w.w.putWithTTL(ctx, key, value, ttl)
}

// Delete marks a key as deleted.
//
// Like Put, the tombstone is buffered first and becomes durable and visible
// after a successful Flush, background flush, or Close.
func (w *Writer) Delete(ctx context.Context, key []byte) error {
	return w.w.delete(ctx, key)
}

// Flush synchronously publishes all currently buffered writes.
//
// Flush rotates the active memtable, writes all frozen memtables as SST files,
// commits their manifest entries, and returns only after the flushed data is
// visible to newly refreshed readers.
func (w *Writer) Flush(ctx context.Context) error {
	return w.w.flush(ctx)
}

// Close stops background flushing and synchronously flushes pending writes.
//
// Close returns the first close or flush error it observes. After Close returns,
// the Writer cannot be used again.
func (w *Writer) Close(ctx context.Context) error {
	err := w.w.close(ctx)
	if err == nil || errors.Is(err, manifest.ErrFenced) || errors.Is(err, ErrWriterFailed) {
		w.releaseWriter()
	}
	return err
}

func (w *Writer) closeDB() error {
	err := w.w.closeWithTimeout(30 * time.Second)
	if err == nil || errors.Is(err, manifest.ErrFenced) || errors.Is(err, ErrWriterFailed) {
		w.releaseWriter()
	}
	return err
}

func (w *Writer) releaseWriter() {
	if w == nil || w.release == nil {
		return
	}
	w.releaseOnce.Do(w.release)
}

// DB encapsulates manifest state for one bucket/prefix. It permits one active
// Writer, one active KV Reader, one active Maintenance handle, and any number
// of independent ChangeReaders.
type DB struct {
	store           *blobstore.Store
	closeStore      bool
	manifestStore   *manifest.Store
	gcCursorStorage manifest.GCCursorStorage
	maintenanceWake chan struct{}

	changeFeedRetention *changeFeedRetentionPolicy
	mu                  sync.Mutex
	closers             []dbCloser
	writerOpen          bool
	readerOpen          bool
	maintenanceOpen     bool
	closed              atomic.Bool
}

type dbCloser interface {
	closeDB() error
}

// DBOptions configures a DB instance.
type DBOptions struct {
	// Prefix is the database's root path inside the bucket or container.
	Prefix string

	// EnableChangeFeed emits a durable ordered mutation feed for all future
	// writer commits. Once enabled for a prefix, it remains enabled.
	EnableChangeFeed bool
}

type dbOpenOptions struct {
	manifestStorage     manifest.Storage
	gcCursorStorage     manifest.GCCursorStorage
	changeFeedEnabled   bool
	changeFeedRetention *changeFeedRetentionPolicy
}

func openDB(ctx context.Context, store *blobstore.Store, opts dbOpenOptions) (*DB, error) {
	manifestStore := newManifestStore(store, opts.manifestStorage)
	gcCursorStorage := opts.gcCursorStorage
	if gcCursorStorage == nil {
		gcCursorStorage = newGCCursorStorage(store)
	}

	if _, err := manifestStore.Replay(ctx); err != nil {
		return nil, err
	}
	if opts.changeFeedEnabled {
		if err := manifestStore.EnableChangeFeed(ctx); err != nil {
			return nil, err
		}
	}
	return &DB{
		store:               store,
		manifestStore:       manifestStore,
		gcCursorStorage:     gcCursorStorage,
		maintenanceWake:     make(chan struct{}, 1),
		changeFeedRetention: opts.changeFeedRetention,
	}, nil
}

// OpenWriter opens the DB's single active writer. A new writer can be opened
// after the previous writer closes successfully or becomes fenced.
func (db *DB) OpenWriter(ctx context.Context, opts WriterOptions) (*Writer, error) {
	if err := db.reserveWriter(); err != nil {
		return nil, err
	}

	changeFeedEnabled, _, _, err := db.manifestStore.ChangeFeedBounds(ctx)
	if err != nil {
		db.releaseWriter(nil)
		return nil, err
	}
	w, err := newWriterWithMaintenanceWake(ctx, db.store, db.manifestStore, opts, db.maintenanceWake)
	if err != nil {
		db.releaseWriter(nil)
		return nil, err
	}
	w.changeFeedEnabled = changeFeedEnabled

	writer := &Writer{w: w}
	writer.release = func() { db.releaseWriter(writer) }
	if err := db.registerCloser(writer); err != nil {
		_ = writer.Close(ctx)
		db.releaseWriter(writer)
		return nil, err
	}
	return writer, nil
}

func (db *DB) reserveWriter() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return errors.New("db closed")
	}
	if db.writerOpen {
		return ErrWriterAlreadyOpen
	}
	db.writerOpen = true
	return nil
}

func (db *DB) releaseWriter(writer *Writer) {
	db.mu.Lock()
	db.writerOpen = false
	if writer != nil {
		db.removeCloserLocked(writer)
	}
	db.mu.Unlock()
}

// OpenReader opens the DB's shared concurrent reader runtime.
func (db *DB) OpenReader(ctx context.Context, opts ReaderOpenOptions) (*Reader, error) {
	if err := db.reserveReader(); err != nil {
		return nil, err
	}

	ropts, err := readerOptionsFromPublic(opts)
	if err != nil {
		db.releaseReader(nil)
		return nil, err
	}
	reader, err := newReader(ctx, db.store, ropts)
	if err != nil {
		db.releaseReader(nil)
		return nil, err
	}
	reader.release = func() { db.releaseReader(reader) }
	if err := db.registerCloser(reader); err != nil {
		_ = reader.Close()
		db.releaseReader(reader)
		return nil, err
	}
	return reader, nil
}

func (db *DB) reserveReader() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return errors.New("db closed")
	}
	if db.readerOpen {
		return ErrReaderAlreadyOpen
	}
	db.readerOpen = true
	return nil
}

func (db *DB) releaseReader(reader *Reader) {
	db.mu.Lock()
	db.readerOpen = false
	if reader != nil {
		db.removeCloserLocked(reader)
	}
	db.mu.Unlock()
}

// OpenChangeReader opens an independent cursor-based reader over the durable
// mutation feed. Change readers do not reserve the DB's shared KV reader slot.
func (db *DB) OpenChangeReader(ctx context.Context) (*ChangeReader, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}
	view, err := db.manifestStore.LoadChangeFeedView(ctx)
	if err != nil {
		return nil, err
	}
	if !view.Enabled() {
		return nil, ErrChangeFeedDisabled
	}

	reader := &ChangeReader{
		store:       db.store,
		manifestLog: db.manifestStore,
		view:        view,
	}
	reader.release = func() {
		db.mu.Lock()
		db.removeCloserLocked(reader)
		db.mu.Unlock()
	}
	if err := db.registerCloser(reader); err != nil {
		return nil, err
	}
	return reader, nil
}

// OpenMaintenance opens the DB's single fenced maintenance owner.
func (db *DB) OpenMaintenance(ctx context.Context, opts MaintenanceOptions) (*Maintenance, error) {
	if err := db.reserveMaintenance(); err != nil {
		return nil, err
	}

	maintenance, err := newMaintenance(ctx, db.store, db.manifestStore, db.gcCursorStorage, opts, db.changeFeedRetention)
	if err != nil {
		db.releaseMaintenance(nil)
		return nil, err
	}
	maintenance.release = func() { db.releaseMaintenance(maintenance) }
	maintenance.writerWake = db.maintenanceWake
	if err := db.registerCloser(maintenance); err != nil {
		_ = maintenance.Close(ctx)
		db.releaseMaintenance(maintenance)
		return nil, err
	}
	return maintenance, nil
}

func (db *DB) reserveMaintenance() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return errors.New("db closed")
	}
	if db.maintenanceOpen {
		return ErrMaintenanceAlreadyOpen
	}
	db.maintenanceOpen = true
	return nil
}

func (db *DB) releaseMaintenance(maintenance *Maintenance) {
	db.mu.Lock()
	db.maintenanceOpen = false
	if maintenance != nil {
		db.removeCloserLocked(maintenance)
	}
	db.mu.Unlock()
}

// Close closes the DB and any writer or maintenance handle opened from it.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}

	db.mu.Lock()
	closers := make([]dbCloser, len(db.closers))
	copy(closers, db.closers)
	clear(db.closers)
	db.closers = nil
	db.mu.Unlock()

	var firstErr error
	for _, c := range closers {
		if err := c.closeDB(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if db.closeStore {
		if err := db.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (db *DB) registerCloser(closer dbCloser) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return errors.New("db closed")
	}
	db.closers = append(db.closers, closer)
	return nil
}

// removeCloserLocked removes a released active handle while preserving the
// registration order used by DB.Close. The caller holds db.mu.
func (db *DB) removeCloserLocked(target dbCloser) {
	for i, closer := range db.closers {
		if closer != target {
			continue
		}
		copy(db.closers[i:], db.closers[i+1:])
		last := len(db.closers) - 1
		db.closers[last] = nil
		db.closers = db.closers[:last]
		return
	}
}
