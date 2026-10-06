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

// ErrChangeFeedPayloadMismatch is returned when an open request attempts to
// change the payload policy of an already-enabled feed.
var ErrChangeFeedPayloadMismatch = manifest.ErrChangeFeedPayloadMismatch

// ErrCommitIndeterminate is returned when a writer retries an unacknowledged
// conditional commit after manifest retention has removed the immutable entry
// needed to prove whether that commit succeeded.
var ErrCommitIndeterminate = manifest.ErrCommitIndeterminate

// ErrFenced is returned once another writer has taken over the writer fence:
// the fenced Writer commits nothing more, so a write WaitCommitted reports
// ErrFenced for was not committed and never will be.
var ErrFenced = manifest.ErrFenced

// ErrManifestUnavailable is returned when a database that has already been
// initialized loses its manifest/CURRENT root. Operations fail closed rather
// than publishing or serving an empty replacement database.
var ErrManifestUnavailable = manifest.ErrCurrentUnavailable

// ChangeFeedPayload controls whether committed PUT records retain their value.
// A zero value is invalid when ChangeFeedOptions is present.
type ChangeFeedPayload uint8

const (
	ChangeFeedKeysOnly ChangeFeedPayload = iota + 1
	ChangeFeedFullValues
)

func (p ChangeFeedPayload) String() string {
	switch p {
	case ChangeFeedKeysOnly:
		return string(manifest.ChangeFeedPayloadKeysOnly)
	case ChangeFeedFullValues:
		return string(manifest.ChangeFeedPayloadFullValues)
	default:
		return "unknown"
	}
}

// ChangeFeedOptions enables the durable ordered mutation feed and selects its
// immutable PUT payload policy.
type ChangeFeedOptions struct {
	Payload ChangeFeedPayload
}

// SSTOutputOptions configures the encoding of newly created SSTs. L0 is used
// by writer flushes; Compacted is used for maintenance output. Existing SSTs
// remain self-describing and readable when these settings change.
type SSTOutputOptions struct {
	L0        SSTEncodingOptions
	Compacted SSTEncodingOptions
}

// SSTEncodingOptions controls the physical encoding of one SST class. Zero
// fields select production defaults.
type SSTEncodingOptions struct {
	Compression     string
	BlockBytes      int
	BloomBitsPerKey int
}

// DefaultSSTOutputOptions returns the current production SST encoding for both
// writer flushes and compacted output.
//
// L0 filters get more bits per key than compacted ones: every overlapping L0
// SST is checked on a point lookup, yet L0 holds few keys, so extra bits there
// remove many false positives for little memory. At 16 and 12 bits per key the
// filters answer "may contain" for about 0.05% and 0.3% of absent keys.
//
// Compacted SSTs use 16 KiB data blocks, L0 SSTs 4 KiB. Compacted SSTs are
// large and hold most of the data, so larger blocks shrink their index about
// fourfold, which is most of the metadata a cold range read fetches, and cut
// the blocks a scan requests. Small, short-lived L0 SSTs keep 4 KiB blocks for
// the cheapest warm point lookups.
func DefaultSSTOutputOptions() SSTOutputOptions {
	l0 := SSTEncodingOptions{
		Compression:     "snappy",
		BlockBytes:      4 << 10,
		BloomBitsPerKey: 16,
	}
	compacted := l0
	compacted.BlockBytes = 16 << 10
	compacted.BloomBitsPerKey = 12
	return SSTOutputOptions{L0: l0, Compacted: compacted}
}

// Writer provides write access to the database.
//
// A Writer owns one fenced write session for a DB bucket/prefix. It buffers
// writes in memory, flushes full memtables into immutable SST files, and then
// commits those SSTs through the manifest. Only manifest-committed SSTs are
// visible to readers.
//
// Put, PutWithTTL, Delete, WaitCommitted and CommittedSequence are safe to
// call concurrently from any goroutines: each mutation gets its own sequence,
// in the order mutations are accepted. Serialize Flush and Close against each
// other.
//
// If WriterOptions.Flush.Interval is greater than zero, the Writer also runs a
// background flush loop. A commit that fails, in the background or in Flush or
// Close, stays queued and is retried; the next attempt finds a commit that was
// applied before its response was lost, so nothing commits twice. The only
// failure that ends a Writer is losing its fence to another writer
// (ErrFenced). Background failures are reported through
// WriterOptions.OnFlushError. A failure that cannot succeed until an operator
// acts, such as missing permissions, is also retried, and never fails the
// Writer on its own: alert on OnFlushError or the oldest-uncommitted gauge.
type Writer struct {
	w           *writer
	releaseOnce sync.Once
	release     func()
}

// Put writes a key-value pair to the active memtable and returns the
// mutation's sequence.
//
// Put returns after the mutation is buffered locally. The mutation becomes
// durable and visible to readers only after a successful Flush, background
// flush, or Close; WaitCommitted with the returned sequence waits for that.
func (w *Writer) Put(ctx context.Context, key, value []byte) (uint64, error) {
	return w.w.put(ctx, key, value)
}

// PutWithTTL writes a key-value pair with a time-to-live duration.
//
// A zero ttl means no expiration. A negative ttl is rejected with
// ErrInvalidMutation. Expired values are filtered by readers. Like Put, it
// returns the mutation's sequence.
func (w *Writer) PutWithTTL(ctx context.Context, key, value []byte, ttl time.Duration) (uint64, error) {
	return w.w.putWithTTL(ctx, key, value, ttl)
}

// Delete marks a key as deleted.
//
// Like Put, the tombstone is buffered first and becomes durable and visible
// after a successful Flush, background flush, or Close, and Delete returns
// its sequence.
func (w *Writer) Delete(ctx context.Context, key []byte) (uint64, error) {
	return w.w.delete(ctx, key)
}

// WaitCommitted waits until the mutation with sequence seq, as returned by
// Put, PutWithTTL or Delete, and every mutation before it are committed to
// object storage, where readers that refresh see them.
//
// It does not flush: the mutation commits with the next background flush,
// Flush or Close, so concurrent callers share one commit. It returns nil once
// seq is committed; ErrFenced once another writer took over, so seq was not
// committed and never will be; or the context's error, which means the outcome
// is not yet known: a failed commit is retried, and seq may still commit.
// Without a background flush interval, committing is the caller's Flush or
// Close.
func (w *Writer) WaitCommitted(ctx context.Context, seq uint64) error {
	return w.w.waitCommitted(ctx, seq)
}

// CommittedSequence returns the highest sequence committed to object
// storage. Every mutation with a sequence at or below it is durable.
func (w *Writer) CommittedSequence() uint64 {
	return w.w.committed.Load()
}

// Flush publishes all currently buffered writes and waits until they are
// visible to newly refreshed readers.
//
// The writer's committer does the work: it rotates the active memtable,
// writes the frozen memtables as SST files and commits their manifest
// entries, each attempt under its own deadline. ctx bounds only the wait:
// when it ends first, Flush returns ctx.Err() and the commit goes on. A
// failed attempt's error is returned; the writes stay queued and are retried.
func (w *Writer) Flush(ctx context.Context) error {
	return w.w.flush(ctx)
}

// Close finishes the Writer: it accepts no more writes, makes one last
// attempt to commit what is pending, and then stops everything the Writer
// runs and waits for it. ctx bounds the wait for the commit.
//
// The Writer is finished whether or not Close succeeds. If the commit fails
// or ctx ends first, Close returns an error naming the writes not known to be
// committed; the last attempt may still land, so they are unknown, not lost,
// and WaitCommitted reports them with ErrWriterClosed. An already-expired ctx
// finishes the Writer without a commit attempt, with every pending write
// reported that way. To ride out an outage before closing, call Flush until
// it succeeds, then Close.
func (w *Writer) Close(ctx context.Context) error {
	err := w.w.close(ctx)
	if w.w.finished() {
		w.releaseWriter()
	}
	return err
}

func (w *Writer) closeDB() error {
	err := w.w.closeWithTimeout(30 * time.Second)
	if w.w.finished() {
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
	maintenanceWake chan struct{}
	storePolicy     StorePolicy
	sstOutput       SSTOutputOptions

	mu              sync.Mutex
	closers         []dbCloser
	writerOpen      bool
	readerOpen      bool
	maintenanceOpen bool
	closed          atomic.Bool
}

type dbCloser interface {
	closeDB() error
}

// DBOptions configures a DB instance.
type DBOptions struct {
	// Prefix is the database's root path inside the bucket or container.
	Prefix string

	// ChangeFeed enables a durable ordered mutation feed for all future writer
	// commits. Nil leaves a new feed disabled and adopts an already-persisted
	// configuration when reopening a prefix.
	ChangeFeed *ChangeFeedOptions

	// SSTOutput configures newly flushed and compacted SSTs. Zero fields select
	// defaults. This is runtime output policy and is not persisted in CURRENT.
	SSTOutput SSTOutputOptions

	// Policy contains store-wide safety settings persisted by the first writer.
	Policy StorePolicy
}

// StorePolicy defines the read-view lifetime shared by readers and garbage
// collection for one database prefix.
type StorePolicy struct {
	// MaxPinnedViewAge is the longest time a loaded manifest view may remain
	// readable. Zero selects DefaultMaxPinnedViewAge.
	MaxPinnedViewAge time.Duration
}

const DefaultMaxPinnedViewAge = manifest.DefaultMaxPinnedViewAge

var ErrStorePolicyMismatch = manifest.ErrStorePolicyMismatch

type dbOpenOptions struct {
	manifestStorage   manifest.Storage
	changeFeedPayload manifest.ChangeFeedPayload
	storePolicy       StorePolicy
	sstOutput         SSTOutputOptions
}

func openDB(ctx context.Context, store *blobstore.Store, opts dbOpenOptions) (*DB, error) {
	storePolicy, err := normalizeStorePolicy(opts.storePolicy)
	if err != nil {
		return nil, err
	}
	sstOutput, err := normalizeSSTOutputOptions(opts.sstOutput)
	if err != nil {
		return nil, err
	}
	manifestStore := newManifestStore(store, opts.manifestStorage)
	if _, err := replayManifestForOpen(ctx, store, manifestStore, false); err != nil {
		return nil, err
	}
	if opts.changeFeedPayload != "" {
		if err := manifestStore.EnableChangeFeed(ctx, opts.changeFeedPayload); err != nil {
			return nil, err
		}
	}
	return &DB{
		store:           store,
		manifestStore:   manifestStore,
		maintenanceWake: make(chan struct{}, 1),
		storePolicy:     storePolicy,
		sstOutput:       sstOutput,
	}, nil
}

// OpenWriter opens the DB's single active writer. A new writer can be opened
// after the previous writer closes successfully or becomes fenced.
func (db *DB) OpenWriter(ctx context.Context, opts WriterOptions) (*Writer, error) {
	if err := db.reserveWriter(); err != nil {
		return nil, err
	}

	changeFeedView, err := db.manifestStore.LoadChangeFeedView(ctx)
	if err != nil {
		db.releaseWriter(nil)
		return nil, err
	}
	w, err := newWriterWithMaintenanceWake(
		ctx,
		db.store,
		db.manifestStore,
		opts,
		db.maintenanceWake,
		db.storePolicy,
		db.sstOutput.L0,
	)
	if err != nil {
		db.releaseWriter(nil)
		return nil, err
	}
	w.changeFeedPayload = publicChangeFeedPayload(changeFeedView.Payload())

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

	maintenance, err := newMaintenance(
		ctx,
		db.store,
		db.manifestStore,
		opts,
		db.sstOutput.Compacted,
	)
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
