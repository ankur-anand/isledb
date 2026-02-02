package isledb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

// Writer provides write access to the database.
type Writer struct {
	w *writer
}

// Put writes a key-value pair to the database.
func (w *Writer) Put(key, value []byte) error {
	return w.w.put(key, value)
}

// PutWithTTL writes a key-value pair with a time-to-live duration.
func (w *Writer) PutWithTTL(key, value []byte, ttl time.Duration) error {
	return w.w.putWithTTL(key, value, ttl)
}

// Delete marks a key as deleted.
func (w *Writer) Delete(key []byte) error {
	return w.w.delete(key)
}

// DeleteWithTTL marks a key as deleted with a time-to-live duration.
func (w *Writer) DeleteWithTTL(key []byte, ttl time.Duration) error {
	return w.w.deleteWithTTL(key, ttl)
}

// Flush forces a flush of the current memtable to a new SST file.
func (w *Writer) Flush(ctx context.Context) error {
	return w.w.flush(ctx)
}

// Close closes the writer, flushing any pending writes.
func (w *Writer) Close() error {
	return w.w.close()
}

// DB encapsulates manifest state for writers and compactors operating on a single bucket/prefix.
// Use OpenDB once, then call db.OpenWriter and/or db.OpenCompactor.
type DB struct {
	store         *blobstore.Store
	manifestStore *manifest.Store
	mu            sync.Mutex
	closers       []dbCloser
	closed        atomic.Bool
}

type dbCloser interface {
	Close() error
}

// DBOptions configures a DB instance.
type DBOptions struct {
	// ManifestStorage allows using a custom manifest storage backend.
	// If nil, the blob store is used.
	ManifestStorage manifest.Storage
}

// OpenDB opens a database and initializes it.
func OpenDB(ctx context.Context, store *blobstore.Store, opts DBOptions) (*DB, error) {
	manifestStore := newManifestStore(store, opts.ManifestStorage)

	if _, err := manifestStore.Replay(ctx); err != nil {
		return nil, err
	}

	return &DB{
		store:         store,
		manifestStore: manifestStore,
	}, nil
}

// OpenWriter opens a writer instance from the DB.
func (db *DB) OpenWriter(ctx context.Context, opts WriterOptions) (*Writer, error) {
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}

	w, err := newWriter(ctx, db.store, db.manifestStore, opts)
	if err != nil {
		return nil, err
	}

	writer := &Writer{w: w}
	if err := db.registerCloser(writer); err != nil {
		_ = writer.Close()
		return nil, err
	}
	return writer, nil
}

// OpenCompactor opens a compactor instance from the DB.
func (db *DB) OpenCompactor(ctx context.Context, opts CompactorOptions) (*Compactor, error) {
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}

	compactor, err := newCompactor(ctx, db.store, db.manifestStore, opts)
	if err != nil {
		return nil, err
	}
	if err := db.registerCloser(compactor); err != nil {
		_ = compactor.Close()
		return nil, err
	}
	return compactor, nil
}

// OpenRetentionCompactor opens a retention compactor for this DB.
func (db *DB) OpenRetentionCompactor(ctx context.Context, opts RetentionCompactorOptions) (*RetentionCompactor, error) {
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}

	retentionCompactor, err := newRetentionCompactor(ctx, db.store, db.manifestStore, opts)
	if err != nil {
		return nil, err
	}
	if err := db.registerCloser(retentionCompactor); err != nil {
		_ = retentionCompactor.Close()
		return nil, err
	}
	return retentionCompactor, nil
}

// Close closes the DB and any writers/compactors opened from it.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}

	db.mu.Lock()
	closers := make([]dbCloser, len(db.closers))
	copy(closers, db.closers)
	db.closers = nil
	db.mu.Unlock()

	var firstErr error
	for _, c := range closers {
		if err := c.Close(); err != nil && firstErr == nil {
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
