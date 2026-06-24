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
// background flush loop. Background flush errors are reported through
// WriterOptions.OnFlushError when configured; they are not returned by later Put
// calls. Call Flush or Close to synchronously observe flush errors.
type Writer struct {
	w *writer
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

// DeleteWithTTL marks a key as deleted with a time-to-live duration.
//
// ttl <= 0 means the tombstone does not expire.
func (w *Writer) DeleteWithTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	return w.w.deleteWithTTL(ctx, key, ttl)
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
	return w.w.close(ctx)
}

func (w *Writer) closeDB() error {
	return w.w.closeWithTimeout(30 * time.Second)
}

// DB encapsulates manifest state for writers and compactors operating on a single bucket/prefix.
// Use OpenDB once, then call db.OpenWriter and/or db.OpenCompactor.
type DB struct {
	store         *blobstore.Store
	manifestStore *manifest.Store
	gcMarkStorage manifest.GCMarkStorage
	mu            sync.Mutex
	closers       []dbCloser
	closed        atomic.Bool
}

type dbCloser interface {
	closeDB() error
}

// DBOptions configures a DB instance.
type DBOptions struct {
	// ManifestStorage allows using a custom manifest storage backend.
	// If nil, the blob store is used.
	ManifestStorage manifest.Storage
	// GCMarkStorage allows using a custom storage backend for GC mark state.
	// If nil, the blob store is used.
	GCMarkStorage manifest.GCMarkStorage
}

// OpenDB opens a database and initializes it.
func OpenDB(ctx context.Context, store *blobstore.Store, opts DBOptions) (*DB, error) {
	manifestStore := newManifestStore(store, opts.ManifestStorage)
	gcMarkStorage := opts.GCMarkStorage
	if gcMarkStorage == nil {
		gcMarkStorage = newGCMarkStorage(store)
	}

	if _, err := manifestStore.Replay(ctx); err != nil {
		return nil, err
	}

	return &DB{
		store:         store,
		manifestStore: manifestStore,
		gcMarkStorage: gcMarkStorage,
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
		_ = writer.Close(ctx)
		return nil, err
	}
	return writer, nil
}

// OpenCompactor opens a compactor instance from the DB.
func (db *DB) OpenCompactor(ctx context.Context, opts CompactorOptions) (*Compactor, error) {
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}
	if opts.GCMarkStorage == nil {
		opts.GCMarkStorage = db.gcMarkStorage
	}

	compactor, err := newCompactor(ctx, db.store, db.manifestStore, opts)
	if err != nil {
		return nil, err
	}
	if err := db.registerCloser(compactor); err != nil {
		_ = compactor.Close(ctx)
		return nil, err
	}
	return compactor, nil
}

// OpenRetentionCompactor opens a retention compactor for this DB.
func (db *DB) OpenRetentionCompactor(ctx context.Context, opts RetentionCompactorOptions) (*RetentionCompactor, error) {
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}
	if opts.GCMarkStorage == nil {
		opts.GCMarkStorage = db.gcMarkStorage
	}

	retentionCompactor, err := newRetentionCompactor(ctx, db.store, db.manifestStore, opts)
	if err != nil {
		return nil, err
	}
	if err := db.registerCloser(retentionCompactor); err != nil {
		_ = retentionCompactor.Close(ctx)
		return nil, err
	}
	return retentionCompactor, nil
}

// OpenChangeFeedCleaner opens a cleaner for retained change-feed history.
func (db *DB) OpenChangeFeedCleaner(ctx context.Context, opts ChangeFeedCleanerOptions) (*ChangeFeedCleaner, error) {
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}

	cleaner, err := newChangeFeedCleaner(ctx, db.store, db.manifestStore, opts)
	if err != nil {
		return nil, err
	}
	if err := db.registerCloser(cleaner); err != nil {
		_ = cleaner.Close(ctx)
		return nil, err
	}
	return cleaner, nil
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
		if err := c.closeDB(); err != nil && firstErr == nil {
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
