package isledb

import (
	"context"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/manifest"
)

// WriterOpenOptions configures a single-writer handle.
type WriterOpenOptions struct {
	MemtableSize    int64
	FlushInterval   time.Duration
	BloomBitsPerKey int
	BlockSize       int
	Compression     string

	ValueOptions    config.ValueOptions
	ManifestStorage manifest.Storage

	OnFlushError   func(error)
	DisableFencing bool
	OwnerID        string
}

// DefaultWriterOpenOptions returns sane defaults for WriterOpenOptions.
func DefaultWriterOpenOptions() WriterOpenOptions {
	defaults := DefaultWriterOptions()
	return WriterOpenOptions{
		MemtableSize:    defaults.MemtableSize,
		FlushInterval:   defaults.FlushInterval,
		BloomBitsPerKey: defaults.BloomBitsPerKey,
		BlockSize:       defaults.BlockSize,
		Compression:     defaults.Compression,
		ValueOptions:    config.DefaultValueOptions(),
	}
}

// Writer provides write-only access to the store.
type Writer struct {
	w *writer
}

// OpenWriter opens a write-only handle. User should ensure there is only one single writer writing at any time
// in the same bucket of the blob store to prevent db corruption.
func OpenWriter(ctx context.Context, store *blobstore.Store, opts WriterOpenOptions) (*Writer, error) {
	valueOpts := opts.ValueOptions
	if valueOpts == (config.ValueOptions{}) {
		valueOpts = config.DefaultValueOptions()
	}

	wopts := WriterOptions{
		MemtableSize:    opts.MemtableSize,
		FlushInterval:   opts.FlushInterval,
		BloomBitsPerKey: opts.BloomBitsPerKey,
		BlockSize:       opts.BlockSize,
		Compression:     opts.Compression,
		ValueStorage: config.ValueStorageConfig{
			ValueOptions:    valueOpts,
			BlobReadOptions: config.DefaultBlobReadOptions(),
			BlobGCOptions:   config.DefaultBlobGCOptions(),
		},
		ManifestStorage: opts.ManifestStorage,
		OnFlushError:    opts.OnFlushError,
		EnableFencing:   !opts.DisableFencing,
		OwnerID:         opts.OwnerID,
	}

	w, err := newWriter(ctx, store, wopts)
	if err != nil {
		return nil, err
	}
	return &Writer{w: w}, nil
}

// Put writes or replaces a key-value pair.
func (w *Writer) Put(key, value []byte) error {
	return w.w.put(key, value)
}

// PutWithTTL writes a key-value pair that expires after the given TTL.
func (w *Writer) PutWithTTL(key, value []byte, ttl time.Duration) error {
	return w.w.putWithTTL(key, value, ttl)
}

// Delete removes a key (tombstone).
func (w *Writer) Delete(key []byte) error {
	return w.w.delete(key)
}

// DeleteWithTTL writes a delete tombstone that expires after the given TTL.
func (w *Writer) DeleteWithTTL(key []byte, ttl time.Duration) error {
	return w.w.deleteWithTTL(key, ttl)
}

// Flush forces the current memtable to be persisted.
func (w *Writer) Flush(ctx context.Context) error {
	return w.w.flush(ctx)
}

// Close stops background flush and persists pending data.
func (w *Writer) Close() error {
	return w.w.close()
}
