package isledb

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/internal/cacheclient"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// Fallback reasons of the Reader's own, beside the read cache client's.
const (
	// reasonChecksumMismatch: bytes from the cache failed the object's
	// checksum, and were read again from storage.
	reasonChecksumMismatch = "checksum_mismatch"
	// reasonAfterDamage: a read of an SST found damaged recently, sent to
	// storage rather than the cache.
	reasonAfterDamage = "after_damage"
)

// readThroughCache sends a Reader's immutable reads to the read cache before
// storage, and counts what happens.
type readThroughCache struct {
	client *cacheclient.Client
	m      *ReaderMetrics
	// warned is when a misconfigured cache was last logged, in Unix
	// nanoseconds.
	warned atomic.Int64
}

// read reads [offset, offset+length) of key, an object of size bytes,
// through the cache. ok reports that it did; otherwise err is the caller's
// context ending, or nil for the caller to read storage instead.
func (c *readThroughCache) read(ctx context.Context, key string, size, offset, length int64, fill bool) (data []byte, ok bool, err error) {
	data, err = c.client.ReadRange(ctx, key, size, offset, length, fill)
	if err == nil {
		c.m.ObserveReadCache("")
		return data, true, nil
	}
	if ctx.Err() != nil {
		return nil, false, ctx.Err()
	}
	reason := cacheclient.ReasonInvalid
	var cacheErr *cacheclient.Error
	if errors.As(err, &cacheErr) {
		reason = cacheErr.Reason
	}
	c.m.ObserveReadCache(reason)
	if reason == cacheclient.ReasonRefused || reason == cacheclient.ReasonSizeMismatch || reason == cacheclient.ReasonInvalid {
		c.warn(err)
	}
	return nil, false, nil
}

// warn logs a cache failure that points at misconfiguration, at most once a
// minute.
func (c *readThroughCache) warn(err error) {
	now := time.Now().UnixNano()
	last := c.warned.Load()
	if now-last < int64(time.Minute) || !c.warned.CompareAndSwap(last, now) {
		return
	}
	slog.Warn("isledb: read cache refused a read; reading storage instead", "error", err)
}

func (c *readThroughCache) close() { c.client.Close() }

// manifestThroughCache reads manifest snapshots and pages through the read
// cache when their reference is known, and everything else from the storage
// it wraps. Bytes from the cache that do not match the reference are read
// again from storage.
type manifestThroughCache struct {
	manifest.Storage
	cache *readThroughCache
}

// manifestPagesThroughCache is manifestThroughCache over a storage with
// pages, which it passes through.
type manifestPagesThroughCache struct {
	*manifestThroughCache
	manifest.PageStorage
}

func newManifestThroughCache(base manifest.Storage, cache *readThroughCache) manifest.Storage {
	s := &manifestThroughCache{Storage: base, cache: cache}
	if pages, ok := base.(manifest.PageStorage); ok {
		return &manifestPagesThroughCache{manifestThroughCache: s, PageStorage: pages}
	}
	return s
}

func (s *manifestThroughCache) ReadSnapshotRef(ctx context.Context, ref manifest.ObjectRef) ([]byte, error) {
	if data, ok, err := s.readRef(ctx, ref); ok || err != nil {
		return data, err
	}
	return s.ReadSnapshot(ctx, ref.Path)
}

func (s *manifestThroughCache) ReadPageRef(ctx context.Context, ref manifest.ObjectRef) ([]byte, error) {
	if data, ok, err := s.readRef(ctx, ref); ok || err != nil {
		return data, err
	}
	pages, ok := s.Storage.(manifest.PageStorage)
	if !ok {
		return nil, manifest.ErrNotFound
	}
	return pages.ReadPage(ctx, ref.Path)
}

// readRef reads ref's object through the cache: ok reports bytes that match
// it; otherwise err is the caller's context ending, or nil to read storage.
func (s *manifestThroughCache) readRef(ctx context.Context, ref manifest.ObjectRef) ([]byte, bool, error) {
	size := int64(ref.EncodedBytes)
	data, ok, err := s.cache.read(ctx, ref.Path, size, 0, size, true)
	if !ok || ref.Matches(data) {
		return data, ok, err
	}
	s.cache.m.ObserveReadCache(reasonChecksumMismatch)
	return nil, false, nil
}
