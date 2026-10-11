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
// cache when their size is known, and everything else from the storage it
// wraps.
type manifestThroughCache struct {
	manifest.Storage
	pages manifest.PageStorage
	cache *readThroughCache
}

func newManifestThroughCache(base manifest.Storage, cache *readThroughCache) manifest.Storage {
	pages, ok := base.(manifest.PageStorage)
	if !ok {
		return base
	}
	return &manifestThroughCache{Storage: base, pages: pages, cache: cache}
}

func (s *manifestThroughCache) ReadSnapshotSized(ctx context.Context, path string, size uint64) ([]byte, error) {
	if data, ok, err := s.cache.read(ctx, path, int64(size), 0, int64(size), true); ok || err != nil {
		return data, err
	}
	return s.ReadSnapshot(ctx, path)
}

func (s *manifestThroughCache) ReadPageSized(ctx context.Context, path string, size uint64) ([]byte, error) {
	if data, ok, err := s.cache.read(ctx, path, int64(size), 0, int64(size), true); ok || err != nil {
		return data, err
	}
	return s.pages.ReadPage(ctx, path)
}

func (s *manifestThroughCache) ReadPage(ctx context.Context, path string) ([]byte, error) {
	return s.pages.ReadPage(ctx, path)
}

func (s *manifestThroughCache) WritePage(ctx context.Context, level uint8, id string, data []byte) (string, error) {
	return s.pages.WritePage(ctx, level, id, data)
}

func (s *manifestThroughCache) PagePath(level uint8, id string) string {
	return s.pages.PagePath(level, id)
}
