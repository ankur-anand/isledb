package isledb

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/internal/cacheclient"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// defaultMaxCurrentAge is ReadCacheOptions.MaxCurrentAge when it is zero.
const defaultMaxCurrentAge = 5 * time.Second

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
	client        *cacheclient.Client
	m             *ReaderMetrics
	maxCurrentAge time.Duration
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

// newReadThroughCache returns the read cache opts describes, or nil when its
// URL is empty.
func newReadThroughCache(opts ReadCacheOptions, metrics *ReaderMetrics) (*readThroughCache, error) {
	if opts.MaxCurrentAge < 0 {
		return nil, fmt.Errorf("read_cache.max_current_age=%s", opts.MaxCurrentAge)
	}
	if opts.URL == "" {
		return nil, nil
	}
	client, err := cacheclient.New(cacheclient.Options{
		URL: opts.URL, Token: opts.Token, HTTPClient: opts.HTTPClient, Timeout: opts.Timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("read_cache: %w", err)
	}
	return &readThroughCache{client: client, m: metrics,
		maxCurrentAge: cmp.Or(opts.MaxCurrentAge, defaultMaxCurrentAge)}, nil
}

// manifestThroughCache reads manifest snapshots and pages through the read
// cache when their reference is known, and everything else from the storage
// it wraps. Bytes from the cache that do not match the reference are read
// again from storage.
type manifestThroughCache struct {
	manifest.Storage
	cache *readThroughCache
	db    string // the database's prefix as the cache names it
}

// manifestPagesThroughCache is manifestThroughCache over a storage with
// pages, which it passes through.
type manifestPagesThroughCache struct {
	*manifestThroughCache
	manifest.PageStorage
}

func newManifestThroughCache(base manifest.Storage, cache *readThroughCache, prefix string) manifest.Storage {
	s := &manifestThroughCache{Storage: base, cache: cache, db: cacheDB(prefix)}
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

// cacheDB is how the read cache names the database at prefix.
func cacheDB(prefix string) string {
	if prefix = strings.Trim(prefix, "/"); prefix == "" {
		return "/"
	}
	return prefix
}

// currentRead is a refresh's request for CURRENT, carried in its context: a
// read without one, or with direct set, goes to storage. A copy from the
// cache is taken only if it is younger than the limit and not behind the
// published view, at minPosition; age reports the copy's age when it is.
type currentRead struct {
	direct      bool
	minPosition uint64
	age         time.Duration
}

type currentReadKey struct{}

// ReadCurrent reads CURRENT from the cache for a refresh that asks for it,
// and from storage otherwise, or when the cache's copy cannot be taken.
func (s *manifestThroughCache) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	read, _ := ctx.Value(currentReadKey{}).(*currentRead)
	if read == nil || read.direct {
		return s.Storage.ReadCurrent(ctx)
	}
	current, err := s.cache.client.ReadCurrent(ctx, s.db)
	outcome := "relayed"
	switch {
	case err != nil && ctx.Err() != nil:
		return nil, "", ctx.Err()
	case err != nil:
		outcome = "failed"
	case current.Age > s.cache.maxCurrentAge:
		outcome = "too_old"
	case current.Position < read.minPosition:
		outcome = "behind"
	}
	s.cache.m.ObserveReadCacheCurrent(outcome, current.Age)
	if outcome != "relayed" {
		return s.Storage.ReadCurrent(ctx)
	}
	read.age = current.Age
	return current.Data, "", nil
}

// minLongPollGap is the least time between the starts of two long polls: a
// cache whose long poll times out sooner than this, misconfigured, is not
// polled faster.
const minLongPollGap = 100 * time.Millisecond

// paceLongPoll waits, after a long poll that started at started and ended
// quiet, until minLongPollGap has passed since it started.
func paceLongPoll(ctx context.Context, started time.Time) {
	if wait := minLongPollGap - time.Since(started); wait > 0 {
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

// runDoorbell holds a long poll on the cache for a position past the
// newest view the Reader has loaded, published or held, and requests a
// refresh when one comes, until ctx ends. It polls past what the Reader has,
// not what the cache said: with replicas, the refresh can read a replica
// still behind the one that rang, and must be rung again.
func (r *Reader) runDoorbell(ctx context.Context, cache *readThroughCache, db string) {
	failures := 0
	for {
		loaded := r.loadedPosition()
		started := time.Now()
		position, err := cache.client.WaitCurrent(ctx, db, loaded)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err != nil:
			r.metrics.ObserveReadCacheDoorbell("error")
			failures++
			// Random, so readers whose cache restarted do not return together.
			wait := time.Duration(rand.Int64N(int64(min(30*time.Second, time.Second<<min(failures, 5)))))
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			continue
		case position > loaded:
			r.metrics.ObserveReadCacheDoorbell("rang")
			r.RequestRefresh()
			// A cache ahead of the refresh's answer rings again at once:
			// wait for the refresh, at most as long as refresh requests are
			// spaced.
			r.awaitLoaded(ctx, loaded, requestRefreshGap)
		default:
			r.metrics.ObserveReadCacheDoorbell("quiet")
			paceLongPoll(ctx, started)
		}
		failures = 0
	}
}

// loadedPosition is the position of the newest view loaded: the published
// one, or the one Manual mode holds.
func (r *Reader) loadedPosition() uint64 {
	position := r.position.Load()
	r.mu.RLock()
	if r.nextView != nil {
		position = max(position, currentNextSeq(r.nextView.current))
	}
	r.mu.RUnlock()
	return position
}

// awaitLoaded waits until a view past from is loaded, for at most d.
func (r *Reader) awaitLoaded(ctx context.Context, from uint64, d time.Duration) {
	deadline := time.Now().Add(d)
	for r.loadedPosition() <= from && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(25 * time.Millisecond):
		}
	}
}
