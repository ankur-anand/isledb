package readcache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// prefetchRetry is how soon a pass that missed something runs again when
// CURRENT has not moved by then.
var prefetchRetry = 30 * time.Second

// prefetchParallel is how many SSTs a pass reads at once. Each read holds at
// most maxRunChunks of the read budget.
const prefetchParallel = 4

// Prefetcher keeps one database's visible SSTs on disk ahead of its readers.
// Each time the relay's CURRENT moves, it replays the manifest from it and
// reads each SST it has not handled before through the cache, a few at a
// time, as a reader's miss would: first the index and Bloom filter of each,
// then the rest. A full disk makes room by evicting what was read
// longest ago, which is mostly SSTs compaction has replaced, as nothing reads
// those any more. A database larger than the disk is not prefetched: room
// for it could only be made by evicting SSTs its readers use. It assumes the
// cache serves no other database, whose reads would evict what it reads
// ahead.
type Prefetcher struct {
	relay    *Relay
	manifest *manifest.Store
	// store names the database's objects; objects reads them by bucket key.
	store   *blobstore.Store
	objects *Objects
	log     *slog.Logger
	// done holds the visible SSTs found on disk or read onto it, so a pass
	// looks only at the ones new to it.
	done map[string]struct{}
	// missed is whether the last pass left something out after an error,
	// so the next runs after prefetchRetry even if CURRENT has not moved.
	missed bool
	// tooLarge is whether the last pass found the database larger than the
	// disk, so that is logged when it changes rather than on every pass.
	tooLarge bool

	paused    atomic.Bool
	readBytes atomic.Int64
	failures  atomic.Int64
}

// NewPrefetcher returns a Prefetcher of the database store names, following
// relay, its CURRENT, and reading through objects.
func NewPrefetcher(relay *Relay, store *blobstore.Store, objects *Objects, log *slog.Logger) *Prefetcher {
	return &Prefetcher{relay: relay, manifest: manifest.NewStore(store), store: store, objects: objects, log: log}
}

// Paused reports whether the last pass found the database larger than the
// disk, and so read nothing.
func (p *Prefetcher) Paused() bool { return p.paused.Load() }

// ReadBytes returns the bytes prefetch has read from storage.
func (p *Prefetcher) ReadBytes() int64 { return p.readBytes.Load() }

// Failures returns the SSTs prefetch could not read and the manifest replays
// that failed, all retried.
func (p *Prefetcher) Failures() int64 { return p.failures.Load() }

// Run prefetches until ctx is done.
func (p *Prefetcher) Run(ctx context.Context) {
	var after uint64
	for {
		wait, cancel := ctx, context.CancelFunc(func() {})
		if p.missed {
			wait, cancel = context.WithTimeout(ctx, prefetchRetry)
		}
		current, loaded := p.relay.Wait(wait, after)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if !loaded {
			continue
		}
		after = current.Position
		if err := p.pass(ctx, current.Data); err != nil && ctx.Err() == nil {
			p.log.Warn("isledb: cache prefetch failed", "err", err)
		}
	}
}

// pass reads every SST visible in CURRENT, given as data, that it has not
// handled before, newest level first.
func (p *Prefetcher) pass(ctx context.Context, data []byte) error {
	p.missed = true
	m, err := p.manifest.ReplayCurrentData(ctx, data)
	if err != nil {
		p.failures.Add(1)
		return fmt.Errorf("replay manifest: %w", err)
	}
	ssts := append([]manifest.SSTMeta(nil), m.L0SSTs...)
	for _, level := range m.Levels {
		ssts = append(ssts, level.SSTs...)
	}
	var total int64
	for _, sst := range ssts {
		total += sst.ObjectSize()
	}
	// Data is kept beside all the metadata, so only the rest is its room.
	room := p.objects.disk.Stats(diskcache.TierData).MaxBytes - p.objects.disk.Stats(diskcache.TierMeta).Bytes
	if tooLarge := total > room; tooLarge != p.tooLarge {
		p.tooLarge = tooLarge
		p.paused.Store(tooLarge)
		if tooLarge {
			p.log.Warn("isledb: cache prefetch paused: database larger than disk", "bytes", total, "disk_bytes", room)
		} else {
			p.log.Info("isledb: cache prefetch resumed: database fits on disk", "bytes", total, "disk_bytes", room)
		}
	}
	if p.tooLarge {
		// Readers may evict any of it meanwhile: check it all on resuming.
		p.done = nil
		p.missed = false
		return nil
	}
	done := make(map[string]struct{}, len(ssts))
	var todo []manifest.SSTMeta
	for _, sst := range ssts {
		if _, ok := p.done[sst.ID]; ok || p.objects.Cached(p.store.SSTPath(sst.ID), sst.ObjectSize()) {
			done[sst.ID] = struct{}{}
			continue
		}
		todo = append(todo, sst)
	}
	// Every lookup reads an SST's index and Bloom filter, at its end, before
	// any of its data, so those of all the new SSTs come first.
	errs := p.each(todo, func(sst manifest.SSTMeta, key string, size int64) error {
		if sst.MetaOffset <= 0 || sst.MetaOffset >= size {
			return nil
		}
		return p.objects.WriteRange(ctx, io.Discard, key, size, sst.MetaOffset, size-sst.MetaOffset, true, nil)
	})
	dataErrs := p.each(todo, func(sst manifest.SSTMeta, key string, size int64) error {
		return p.objects.WriteRange(ctx, io.Discard, key, size, 0, size, true, nil)
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var fetchedBytes, failed int64
	var lastErr error
	for i, sst := range todo {
		err := errs[i]
		if err == nil {
			err = dataErrs[i]
		}
		if err != nil {
			// ErrNotFound too: just after a compaction it is an SST removed
			// since this CURRENT, but a visible SST staying missing is a
			// problem worth seeing.
			failed++
			lastErr = err
			continue
		}
		done[sst.ID] = struct{}{}
		fetchedBytes += sst.ObjectSize()
	}
	p.readBytes.Add(fetchedBytes)
	p.done = done
	p.missed = len(done) < len(ssts)
	if fetched := int64(len(todo)) - failed; fetched > 0 {
		p.log.Info("isledb: cache prefetched", "ssts", fetched, "bytes", fetchedBytes)
	}
	if failed > 0 {
		p.failures.Add(failed)
		return fmt.Errorf("%d of %d ssts not read, retrying in %s: %w", failed, len(ssts), prefetchRetry, lastErr)
	}
	return nil
}

// each calls read for every SST of ssts, with its key and the size readers
// ask for, which names its chunks on disk; prefetchParallel at a time. It
// returns their errors by position.
func (p *Prefetcher) each(ssts []manifest.SSTMeta, read func(sst manifest.SSTMeta, key string, size int64) error) []error {
	errs := make([]error, len(ssts))
	slots := make(chan struct{}, prefetchParallel)
	var wg sync.WaitGroup
	for i, sst := range ssts {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			errs[i] = read(sst, p.store.SSTPath(sst.ID), sst.ObjectSize())
		})
	}
	wg.Wait()
	return errs
}
