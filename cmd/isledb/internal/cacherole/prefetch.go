package cacherole

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
)

// prefetchRetry is how soon a pass that missed something runs again when
// CURRENT has not moved by then.
var prefetchRetry = 30 * time.Second

// prefetcher keeps one database's visible SSTs on disk ahead of its readers.
// Each time the relay's CURRENT moves, it replays the manifest from it and
// reads each SST it has not handled before through the cache, one at a time,
// as a reader's miss would. A full disk makes room by evicting what was read
// longest ago, which is mostly SSTs compaction has replaced, as nothing reads
// those any more. A database larger than the disk is not prefetched: room
// for it could only be made by evicting SSTs its readers use. The cache
// serves no other database, whose reads would evict what this reads ahead.
type prefetcher struct {
	relay    *readcache.Relay
	manifest *manifest.Store
	// store names the database's objects; objects reads them by bucket key.
	store   *blobstore.Store
	objects *readcache.Objects
	disk    *diskcache.Cache
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

	// Reported as metrics. failures counts SSTs not read and failed
	// replays.
	paused    atomic.Bool
	readBytes atomic.Int64
	failures  atomic.Int64
}

func (p *prefetcher) run(ctx context.Context) {
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
func (p *prefetcher) pass(ctx context.Context, data []byte) error {
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
	room := p.disk.Stats(diskcache.TierData).MaxBytes - p.disk.Stats(diskcache.TierMeta).Bytes
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
	var fetched, fetchedBytes, failed int64
	var lastErr error
	for _, sst := range ssts {
		if _, ok := p.done[sst.ID]; ok {
			done[sst.ID] = struct{}{}
			continue
		}
		key := p.store.SSTPath(sst.ID)
		// The size readers ask for, which names the chunks on disk.
		size := sst.ObjectSize()
		if !p.objects.Cached(key, size) {
			err := p.objects.WriteRange(ctx, io.Discard, key, size, 0, size, true, nil)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil {
				// ErrNotFound too: just after a compaction it is an SST
				// removed since this CURRENT, but a visible SST staying
				// missing is a problem worth seeing.
				failed++
				lastErr = err
				continue
			}
			fetched++
			fetchedBytes += size
			p.readBytes.Add(size)
		}
		done[sst.ID] = struct{}{}
	}
	p.done = done
	p.missed = len(done) < len(ssts)
	if fetched > 0 {
		p.log.Info("isledb: cache prefetched", "ssts", fetched, "bytes", fetchedBytes)
	}
	if failed > 0 {
		p.failures.Add(failed)
		return fmt.Errorf("%d of %d ssts not read, retrying in %s: %w", failed, len(ssts), prefetchRetry, lastErr)
	}
	return nil
}
