package isledb

import (
	"sync"
	"sync/atomic"
	"time"
)

// deadSSTDeleteRate caps how many files a Reader's dead-SST goroutine deletes
// per second. Unpaced deletes cut lookups on the same disk by about a quarter
// while they run; see docs/design/reader-next-view.md. A variable so tests can
// change it before opening a Reader.
var deadSSTDeleteRate = 1000

// deadSSTs deletes from the disk cache the SSTs a publish retired, so the
// cache keeps its space for data a view can read and a prefetch has free
// space to use. It runs on its own goroutine: a compaction can retire
// thousands of cached files, and a publish, a Refresh or a lookup must not
// wait for them. A snapshot of an older view still reads correctly after,
// fetching again what was dropped.
type deadSSTs struct {
	r       *Reader
	mu      sync.Mutex
	pending map[string]sstMetadata
	wake    chan struct{}
	done    chan struct{}
	stopped sync.WaitGroup
	drops   atomic.Int64
	// gate, set only by tests, holds each drop until it receives.
	gate chan struct{}
}

func (d *deadSSTs) start(r *Reader) {
	d.r = r
	d.pending = map[string]sstMetadata{}
	d.wake = make(chan struct{}, 1)
	d.done = make(chan struct{})
	d.stopped.Add(1)
	go d.run()
}

// retire queues the SSTs before names that after does not.
func (d *deadSSTs) retire(before, after *manifestState) {
	if d.wake == nil || before == nil || before == after {
		return
	}
	live := sstsByID(after)
	d.mu.Lock()
	queued := false
	for _, sst := range sstMetadataOf(before) {
		if _, ok := live[sst.ID]; !ok {
			d.pending[sst.ID] = sst
			queued = true
		}
	}
	d.mu.Unlock()
	if queued {
		select {
		case d.wake <- struct{}{}:
		default:
		}
	}
}

func (d *deadSSTs) run() {
	defer d.stopped.Done()
	for {
		select {
		case <-d.done:
			return
		case <-d.wake:
		}
		// An SST a later publish names again is not dead. live is taken once
		// per wake, so a publish during a long drain is not seen until the
		// next; that matters only if a retired SST reappeared in a later view,
		// which manifests never do.
		live := sstsByID(d.r.currentManifest())
		for {
			d.mu.Lock()
			var sst sstMetadata
			var ok bool
			for id, s := range d.pending {
				sst, ok = s, true
				delete(d.pending, id)
				break
			}
			d.mu.Unlock()
			if !ok {
				break
			}
			select {
			case <-d.done:
				return
			default:
			}
			if _, named := live[sst.ID]; named {
				continue
			}
			if d.gate != nil {
				select {
				case <-d.gate:
				case <-d.done:
					return
				}
			}
			removed := d.r.fetcher.dropDeadObject(d.r.fetcher.object(sst))
			d.drops.Add(1)
			if removed > 0 && !d.pause(removed) {
				return
			}
		}
	}
}

// pause waits as long as deleting removed files takes at deadSSTDeleteRate,
// and reports false if the Reader closed meanwhile.
func (d *deadSSTs) pause(removed int) bool {
	t := time.NewTimer(time.Duration(removed) * time.Second / time.Duration(deadSSTDeleteRate))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-d.done:
		return false
	}
}

func (d *deadSSTs) stop() {
	if d.done == nil {
		return
	}
	close(d.done)
	d.stopped.Wait()
}
