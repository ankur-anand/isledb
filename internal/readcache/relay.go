// Package readcache is a regional read cache for IsleDB readers: it relays
// CURRENT to many readers from one copy, and serves immutable objects from
// chunks kept on disk. See docs/design/read-cache.md.
//
// The relay never moves its copy of CURRENT back. A store recreated under the
// same prefix, whose position starts again below the copy, is reported on
// every poll and needs the relay restarted.
package readcache

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/internal/manifest"
)

// CurrentSource reads CURRENT from storage. manifest.Storage satisfies it.
type CurrentSource interface {
	ReadCurrent(ctx context.Context) ([]byte, string, error)
}

// Current is the relay's copy of CURRENT: its bytes, its log position, and
// how long ago it was read from storage.
type Current struct {
	Data     []byte
	Position uint64
	Age      time.Duration
}

// Relay keeps one copy of CURRENT, read from storage every interval, and
// hands it to any number of readers. It only moves forward: a CURRENT at a
// lower log position than its copy is ignored. When storage cannot be read
// it keeps its copy, whose age then grows; readers decide whether it is
// still young enough.
type Relay struct {
	source   CurrentSource
	interval time.Duration

	// pollMu runs polls one at a time: an earlier read finishing after a
	// later one would otherwise look like the store going back.
	pollMu sync.Mutex

	mu        sync.Mutex
	loaded    bool
	data      []byte
	position  uint64
	fetchedAt time.Time // carries a monotonic reading, so Age ignores clock changes
	changed   chan struct{}
}

func NewRelay(source CurrentSource, interval time.Duration) *Relay {
	return &Relay{source: source, interval: interval, changed: make(chan struct{})}
}

// Run polls storage until ctx ends.
func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		if err := r.Poll(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("readcache: read CURRENT", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Poll reads CURRENT from storage once and takes it. A CURRENT at the same
// position with other bytes, as a checkpoint writes, replaces the copy.
// Storage reads are strongly consistent, so a CURRENT behind the copy means
// the store went back; Poll keeps the copy and returns an error.
func (r *Relay) Poll(ctx context.Context) error {
	r.pollMu.Lock()
	defer r.pollMu.Unlock()
	// Taken before the read: storage answered at some moment during it, so
	// the copy's age is never less than it really is.
	sent := time.Now()
	data, _, err := r.source.ReadCurrent(ctx)
	if err != nil {
		return err
	}
	current, err := manifest.DecodeCurrent(data)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loaded && current.NextSeq < r.position {
		return fmt.Errorf("storage CURRENT is at log position %d, behind the relay's copy at %d; restart the relay if the store was recreated",
			current.NextSeq, r.position)
	}
	advanced := !r.loaded || current.NextSeq > r.position || !bytes.Equal(data, r.data)
	r.loaded, r.data, r.position, r.fetchedAt = true, data, current.NextSeq, sent
	if advanced {
		close(r.changed)
		r.changed = make(chan struct{})
	}
	return nil
}

// Current returns the copy, and false before the first successful read.
func (r *Relay) Current() (Current, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentLocked(), r.loaded
}

func (r *Relay) currentLocked() Current {
	return Current{Data: r.data, Position: r.position, Age: time.Since(r.fetchedAt)}
}

// Wait returns the copy once its position is past after, or the copy as it
// is when ctx ends. It reports false if nothing has been read yet.
func (r *Relay) Wait(ctx context.Context, after uint64) (Current, bool) {
	for {
		r.mu.Lock()
		if r.loaded && r.position > after {
			c := r.currentLocked()
			r.mu.Unlock()
			return c, true
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return r.Current()
		}
	}
}
