// Package readcache is a regional read cache for IsleDB readers: it relays
// CURRENT to many readers from one copy, and serves immutable objects from
// chunks kept on disk.
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
	"sync/atomic"
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

	pollErrors atomic.Int64
}

func NewRelay(source CurrentSource, interval time.Duration) *Relay {
	return &Relay{source: source, interval: interval, changed: make(chan struct{})}
}

// failureLogEvery is how often Run repeats that storage is still failing.
const failureLogEvery = 30 * time.Second

// Run polls storage until ctx ends, logging to log, or slog's default when
// nil. A run of failed polls logs once when it starts, then every
// failureLogEvery, and once more when a poll succeeds again.
func (r *Relay) Run(ctx context.Context, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	t := time.NewTicker(r.interval)
	defer t.Stop()
	var failingSince, logged time.Time
	for {
		err := r.Poll(ctx)
		now := time.Now()
		switch {
		case ctx.Err() != nil:
		case err != nil && failingSince.IsZero():
			failingSince, logged = now, now
			log.Warn("readcache: read CURRENT", "error", err)
		case err != nil && now.Sub(logged) >= failureLogEvery:
			logged = now
			kept, _ := r.Current()
			log.Warn("readcache: still failing to read CURRENT", "error", err,
				"for", now.Sub(failingSince).Round(time.Second), "copy_age", kept.Age.Round(time.Millisecond))
		case err == nil && !failingSince.IsZero():
			log.Info("readcache: reading CURRENT again", "after", now.Sub(failingSince).Round(time.Second))
			failingSince = time.Time{}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// PollErrors is how many polls have failed.
func (r *Relay) PollErrors() int64 { return r.pollErrors.Load() }

// Poll reads CURRENT from storage once and takes it. A CURRENT at the same
// position with other bytes, as a checkpoint writes, replaces the copy.
// Storage reads are strongly consistent, so a CURRENT behind the copy means
// the store went back; Poll keeps the copy and returns an error.
func (r *Relay) Poll(ctx context.Context) error {
	err := r.poll(ctx)
	if err != nil {
		r.pollErrors.Add(1)
	}
	return err
}

func (r *Relay) poll(ctx context.Context) error {
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
