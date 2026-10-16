package blobstore

import (
	"context"
	"sync"
	"time"
)

// GCSOverwriteGap is how long a Store on Google Cloud Storage waits between
// conditional writes of the same object. GCS allows about one write per
// second to an object and answers faster ones with 429.
const GCSOverwriteGap = time.Second

// overwritePacer spaces writes of the same key at least gap apart, measured
// from the end of one write to the start of the next. Writes of one key run
// one at a time; writes of different keys do not wait for each other.
type overwritePacer struct {
	gap time.Duration

	mu   sync.Mutex
	keys map[string]*pacedKey
}

type pacedKey struct {
	turn chan struct{} // holds a token while a write of the key runs
	// Guarded by the pacer's mu.
	users int       // calls of do holding or waiting for the key
	last  time.Time // when the last write of the key ended
}

// pacerSweepAt is how many keys the pacer holds before it forgets the idle
// ones, whose last write ended more than gap ago.
const pacerSweepAt = 64

func newOverwritePacer(gap time.Duration) *overwritePacer {
	return &overwritePacer{gap: gap, keys: make(map[string]*pacedKey)}
}

// do runs write once gap has passed since the last write of key ended. It
// returns ctx's error, without writing, if ctx ends first.
func (p *overwritePacer) do(ctx context.Context, key string, write func() error) error {
	k := p.acquire(key)
	defer p.release(k)
	select {
	case k.turn <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-k.turn }()
	p.mu.Lock()
	last := k.last
	p.mu.Unlock()
	if wait := time.Until(last.Add(p.gap)); wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	err := write()
	// A failed write counts too: GCS may have counted it.
	p.mu.Lock()
	k.last = time.Now()
	p.mu.Unlock()
	return err
}

func (p *overwritePacer) acquire(key string) *pacedKey {
	p.mu.Lock()
	defer p.mu.Unlock()
	k, ok := p.keys[key]
	if !ok {
		if len(p.keys) >= pacerSweepAt {
			p.sweepLocked()
		}
		k = &pacedKey{turn: make(chan struct{}, 1)}
		p.keys[key] = k
	}
	k.users++
	return k
}

func (p *overwritePacer) release(k *pacedKey) {
	p.mu.Lock()
	k.users--
	p.mu.Unlock()
}

// sweepLocked forgets keys that no call uses and whose last write ended at
// least gap ago, so a new write of them would not wait anyway.
func (p *overwritePacer) sweepLocked() {
	for name, k := range p.keys {
		if k.users == 0 && time.Since(k.last) >= p.gap {
			delete(p.keys, name)
		}
	}
}
