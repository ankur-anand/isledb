package isledb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// readDeadline is a read's context, bounded by its view's expiry, that makes
// no timer until something waits on it.
//
// A read must not outlive its view: once the view expires, maintenance may
// delete the SSTs it names. context.WithDeadlineCause enforces that, but it
// starts a timer for every read, and a warm read, answered from memory in a
// microsecond, never waits for anything; the timer, and its allocations,
// cost more than the read. readDeadline reports the expiry through Deadline
// and Err, which need only the clock, and starts the timer the first time a
// caller asks for Done: a fetch from disk or object storage, the only part of
// a read that waits.
type readDeadline struct {
	parent    context.Context
	expiresAt time.Time
	cause     error

	once  sync.Once
	timed atomic.Pointer[timedContext]
}

type timedContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// withReadDeadline returns ctx bounded by expiresAt, failing with cause once
// it passes. Callers defer its release, which stops the timer if one was
// started.
func withReadDeadline(ctx context.Context, expiresAt time.Time, cause error) *readDeadline {
	return &readDeadline{parent: ctx, expiresAt: expiresAt, cause: cause}
}

func (d *readDeadline) Deadline() (time.Time, bool) {
	if deadline, ok := d.parent.Deadline(); ok && deadline.Before(d.expiresAt) {
		return deadline, true
	}
	return d.expiresAt, true
}

// Done starts the deadline's timer, once, and returns its channel.
func (d *readDeadline) Done() <-chan struct{} {
	return d.timedCtx().Done()
}

func (d *readDeadline) Err() error {
	if t := d.timed.Load(); t != nil {
		return t.ctx.Err()
	}
	if err := d.parent.Err(); err != nil {
		return err
	}
	if !time.Now().Before(d.expiresAt) {
		return context.DeadlineExceeded
	}
	return nil
}

// Value answers from the timed context once it exists, so context.Cause
// finds the expiry cause; before that, from the parent.
func (d *readDeadline) Value(key any) any {
	if t := d.timed.Load(); t != nil {
		return t.ctx.Value(key)
	}
	return d.parent.Value(key)
}

// expired reports whether the read failed because its view expired.
func (d *readDeadline) expired() bool {
	if t := d.timed.Load(); t != nil {
		return context.Cause(t.ctx) == d.cause
	}
	return d.parent.Err() == nil && !time.Now().Before(d.expiresAt)
}

func (d *readDeadline) timedCtx() context.Context {
	d.once.Do(func() {
		ctx, cancel := context.WithDeadlineCause(d.parent, d.expiresAt, d.cause)
		d.timed.Store(&timedContext{ctx: ctx, cancel: cancel})
	})
	return d.timed.Load().ctx
}

func (d *readDeadline) release() {
	if t := d.timed.Load(); t != nil {
		t.cancel()
	}
}

// err reports a failed read whose view expired as the expiry, like
// readViewError. A read that succeeded keeps its result, even if the view
// expired as it finished.
func (d *readDeadline) err(err error) error {
	if err == nil {
		return nil
	}
	if d.expired() {
		return d.cause
	}
	return readViewError(d, err)
}
