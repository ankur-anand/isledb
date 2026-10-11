package cacheclient

import (
	"errors"
	"log/slog"
	"sync/atomic"
	"time"
)

// Breaker settings: after breakerTrips timeouts or unavailable answers in a
// row, reads skip the cache for breakerCooldown; then one read probes it.
const (
	breakerTrips    = 3
	breakerCooldown = 5 * time.Second
)

// ReasonBypassed is a read that did not try the cache, because it has
// stopped answering.
const ReasonBypassed = "bypassed"

var errBypassed = errors.New("the cache stopped answering; skipping it until it does")

// breaker stops a cache that does not answer from costing every read its
// timeout. Only an outage trips it: an answer of any kind, a refusal
// included, shows the cache is up.
type breaker struct {
	trips    int32
	cooldown time.Duration

	failures  atomic.Int32
	openUntil atomic.Int64 // Unix nanoseconds; zero while closed
	probing   atomic.Bool
}

func newBreaker() *breaker {
	return &breaker{trips: breakerTrips, cooldown: breakerCooldown}
}

// allow reports whether a read may try the cache, and whether it is the one
// probe allowed after a cooldown.
func (b *breaker) allow(now time.Time) (ok, probe bool) {
	until := b.openUntil.Load()
	switch {
	case until == 0:
		return true, false
	case now.UnixNano() < until:
		return false, false
	default:
		return b.probing.CompareAndSwap(false, true), true
	}
}

// record takes the outcome of a read allow let through. A read the caller
// abandoned says nothing about the cache.
func (b *breaker) record(now time.Time, probe bool, err error) {
	var e *Error
	outage := errors.As(err, &e) && (e.Reason == ReasonTimeout || e.Reason == ReasonUnavailable)
	answered := err == nil || (errors.As(err, &e) && !outage)
	switch {
	case answered:
		b.failures.Store(0)
		if b.openUntil.Swap(0) != 0 {
			slog.Info("isledb: read cache answering again")
		}
	case outage && probe:
		b.openUntil.Store(now.Add(b.cooldown).UnixNano())
	case outage && b.failures.Add(1) >= b.trips:
		if b.openUntil.CompareAndSwap(0, now.Add(b.cooldown).UnixNano()) {
			slog.Warn("isledb: read cache not answering; reading storage directly until it does",
				"error", err, "retry_in", b.cooldown)
		}
	}
	if probe {
		b.probing.Store(false)
	}
}
