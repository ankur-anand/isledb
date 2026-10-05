package isledb

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestReadDeadlineNoTimerUntilWaited checks a warm read's view of the
// deadline: Deadline and Err answer from the clock, and no timer starts until
// something asks for Done.
func TestReadDeadlineNoTimerUntilWaited(t *testing.T) {
	expiresAt := time.Now().Add(time.Hour)
	d := withReadDeadline(context.Background(), expiresAt, ErrReadViewExpired)
	defer d.release()
	if got, ok := d.Deadline(); !ok || !got.Equal(expiresAt) {
		t.Fatalf("Deadline = %v, %v; want %v", got, ok, expiresAt)
	}
	if err := d.Err(); err != nil {
		t.Fatalf("Err before expiry = %v", err)
	}
	if d.timed.Load() != nil {
		t.Fatal("a timer started before anything waited")
	}
	if d.err(errors.New("io")) == ErrReadViewExpired {
		t.Fatal("a failure before expiry was reported as the expiry")
	}
}

// TestReadDeadlineExpiresWithoutTimer reads after the expiry with no waiter:
// Err reports it, and a failed read reports the expiry as its cause.
func TestReadDeadlineExpiresWithoutTimer(t *testing.T) {
	d := withReadDeadline(context.Background(), time.Now().Add(-time.Millisecond), ErrReadViewExpired)
	defer d.release()
	if err := d.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Err after expiry = %v, want %v", err, context.DeadlineExceeded)
	}
	if err := d.err(errors.New("fetch failed")); err != ErrReadViewExpired {
		t.Fatalf("failed read after expiry = %v, want %v", err, ErrReadViewExpired)
	}
	if err := d.err(nil); err != nil {
		t.Fatalf("a successful read after expiry = %v, want its result kept", err)
	}
}

// TestReadDeadlineDoneFiresAtExpiry waits on Done, as a fetch does: it fires
// at the expiry with ErrReadViewExpired as the cause, and so does a child.
func TestReadDeadlineDoneFiresAtExpiry(t *testing.T) {
	d := withReadDeadline(context.Background(), time.Now().Add(30*time.Millisecond), ErrReadViewExpired)
	defer d.release()
	child, cancel := context.WithCancel(d)
	defer cancel()
	select {
	case <-d.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not fire at the expiry")
	}
	if cause := context.Cause(d); cause != ErrReadViewExpired {
		t.Fatalf("Cause = %v, want %v", cause, ErrReadViewExpired)
	}
	select {
	case <-child.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a child context outlived the read's expiry")
	}
	if err := d.err(errors.New("fetch interrupted")); err != ErrReadViewExpired {
		t.Fatalf("read interrupted by expiry = %v, want %v", err, ErrReadViewExpired)
	}
}

// TestReadDeadlineParentCancel cancels the caller's context: the read sees
// the cancellation, which is not reported as the view expiring.
func TestReadDeadlineParentCancel(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	d := withReadDeadline(parent, time.Now().Add(time.Hour), ErrReadViewExpired)
	defer d.release()
	cancel()
	if err := d.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err after the caller cancelled = %v, want %v", err, context.Canceled)
	}
	select {
	case <-d.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not follow the caller's cancellation")
	}
	if err := d.err(context.Canceled); err == ErrReadViewExpired {
		t.Fatal("the caller's cancellation was reported as the view expiring")
	}
}

// TestReadDeadlineEarlierParentDeadline keeps the caller's own, earlier
// deadline.
func TestReadDeadlineEarlierParentDeadline(t *testing.T) {
	parentDeadline := time.Now().Add(time.Minute)
	parent, cancel := context.WithDeadline(context.Background(), parentDeadline)
	defer cancel()
	d := withReadDeadline(parent, time.Now().Add(time.Hour), ErrReadViewExpired)
	defer d.release()
	if got, _ := d.Deadline(); !got.Equal(parentDeadline) {
		t.Fatalf("Deadline = %v, want the caller's earlier %v", got, parentDeadline)
	}
}
