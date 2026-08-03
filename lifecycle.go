package isledb

import (
	"context"
	"sync"
)

// waitGroupContext is used by multi-worker maintenance components. Components
// with one long-lived worker should expose a persistent completion channel
// instead of creating a waiter for each shutdown attempt.
func waitGroupContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
