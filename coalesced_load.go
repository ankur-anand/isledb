package isledb

import (
	"context"
	"sync"
)

// coalescedLoadGroup shares one load for a key while keeping caller
// cancellation independent. A load is canceled when its final waiter leaves
// or when Close shuts down the owning reader. The zero value is ready for use.
type coalescedLoadGroup struct {
	mu       sync.Mutex
	calls    map[string]*coalescedLoadCall
	active   sync.WaitGroup
	closed   bool
	closeErr error
}

type coalescedLoadCall struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	done     chan struct{}
	waiters  int
	finished bool
	value    any
	err      error
}

func (g *coalescedLoadGroup) Do(
	ctx context.Context,
	key string,
	load func(context.Context) (any, error),
) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	g.mu.Lock()
	if g.closed {
		err := g.closeErr
		g.mu.Unlock()
		return nil, err
	}
	if call := g.calls[key]; call != nil {
		call.waiters++
		g.mu.Unlock()
		return g.wait(ctx, key, call)
	}
	if g.calls == nil {
		g.calls = make(map[string]*coalescedLoadCall)
	}
	loadCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	call := &coalescedLoadCall{
		ctx:     loadCtx,
		cancel:  cancel,
		done:    make(chan struct{}),
		waiters: 1,
	}
	g.calls[key] = call
	g.active.Add(1)
	g.mu.Unlock()

	go g.run(key, call, load)
	return g.wait(ctx, key, call)
}

func (g *coalescedLoadGroup) run(
	key string,
	call *coalescedLoadCall,
	load func(context.Context) (any, error),
) {
	value, err := load(call.ctx)
	if cause := context.Cause(call.ctx); cause != nil {
		value = nil
		err = cause
	}

	g.mu.Lock()
	call.value = value
	call.err = err
	call.finished = true
	if g.calls[key] == call {
		delete(g.calls, key)
	}
	close(call.done)
	g.mu.Unlock()

	call.cancel(context.Canceled)
	g.active.Done()
}

func (g *coalescedLoadGroup) wait(
	ctx context.Context,
	key string,
	call *coalescedLoadCall,
) (any, error) {
	select {
	case <-ctx.Done():
		g.releaseWaiter(key, call)
		return nil, ctx.Err()
	case <-call.done:
		g.releaseWaiter(key, call)
		return call.value, call.err
	}
}

func (g *coalescedLoadGroup) releaseWaiter(key string, call *coalescedLoadCall) {
	g.mu.Lock()
	defer g.mu.Unlock()
	call.waiters--
	if call.waiters == 0 && !call.finished {
		if g.calls[key] == call {
			delete(g.calls, key)
		}
		call.cancel(context.Canceled)
	}
}

// Close prevents new loads, cancels active loads, and waits until they can no
// longer access resources owned by the reader.
func (g *coalescedLoadGroup) Close(err error) {
	if err == nil {
		err = context.Canceled
	}
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		g.closeErr = err
		for _, call := range g.calls {
			call.cancel(err)
		}
	}
	g.mu.Unlock()
	g.active.Wait()
}
