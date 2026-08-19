package isledb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCoalescedLoadGroup_CanceledLeaderDoesNotCancelWaiter(t *testing.T) {
	var group coalescedLoadGroup
	started := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int64

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderResult := make(chan error, 1)
	go func() {
		_, err := group.Do(leaderCtx, "shared", func(ctx context.Context) (any, error) {
			loads.Add(1)
			close(started)
			select {
			case <-release:
				return "loaded", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		leaderResult <- err
	}()
	<-started

	waiterResult := make(chan struct {
		value any
		err   error
	}, 1)
	go func() {
		value, err := group.Do(context.Background(), "shared", func(context.Context) (any, error) {
			loads.Add(1)
			return "duplicate", nil
		})
		waiterResult <- struct {
			value any
			err   error
		}{value: value, err: err}
	}()

	waitForCoalescedLoadWaiters(t, &group, "shared", 2)
	cancelLeader()
	if err := <-leaderResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error=%v want context.Canceled", err)
	}

	close(release)
	result := <-waiterResult
	if result.err != nil {
		t.Fatalf("healthy waiter failed after leader cancellation: %v", result.err)
	}
	if result.value != "loaded" {
		t.Fatalf("waiter value=%v want loaded", result.value)
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("loads=%d want=1", got)
	}
}

func TestCoalescedLoadGroup_CloseCancelsAndWaits(t *testing.T) {
	var group coalescedLoadGroup
	started := make(chan struct{})
	stopped := make(chan struct{})
	result := make(chan error, 1)

	go func() {
		_, err := group.Do(context.Background(), "shared", func(ctx context.Context) (any, error) {
			close(started)
			<-ctx.Done()
			close(stopped)
			return nil, ctx.Err()
		})
		result <- err
	}()
	<-started

	closed := make(chan struct{})
	go func() {
		group.Close(ErrReaderClosed)
		close(closed)
	}()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("load context was not canceled by Close")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not wait for the load to stop")
	}
	if err := <-result; !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("load error=%v want ErrReaderClosed", err)
	}
	if _, err := group.Do(context.Background(), "new", func(context.Context) (any, error) {
		return "unexpected", nil
	}); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("Do after Close error=%v want ErrReaderClosed", err)
	}
}

func TestCoalescedLoadGroup_LastCanceledWaiterStopsLoad(t *testing.T) {
	var group coalescedLoadGroup
	started := make(chan struct{})
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)

	go func() {
		_, err := group.Do(ctx, "shared", func(loadCtx context.Context) (any, error) {
			close(started)
			<-loadCtx.Done()
			close(stopped)
			return nil, loadCtx.Err()
		})
		result <- err
	}()
	<-started
	cancel()

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error=%v want context.Canceled", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("load continued after its final waiter canceled")
	}
	group.Close(ErrReaderClosed)
}

func TestCoalescedLoadGroup_OldCompletionDoesNotRemoveReplacement(t *testing.T) {
	var group coalescedLoadGroup
	oldStarted := make(chan struct{})
	oldCanceled := make(chan struct{})
	allowOldReturn := make(chan struct{})

	oldCtx, cancelOld := context.WithCancel(context.Background())
	oldResult := make(chan error, 1)
	go func() {
		_, err := group.Do(oldCtx, "shared", func(loadCtx context.Context) (any, error) {
			close(oldStarted)
			<-loadCtx.Done()
			close(oldCanceled)
			<-allowOldReturn
			return nil, loadCtx.Err()
		})
		oldResult <- err
	}()
	<-oldStarted

	group.mu.Lock()
	oldCall := group.calls["shared"]
	group.mu.Unlock()
	if oldCall == nil {
		t.Fatal("old load was not registered")
	}

	cancelOld()
	if err := <-oldResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("old waiter error=%v want context.Canceled", err)
	}
	<-oldCanceled

	newStarted := make(chan struct{})
	releaseNew := make(chan struct{})
	newResult := make(chan struct {
		value any
		err   error
	}, 1)
	go func() {
		value, err := group.Do(context.Background(), "shared", func(context.Context) (any, error) {
			close(newStarted)
			<-releaseNew
			return "replacement", nil
		})
		newResult <- struct {
			value any
			err   error
		}{value: value, err: err}
	}()
	<-newStarted

	group.mu.Lock()
	newCall := group.calls["shared"]
	group.mu.Unlock()
	if newCall == nil || newCall == oldCall {
		close(releaseNew)
		t.Fatal("replacement load was not registered independently")
	}

	close(allowOldReturn)
	waitForCoalescedLoadFinished(t, &group, oldCall)

	group.mu.Lock()
	replacementPreserved := group.calls["shared"] == newCall
	group.mu.Unlock()
	close(releaseNew)
	result := <-newResult

	if !replacementPreserved {
		t.Fatal("old load completion removed the replacement map entry")
	}
	if result.err != nil || result.value != "replacement" {
		t.Fatalf("replacement result value=%v err=%v", result.value, result.err)
	}
	group.Close(ErrReaderClosed)
}

func TestCoalescedLoadGroup_PreCanceledContextDoesNotStartLoad(t *testing.T) {
	var group coalescedLoadGroup
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var loads atomic.Int64
	_, err := group.Do(ctx, "shared", func(context.Context) (any, error) {
		loads.Add(1)
		return "unexpected", nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do error=%v want context.Canceled", err)
	}
	if got := loads.Load(); got != 0 {
		t.Fatalf("loads=%d want=0", got)
	}
}

func TestCoalescedLoadGroup_CanceledFollowerStopsWaitingOnly(t *testing.T) {
	var group coalescedLoadGroup
	started := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int64

	type result struct {
		value any
		err   error
	}
	leaderResult := make(chan result, 1)
	go func() {
		value, err := group.Do(context.Background(), "shared", func(ctx context.Context) (any, error) {
			loads.Add(1)
			close(started)
			select {
			case <-release:
				return "loaded", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		leaderResult <- result{value: value, err: err}
	}()
	<-started

	followerCtx, cancelFollower := context.WithCancel(context.Background())
	followerResult := make(chan error, 1)
	go func() {
		_, err := group.Do(followerCtx, "shared", func(context.Context) (any, error) {
			loads.Add(1)
			return "duplicate", nil
		})
		followerResult <- err
	}()
	waitForCoalescedLoadWaiters(t, &group, "shared", 2)

	cancelFollower()
	if err := <-followerResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("follower error=%v want context.Canceled", err)
	}

	group.mu.Lock()
	call := group.calls["shared"]
	remaining := 0
	loadCanceled := false
	if call != nil {
		remaining = call.waiters
		loadCanceled = call.ctx.Err() != nil
	}
	group.mu.Unlock()
	if call == nil {
		t.Fatal("follower cancellation removed the call while the leader remained")
	}
	if remaining != 1 {
		t.Fatalf("waiters=%d want=1 after follower cancellation", remaining)
	}
	if loadCanceled {
		t.Fatal("follower cancellation stopped the shared load")
	}

	close(release)
	leader := <-leaderResult
	if leader.err != nil || leader.value != "loaded" {
		t.Fatalf("leader result value=%v err=%v", leader.value, leader.err)
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("loads=%d want=1", got)
	}
	group.Close(ErrReaderClosed)
}

func TestCoalescedLoadGroup_ManyWaitersShareOneLoad(t *testing.T) {
	const waiters = 32

	var group coalescedLoadGroup
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	var loads atomic.Int64
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	type result struct {
		value any
		err   error
	}
	results := make(chan result, waiters)
	start := make(chan struct{})
	for range waiters {
		go func() {
			<-start
			value, err := group.Do(context.Background(), "shared", func(context.Context) (any, error) {
				loads.Add(1)
				startedOnce.Do(func() { close(started) })
				<-release
				return "loaded", nil
			})
			results <- result{value: value, err: err}
		}()
	}
	close(start)
	<-started
	waitForCoalescedLoadWaiters(t, &group, "shared", waiters)
	releaseOnce.Do(func() { close(release) })

	for i := 0; i < waiters; i++ {
		result := <-results
		if result.err != nil || result.value != "loaded" {
			t.Fatalf("waiter %d result value=%v err=%v", i, result.value, result.err)
		}
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("loads=%d want=1", got)
	}
	group.Close(ErrReaderClosed)
}

func TestCoalescedLoadGroup_DifferentKeysLoadConcurrently(t *testing.T) {
	var group coalescedLoadGroup
	aStarted := make(chan struct{})
	bStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	type result struct {
		value any
		err   error
	}
	results := make(chan result, 2)
	start := func(key string, started chan struct{}) {
		go func() {
			value, err := group.Do(context.Background(), key, func(context.Context) (any, error) {
				close(started)
				<-release
				return key, nil
			})
			results <- result{value: value, err: err}
		}()
	}

	start("a", aStarted)
	<-aStarted
	start("b", bStarted)
	select {
	case <-bStarted:
	case <-time.After(time.Second):
		t.Fatal("key b did not start while key a was blocked")
	}
	releaseOnce.Do(func() { close(release) })

	seen := make(map[any]bool, 2)
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("load error=%v", result.err)
		}
		seen[result.value] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("results=%v want keys a and b", seen)
	}
	group.Close(ErrReaderClosed)
}

func TestCoalescedLoadGroup_ErrorIsSharedAndLaterCallRetries(t *testing.T) {
	var group coalescedLoadGroup
	started := make(chan struct{})
	release := make(chan struct{})
	wantErr := errors.New("backend failed")
	var loads atomic.Int64

	results := make(chan error, 2)
	go func() {
		_, err := group.Do(context.Background(), "shared", func(context.Context) (any, error) {
			loads.Add(1)
			close(started)
			<-release
			return nil, wantErr
		})
		results <- err
	}()
	<-started
	go func() {
		_, err := group.Do(context.Background(), "shared", func(context.Context) (any, error) {
			loads.Add(1)
			return "duplicate", nil
		})
		results <- err
	}()
	waitForCoalescedLoadWaiters(t, &group, "shared", 2)
	close(release)

	for i := 0; i < 2; i++ {
		if err := <-results; !errors.Is(err, wantErr) {
			t.Fatalf("waiter %d error=%v want backend error", i, err)
		}
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("loads after shared error=%d want=1", got)
	}

	value, err := group.Do(context.Background(), "shared", func(context.Context) (any, error) {
		loads.Add(1)
		return "retried", nil
	})
	if err != nil || value != "retried" {
		t.Fatalf("retry result value=%v err=%v", value, err)
	}
	if got := loads.Load(); got != 2 {
		t.Fatalf("loads after retry=%d want=2", got)
	}
	group.Close(ErrReaderClosed)
}

func TestCoalescedLoadGroup_CloseNilUsesContextCanceled(t *testing.T) {
	var group coalescedLoadGroup
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := group.Do(context.Background(), "shared", func(ctx context.Context) (any, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		result <- err
	}()
	<-started

	group.Close(nil)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("active load error=%v want context.Canceled", err)
	}
	if _, err := group.Do(context.Background(), "new", func(context.Context) (any, error) {
		return "unexpected", nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Do after Close(nil) error=%v want context.Canceled", err)
	}
}

func TestCoalescedLoadGroup_CloseIsIdempotent(t *testing.T) {
	var group coalescedLoadGroup
	group.Close(ErrReaderClosed)
	group.Close(errors.New("second close"))

	if _, err := group.Do(context.Background(), "new", func(context.Context) (any, error) {
		return "unexpected", nil
	}); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("Do after repeated Close error=%v want first close error", err)
	}
}

func TestCoalescedLoadGroup_SharedContextPreservesValuesWithoutDeadline(t *testing.T) {
	type contextKey string
	const key contextKey = "trace"

	var group coalescedLoadGroup
	parent, cancel := context.WithTimeout(
		context.WithValue(context.Background(), key, "trace-123"),
		time.Hour,
	)
	defer cancel()

	value, err := group.Do(parent, "shared", func(ctx context.Context) (any, error) {
		if _, ok := ctx.Deadline(); ok {
			return nil, errors.New("shared context retained caller deadline")
		}
		return ctx.Value(key), nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if value != "trace-123" {
		t.Fatalf("context value=%v want trace-123", value)
	}
	group.Close(ErrReaderClosed)
}

func TestCoalescedLoadGroup_CloseWaitsForRemovedCall(t *testing.T) {
	var group coalescedLoadGroup
	started := make(chan struct{})
	canceled := make(chan struct{})
	allowReturn := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := group.Do(ctx, "shared", func(loadCtx context.Context) (any, error) {
			close(started)
			<-loadCtx.Done()
			close(canceled)
			<-allowReturn
			return nil, loadCtx.Err()
		})
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error=%v want context.Canceled", err)
	}
	<-canceled

	group.mu.Lock()
	_, registered := group.calls["shared"]
	group.mu.Unlock()
	if registered {
		close(allowReturn)
		t.Fatal("canceled call remained joinable")
	}

	closed := make(chan struct{})
	go func() {
		group.Close(ErrReaderClosed)
		close(closed)
	}()
	waitForCoalescedLoadClosed(t, &group)
	select {
	case <-closed:
		close(allowReturn)
		t.Fatal("Close returned before the removed load goroutine stopped")
	default:
	}

	close(allowReturn)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the removed load stopped")
	}
}

func waitForCoalescedLoadWaiters(
	t *testing.T,
	group *coalescedLoadGroup,
	key string,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		group.mu.Lock()
		call := group.calls[key]
		got := 0
		if call != nil {
			got = call.waiters
		}
		group.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("load %q did not reach %d waiters", key, want)
}

func waitForCoalescedLoadFinished(
	t *testing.T,
	group *coalescedLoadGroup,
	call *coalescedLoadCall,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		group.mu.Lock()
		finished := call.finished
		group.mu.Unlock()
		if finished {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("load did not finish")
}

func waitForCoalescedLoadClosed(t *testing.T, group *coalescedLoadGroup) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		group.mu.Lock()
		closed := group.closed
		group.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("load group did not start closing")
}
