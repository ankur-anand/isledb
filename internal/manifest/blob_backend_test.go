package manifest

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

// TestBlobStoreBackendCurrentIncrementRace has several writers each read
// CURRENT, increment a counter in it, and write it back conditionally, on the
// in-memory store. Every conditional write that succeeds must have been built
// on the latest CURRENT: the final counter equals the number of successful
// writes. A read that pairs old bytes with a newer object's token would let
// two writers both build on the same value, losing an increment.
func TestBlobStoreBackendCurrentIncrementRace(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("current-increment-race")
	defer store.Close()
	backend := NewBlobStoreBackend(store)
	if _, err := backend.WriteCurrentCAS(ctx, []byte("0"), ""); err != nil {
		t.Fatalf("seed CURRENT: %v", err)
	}

	var successes atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 300 {
				data, etag, err := backend.ReadCurrent(ctx)
				if err != nil {
					t.Errorf("ReadCurrent: %v", err)
					return
				}
				n, err := strconv.Atoi(string(data))
				if err != nil {
					t.Errorf("CURRENT %q: %v", data, err)
					return
				}
				_, err = backend.WriteCurrentCAS(ctx, []byte(strconv.Itoa(n+1)), etag)
				switch {
				case err == nil:
					successes.Add(1)
				case !errors.Is(err, ErrPreconditionFailed):
					t.Errorf("WriteCurrentCAS: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	data, _, err := backend.ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if got, want := string(data), strconv.FormatInt(successes.Load(), 10); got != want {
		t.Fatalf("counter = %s after %s successful conditional writes: a write overwrote a newer CURRENT", got, want)
	}
}
