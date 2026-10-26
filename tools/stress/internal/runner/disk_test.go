package runner

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiskGuardHoldsAndLetsGo(t *testing.T) {
	var usedPct atomic.Int64
	st := &runState{
		spec:      &Spec{Storage: StorageSpec{Disk: &DiskSpec{Path: "/data", MaxUsedPct: 85}}},
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		diskEvery: 5 * time.Millisecond,
		statDisk: func(string) (uint64, uint64, error) {
			return 100, uint64(100 - usedPct.Load()), nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go st.guardDisk(ctx)
	waitFor := func(held bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for st.diskHeld.Load() != held {
			if time.Now().After(deadline) {
				t.Fatalf("held %v at %d%% used, want %v", !held, usedPct.Load(), held)
			}
			time.Sleep(time.Millisecond)
		}
	}
	usedPct.Store(80)
	time.Sleep(20 * time.Millisecond)
	waitFor(false)
	usedPct.Store(86)
	waitFor(true)
	usedPct.Store(84) // below the limit, but not 2 points below
	time.Sleep(20 * time.Millisecond)
	waitFor(true)
	usedPct.Store(82)
	waitFor(false)
	st.diskMu.Lock()
	defer st.diskMu.Unlock()
	if st.heldBefore <= 0 || st.lastDisk == nil || st.lastDisk.UsedPct != 82 {
		t.Fatalf("held for %s, last %+v", st.heldBefore, st.lastDisk)
	}
}
