package read

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/client"
	"github.com/ankur-anand/isledb/tools/stress/internal/ledger"
	"github.com/ankur-anand/isledb/tools/stress/internal/verify"
	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

func TestScansStartAmongTheShapesKeys(t *testing.T) {
	const n = 1000
	var mu sync.Mutex
	var starts [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		starts = append(starts, []byte(r.URL.Query().Get("start")))
		mu.Unlock()
		w.Header().Set("Isledb-Revision", "1")
		_, _ = io.WriteString(w, `{"items":[]}`)
	}))
	defer srv.Close()
	d, _ := workload.ParseSizeDist("fixed:32")
	s, _ := workload.NewSchedule(n, 10, 1, 0, d)
	shape, _ := workload.NewShape("sequential", n, 1)
	l := ledger.New()
	for no := range s.BatchesPerRound() {
		a, _ := l.Begin(no)
		_ = a.Committed(1)
	}
	r, err := New(Config{Schedule: s, Shape: shape, Ledger: l, Verifier: verify.New(&verify.Model{Schedule: s, Ledger: l}, shape, nil),
		Readers: []*client.Client{client.New(srv.URL, "")}, Workers: 2, ScanRatio: 1, Rate: 500})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r.Run(ctx)
	mu.Lock()
	defer mu.Unlock()
	if len(starts) == 0 {
		t.Fatal("no scans")
	}
	for _, start := range starts {
		if _, ok := shape.Index(start); !ok {
			t.Fatalf("a scan started at %x, not at a key of the shape", start)
		}
	}
}

// Reads are due at their times whether or not workers keep up: one a worker
// takes late keeps its time, and those due more than the lag ago are
// dropped, counted, and never handed out.
func TestArrivalsDropReadsDueTooLongAgo(t *testing.T) {
	a := &arrivals{interval: 100 * time.Millisecond, maxLag: time.Second, next: time.Now().Add(-10 * time.Second)}
	due, dropped, err := a.take(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if dropped < 89 || dropped > 91 {
		t.Fatalf("%d dropped after falling 10s behind with a 1s lag at 10/s, want about 90", dropped)
	}
	if late := time.Since(due); late < 900*time.Millisecond || late > 1100*time.Millisecond {
		t.Fatalf("the read handed out was due %v ago, want about the 1s lag", late)
	}
	// The next is due an interval later, still late, and nothing more drops.
	next, dropped, _ := a.take(context.Background())
	if dropped != 0 || next.Sub(due) != a.interval {
		t.Fatalf("next due %v after, %d dropped; want %v after and none", next.Sub(due), dropped, a.interval)
	}
}

func TestArrivalsWaitForTheirTime(t *testing.T) {
	a := &arrivals{interval: 50 * time.Millisecond, maxLag: time.Second}
	start := time.Now()
	for range 5 {
		if _, dropped, err := a.take(context.Background()); err != nil || dropped != 0 {
			t.Fatalf("dropped %d, %v", dropped, err)
		}
	}
	if took := time.Since(start); took < 190*time.Millisecond {
		t.Fatalf("five reads at 20/s handed out in %v, want about 200ms", took)
	}
}
