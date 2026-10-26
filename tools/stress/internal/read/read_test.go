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
