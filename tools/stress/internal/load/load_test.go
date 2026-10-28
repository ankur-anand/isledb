package load

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/client"
	"github.com/ankur-anand/isledb/tools/stress/internal/ledger"
	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

// fakeWriter answers each write with the next of its scripted answers,
// then commits everything after them.
type fakeWriter struct {
	mu      sync.Mutex
	script  []func(w http.ResponseWriter, r *http.Request)
	writes  []time.Time
	rev     int
	pending int // status answers left saying pending
}

func (f *fakeWriter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodGet {
		if f.pending > 0 {
			f.pending--
			_, _ = io.WriteString(w, `{"status":"pending"}`)
			return
		}
		f.rev++
		_, _ = io.WriteString(w, `{"status":"committed","revision":"`+strconv.Itoa(f.rev)+`"}`)
		return
	}
	_, _ = io.Copy(io.Discard, r.Body)
	f.writes = append(f.writes, time.Now())
	if len(f.script) > 0 {
		next := f.script[0]
		f.script = f.script[1:]
		next(w, r)
		return
	}
	f.rev++
	_ = json.NewEncoder(w).Encode(map[string]string{"write_id": "1." + strconv.Itoa(f.rev), "revision": strconv.Itoa(f.rev)})
}

func setup(t *testing.T, f *fakeWriter, rounds uint64, quarantine time.Duration) (*Loader, *ledger.Ledger) {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	d, _ := workload.ParseSizeDist("fixed:32")
	s, err := workload.NewSchedule(4, 4, 1, 0, d) // one batch a round
	if err != nil {
		t.Fatal(err)
	}
	shape, _ := workload.NewShape("random", 4, 1)
	l := ledger.New()
	ld, err := New(Config{Schedule: s, Shape: shape, Ledger: l, Writer: client.New(srv.URL, ""), From: 0, To: rounds,
		Workers: 2, Quarantine: quarantine})
	if err != nil {
		t.Fatal(err)
	}
	return ld, l
}

func run(t *testing.T, ld *Loader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ld.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNotCommittedWaitsBeforeResending(t *testing.T) {
	f := &fakeWriter{script: []func(http.ResponseWriter, *http.Request){
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"not_committed","write_id":"1.0"}`)
		},
	}}
	ld, l := setup(t, f, 1, time.Minute)
	run(t, ld)
	if len(f.writes) != 2 || f.writes[1].Sub(f.writes[0]) < time.Second {
		t.Fatalf("%d writes, %v apart; want the resend after the 1s Retry-After", len(f.writes), f.writes)
	}
	if ld.C.NotSaved.Load() != 1 || l.Get(0).Revision == 0 {
		t.Fatalf("not saved %d, batch 0 %+v", ld.C.NotSaved.Load(), l.Get(0))
	}
}

func TestPendingWritesAreAskedAboutUntilSettled(t *testing.T) {
	f := &fakeWriter{pending: 4, script: []func(http.ResponseWriter, *http.Request){
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusGatewayTimeout)
			_, _ = io.WriteString(w, `{"error":"outcome_unknown","write_id":"1.0"}`)
		},
	}}
	ld, l := setup(t, f, 1, time.Minute)
	run(t, ld)
	if len(f.writes) != 1 {
		t.Fatalf("%d writes: a pending write was sent again instead of asked about", len(f.writes))
	}
	if e := l.Get(0); e.Revision == 0 || e.MaybeEarlier || ld.C.Unresolved.Load() != 0 {
		t.Fatalf("batch 0 %+v, unresolved %d; want committed from its status", e, ld.C.Unresolved.Load())
	}
}

func TestLostWritesHoldTheNextRound(t *testing.T) {
	const quarantine = 400 * time.Millisecond
	f := &fakeWriter{script: []func(http.ResponseWriter, *http.Request){
		func(w http.ResponseWriter, _ *http.Request) {
			// The connection drops before any answer: no write ID.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		},
	}}
	ld, l := setup(t, f, 2, quarantine)
	run(t, ld)
	// Round 0: the lost attempt and its resend; round 1: one write, held
	// back until the lost attempt's quarantine has passed.
	if len(f.writes) != 3 {
		t.Fatalf("%d writes, want 3", len(f.writes))
	}
	if gap := f.writes[2].Sub(f.writes[0]); gap < quarantine {
		t.Fatalf("round 1 began %s after a lost round-0 write, within the %s quarantine", gap, quarantine)
	}
	if e := l.Get(0); !e.MaybeEarlier || e.Revision == 0 {
		t.Fatalf("batch 0 %+v: want committed, maybe earlier", e)
	}
}

func TestLiveBytesFollowEachKeysNewestState(t *testing.T) {
	d, _ := workload.ParseSizeDist("mixed")
	s, _ := workload.NewSchedule(300, 7, 2, 300, d)
	shape, _ := workload.NewShape("random", 300, 2)
	ld, err := New(Config{Schedule: s, Shape: shape, Ledger: ledger.New(), Writer: client.New("http://unused", "")})
	if err != nil {
		t.Fatal(err)
	}
	per := s.BatchesPerRound()
	for no := range 3 * per {
		ld.C.Live.Add(ld.liveDelta(s.Batch(no)))
	}
	var want int64
	for idx := range s.N {
		if st := s.After(2, idx); st.Present {
			want += int64(16 + d.Size(idx, 2))
		}
	}
	if got := ld.C.Live.Load(); got != want {
		t.Fatalf("live bytes after three rounds %d, want %d", got, want)
	}
}

func TestLiveBytesFollowPartialRounds(t *testing.T) {
	d, _ := workload.ParseSizeDist("mixed")
	s, err := workload.NewScheduleWith(400, 5, 2, 300, d, workload.Options{UpdatePermille: 150, InsertsPerRound: 60, Rounds: 4})
	if err != nil {
		t.Fatal(err)
	}
	shape, _ := workload.NewShape("random", s.MaxKeys(), 2)
	ld, err := New(Config{Schedule: s, Shape: shape, Ledger: ledger.New(), Writer: client.New("http://unused", "")})
	if err != nil {
		t.Fatal(err)
	}
	for no := range s.FirstBatch(5) {
		ld.C.Live.Add(ld.liveDelta(s.Batch(no)))
	}
	var want int64
	for idx := range s.MaxKeys() {
		if r, ok := s.Last(4, idx); ok {
			if st := s.After(r, idx); st.Present {
				want += int64(16 + d.Size(idx, r))
			}
		}
	}
	if got := ld.C.Live.Load(); got != want {
		t.Fatalf("live bytes after four partial rounds %d, want %d", got, want)
	}
}
