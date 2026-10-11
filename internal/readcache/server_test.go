package readcache

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func startServer(t *testing.T, src *fakeSource, longPoll time.Duration) (*Relay, *httptest.Server) {
	t.Helper()
	relay := NewRelay(src, time.Hour)
	server := httptest.NewServer(Handler(relay, longPoll))
	t.Cleanup(server.Close)
	return relay, server
}

type answer struct {
	status    int
	position  uint64
	age       time.Duration
	ageHeader bool
	body      string
}

func fetch(t *testing.T, url string) answer {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	a := answer{status: resp.StatusCode, body: string(body)}
	if resp.StatusCode == http.StatusOK {
		a.position, _ = strconv.ParseUint(resp.Header.Get(HeaderPosition), 10, 64)
		ms, _ := strconv.ParseInt(resp.Header.Get(HeaderAgeMillis), 10, 64)
		a.age = time.Duration(ms) * time.Millisecond
		a.ageHeader = resp.Header.Get(HeaderAgeMillis) != ""
	}
	return a
}

func TestServeCurrentWithPositionAndAge(t *testing.T) {
	src := &fakeSource{}
	relay, server := startServer(t, src, time.Second)
	if a := fetch(t, server.URL+"/v1/current"); a.status != http.StatusServiceUnavailable {
		t.Fatalf("before the first read: status %d, want 503", a.status)
	}
	data := currentBytes(t, 9, 1)
	src.set(data, nil)
	if err := relay.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	a := fetch(t, server.URL+"/v1/current")
	if a.status != http.StatusOK || a.position != 9 || a.body != string(data) {
		t.Fatalf("answer %+v, want CURRENT at position 9", a)
	}
	if a.age < 30*time.Millisecond {
		t.Fatalf("age %s, want at least the 30ms since the read", a.age)
	}
}

func TestLongPollBeforeTheFirstReadAnswersAtOnce(t *testing.T) {
	_, server := startServer(t, &fakeSource{}, 2*time.Second)
	start := time.Now()
	if a := fetch(t, server.URL+"/v1/current?after=1"); a.status != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", a.status)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("answered after %s, want at once rather than at the 2s timeout", took)
	}
}

func TestLongPollAnswersOnAdvanceOrTimeout(t *testing.T) {
	src := &fakeSource{}
	relay, server := startServer(t, src, 200*time.Millisecond)
	src.set(currentBytes(t, 3, 1), nil)
	if err := relay.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if a := fetch(t, server.URL+"/v1/current?after=3"); a.status != http.StatusOK || a.position != 3 || a.body != "" {
		t.Fatalf("timed-out long poll: %+v, want position 3 and no body", a)
	}
	if took := time.Since(start); took < 200*time.Millisecond {
		t.Fatalf("long poll answered after %s, want it held for the 200ms timeout", took)
	}

	got := make(chan answer, 1)
	go func() { got <- fetch(t, server.URL+"/v1/current?after=3") }()
	time.Sleep(50 * time.Millisecond)
	src.set(currentBytes(t, 4, 1), nil)
	if err := relay.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case a := <-got:
		if a.position != 4 || a.body != "" || a.ageHeader {
			t.Fatalf("long poll answered %+v, want position 4 only", a)
		}
	case <-time.After(time.Second):
		t.Fatal("long poll did not answer when the position advanced")
	}

	if a := fetch(t, server.URL+"/v1/current?after=x"); a.status != http.StatusBadRequest {
		t.Fatalf("after=x: status %d, want 400", a.status)
	}
}

func TestCeilMillisRoundsUp(t *testing.T) {
	for d, want := range map[time.Duration]int64{
		0:                       0,
		time.Microsecond:        1,
		time.Millisecond:        1,
		1500 * time.Microsecond: 2,
	} {
		if got := ceilMillis(d); got != want {
			t.Errorf("ceilMillis(%s) = %d, want %d", d, got, want)
		}
	}
}
