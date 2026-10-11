package readcache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
)

// countingSource counts storage requests; gate, when set, holds every range
// read until it is closed, and delay is added to every request.
type countingSource struct {
	*blobstore.Store
	attrs, reads atomic.Int64
	gate         chan struct{}
	fail         atomic.Bool
	delay        time.Duration
}

func (s *countingSource) wait(ctx context.Context) error {
	if s.delay == 0 {
		return nil
	}
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *countingSource) Attributes(ctx context.Context, key string) (blobstore.Attributes, error) {
	s.attrs.Add(1)
	if err := s.wait(ctx); err != nil {
		return blobstore.Attributes{}, err
	}
	return s.Store.Attributes(ctx, key)
}

func (s *countingSource) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.reads.Add(1)
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	if s.gate != nil {
		<-s.gate
	}
	if s.fail.Load() {
		return nil, errors.New("storage unreachable")
	}
	return s.Store.ReadRange(ctx, key, offset, length)
}

func newObjects(t *testing.T, size int) (*Objects, *countingSource, string, []byte) {
	t.Helper()
	store := blobstore.NewMemory("db")
	key := store.SSTPath("000001.sst")
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + i/chunkSize)
	}
	if _, err := store.Write(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}
	disk, err := diskcache.Open(diskcache.Options{Dir: t.TempDir(), MaxBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	src := &countingSource{Store: store}
	return NewObjects(src, disk), src, key, data
}

func readRange(t *testing.T, o *Objects, key string, offset, length int64, fill bool) []byte {
	t.Helper()
	size, err := o.Size(soon(t), key)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := o.WriteRange(soon(t), &buf, key, size, offset, length, fill); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestObjectsServeExactBytesThenFromDisk(t *testing.T) {
	size := 10*chunkSize + 1234
	o, src, key, data := newObjects(t, size)
	rng := rand.New(rand.NewPCG(1, 2))
	ranges := [][2]int64{{0, int64(size)}, {chunkSize - 1, 2}, {int64(size) - 1, 1}, {10 * chunkSize, 1234}}
	for range 50 {
		off := rng.Int64N(int64(size))
		ranges = append(ranges, [2]int64{off, rng.Int64N(int64(size)-off) + 1})
	}
	for _, r := range ranges {
		if got := readRange(t, o, key, r[0], r[1], true); !bytes.Equal(got, data[r[0]:r[0]+r[1]]) {
			t.Fatalf("range [%d, +%d): wrong bytes", r[0], r[1])
		}
	}
	if n := src.reads.Load(); n != 1 {
		t.Fatalf("%d storage reads, want 1: the first range covers the object and the rest come from disk", n)
	}
	if n := src.attrs.Load(); n != 1 {
		t.Fatalf("%d size lookups, want 1", n)
	}
}

func TestObjectsMergeConcurrentMisses(t *testing.T) {
	o, src, key, data := newObjects(t, 4*chunkSize)
	src.gate = make(chan struct{})
	if _, err := o.Size(soon(t), key); err != nil {
		t.Fatal(err)
	}
	const clients = 20
	var wg sync.WaitGroup
	got := make([][]byte, clients)
	for i := range clients {
		wg.Go(func() { got[i] = readRange(t, o, key, chunkSize+10, 100, true) })
	}
	time.Sleep(50 * time.Millisecond)
	close(src.gate)
	wg.Wait()
	for i := range clients {
		if !bytes.Equal(got[i], data[chunkSize+10:chunkSize+110]) {
			t.Fatalf("client %d: wrong bytes", i)
		}
	}
	if n := src.reads.Load(); n != 1 {
		t.Fatalf("%d storage reads for %d concurrent misses of one chunk, want 1", n, clients)
	}
	if s := o.Stats(); s.ChunkMisses != 1 || s.ChunkShared != clients-1 {
		t.Fatalf("stats %+v, want 1 miss and %d shared", s, clients-1)
	}
}

func TestObjectsReadRunsAndBridgeShortGaps(t *testing.T) {
	o, src, key, data := newObjects(t, 40*chunkSize)
	readRange(t, o, key, 2*chunkSize, 3*chunkSize, true) // chunks 2-4
	src.reads.Store(0)

	if got := readRange(t, o, key, 0, 8*chunkSize, true); !bytes.Equal(got, data[:8*chunkSize]) {
		t.Fatal("wrong bytes")
	}
	if n := src.reads.Load(); n != 1 {
		t.Fatalf("%d storage reads for chunks 0-1 and 5-7 around 3 cached ones, want 1", n)
	}

	o3, src3, key3, data3 := newObjects(t, 40*chunkSize)
	if got := readRange(t, o3, key3, 0, 33*chunkSize, true); !bytes.Equal(got, data3[:33*chunkSize]) {
		t.Fatal("wrong bytes")
	}
	if n := src3.reads.Load(); n != 2 {
		t.Fatalf("%d storage reads for 33 cold chunks, want 2: a read is at most %d chunks", n, maxRunChunks)
	}

	o2, src2, key2, _ := newObjects(t, 40*chunkSize)
	readRange(t, o2, key2, 1*chunkSize, 9*chunkSize, true) // chunks 1-9
	src2.reads.Store(0)
	readRange(t, o2, key2, 0, 11*chunkSize, true) // 0 and 10 missing, 9 cached between
	if n := src2.reads.Load(); n != 2 {
		t.Fatalf("%d storage reads around a gap of 9 cached chunks, want 2", n)
	}
}

func TestObjectsNoCacheServesWithoutKeeping(t *testing.T) {
	o, src, key, data := newObjects(t, 3*chunkSize)
	if got := readRange(t, o, key, 0, 3*chunkSize, false); !bytes.Equal(got, data) {
		t.Fatal("wrong bytes")
	}
	readRange(t, o, key, 0, 3*chunkSize, true)
	if n := src.reads.Load(); n != 2 {
		t.Fatalf("%d storage reads, want 2: a no-cache read keeps nothing", n)
	}
	readRange(t, o, key, 0, 3*chunkSize, false)
	if n := src.reads.Load(); n != 2 {
		t.Fatalf("%d storage reads, want a no-cache read served from disk", n)
	}
}

func TestObjectsStorageErrorReachesWaitersAndIsNotKept(t *testing.T) {
	o, src, key, data := newObjects(t, 2*chunkSize)
	size, err := o.Size(soon(t), key)
	if err != nil {
		t.Fatal(err)
	}
	src.fail.Store(true)
	src.gate = make(chan struct{})
	errs := make(chan error, 5)
	for range cap(errs) {
		go func() { errs <- o.WriteRange(soon(t), io.Discard, key, size, 0, 10, true) }()
	}
	time.Sleep(50 * time.Millisecond)
	close(src.gate)
	for range cap(errs) {
		if err := <-errs; err == nil {
			t.Fatal("a read succeeded while storage failed")
		}
	}
	src.fail.Store(false)
	if got := readRange(t, o, key, 0, 10, true); !bytes.Equal(got, data[:10]) {
		t.Fatal("wrong bytes after storage recovered")
	}
}

func TestImmutableKeys(t *testing.T) {
	store := blobstore.NewMemory("sstable")
	for key, want := range map[string]bool{
		store.SSTPath("000001.sst"):            true,
		store.ChangeBatchPath("batch-1"):       true,
		store.ManifestSnapshotPath("s1"):       true,
		store.ManifestPagePath(3, "p1"):        true,
		store.ManifestPath():                   false,
		store.MaintenanceHeadPath():            false,
		"sstable/manifest/CURRENT":             false,
		"db/sstable/000/000001.sst":            false,
		"db/manifest/snapshots/.manifest.zst":  false,
		"db/manifest/pages/x1/p1.page.zst":     false,
		"db/manifest/snapshots/s1.manifest.zs": false,
	} {
		if got := Immutable(key); got != want {
			t.Errorf("Immutable(%q) = %t, want %t", key, got, want)
		}
	}
}

func TestObjectHandler(t *testing.T) {
	o, _, key, data := newObjects(t, 3*chunkSize)
	server := httptest.NewServer(ObjectHandler(o))
	t.Cleanup(server.Close)
	get := func(path, rangeHeader string) (int, string, []byte) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/object/"+path, nil)
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get("Content-Range"), body
	}

	if status, cr, body := get(key, "bytes=100-199"); status != http.StatusPartialContent || cr != "bytes 100-199/393216" || !bytes.Equal(body, data[100:200]) {
		t.Fatalf("range: %d %q %d bytes", status, cr, len(body))
	}
	if status, _, body := get(key, ""); status != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("whole object: %d, %d bytes", status, len(body))
	}
	if status, _, _ := get("db/manifest/CURRENT", ""); status != http.StatusForbidden {
		t.Fatalf("CURRENT: %d, want 403", status)
	}
	if status, _, _ := get(blobstore.NewMemory("db").SSTPath("missing.sst"), ""); status != http.StatusNotFound {
		t.Fatalf("missing object: %d, want 404", status)
	}
	if status, cr, _ := get(key, "bytes=393216-"); status != http.StatusRequestedRangeNotSatisfiable || cr != "bytes */393216" {
		t.Fatalf("range past the end: %d %q, want 416", status, cr)
	}
}

func TestParseRange(t *testing.T) {
	for _, c := range []struct {
		header               string
		offset, length       int64
		partial, satisfiable bool
	}{
		{"", 0, 1000, false, true},
		{"bytes=0-99", 0, 100, true, true},
		{"bytes=900-", 900, 100, true, true},
		{"bytes=900-5000", 900, 100, true, true},
		{"bytes=-10", 990, 10, true, true},
		{"bytes=-5000", 0, 1000, true, true},
		{"bytes=1000-", 0, 0, false, false},
		{"bytes=5-4", 0, 0, false, false},
		{"bytes=0-1,5-6", 0, 0, false, false},
		{"bytes=-0", 0, 0, false, false},
		{"items=0-1", 0, 0, false, false},
	} {
		off, n, partial, ok := parseRange(c.header, 1000)
		if off != c.offset || n != c.length || partial != c.partial || ok != c.satisfiable {
			t.Errorf("parseRange(%q) = %d, %d, %t, %t", c.header, off, n, partial, ok)
		}
	}
}
