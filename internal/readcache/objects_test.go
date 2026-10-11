package readcache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"gocloud.dev/blob"
)

// countingSource counts storage requests; gate and attrGate, when set, hold
// range reads and size lookups until closed, and delay is added to every
// request.
type countingSource struct {
	*blobstore.Store
	attrs, reads atomic.Int64
	gate         chan struct{}
	attrGate     chan struct{}
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
	if s.attrGate != nil {
		select {
		case <-s.attrGate:
		case <-ctx.Done():
			return blobstore.Attributes{}, ctx.Err()
		}
	}
	return s.Store.Attributes(ctx, key)
}

func (s *countingSource) ReadRangeStream(ctx context.Context, key string, offset, length int64) (*blob.Reader, error) {
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
	return s.Store.ReadRangeStream(ctx, key, offset, length)
}

func newObjects(t *testing.T, size int) (*Objects, *countingSource, string, []byte) {
	t.Helper()
	return newObjectsIn(t, t.TempDir(), blobstore.NewMemory("db"), size)
}

func newObjectsIn(t *testing.T, dir string, store *blobstore.Store, size int) (*Objects, *countingSource, string, []byte) {
	t.Helper()
	key := store.SSTPath("000001.sst")
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + i/chunkSize)
	}
	if _, err := store.Write(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}
	disk, err := diskcache.Open(diskcache.Options{Dir: dir, MaxBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	src := &countingSource{Store: store}
	return mustObjects(t, src, disk, ObjectsOptions{}), src, key, data
}

func mustObjects(t testing.TB, src ObjectSource, disk *diskcache.Cache, opts ObjectsOptions) *Objects {
	t.Helper()
	o, err := NewObjects(src, disk, opts)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func readRange(t *testing.T, o *Objects, key string, offset, length int64, fill bool) []byte {
	t.Helper()
	size, err := o.Size(soon(t), key)
	if err != nil {
		t.Fatal(err)
	}
	return readSized(t, o, key, size, offset, length, fill)
}

func readSized(t *testing.T, o *Objects, key string, size, offset, length int64, fill bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := o.WriteRange(soon(t), &buf, key, size, offset, length, fill, nil); err != nil {
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
	statuses := make([]CacheStatus, clients)
	for i := range clients {
		wg.Go(func() {
			var buf bytes.Buffer
			ready := func(s CacheStatus) { statuses[i] = s }
			if err := o.WriteRange(soon(t), &buf, key, 4*chunkSize, chunkSize+10, 100, true, ready); err != nil {
				t.Error(err)
			}
			got[i] = buf.Bytes()
		})
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
	count := map[CacheStatus]int{}
	for _, s := range statuses {
		count[s]++
	}
	if count[StatusMiss] != 1 || count[StatusShared] != clients-1 {
		t.Fatalf("statuses %v, want 1 miss and %d shared", count, clients-1)
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
	if s := o.Stats(); s.ChunkBridged != 3 || s.ChunkHits != 0 {
		t.Fatalf("stats %+v, want the 3 chunks read through counted as bridged, not hits", s)
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
		go func() { errs <- o.WriteRange(soon(t), io.Discard, key, size, 0, 10, true, nil) }()
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
	o, src, key, data := newObjects(t, 3*chunkSize)
	server := httptest.NewServer(ObjectHandler(o))
	t.Cleanup(server.Close)
	type answer struct {
		status       int
		contentRange string
		cache        string
		body         []byte
	}
	get := func(path string, headers ...string) answer {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/object/"+path, nil)
		for i := 0; i < len(headers); i += 2 {
			req.Header.Set(headers[i], headers[i+1])
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return answer{resp.StatusCode, resp.Header.Get("Content-Range"), resp.Header.Get(HeaderCache), body}
	}
	sized := func(headers ...string) []string {
		return append([]string{HeaderObjectSize, "393216"}, headers...)
	}

	a := get(key, sized("Range", "bytes=100-199")...)
	if a.status != http.StatusPartialContent || a.contentRange != "bytes 100-199/393216" || a.cache != "miss" || !bytes.Equal(a.body, data[100:200]) {
		t.Fatalf("first range: %d %q %q %d bytes", a.status, a.contentRange, a.cache, len(a.body))
	}
	if a := get(key, sized("Range", "bytes=200-299")...); a.cache != "hit" || !bytes.Equal(a.body, data[200:300]) {
		t.Fatalf("second range: cache %q, want hit", a.cache)
	}
	if n := src.attrs.Load(); n != 0 {
		t.Fatalf("%d size lookups with the size given, want 0", n)
	}
	if a := get(key); a.status != http.StatusOK || !bytes.Equal(a.body, data) {
		t.Fatalf("whole object without a size: %d, %d bytes", a.status, len(a.body))
	}
	reads := src.reads.Load()
	head, _ := http.NewRequest(http.MethodHead, server.URL+"/v1/object/"+key, nil)
	head.Header.Set(HeaderObjectSize, "393216")
	resp, err := http.DefaultClient.Do(head)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "GET" || src.reads.Load() != reads {
		t.Fatalf("HEAD: status %d, Allow %q, %d storage reads; want 405, GET, none", resp.StatusCode, resp.Header.Get("Allow"), src.reads.Load()-reads)
	}
	for _, c := range []struct {
		name    string
		path    string
		headers []string
		status  int
	}{
		{"CURRENT", "db/manifest/CURRENT", nil, http.StatusForbidden},
		{"missing", blobstore.NewMemory("db").SSTPath("missing.sst"), nil, http.StatusNotFound},
		{"missing with size", blobstore.NewMemory("db").SSTPath("missing.sst"), []string{HeaderObjectSize, "10"}, http.StatusNotFound},
		{"bad size", key, []string{HeaderObjectSize, "x"}, http.StatusBadRequest},
		{"zero size", key, []string{HeaderObjectSize, "0"}, http.StatusBadRequest},
		{"size above 5 TiB", key, []string{HeaderObjectSize, "5497558138881"}, http.StatusBadRequest},
		{"largest int64", key, []string{HeaderObjectSize, "9223372036854775807"}, http.StatusBadRequest},
		{"largest int64, last byte", key, []string{HeaderObjectSize, "9223372036854775807", "Range", "bytes=-1"}, http.StatusBadRequest},
		{"wrong size", key, []string{HeaderObjectSize, "393215"}, http.StatusBadRequest},
		{"past the end", key, sized("Range", "bytes=393216-"), http.StatusRequestedRangeNotSatisfiable},
	} {
		if a := get(c.path, c.headers...); a.status != c.status {
			t.Errorf("%s: status %d, want %d", c.name, a.status, c.status)
		}
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

func TestObjectsWrongSizeIsRefusedAndHarmsNothing(t *testing.T) {
	o, src, key, data := newObjects(t, 3*chunkSize+100)
	size := int64(len(data))
	readSized(t, o, key, size, 0, size, true)
	for _, wrong := range []int64{size - 1, size + 1, size + 4*chunkSize} {
		for _, offset := range []int64{0, 3 * chunkSize} {
			err := o.WriteRange(soon(t), io.Discard, key, wrong, offset, 10, true, nil)
			if !errors.Is(err, ErrSizeMismatch) {
				t.Fatalf("size %d at %d: %v, want ErrSizeMismatch", wrong, offset, err)
			}
		}
	}
	reads := src.reads.Load()
	if got := readSized(t, o, key, size, 0, size, true); !bytes.Equal(got, data) {
		t.Fatal("wrong bytes with the right size")
	}
	if n := src.reads.Load() - reads; n != 0 {
		t.Fatalf("%d storage reads with the right size after wrong ones, want 0: the cached chunks are untouched", n)
	}
	if n := src.attrs.Load(); n != 0 {
		t.Fatalf("%d size lookups with the size given, want 0", n)
	}
}

func TestObjectsRefuseSizesAboveTheLimit(t *testing.T) {
	o, src, key, _ := newObjects(t, chunkSize)
	for _, c := range [][2]int64{{MaxObjectSize + 1, 0}, {math.MaxInt64, 0}, {math.MaxInt64, math.MaxInt64 - 1}} {
		if err := o.WriteRange(soon(t), io.Discard, key, c[0], c[1], 1, true, nil); err == nil {
			t.Fatalf("size %d at %d: no error", c[0], c[1])
		}
	}
	if n := src.reads.Load(); n != 0 {
		t.Fatalf("%d storage reads for sizes above the limit, want 0", n)
	}
}

func TestObjectsServeFromDiskAfterRestart(t *testing.T) {
	dir, store := t.TempDir(), blobstore.NewMemory("db")
	key := store.SSTPath("000001.sst")
	data := bytes.Repeat([]byte("isle"), (2*chunkSize+500)/4)
	if _, err := store.Write(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}
	size := int64(len(data))
	open := func() (*Objects, *countingSource, *diskcache.Cache) {
		disk, err := diskcache.Open(diskcache.Options{Dir: dir, MaxBytes: 64 << 20})
		if err != nil {
			t.Fatal(err)
		}
		src := &countingSource{Store: store}
		return mustObjects(t, src, disk, ObjectsOptions{}), src, disk
	}
	o, _, disk := open()
	readSized(t, o, key, size, 0, size, true)
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}

	o, src, disk := open()
	defer disk.Close()
	if got := readSized(t, o, key, size, size-700, 700, true); !bytes.Equal(got, data[size-700:]) {
		t.Fatal("wrong bytes after restart")
	}
	if r, a := src.reads.Load(), src.attrs.Load(); r != 0 || a != 0 {
		t.Fatalf("%d storage reads and %d size lookups after restart, want none", r, a)
	}
}

func TestObjectsSizeLookupOutlivesItsFirstCaller(t *testing.T) {
	o, src, key, data := newObjects(t, chunkSize)
	src.attrGate = make(chan struct{})
	first, cancel := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	go func() {
		_, err := o.Size(first, key)
		firstErr <- err
	}()
	time.Sleep(20 * time.Millisecond) // the first caller starts the lookup
	second := make(chan int64, 1)
	go func() {
		size, err := o.Size(soon(t), key)
		if err != nil {
			t.Error(err)
		}
		second <- size
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller: %v, want its own cancellation", err)
	}
	close(src.attrGate)
	if size := <-second; size != int64(len(data)) {
		t.Fatalf("second caller got size %d, want %d", size, len(data))
	}
	if n := src.attrs.Load(); n != 1 {
		t.Fatalf("%d size lookups, want 1 shared", n)
	}
}

func limitedObjects(t *testing.T, opts ObjectsOptions) (*Objects, *countingSource, []string) {
	t.Helper()
	store := blobstore.NewMemory("db")
	keys := []string{store.SSTPath("000001.sst"), store.SSTPath("000002.sst"), store.SSTPath("000003.sst")}
	for _, key := range keys {
		if _, err := store.Write(context.Background(), key, bytes.Repeat([]byte{7}, maxRunChunks*chunkSize)); err != nil {
			t.Fatal(err)
		}
	}
	disk, err := diskcache.Open(diskcache.Options{Dir: t.TempDir(), MaxBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	src := &countingSource{Store: store}
	return mustObjects(t, src, disk, opts), src, keys
}

func TestObjectsReadLimitQueuesStorageReadsNotHits(t *testing.T) {
	const size = maxRunChunks * chunkSize
	o, src, keys := limitedObjects(t, ObjectsOptions{MaxReadBytes: size})
	readSized(t, o, keys[2], size, 0, size, true)

	src.reads.Store(0)
	src.gate = make(chan struct{})
	done := make(chan error, 2)
	for _, key := range keys[:2] {
		go func() { done <- o.WriteRange(soon(t), io.Discard, key, size, 0, size, true, nil) }()
	}
	time.Sleep(50 * time.Millisecond)
	if n := src.reads.Load(); n != 1 {
		t.Fatalf("%d storage reads under way with room for one, want 1", n)
	}
	if s := o.Stats(); s.StorageQueued != 1 || s.StorageBytesReading != size {
		t.Fatalf("stats %+v, want 1 queued and %d bytes reading", s, size)
	}
	if got := readSized(t, o, keys[2], size, 0, size, true); len(got) != size {
		t.Fatal("a hit waited for the read limit")
	}
	close(src.gate)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if n := src.reads.Load(); n != 2 {
		t.Fatalf("%d storage reads, want 2", n)
	}
}

func TestObjectsReadLimitBelowTheLargestReadIsRefused(t *testing.T) {
	disk, err := diskcache.Open(diskcache.Options{Dir: t.TempDir(), MaxBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	src := &countingSource{Store: blobstore.NewMemory("db")}
	if _, err := NewObjects(src, disk, ObjectsOptions{MaxReadBytes: maxRunChunks*chunkSize - 1}); err == nil {
		t.Fatal("NewObjects accepted a limit below the largest single read")
	}
	if _, err := NewObjects(src, disk, ObjectsOptions{MaxReadWait: -time.Second}); err == nil {
		t.Fatal("NewObjects accepted a negative wait")
	}
}

// holdBudget takes all of o's read budget until the returned func is called.
func holdBudget(t *testing.T, o *Objects) func() {
	t.Helper()
	if err := o.reads.Acquire(soon(t), maxRunChunks*chunkSize); err != nil {
		t.Fatal(err)
	}
	return func() { o.reads.Release(maxRunChunks * chunkSize) }
}

func TestObjectsWaitForBudgetDoesNotShortenTheRead(t *testing.T) {
	old := fetchTimeout
	fetchTimeout = 200 * time.Millisecond
	t.Cleanup(func() { fetchTimeout = old })
	o, src, keys := limitedObjects(t, ObjectsOptions{MaxReadBytes: maxRunChunks * chunkSize, MaxReadWait: 2 * time.Second})
	src.delay = 100 * time.Millisecond
	time.AfterFunc(300*time.Millisecond, holdBudget(t, o))
	if err := o.WriteRange(soon(t), io.Discard, keys[0], maxRunChunks*chunkSize, 0, 1, true, nil); err != nil {
		t.Fatalf("a read that waited 300ms for budget, then took 100ms, failed with a 200ms read timeout: %v", err)
	}
}

func TestObjectsWaitForBudgetIsBounded(t *testing.T) {
	o, _, keys := limitedObjects(t, ObjectsOptions{MaxReadBytes: maxRunChunks * chunkSize, MaxReadWait: 100 * time.Millisecond})
	release := holdBudget(t, o)
	time.AfterFunc(2*time.Second, release)
	start := time.Now()
	err := o.WriteRange(soon(t), io.Discard, keys[0], maxRunChunks*chunkSize, 0, 1, true, nil)
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("queued read: %v after %s, want an error after about 100ms", err, time.Since(start))
	}
}
