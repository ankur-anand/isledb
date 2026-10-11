package readcache

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
)

// benchStorageDelay is a simulated storage round trip, added to every
// storage request; it is not a measurement of any provider.
const benchStorageDelay = 20 * time.Millisecond

type benchEnv struct {
	store   *blobstore.Store
	src     *countingSource
	objects *Objects
	url     string
	client  *http.Client
	next    atomic.Int64
}

func newBenchEnv(b *testing.B, diskBytes int64, delay time.Duration) *benchEnv {
	b.Helper()
	store := blobstore.NewMemory("db")
	disk, err := diskcache.Open(diskcache.Options{Dir: b.TempDir(), MaxBytes: diskBytes})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = disk.Close() })
	src := &countingSource{Store: store, delay: delay}
	objects := mustObjects(b, src, disk, ObjectsOptions{})
	server := httptest.NewServer(ObjectHandler(objects))
	b.Cleanup(server.Close)
	transport := &http.Transport{MaxIdleConns: 2048, MaxIdleConnsPerHost: 2048}
	b.Cleanup(transport.CloseIdleConnections)
	return &benchEnv{store: store, src: src, objects: objects, url: server.URL, client: &http.Client{Transport: transport}}
}

func (e *benchEnv) put(b *testing.B, size int) string {
	b.Helper()
	key := e.store.SSTPath(fmt.Sprintf("%06d.sst", e.next.Add(1)))
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := e.store.Write(context.Background(), key, data); err != nil {
		b.Fatal(err)
	}
	return key
}

func (e *benchEnv) get(key string, offset, length int64, noCache bool) (int, error) {
	req, err := http.NewRequest(http.MethodGet, e.url+"/v1/object/"+key, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(offset+length-1, 10))
	if noCache {
		req.Header.Set(HeaderNoCache, "1")
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	if err == nil && resp.StatusCode == http.StatusPartialContent && n != length {
		err = fmt.Errorf("read %d bytes, want %d", n, length)
	}
	return resp.StatusCode, err
}

func (e *benchEnv) mustGet(b *testing.B, key string, offset, length int64, noCache bool) {
	if status, err := e.get(key, offset, length, noCache); err != nil || status != http.StatusPartialContent {
		b.Fatalf("GET %s [%d, +%d): status %d, %v", key, offset, length, status, err)
	}
}

func (e *benchEnv) report(b *testing.B) {
	b.ReportMetric(float64(e.src.reads.Load())/float64(b.N), "reads/op")
	b.ReportMetric(float64(e.src.attrs.Load())/float64(b.N), "heads/op")
}

// Warm reads served from disk, many clients at once.
func BenchmarkObjectHit(b *testing.B) {
	for _, size := range []int64{4 << 10, chunkSize, 4 << 20} {
		b.Run(byteSize(size), func(b *testing.B) {
			e := newBenchEnv(b, 1<<30, benchStorageDelay)
			const objectSize = 16 << 20
			key := e.put(b, objectSize)
			e.mustGet(b, key, 0, objectSize, false)
			e.src.reads.Store(0)
			e.src.attrs.Store(0)
			b.SetBytes(size)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewPCG(rand.Uint64(), 0))
				for pb.Next() {
					e.mustGet(b, key, rng.Int64N(objectSize-size+1), size, false)
				}
			})
			e.report(b)
		})
	}
}

// Cold reads: the first touch of an object pays a HEAD for its size and a
// read; a new chunk of an object already seen pays the read only.
func BenchmarkObjectMiss(b *testing.B) {
	for _, c := range []struct {
		name      string
		size      int64
		knownSize bool
	}{
		{"first-touch/4KiB", 4 << 10, false},
		{"known-size/4KiB", 4 << 10, true},
		{"first-touch/4MiB", 4 << 20, false},
	} {
		b.Run(c.name, func(b *testing.B) {
			e := newBenchEnv(b, 1<<30, benchStorageDelay)
			const objectSize = 32 << 20
			var key string
			var chunk int64
			b.SetBytes(c.size)
			for b.Loop() {
				if !c.knownSize || key == "" || (chunk+1)*chunkSize > objectSize {
					b.StopTimer()
					key, chunk = e.put(b, objectSize), 0
					if c.knownSize {
						if _, err := e.objects.Size(context.Background(), key); err != nil {
							b.Fatal(err)
						}
					}
					b.StartTimer()
				}
				e.mustGet(b, key, chunk*chunkSize, c.size, false)
				chunk++
			}
			if c.knownSize {
				b.ReportMetric(float64(e.src.reads.Load())/float64(b.N), "reads/op")
			} else {
				e.report(b)
			}
		})
	}
}

// Many clients miss the same chunk at the same moment.
func BenchmarkObjectHerd(b *testing.B) {
	for _, clients := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("clients=%d", clients), func(b *testing.B) {
			e := newBenchEnv(b, 1<<30, benchStorageDelay)
			for b.Loop() {
				b.StopTimer()
				key := e.put(b, chunkSize)
				b.StartTimer()
				var wg sync.WaitGroup
				for range clients {
					wg.Go(func() { e.mustGet(b, key, 100, 4<<10, false) })
				}
				wg.Wait()
			}
			e.report(b)
		})
	}
}

// Every other chunk of a 4 MiB range is cached: the 16 missing ones are
// read in one request through the cached ones.
func BenchmarkObjectBridge(b *testing.B) {
	e := newBenchEnv(b, 1<<30, benchStorageDelay)
	const size = maxRunChunks * chunkSize
	b.SetBytes(size)
	var reads int64
	for b.Loop() {
		b.StopTimer()
		key := e.put(b, size)
		for i := int64(0); i < maxRunChunks; i += 2 {
			e.mustGet(b, key, i*chunkSize, 1, false)
		}
		before := e.src.reads.Load()
		b.StartTimer()
		e.mustGet(b, key, 0, size, false)
		reads += e.src.reads.Load() - before
	}
	b.ReportMetric(float64(reads)/float64(b.N), "reads/op")
}

// A scan of a cold 4 MiB range that asks not to fill the cache: every pass
// goes to storage.
func BenchmarkObjectScanNoCache(b *testing.B) {
	e := newBenchEnv(b, 1<<30, benchStorageDelay)
	const size = 4 << 20
	key := e.put(b, size)
	e.mustGet(b, key, 0, 1, true)
	e.src.reads.Store(0)
	e.src.attrs.Store(0)
	b.SetBytes(size)
	for b.Loop() {
		e.mustGet(b, key, 0, size, true)
	}
	e.report(b)
}

// Random 128 KiB reads over a working set 8x the disk budget.
func BenchmarkObjectEvictionChurn(b *testing.B) {
	const budget, objectSize, objectCount = 8 << 20, 4 << 20, 16
	e := newBenchEnv(b, budget, benchStorageDelay)
	keys := make([]string, objectCount)
	for i := range keys {
		keys[i] = e.put(b, objectSize)
		if _, err := e.objects.Size(context.Background(), keys[i]); err != nil {
			b.Fatal(err)
		}
	}
	e.src.attrs.Store(0)
	b.SetBytes(chunkSize)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewPCG(rand.Uint64(), 0))
		for pb.Next() {
			e.mustGet(b, keys[rng.IntN(objectCount)], rng.Int64N(objectSize/chunkSize)*chunkSize, chunkSize, false)
		}
	})
	b.StopTimer()
	s := e.objects.Stats()
	b.ReportMetric(float64(s.ChunkHits)/float64(s.ChunkHits+s.ChunkMisses+s.ChunkShared), "hit-ratio")
	e.report(b)
}

// A key that does not exist: nothing is remembered, so every request asks
// storage again.
func BenchmarkObjectNotFound(b *testing.B) {
	e := newBenchEnv(b, 1<<30, benchStorageDelay)
	key := e.store.SSTPath("missing.sst")
	for b.Loop() {
		if status, err := e.get(key, 0, 1, false); err != nil || status != http.StatusNotFound {
			b.Fatalf("status %d, %v; want 404", status, err)
		}
	}
	e.report(b)
}

// Requests refused before any storage request.
func BenchmarkObjectForbidden(b *testing.B) {
	e := newBenchEnv(b, 1<<30, benchStorageDelay)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if status, err := e.get("db/manifest/CURRENT", 0, 1, false); err != nil || status != http.StatusForbidden {
				b.Fatalf("status %d, %v; want 403", status, err)
			}
		}
	})
	e.report(b)
}

// Long polls parked on CURRENT, all released by one advance: the time from
// the relay taking the new CURRENT to the last client getting it.
func BenchmarkCurrentFanout(b *testing.B) {
	for _, waiters := range []int{100, 1000} {
		b.Run(fmt.Sprintf("waiters=%d", waiters), func(b *testing.B) {
			src := &fakeSource{}
			src.set(currentBytes(b, 1, 1), nil)
			relay := NewRelay(src, time.Hour)
			if err := relay.Poll(context.Background()); err != nil {
				b.Fatal(err)
			}
			server := httptest.NewServer(Handler(relay, time.Minute))
			b.Cleanup(server.Close)
			transport := &http.Transport{MaxIdleConns: 2 * waiters, MaxIdleConnsPerHost: 2 * waiters}
			b.Cleanup(transport.CloseIdleConnections)
			client := &http.Client{Transport: transport}
			position := uint64(1)
			for b.Loop() {
				b.StopTimer()
				after := position
				var wg sync.WaitGroup
				errs := make(chan error, waiters)
				for range waiters {
					wg.Go(func() {
						resp, err := client.Get(server.URL + "/v1/current?after=" + strconv.FormatUint(after, 10))
						if err != nil {
							errs <- err
							return
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
						if got := resp.Header.Get(HeaderPosition); got != strconv.FormatUint(after+1, 10) {
							errs <- fmt.Errorf("position %s, want %d", got, after+1)
						}
					})
				}
				time.Sleep(100 * time.Millisecond) // let every request park
				position++
				src.set(currentBytes(b, position, 1), nil)
				b.StartTimer()
				if err := relay.Poll(context.Background()); err != nil {
					b.Fatal(err)
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					b.Fatal(err)
				}
			}
		})
	}
}

func byteSize(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMiB", n>>20)
	default:
		return fmt.Sprintf("%dKiB", n>>10)
	}
}
