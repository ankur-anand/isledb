// Command cachebench measures the read cache against S3-compatible storage,
// with the requests IsleDB readers make, each sent once straight to storage
// and once through the cache, cold and then warm.
//
//	cachebench -bucket-url 's3://isledb?endpoint=http://127.0.0.1:9100&region=us-east-1&use_path_style=true'
//
// Point the bucket at latproxy to add network latency and count the requests
// that reach storage.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
	_ "gocloud.dev/blob/s3blob"
)

const chunk = 128 << 10

type options struct {
	bucketURL, prefix, statsURL string
	largeObjects, largeSize     int
	smallObjects, smallSize     int
	changeObjects, changeSize   int
	clients, herdClients, herds int
	diskBytes                   int64
}

// read is one request a reader makes: a range of an object whose size it
// knows.
type read struct {
	key           string
	size          int64
	offset, n     int64
	whole, noFill bool
}

type workload struct {
	name  string
	reads []read
}

func main() {
	var o options
	flag.StringVar(&o.bucketURL, "bucket-url", "s3://isledb?endpoint=http://127.0.0.1:9100&region=us-east-1&use_path_style=true", "bucket, through latproxy")
	flag.StringVar(&o.prefix, "prefix", "cachebench", "prefix the objects are written under")
	flag.StringVar(&o.statsURL, "stats-url", "http://127.0.0.1:9100/_latproxy/stats", "latproxy stats; empty to skip")
	flag.IntVar(&o.largeObjects, "large-objects", 8, "large SSTs, read in chunks")
	flag.IntVar(&o.largeSize, "large-size", 32<<20, "bytes per large SST")
	flag.IntVar(&o.smallObjects, "small-objects", 64, "small SSTs, read whole")
	flag.IntVar(&o.smallSize, "small-size", 1<<20, "bytes per small SST")
	flag.IntVar(&o.changeObjects, "change-objects", 4, "change batches, read by block")
	flag.IntVar(&o.changeSize, "change-size", 8<<20, "bytes per change batch")
	flag.IntVar(&o.clients, "clients", 64, "concurrent clients")
	flag.IntVar(&o.herdClients, "herd-clients", 256, "clients asking for one cold chunk at once")
	flag.IntVar(&o.herds, "herds", 20, "cold chunks asked for by a herd")
	flag.Int64Var(&o.diskBytes, "disk-bytes", 2<<30, "cache disk budget")
	flag.Parse()
	if err := run(context.Background(), o); err != nil {
		log.Fatal(err)
	}
}

// extraRuns are measured after the rest, each by a file of its own, and
// given the large SSTs.
var extraRuns []func(ctx context.Context, o options, store *blobstore.Store, large []object, rng *mrand.Rand) error

func run(ctx context.Context, o options) error {
	store, err := blobstore.Open(ctx, o.bucketURL, o.prefix)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	large, small, changes, err := setup(ctx, store, o)
	if err != nil {
		return err
	}
	rng := mrand.New(mrand.NewPCG(1, 2))
	workloads := []workload{
		{"lookup: one 128 KiB chunk", lookups(large)},
		{"scan: 4 MiB runs", scans(large)},
		{"small SST, whole", wholes(small)},
		{"change blocks, 8-64 KiB unaligned", blocks(changes, rng)},
	}

	fmt.Printf("storage: %s (prefix %s)\n", o.bucketURL, o.prefix)
	fmt.Printf("clients: %d; the cache runs in this process, reached over loopback\n\n", o.clients)
	fmt.Println("| workload | path | requests | p50 | p90 | p99 | max | MB/s | storage requests |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|")
	for _, w := range workloads {
		if len(w.reads) == 0 {
			continue
		}
		if err := measureWorkload(ctx, o, store, w, rng); err != nil {
			return fmt.Errorf("%s: %w", w.name, err)
		}
	}
	fmt.Println()
	if o.herds > 0 && len(large) > 0 {
		if err := measureHerds(ctx, o, store, large, rng); err != nil {
			return err
		}
	}
	for _, extra := range extraRuns {
		if err := extra(ctx, o, store, large, rng); err != nil {
			return err
		}
	}
	return nil
}

func measureWorkload(ctx context.Context, o options, store *blobstore.Store, w workload, rng *mrand.Rand) error {
	shuffle(rng, w.reads)
	r, err := measure(ctx, o, w.reads, func(ctx context.Context, rd read) error { return direct(ctx, store, rd) })
	if err != nil {
		return err
	}
	r.print(w.name, "storage")

	c, err := startCache(o, store, idleRelays(o, store))
	if err != nil {
		return err
	}
	defer c.close()
	shuffle(rng, w.reads)
	r, err = measure(ctx, o, w.reads, c.get)
	if err != nil {
		return err
	}
	r.print(w.name, "cache, cold")
	shuffle(rng, w.reads)
	r, err = measure(ctx, o, w.reads, c.get)
	if err != nil {
		return err
	}
	r.print(w.name, "cache, warm")
	return nil
}

// measureHerds asks, herdClients at once, for one cold chunk, herds times.
func measureHerds(ctx context.Context, o options, store *blobstore.Store, large []object, rng *mrand.Rand) error {
	c, err := startCache(o, store, idleRelays(o, store))
	if err != nil {
		return err
	}
	defer c.close()
	reads := lookups(large)
	shuffle(rng, reads)
	reads = reads[:min(o.herds, len(reads))]
	before := storageRequests(o)
	var all []time.Duration
	start := time.Now()
	for _, rd := range reads {
		times := make([]time.Duration, o.herdClients)
		errs := make(chan error, o.herdClients)
		var wg sync.WaitGroup
		for i := range o.herdClients {
			wg.Go(func() {
				t := time.Now()
				if err := c.get(ctx, rd); err != nil {
					errs <- err
				}
				times[i] = time.Since(t)
			})
		}
		wg.Wait()
		close(errs)
		if err := <-errs; err != nil {
			return err
		}
		all = append(all, times...)
	}
	r := result{times: all, wall: time.Since(start), storage: storageRequests(o) - before}
	fmt.Printf("herd: %d clients at once on one cold 128 KiB chunk, %d times\n\n", o.herdClients, len(reads))
	fmt.Println("| requests | p50 | p90 | p99 | max | storage requests | per herd |")
	fmt.Println("|---|---|---|---|---|---|---|")
	fmt.Printf("| %d | %s | %s | %s | %s | %s | %s |\n", len(all), r.pct(50), r.pct(90), r.pct(99), r.pct(100),
		r.storageText(), perHerd(r.storage, len(reads)))
	s := c.objects.Stats()
	fmt.Printf("\ncache: %d chunks read from storage, %d served by another request's read\n", s.ChunkMisses, s.ChunkShared)
	return nil
}

func perHerd(n int64, herds int) string {
	if n < 0 {
		return "–"
	}
	return strconv.FormatFloat(float64(n)/float64(herds), 'f', 1, 64)
}

type object struct {
	key  string
	size int64
}

// setup writes the objects, or finds them from an earlier run.
func setup(ctx context.Context, store *blobstore.Store, o options) (large, small, changes []object, err error) {
	put := func(key string, size int) (object, error) {
		if attrs, err := store.Attributes(ctx, key); err == nil && attrs.Size == int64(size) {
			return object{key, attrs.Size}, nil
		} else if err != nil && !errors.Is(err, blobstore.ErrNotFound) {
			return object{}, err
		}
		data := make([]byte, size)
		_, _ = rand.Read(data)
		if _, err := store.Write(ctx, key, data); err != nil {
			return object{}, err
		}
		return object{key, int64(size)}, nil
	}
	for i := range o.largeObjects {
		obj, err := put(store.SSTPath(fmt.Sprintf("large-%03d.sst", i)), o.largeSize)
		if err != nil {
			return nil, nil, nil, err
		}
		large = append(large, obj)
	}
	for i := range o.smallObjects {
		obj, err := put(store.SSTPath(fmt.Sprintf("small-%03d.sst", i)), o.smallSize)
		if err != nil {
			return nil, nil, nil, err
		}
		small = append(small, obj)
	}
	for i := range o.changeObjects {
		obj, err := put(store.ChangeBatchPath(fmt.Sprintf("batch-%03d", i)), o.changeSize)
		if err != nil {
			return nil, nil, nil, err
		}
		changes = append(changes, obj)
	}
	return large, small, changes, nil
}

// lookups is every aligned chunk of the large SSTs, read once each.
func lookups(large []object) []read {
	var reads []read
	for _, obj := range large {
		for off := int64(0); off < obj.size; off += chunk {
			reads = append(reads, read{key: obj.key, size: obj.size, offset: off, n: min(chunk, obj.size-off)})
		}
	}
	return reads
}

// scans is every aligned 4 MiB run of the large SSTs.
func scans(large []object) []read {
	const run = 32 * chunk
	var reads []read
	for _, obj := range large {
		for off := int64(0); off < obj.size; off += run {
			reads = append(reads, read{key: obj.key, size: obj.size, offset: off, n: min(run, obj.size-off)})
		}
	}
	return reads
}

func wholes(small []object) []read {
	reads := make([]read, len(small))
	for i, obj := range small {
		reads[i] = read{key: obj.key, size: obj.size, n: obj.size, whole: true}
	}
	return reads
}

// blocks cuts each change batch into consecutive blocks of 8 to 64 KiB, as
// compressed change blocks fall.
func blocks(changes []object, rng *mrand.Rand) []read {
	var reads []read
	for _, obj := range changes {
		for off := int64(0); off < obj.size; {
			n := min(8<<10+rng.Int64N(56<<10), obj.size-off)
			reads = append(reads, read{key: obj.key, size: obj.size, offset: off, n: n})
			off += n
		}
	}
	return reads
}

func shuffle(rng *mrand.Rand, reads []read) {
	rng.Shuffle(len(reads), func(i, j int) { reads[i], reads[j] = reads[j], reads[i] })
}

func direct(ctx context.Context, store *blobstore.Store, rd read) error {
	data, err := store.ReadRange(ctx, rd.key, rd.offset, rd.n)
	if err == nil && int64(len(data)) != rd.n {
		err = fmt.Errorf("read %d bytes, want %d", len(data), rd.n)
	}
	return err
}

type cache struct {
	objects *readcache.Objects
	disk    *diskcache.Cache
	dir     string
	server  *http.Server
	url     string
	client  *http.Client
}

// idleRelays serves the bench's own objects, under o.prefix. The relay is
// never run: those workloads read objects only.
func idleRelays(o options, store *blobstore.Store) map[string]*readcache.Relay {
	return map[string]*readcache.Relay{o.prefix: readcache.NewRelay(manifest.NewBlobStoreBackend(store), time.Hour)}
}

// startCache starts a cache in this process serving the databases relays
// names, by prefix, reading any key of store's bucket.
func startCache(o options, store *blobstore.Store, relays map[string]*readcache.Relay) (*cache, error) {
	dir, err := os.MkdirTemp("", "cachebench-*")
	if err != nil {
		return nil, err
	}
	disk, err := diskcache.Open(diskcache.Options{Dir: dir, MaxBytes: o.diskBytes})
	if err != nil {
		return nil, err
	}
	objects, err := readcache.NewObjects(store, disk, readcache.ObjectsOptions{})
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	handler, err := readcache.NewHandler(objects, relays, readcache.MaxLongPoll)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(listener) }()
	conns := max(o.clients, o.herdClients) * 2
	return &cache{
		objects: objects, disk: disk, dir: dir, server: server,
		url:    "http://" + listener.Addr().String(),
		client: &http.Client{Transport: &http.Transport{MaxIdleConns: conns, MaxIdleConnsPerHost: conns}},
	}, nil
}

func (c *cache) get(ctx context.Context, rd read) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+"/v1/object/"+rd.key, nil)
	if err != nil {
		return err
	}
	req.Header.Set(readcache.HeaderObjectSize, strconv.FormatInt(rd.size, 10))
	if !rd.whole {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", rd.offset, rd.offset+rd.n-1))
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	if n != rd.n {
		return fmt.Errorf("read %d bytes, want %d", n, rd.n)
	}
	return nil
}

func (c *cache) close() {
	_ = c.server.Close()
	_ = c.disk.Close()
	_ = os.RemoveAll(c.dir)
}

type result struct {
	times   []time.Duration
	wall    time.Duration
	bytes   int64
	storage int64
}

// measure sends each read once, from o.clients clients.
func measure(ctx context.Context, o options, reads []read, do func(context.Context, read) error) (result, error) {
	before := storageRequests(o)
	times := make([]time.Duration, len(reads))
	var next atomic.Int64
	var firstErr atomic.Pointer[error]
	var bytes atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for range o.clients {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(reads) || firstErr.Load() != nil {
					return
				}
				t := time.Now()
				if err := do(ctx, reads[i]); err != nil {
					firstErr.CompareAndSwap(nil, &err)
					return
				}
				times[i] = time.Since(t)
				bytes.Add(reads[i].n)
			}
		})
	}
	wg.Wait()
	if err := firstErr.Load(); err != nil {
		return result{}, *err
	}
	return result{times: times, wall: time.Since(start), bytes: bytes.Load(), storage: storageRequests(o) - before}, nil
}

func (r result) pct(p int) string {
	s := slices.Clone(r.times)
	slices.Sort(s)
	i := min(len(s)-1, (len(s)*p+99)/100-1)
	return fmtDuration(s[max(i, 0)])
}

func fmtDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.2f ms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
}

func (r result) storageText() string {
	if r.storage < 0 {
		return "–"
	}
	return strconv.FormatInt(r.storage, 10)
}

func (r result) print(name, path string) {
	mbps := float64(r.bytes) / r.wall.Seconds() / 1e6
	fmt.Printf("| %s | %s | %d | %s | %s | %s | %s | %.0f | %s |\n",
		name, path, len(r.times), r.pct(50), r.pct(90), r.pct(99), r.pct(100), mbps, r.storageText())
}

// storageRequests is the requests latproxy has forwarded, or -1 without it.
func storageRequests(o options) int64 {
	stats, ok := latproxyStats(o)
	if !ok {
		return -1
	}
	var total int64
	for _, requests := range stats {
		total += requests
	}
	return total
}

// latproxyStats is the requests latproxy has forwarded, by kind of object.
func latproxyStats(o options) (map[string]int64, bool) {
	if o.statsURL == "" {
		return nil, false
	}
	resp, err := http.Get(o.statsURL)
	if err != nil {
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	var stats map[string]struct {
		Requests int64 `json:"requests"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, false
	}
	requests := make(map[string]int64, len(stats))
	for kind, s := range stats {
		requests[kind] = s.Requests
	}
	return requests, true
}
