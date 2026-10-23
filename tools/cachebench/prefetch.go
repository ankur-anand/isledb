package main

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
)

// prefetchPoll is how often a prefetch run's relay reads CURRENT.
const prefetchPoll = 50 * time.Millisecond

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// prefetchOptions are the prefetch runs' flags.
type prefetchOptions struct {
	ssts, size, lookups int
	spread              time.Duration
}

var po prefetchOptions

func init() {
	flag.IntVar(&po.ssts, "prefetch-ssts", 8, "new SSTs added in each prefetch run; 0 to skip them")
	flag.IntVar(&po.size, "prefetch-size", 32<<20, "bytes per new SST in the prefetch runs")
	flag.IntVar(&po.lookups, "prefetch-lookups", 2000, "lookups of the new SSTs in the spread prefetch runs")
	flag.DurationVar(&po.spread, "prefetch-spread", 2*time.Second, "time the spread lookups start over")
	extraRuns = append(extraRuns, measurePrefetch)
}

// measurePrefetch adds new SSTs to a database of its own for each run, and
// measures what prefetch costs and what readers of new SSTs get with it on
// and off.
func measurePrefetch(ctx context.Context, o options, bucket *blobstore.Store, large []object, rng *mrand.Rand) error {
	if po.ssts == 0 {
		return nil
	}
	fmt.Println()
	run := fmt.Sprintf("%s-prefetch/%s", o.prefix, time.Now().UTC().Format("20060102-150405"))
	fmt.Printf("prefetch: %d new SSTs of %d MiB per run, databases under %s\n\n", po.ssts, po.size>>20, run)
	if err := measurePrefetchCopy(ctx, o, bucket, run+"/copy"); err != nil {
		return fmt.Errorf("prefetch copy: %w", err)
	}
	fmt.Println()
	fmt.Println("| workload | path | requests | p50 | p90 | p99 | max | MB/s | storage requests |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|")
	for _, mode := range []struct {
		name, path string
		prefetch   bool
		late       bool
	}{
		{"race-off", "cache, no prefetch", false, false},
		{"race-on", "cache, prefetch, readers at once", true, false},
		{"race-late", "cache, prefetch, readers after it", true, true},
	} {
		if err := measurePrefetchRace(ctx, o, bucket, run+"/"+mode.name, mode.path, mode.prefetch, mode.late, rng); err != nil {
			return fmt.Errorf("prefetch %s: %w", mode.name, err)
		}
	}
	for _, mode := range []struct {
		name, path string
		prefetch   bool
	}{
		{"spread-off", "cache, no prefetch", false},
		{"spread-on", "cache, prefetch", true},
	} {
		if err := measurePrefetchSpread(ctx, o, bucket, run+"/"+mode.name, mode.path, mode.prefetch, rng); err != nil {
			return fmt.Errorf("prefetch %s: %w", mode.name, err)
		}
	}
	for _, mode := range []struct {
		name, path string
		prefetch   bool
	}{
		{"busy-off", "cache, cold, no prefetch", false},
		{"busy-on", "cache, cold, prefetch copying", true},
	} {
		if len(large) == 0 {
			break
		}
		if err := measurePrefetchBusy(ctx, o, bucket, run+"/"+mode.name, mode.path, mode.prefetch, large, rng); err != nil {
			return fmt.Errorf("prefetch %s: %w", mode.name, err)
		}
	}
	fmt.Println()
	fmt.Println("new SSTs: every chunk of the run's new SSTs, read once each from the moment CURRENT shows them;")
	fmt.Println("storage requests are SST reads until the readers and prefetch are done.")
	fmt.Printf("spread lookups: %d lookups of the new SSTs, each its index then one 128 KiB chunk, started\n", po.lookups)
	fmt.Printf("evenly over the %s after CURRENT shows them; latency is the pair's.\n", po.spread)
	fmt.Println("lookup while copying: cold lookups of other SSTs while the new SSTs arrive;")
	fmt.Println("storage requests are SST reads while the readers run.")
	return nil
}

// measurePrefetchCopy times prefetch copying new SSTs, with no readers.
func measurePrefetchCopy(ctx context.Context, o options, bucket *blobstore.Store, prefix string) error {
	d, err := newPrefetchDB(ctx, o.bucketURL, prefix)
	if err != nil {
		return err
	}
	defer d.cleanup()
	c, err := startPrefetchCache(ctx, o, bucket, d, nil, true)
	if err != nil {
		return err
	}
	defer c.stop()
	metas, err := d.write(ctx, po.ssts, po.size)
	if err != nil {
		return err
	}
	total := objectBytes(metas)
	before := sstRequests(o)
	start := time.Now()
	if _, err := d.publish(ctx, metas); err != nil {
		return err
	}
	if err := c.waitPrefetched(ctx, d.objects(metas)); err != nil {
		return err
	}
	took := time.Since(start)
	requests := int64(-1)
	if before >= 0 {
		requests = sstRequestsSettled(ctx, o) - before
	}
	fmt.Println("| new SSTs | MiB | time to prefetch | MB/s | storage requests | as 128 KiB misses |")
	fmt.Println("|---|---|---|---|---|---|")
	fmt.Printf("| %d | %d | %s | %.0f | %s | %d |\n", len(metas), total>>20, fmtDuration(took),
		float64(total)/took.Seconds()/1e6, result{storage: requests}.storageText(), (total+chunk-1)/chunk)
	return nil
}

// measurePrefetchRace has readers look up every chunk of new SSTs, from when
// CURRENT shows them, or from when prefetch has copied them if late.
func measurePrefetchRace(ctx context.Context, o options, bucket *blobstore.Store, prefix, path string, prefetch, late bool, rng *mrand.Rand) error {
	d, err := newPrefetchDB(ctx, o.bucketURL, prefix)
	if err != nil {
		return err
	}
	defer d.cleanup()
	c, err := startPrefetchCache(ctx, o, bucket, d, nil, prefetch)
	if err != nil {
		return err
	}
	defer c.stop()
	metas, err := d.write(ctx, po.ssts, po.size)
	if err != nil {
		return err
	}
	reads := lookups(d.objects(metas))
	shuffle(rng, reads)
	before := sstRequests(o)
	position, err := d.publish(ctx, metas)
	if err != nil {
		return err
	}
	if late {
		err = c.waitPrefetched(ctx, d.objects(metas))
	} else {
		err = d.waitVisible(ctx, position)
	}
	if err != nil {
		return err
	}
	r, err := measure(ctx, o, reads, c.get)
	if err != nil {
		return err
	}
	// Prefetch's reads count too, finished or not when the readers were.
	r.storage = -1
	if before >= 0 {
		r.storage = sstRequestsSettled(ctx, o) - before
	}
	r.print("new SSTs: one 128 KiB chunk", path)
	return nil
}

// measurePrefetchSpread has lookups of new SSTs start evenly over
// po.spread from when CURRENT shows them, as lookups reach a
// compaction's outputs: each reads an SST's index, then one chunk of its data.
func measurePrefetchSpread(ctx context.Context, o options, bucket *blobstore.Store, prefix, path string, prefetch bool, rng *mrand.Rand) error {
	d, err := newPrefetchDB(ctx, o.bucketURL, prefix)
	if err != nil {
		return err
	}
	defer d.cleanup()
	c, err := startPrefetchCache(ctx, o, bucket, d, nil, prefetch)
	if err != nil {
		return err
	}
	defer c.stop()
	metas, err := d.write(ctx, po.ssts, po.size)
	if err != nil {
		return err
	}
	type lookup struct {
		at          time.Duration
		index, data read
	}
	lookups := make([]lookup, po.lookups)
	for i := range lookups {
		meta := metas[rng.IntN(len(metas))]
		key, size := d.store.SSTPath(meta.ID), meta.ObjectSize()
		lookups[i] = lookup{
			at:    time.Duration(rng.Int64N(int64(po.spread))),
			index: read{key: key, size: size, offset: meta.MetaOffset, n: size - meta.MetaOffset},
			data:  read{key: key, size: size, offset: rng.Int64N(meta.MetaOffset/chunk) * chunk, n: chunk},
		}
	}
	slices.SortFunc(lookups, func(a, b lookup) int { return cmp.Compare(a.at, b.at) })
	before := sstRequests(o)
	position, err := d.publish(ctx, metas)
	if err != nil {
		return err
	}
	if err := d.waitVisible(ctx, position); err != nil {
		return err
	}
	times := make([]time.Duration, len(lookups))
	var next atomic.Int64
	var firstErr atomic.Pointer[error]
	var bytes atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for range o.clients {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(lookups) || firstErr.Load() != nil {
					return
				}
				time.Sleep(time.Until(start.Add(lookups[i].at)))
				t := time.Now()
				for _, rd := range []read{lookups[i].index, lookups[i].data} {
					if err := c.get(ctx, rd); err != nil {
						firstErr.CompareAndSwap(nil, &err)
						return
					}
					bytes.Add(rd.n)
				}
				times[i] = time.Since(t)
			}
		})
	}
	wg.Wait()
	if err := firstErr.Load(); err != nil {
		return *err
	}
	r := result{times: times, wall: time.Since(start), bytes: bytes.Load(), storage: -1}
	if before >= 0 {
		r.storage = sstRequestsSettled(ctx, o) - before
	}
	r.print(fmt.Sprintf("new SSTs: spread lookups over %s", po.spread), path)
	return nil
}

// measurePrefetchBusy has readers look up the bench's large SSTs cold while
// new SSTs arrive, which prefetch copies if on.
func measurePrefetchBusy(ctx context.Context, o options, bucket *blobstore.Store, prefix, path string, prefetch bool, large []object, rng *mrand.Rand) error {
	d, err := newPrefetchDB(ctx, o.bucketURL, prefix)
	if err != nil {
		return err
	}
	defer d.cleanup()
	c, err := startPrefetchCache(ctx, o, bucket, d, idleRelays(o, bucket), prefetch)
	if err != nil {
		return err
	}
	defer c.stop()
	metas, err := d.write(ctx, po.ssts, po.size)
	if err != nil {
		return err
	}
	reads := lookups(large)
	shuffle(rng, reads)
	position, err := d.publish(ctx, metas)
	if err != nil {
		return err
	}
	if err := d.waitVisible(ctx, position); err != nil {
		return err
	}
	before := sstRequests(o)
	r, err := measure(ctx, o, reads, c.get)
	if err != nil {
		return err
	}
	r.storage = -1
	if before >= 0 {
		r.storage = sstRequests(o) - before
	}
	r.print("lookup while copying", path)
	return nil
}

// prefetchDB is a database of one prefetch run. Its SSTs are written to
// storage first and added to its manifest later, as a writer does.
type prefetchDB struct {
	store    *blobstore.Store
	manifest *manifest.Store
	relay    *readcache.Relay
	seq      uint64
	keys     []string
}

// newPrefetchDB opens the database at prefix of the bucket at bucketURL, as
// its writer.
func newPrefetchDB(ctx context.Context, bucketURL, prefix string) (*prefetchDB, error) {
	store, err := blobstore.Open(ctx, bucketURL, prefix)
	if err != nil {
		return nil, err
	}
	ms := manifest.NewStore(store)
	if _, err := ms.ClaimWriter(ctx, "cachebench"); err != nil {
		_ = store.Close()
		return nil, err
	}
	return &prefetchDB{store: store, manifest: ms, relay: readcache.NewRelay(manifest.NewBlobStoreBackend(store), prefetchPoll)}, nil
}

// write stores n SSTs of size random bytes, not yet in the manifest.
func (d *prefetchDB) write(ctx context.Context, n, size int) ([]manifest.SSTMeta, error) {
	metas := make([]manifest.SSTMeta, 0, n)
	data := make([]byte, size)
	for range n {
		id := fmt.Sprintf("%06d.sst", len(d.keys)+1)
		_, _ = rand.Read(data)
		key := d.store.SSTPath(id)
		if _, err := d.store.Write(ctx, key, data); err != nil {
			return nil, err
		}
		d.keys = append(d.keys, key)
		// The last 1% stands for the index and Bloom filter, as in real SSTs.
		metas = append(metas, manifest.SSTMeta{ID: id, Epoch: 1, Size: int64(size), MetaOffset: int64(size - size/100), CreatedAt: time.Now()})
	}
	return metas, nil
}

// publish adds metas to the manifest and returns CURRENT's position after.
func (d *prefetchDB) publish(ctx context.Context, metas []manifest.SSTMeta) (uint64, error) {
	for _, meta := range metas {
		d.seq++
		meta.SeqLo, meta.SeqHi = d.seq, d.seq
		if _, err := d.manifest.AppendAddSSTableWithFence(ctx, meta); err != nil {
			return 0, err
		}
	}
	current, err := d.manifest.ReadCurrentData(ctx)
	if err != nil {
		return 0, err
	}
	return current.NextSeq, nil
}

// waitVisible returns once the relay's CURRENT is at position: when readers
// asking the cache for CURRENT would see the SSTs published.
func (d *prefetchDB) waitVisible(ctx context.Context, position uint64) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if current, ok := d.relay.Wait(ctx, position-1); !ok || current.Position < position {
		return errors.New("the relay never showed the new SSTs")
	}
	return nil
}

func (d *prefetchDB) objects(metas []manifest.SSTMeta) []object {
	objects := make([]object, len(metas))
	for i, meta := range metas {
		objects[i] = object{key: d.store.SSTPath(meta.ID), size: meta.ObjectSize()}
	}
	return objects
}

// cleanup deletes the run's SSTs; its manifest, a few KiB, stays.
func (d *prefetchDB) cleanup() {
	for _, key := range d.keys {
		_ = d.store.Delete(context.Background(), key)
	}
	_ = d.store.Close()
}

func objectBytes(metas []manifest.SSTMeta) int64 {
	var total int64
	for _, meta := range metas {
		total += meta.ObjectSize()
	}
	return total
}

// prefetchCache is a cache serving a prefetch run's database, with its relay
// running and, if on, a prefetcher.
type prefetchCache struct {
	*cache
	prefetcher *readcache.Prefetcher
	stop       func()
}

func startPrefetchCache(ctx context.Context, o options, bucket *blobstore.Store, d *prefetchDB, others map[string]*readcache.Relay, prefetch bool) (*prefetchCache, error) {
	relays := map[string]*readcache.Relay{d.store.Prefix(): d.relay}
	for prefix, relay := range others {
		relays[prefix] = relay
	}
	c, err := startCache(o, bucket, relays)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { d.relay.Run(runCtx, quiet) })
	pc := &prefetchCache{cache: c}
	if prefetch {
		pc.prefetcher = readcache.NewPrefetcher(d.relay, d.store, c.objects, quiet)
		wg.Go(func() { pc.prefetcher.Run(runCtx) })
	}
	pc.stop = func() {
		cancel()
		wg.Wait()
		c.close()
	}
	return pc, nil
}

// waitPrefetched returns once all of objects is on disk.
func (c *prefetchCache) waitPrefetched(ctx context.Context, objects []object) error {
	deadline := time.Now().Add(10 * time.Minute)
	for _, obj := range objects {
		for !c.objects.Cached(obj.key, obj.size) {
			if c.prefetcher.Paused() {
				return errors.New("prefetch paused: the new SSTs are larger than -disk-bytes")
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("%s not on disk after 10m; prefetch read %d bytes, with %d failures",
					obj.key, c.prefetcher.ReadBytes(), c.prefetcher.Failures())
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	return nil
}

// sstRequestsSettled returns the SST requests latproxy has forwarded once
// they stop growing, so reads still in flight are counted, or -1 without
// latproxy.
func sstRequestsSettled(ctx context.Context, o options) int64 {
	last := sstRequests(o)
	for last >= 0 && ctx.Err() == nil {
		time.Sleep(4 * prefetchPoll)
		now := sstRequests(o)
		if now == last {
			break
		}
		last = now
	}
	return last
}

// sstRequests is the requests for SSTs latproxy has forwarded, or -1
// without it: not CURRENT polls or the rest of the manifest.
func sstRequests(o options) int64 {
	stats, ok := latproxyStats(o)
	if !ok {
		return -1
	}
	return stats["sst"]
}
