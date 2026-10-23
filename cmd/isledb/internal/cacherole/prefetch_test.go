package cacherole

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
)

// prefetchFixture is a database of the given prefix and a cache over its
// bucket, with a disk budget of diskBytes.
type prefetchFixture struct {
	store    *blobstore.Store
	manifest *manifest.Store
	objects  *readcache.Objects
	disk     *diskcache.Cache
	seq      uint64
}

func newPrefetchFixture(t *testing.T, diskBytes int64) *prefetchFixture {
	t.Helper()
	ctx := context.Background()
	bucket, err := blobstore.Open(ctx, "file://"+t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	disk, err := diskcache.Open(diskcache.Options{Dir: t.TempDir(), MaxBytes: diskBytes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	objects, err := readcache.NewObjects(bucket, disk, readcache.ObjectsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := blobstore.New(bucket.Bucket(), "", "prod/a")
	f := &prefetchFixture{store: store, manifest: manifest.NewStore(store), objects: objects, disk: disk}
	if _, err := f.manifest.ClaimWriter(ctx, "test-writer"); err != nil {
		t.Fatal(err)
	}
	return f
}

// addSST stores an object of size bytes and adds it to L0.
func (f *prefetchFixture) addSST(t *testing.T, id string, size int) string {
	t.Helper()
	ctx := context.Background()
	key := f.store.SSTPath(id)
	if _, err := f.store.Write(ctx, key, bytes.Repeat([]byte{byte(len(id))}, size)); err != nil {
		t.Fatal(err)
	}
	f.seq++
	if _, err := f.manifest.AppendAddSSTableWithFence(ctx, manifest.SSTMeta{
		ID: id, Epoch: 1, SeqLo: f.seq, SeqHi: f.seq, Size: int64(size), CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	return key
}

// current returns the bytes of the database's CURRENT, as the relay hands them.
func (f *prefetchFixture) current(t *testing.T) []byte {
	t.Helper()
	data, _, err := manifest.NewBlobStoreBackend(f.store).ReadCurrent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (f *prefetchFixture) prefetcher(relay *readcache.Relay) *prefetcher {
	return &prefetcher{relay: relay, manifest: manifest.NewStore(f.store), store: f.store,
		objects: f.objects, disk: f.disk, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestPrefetchReadsVisibleSSTsOnce(t *testing.T) {
	f := newPrefetchFixture(t, 64<<20)
	small := f.addSST(t, "000001.sst", 300<<10)
	large := f.addSST(t, "000002.sst", 3<<20)
	p := f.prefetcher(nil)

	if err := p.pass(context.Background(), f.current(t)); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{small, large} {
		size, _ := f.objects.Size(context.Background(), key)
		if !f.objects.Cached(key, size) {
			t.Fatalf("%s is not on disk after a pass", key)
		}
	}
	reads := f.objects.Stats().StorageReads
	if reads == 0 {
		t.Fatal("a pass read nothing from storage")
	}

	if err := p.pass(context.Background(), f.current(t)); err != nil {
		t.Fatal(err)
	}
	if got := f.objects.Stats().StorageReads; got != reads {
		t.Fatalf("a second pass made %d more storage reads, want none", got-reads)
	}

	// After a restart nothing already on disk is read again.
	if err := f.prefetcher(nil).pass(context.Background(), f.current(t)); err != nil {
		t.Fatal(err)
	}
	if got := f.objects.Stats().StorageReads; got != reads {
		t.Fatalf("a restarted prefetcher made %d more storage reads, want none", got-reads)
	}
}

func TestPrefetchSkipsDatabaseLargerThanDisk(t *testing.T) {
	f := newPrefetchFixture(t, 4<<20)
	small := f.addSST(t, "000001.sst", 300<<10)
	f.addSST(t, "000002.sst", 5<<20)
	if err := f.prefetcher(nil).pass(context.Background(), f.current(t)); err != nil {
		t.Fatal(err)
	}
	if size, _ := f.objects.Size(context.Background(), small); f.objects.Cached(small, size) {
		t.Fatal("an SST of a database larger than the disk was prefetched")
	}
	if got := f.objects.Stats().StorageReads; got != 0 {
		t.Fatalf("prefetch made %d storage reads for a database larger than the disk", got)
	}
}

func TestPrefetchEvictsReplacedSSTs(t *testing.T) {
	ctx := context.Background()
	f := newPrefetchFixture(t, 4<<20)
	old := f.addSST(t, "000001.sst", 3<<20)
	p := f.prefetcher(nil)
	if err := p.pass(ctx, f.current(t)); err != nil {
		t.Fatal(err)
	}
	oldSize, _ := f.objects.Size(ctx, old)
	if !f.objects.Cached(old, oldSize) {
		t.Fatal("the first SST was not prefetched")
	}

	// A merge replaces it, and the disk has no room for both.
	replacement := f.store.SSTPath("000002.sst")
	if _, err := f.store.Write(ctx, replacement, bytes.Repeat([]byte{2}, 3<<20)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manifest.AppendWriterL0Merge(ctx, manifest.WriterL0Merge{
		ID:       "merge-1",
		InputIDs: []string{"000001.sst"},
		Output:   manifest.SSTMeta{ID: "000002.sst", Epoch: 1, SeqLo: 1, SeqHi: 1, Size: 3 << 20, CreatedAt: time.Now()},
		Retired:  []manifest.RetiredObject{{Kind: manifest.RetiredObjectSST, ID: "000001.sst", Key: old, Size: 3 << 20}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.pass(ctx, f.current(t)); err != nil {
		t.Fatal(err)
	}
	size, _ := f.objects.Size(ctx, replacement)
	if !f.objects.Cached(replacement, size) {
		t.Fatal("the replacement was not prefetched into a full disk")
	}
	if f.objects.Cached(old, oldSize) {
		t.Fatal("the replaced SST is still on disk")
	}
}

func TestPrefetchFollowsTheRelay(t *testing.T) {
	f := newPrefetchFixture(t, 64<<20)
	f.addSST(t, "000001.sst", 200<<10)
	relay := readcache.NewRelay(manifest.NewBlobStoreBackend(f.store), 10*time.Millisecond)
	ctx, stop := context.WithCancel(context.Background())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan struct{}, 2)
	go func() { relay.Run(ctx, log); done <- struct{}{} }()
	go func() { f.prefetcher(relay).run(ctx); done <- struct{}{} }()
	defer func() {
		stop()
		<-done
		<-done
	}()

	key := f.addSST(t, "000002.sst", 500<<10)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if size, err := f.objects.Size(ctx, key); err == nil && f.objects.Cached(key, size) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("an SST added after the prefetcher started was not prefetched")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPrefetchRetriesMissedSSTWithoutNewCurrent(t *testing.T) {
	retry := prefetchRetry
	prefetchRetry = 20 * time.Millisecond
	t.Cleanup(func() { prefetchRetry = retry })

	f := newPrefetchFixture(t, 64<<20)
	// The manifest lists an SST whose object is not readable yet.
	key := f.store.SSTPath("000001.sst")
	f.seq++
	if _, err := f.manifest.AppendAddSSTableWithFence(context.Background(), manifest.SSTMeta{
		ID: "000001.sst", Epoch: 1, SeqLo: f.seq, SeqHi: f.seq, Size: 200 << 10, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	relay := readcache.NewRelay(manifest.NewBlobStoreBackend(f.store), 10*time.Millisecond)
	ctx, stop := context.WithCancel(context.Background())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan struct{}, 2)
	go func() { relay.Run(ctx, log); done <- struct{}{} }()
	p := f.prefetcher(relay)
	go func() { p.run(ctx); done <- struct{}{} }()
	defer func() {
		stop()
		<-done
		<-done
	}()

	for p.failures.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := f.store.Write(ctx, key, bytes.Repeat([]byte{1}, 200<<10)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if size, err := f.objects.Size(ctx, key); err == nil && f.objects.Cached(key, size) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("an SST missed by a pass was not read again while CURRENT stayed put")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPrefetchSizesSSTsFromTheManifest(t *testing.T) {
	ctx := context.Background()
	f := newPrefetchFixture(t, 64<<20)
	// An SST followed by a Bloom sidecar and its trailer, as writers store it.
	const sstBytes, bloomBytes = 300 << 10, 4 << 10
	meta := manifest.SSTMeta{ID: "000001.sst", Epoch: 1, SeqLo: 1, SeqHi: 1, Size: sstBytes, CreatedAt: time.Now(),
		Bloom: manifest.BloomMeta{Offset: sstBytes, Length: bloomBytes}}
	key := f.store.SSTPath(meta.ID)
	if _, err := f.store.Write(ctx, key, bytes.Repeat([]byte{1}, int(meta.ObjectSize()))); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manifest.AppendAddSSTableWithFence(ctx, meta); err != nil {
		t.Fatal(err)
	}

	if err := f.prefetcher(nil).pass(ctx, f.current(t)); err != nil {
		t.Fatal(err)
	}
	// Cached under the size a reader asks for.
	if !f.objects.Cached(key, meta.ObjectSize()) {
		t.Fatal("the SST is not on disk under the size readers ask for")
	}
	stats := f.objects.Stats()
	if stats.SizeLookups != 0 {
		t.Fatalf("prefetch asked storage for %d sizes, want none", stats.SizeLookups)
	}

	// A restarted prefetcher asks storage for nothing.
	if err := f.prefetcher(nil).pass(ctx, f.current(t)); err != nil {
		t.Fatal(err)
	}
	if got := f.objects.Stats(); got.StorageReads != stats.StorageReads || got.SizeLookups != 0 {
		t.Fatalf("a restarted prefetcher made %d storage reads and %d size lookups, want none",
			got.StorageReads-stats.StorageReads, got.SizeLookups)
	}
}

func TestPrefetchChecksEverySSTAfterAPause(t *testing.T) {
	ctx := context.Background()
	f := newPrefetchFixture(t, 4<<20)
	f.addSST(t, "000001.sst", 1<<20)
	p := f.prefetcher(nil)
	if err := p.pass(ctx, f.current(t)); err != nil {
		t.Fatal(err)
	}
	if len(p.done) != 1 {
		t.Fatalf("done holds %d SSTs, want 1", len(p.done))
	}
	f.addSST(t, "000002.sst", 4<<20)
	if err := p.pass(ctx, f.current(t)); err != nil {
		t.Fatal(err)
	}
	if !p.paused.Load() || p.done != nil {
		t.Fatalf("after a pass too large: paused %v, done %v; want paused and done cleared", p.paused.Load(), p.done)
	}
}
