package diskcache

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func object(seed byte) [32]byte {
	var o [32]byte
	for i := range o {
		o[i] = seed + byte(i)
	}
	return o
}

func content(seed string, size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = seed[i%len(seed)] + byte(i/len(seed))
	}
	return data
}

func openCache(t *testing.T, dir string, metaMax, dataMax int64) *Cache {
	t.Helper()
	c, err := Open(Options{Dir: dir, MetaMaxBytes: metaMax, DataMaxBytes: dataMax})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func put(t *testing.T, c *Cache, k Key, data []byte) {
	t.Helper()
	if err := c.Put(k, data); err != nil {
		t.Fatalf("Put(%v): %v", k, err)
	}
}

func read(c *Cache, k Key, size int64, off, n int) ([]byte, bool) {
	p := make([]byte, n)
	ok := c.ReadAt(k, size, p, int64(off))
	return p, ok
}

func TestPutAndReadAt(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	data := content("chunk", 1000)
	k := Key{Object: object(1), Kind: KindChunk, Index: 7}
	put(t, c, k, data)

	got, ok := read(c, k, 1000, 100, 50)
	if !ok || !bytes.Equal(got, data[100:150]) {
		t.Fatalf("ReadAt = %v, %q", ok, got)
	}
	if _, ok := read(c, k, 1000, 990, 20); ok {
		t.Fatal("read past the entry's end succeeded")
	}
	if !c.Contains(k, 1000) || c.Contains(k, 999) {
		t.Fatal("Contains does not match the expected size")
	}
	if stats := c.Stats(TierData); stats.Hits != 1 || stats.Entries != 1 || stats.Bytes != 1000 {
		t.Fatalf("stats = %+v", stats)
	}
}

// TestWrongSizeIsCorrupt reads an entry expecting another size, as after a
// crash left it short: the entry is dropped and counted as corrupt.
func TestWrongSizeIsCorrupt(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	k := Key{Object: object(2), Kind: KindMeta}
	put(t, c, k, content("meta", 500))
	if _, ok := read(c, k, 600, 0, 10); ok {
		t.Fatal("read of a short entry succeeded")
	}
	if stats := c.Stats(TierMeta); stats.Corruptions != 1 || stats.Entries != 0 {
		t.Fatalf("stats = %+v, want the entry dropped as corrupt", stats)
	}
}

func TestWriteIsReadableBeforeAndAfterStoring(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	data := content("pending", 4096)
	k := Key{Object: object(3), Kind: KindChunk, Index: 1}
	if !c.Write(k, data) {
		t.Fatal("Write dropped")
	}
	if got, ok := read(c, k, 4096, 0, 4096); !ok || !bytes.Equal(got, data) {
		t.Fatal("queued write not readable")
	}
	c.Sync()
	if c.Stats(TierData).Entries != 1 {
		t.Fatal("queued write not stored after Sync")
	}
	if got, ok := read(c, k, 4096, 1000, 10); !ok || !bytes.Equal(got, data[1000:1010]) {
		t.Fatal("stored write not readable")
	}
}

func TestWriteDropsWhenQueueFull(t *testing.T) {
	c, err := Open(Options{Dir: t.TempDir(), MetaMaxBytes: 1 << 20, DataMaxBytes: 1 << 20, QueueSize: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Swap in a queue no writer reads, so it fills; restore it to close.
	running := c.queue
	c.queue = make(chan write, 2)
	defer func() {
		c.queue = running
		_ = c.Close()
	}()

	for i := range 2 {
		if !c.Write(Key{Object: object(4), Kind: KindChunk, Index: uint32(i)}, content("x", 10)) {
			t.Fatalf("write %d dropped with room in the queue", i)
		}
	}
	if c.Write(Key{Object: object(4), Kind: KindChunk, Index: 9}, content("x", 10)) {
		t.Fatal("write queued past the queue's size")
	}
	if got := c.Stats(TierData).Dropped; got != 1 {
		t.Fatalf("Dropped = %d, want 1", got)
	}
}

func TestTierLRUEviction(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 300)
	chunk := func(i uint32) Key { return Key{Object: object(5), Kind: KindChunk, Index: i} }
	put(t, c, chunk(0), content("a", 100))
	put(t, c, chunk(1), content("b", 100))
	put(t, c, chunk(2), content("c", 100))
	if _, ok := read(c, chunk(0), 100, 0, 1); !ok { // 0 is now the most recently used
		t.Fatal("chunk 0 missing")
	}
	put(t, c, chunk(3), content("d", 100))
	if c.Contains(chunk(1), 100) || !c.Contains(chunk(0), 100) {
		t.Fatal("eviction did not take the least recently used entry")
	}

	// The meta tier has its own budget: data churn never evicts it.
	meta := Key{Object: object(5), Kind: KindMeta}
	put(t, c, meta, content("m", 200))
	for i := uint32(10); i < 20; i++ {
		put(t, c, chunk(i), content("e", 100))
	}
	if !c.Contains(meta, 200) {
		t.Fatal("data churn evicted a meta entry")
	}
	if stats := c.Stats(TierData); stats.Bytes > 300 || stats.Evictions == 0 {
		t.Fatalf("data tier stats = %+v", stats)
	}

	// An entry larger than its whole tier is not stored.
	put(t, c, chunk(99), content("big", 301))
	if c.Contains(chunk(99), 301) || c.Stats(TierData).Bypasses != 1 {
		t.Fatal("oversized entry stored")
	}
}

func TestRemovePurgeAndVanishedFiles(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	a := Key{Object: object(6), Kind: KindWhole}
	b := Key{Object: object(7), Kind: KindChunk, Index: 3}
	bloom := Key{Object: object(6), Kind: KindBloom}
	put(t, c, a, content("a", 100))
	put(t, c, b, content("b", 100))
	put(t, c, bloom, content("f", 100))

	c.ReportCorrupt(a)
	if c.Contains(a, 100) || c.Stats(TierData).Corruptions != 1 {
		t.Fatal("ReportCorrupt left the entry or did not count it")
	}

	// A file deleted behind the cache's back reads as a miss and is dropped.
	if err := os.Remove(c.path(b)); err != nil {
		t.Fatalf("remove file: %v", err)
	}
	if _, ok := read(c, b, 100, 0, 10); ok || c.Contains(b, 100) {
		t.Fatal("vanished file still served or indexed")
	}

	c.Purge(TierMeta)
	if c.Contains(bloom, 100) {
		t.Fatal("Purge left a meta-tier entry")
	}
}

func TestRestartKeepsEntriesAndClearsDebris(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MetaMaxBytes: 1 << 20, DataMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	keep := Key{Object: object(8), Kind: KindChunk, Index: 12}
	meta := Key{Object: object(8), Kind: KindMeta}
	put(t, c, keep, content("keep", 300))
	put(t, c, meta, content("meta", 200))
	root := c.root
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Debris a crash or an older version could leave behind.
	empty := Key{Object: object(9), Kind: KindChunk, Index: 1}
	mustWrite := func(path string, data []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	emptyPath := filepath.Join(root, "data", empty.name()[:2], empty.name())
	mustWrite(emptyPath, nil)
	wrongTier := filepath.Join(root, "meta", keep.name()[:2], (Key{Object: object(10), Kind: KindChunk}).name())
	mustWrite(wrongTier, []byte("x"))
	unfinished := filepath.Join(root, incomingName, "entry-123")
	mustWrite(unfinished, []byte("partial"))
	oldLayout := filepath.Join(dir, "v2", "sst", "aa", "file")
	mustWrite(oldLayout, []byte("old"))
	foreign := filepath.Join(dir, "notes.txt")
	mustWrite(foreign, []byte("not ours"))

	c = openCache(t, dir, 1<<20, 1<<20)
	if got, ok := read(c, keep, 300, 0, 300); !ok || !bytes.Equal(got, content("keep", 300)) {
		t.Fatal("chunk lost across restart")
	}
	if !c.Contains(meta, 200) {
		t.Fatal("meta entry lost across restart")
	}
	for _, gone := range []string{emptyPath, wrongTier, unfinished, filepath.Join(dir, "v2")} {
		if _, err := os.Stat(gone); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s survived recovery", gone)
		}
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatal("recovery removed a file that is not the cache's")
	}
}

func TestRestartTrimsToBudgetOldestFirst(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MetaMaxBytes: 1 << 20, DataMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range uint32(3) {
		k := Key{Object: object(11), Kind: KindChunk, Index: i}
		put(t, c, k, content("z", 100))
		old := time.Now().Add(time.Duration(i-3) * time.Hour)
		if err := os.Chtimes(c.path(k), old, old); err != nil {
			t.Fatal(err)
		}
	}
	_ = c.Close()

	c = openCache(t, dir, 1<<20, 200)
	if c.Contains(Key{Object: object(11), Kind: KindChunk, Index: 0}, 100) ||
		!c.Contains(Key{Object: object(11), Kind: KindChunk, Index: 2}, 100) {
		t.Fatal("restart did not drop the oldest entry beyond budget")
	}
}

// TestCloseStoresQueuedWrites closes right after queueing writes: they are
// stored, so a reopened cache has them.
func TestCloseStoresQueuedWrites(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MetaMaxBytes: 1 << 20, DataMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range uint32(20) {
		c.Write(Key{Object: object(13), Kind: KindChunk, Index: i}, content("q", 100))
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c = openCache(t, dir, 1<<20, 1<<20)
	if got := c.Stats(TierData).Entries; got != 20 {
		t.Fatalf("reopened cache has %d entries, want 20", got)
	}
}

func TestLockedDirectory(t *testing.T) {
	dir := t.TempDir()
	openCache(t, dir, 1<<20, 1<<20)
	if _, err := Open(Options{Dir: dir, MetaMaxBytes: 1 << 20, DataMaxBytes: 1 << 20}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open error = %v, want ErrLocked", err)
	}
}

func TestParseNameRoundTrip(t *testing.T) {
	for _, k := range []Key{
		{Object: object(12), Kind: KindMeta},
		{Object: object(12), Kind: KindBloom},
		{Object: object(12), Kind: KindWhole},
		{Object: object(12), Kind: KindChunk, Index: 0},
		{Object: object(12), Kind: KindChunk, Index: 4_294_967_295},
	} {
		got, ok := parseName(k.name())
		if !ok || got != k {
			t.Fatalf("parseName(%q) = %v, %t; want %v", k.name(), got, ok, k)
		}
	}
	for _, bad := range []string{"", "abc.meta", fmt.Sprintf("%064x.c01", 0), fmt.Sprintf("%064x.c", 0), fmt.Sprintf("%064x.other", 0), fmt.Sprintf("%064X.meta", 0xabc)} {
		if _, ok := parseName(bad); ok {
			t.Fatalf("parseName(%q) accepted", bad)
		}
	}
}

// TestConcurrentUse writes, reads and evicts from many goroutines.
func TestConcurrentUse(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 8<<10)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				k := Key{Object: object(byte(g)), Kind: KindChunk, Index: uint32(i % 20)}
				data := content(fmt.Sprintf("g%d-%d", g, i%20), 512)
				if i%2 == 0 {
					c.Write(k, data)
				} else {
					_ = c.Put(k, data)
				}
				if got, ok := read(c, k, 512, 0, 512); ok && !bytes.Equal(got, data) {
					t.Errorf("goroutine %d read wrong bytes for %v", g, k)
					return
				}
			}
		})
	}
	wg.Wait()
	if stats := c.Stats(TierData); stats.Bytes > 8<<10 {
		t.Fatalf("data tier over budget: %+v", stats)
	}
}
