package diskcache

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
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

func TestWriteDropsBeyondQueueBytes(t *testing.T) {
	c, err := Open(Options{Dir: t.TempDir(), MetaMaxBytes: 1 << 20, DataMaxBytes: 1 << 20, QueueBytes: 10_000})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	// Hold the writers so queued writes stay pending.
	resume := make(chan struct{})
	var once sync.Once
	c.testHook = func(point string, _ Key) {
		if point == "picked" {
			<-resume
		}
	}
	t.Cleanup(func() { once.Do(func() { close(resume) }) })

	first := Key{Object: object(20), Kind: KindChunk, Index: 0}
	second := Key{Object: object(20), Kind: KindChunk, Index: 1}
	if !c.Write(first, content("a", 6_000)) {
		t.Fatal("write within the byte budget dropped")
	}
	if c.Write(second, content("b", 6_000)) {
		t.Fatal("write beyond the byte budget queued")
	}
	if stats := c.Stats(TierData); stats.Dropped != 1 {
		t.Fatalf("dropped=%d, want 1", stats.Dropped)
	}
	once.Do(func() { close(resume) })
	c.Sync()
	if !c.Write(second, content("b", 6_000)) {
		t.Fatal("write after the queue drained dropped")
	}
	c.Sync()
	if !c.Contains(first, 6_000) || !c.Contains(second, 6_000) {
		t.Fatal("queued writes not stored")
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
	if err := os.Remove(c.path(b, 100)); err != nil {
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

// TestDropDuringStoreIsNotUndone drops an entry while a write of it is in
// progress: just after the writer took it from the queue, or between the
// store's rename and its commit, through Write and through Put. The write is
// not kept, so the entry stays gone across a restart, and a later write of it
// is kept.
func TestDropDuringStoreIsNotUndone(t *testing.T) {
	cases := []struct{ mode, point string }{
		{"write", "picked"}, {"write", "renamed"}, {"put", "renamed"},
	}
	for _, tc := range cases {
		mode := tc.mode
		t.Run(tc.mode+"/"+tc.point, func(t *testing.T) {
			dir := t.TempDir()
			c, err := Open(Options{Dir: dir, MetaMaxBytes: 1 << 20, DataMaxBytes: 1 << 20})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			k := Key{Object: object(9), Kind: KindChunk, Index: 2}
			renamed, resume := make(chan struct{}), make(chan struct{})
			c.testHook = func(point string, _ Key) {
				if point == tc.point {
					close(renamed)
					<-resume
				}
			}
			stored := make(chan struct{})
			go func() {
				defer close(stored)
				if mode == "write" {
					if !c.Write(k, content("bad", 4096)) {
						t.Error("Write dropped")
					}
				} else if err := c.Put(k, content("bad", 4096)); err != nil {
					t.Errorf("Put: %v", err)
				}
			}()
			<-renamed
			c.Remove(k)
			close(resume)
			<-stored
			// Close waits for the background writer to finish.
			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			c = openCache(t, dir, 1<<20, 1<<20)
			if c.Contains(k, 4096) {
				t.Fatal("dropped entry came back from a write in progress")
			}
			good := content("good", 4096)
			put(t, c, k, good)
			if got, ok := read(c, k, 4096, 0, 4096); !ok || !bytes.Equal(got, good) {
				t.Fatal("write after the drop not kept")
			}
		})
	}
}

// TestFailedReadKeepsReplacedEntry fails a read of an entry whose file
// vanished, while the entry is stored again before the read takes the lock:
// the new entry stays.
func TestFailedReadKeepsReplacedEntry(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	k := Key{Object: object(21), Kind: KindChunk, Index: 0}
	put(t, c, k, content("old", 100))
	if err := os.Remove(c.path(k, 100)); err != nil {
		t.Fatal(err)
	}
	c.testHook = func(point string, _ Key) {
		if point == "read" {
			c.testHook = nil
			c.Remove(k)
			put(t, c, k, content("new", 100))
		}
	}
	if _, ok := read(c, k, 100, 0, 10); ok {
		t.Fatal("read of a vanished file succeeded")
	}
	if got, ok := read(c, k, 100, 0, 100); !ok || !bytes.Equal(got, content("new", 100)) {
		t.Fatal("failed read removed the entry stored meanwhile")
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
	unnamed := Key{Object: object(9), Kind: KindChunk, Index: 1}
	mustWrite := func(path string, data []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A name without a size, as the previous layout used.
	sizeless := filepath.Join(root, "data", unnamed.name()[:2], unnamed.name())
	mustWrite(sizeless, []byte("x"))
	wrongTier := filepath.Join(root, "meta", keep.name()[:2], (Key{Object: object(10), Kind: KindChunk}).name()+".1")
	mustWrite(wrongTier, []byte("x"))
	// A second size for an entry already found.
	duplicate := filepath.Join(root, "data", keep.name()[:2], keep.name()+".301")
	mustWrite(duplicate, content("keep", 301))
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
	if stats := c.Stats(TierData); stats.Entries != 1 {
		t.Fatalf("data tier entries=%d after recovery, want 1", stats.Entries)
	}
	for _, gone := range []string{sizeless, wrongTier, unfinished, filepath.Join(dir, "v2")} {
		if _, err := os.Stat(gone); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s survived recovery", gone)
		}
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatal("recovery removed a file that is not the cache's")
	}
}

func TestRestartTrimsToBudget(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MetaMaxBytes: 1 << 20, DataMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range uint32(3) {
		put(t, c, Key{Object: object(11), Kind: KindChunk, Index: i}, content("z", 100))
	}
	_ = c.Close()

	c = openCache(t, dir, 1<<20, 200)
	if stats := c.Stats(TierData); stats.Entries != 2 || stats.Bytes != 200 {
		t.Fatalf("restart kept %d entries of %d bytes, want 2 of 200", stats.Entries, stats.Bytes)
	}
	files, err := filepath.Glob(filepath.Join(dir, versionDir, "data", "*", "*"))
	if err != nil || len(files) != 2 {
		t.Fatalf("files after trim=%v err=%v, want 2", files, err)
	}
}

// TestShortFileDroppedOnRead stores an entry, then truncates its file, as a
// crash before its data reached the disk can: the read past the end fails,
// counts a corruption and drops the entry.
func TestShortFileDroppedOnRead(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MetaMaxBytes: 1 << 20, DataMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	k := Key{Object: object(22), Kind: KindChunk, Index: 0}
	put(t, c, k, content("short", 100))
	path := c.path(k, 100)
	_ = c.Close()
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}

	c = openCache(t, dir, 1<<20, 1<<20)
	if !c.Contains(k, 100) {
		t.Fatal("recovery did not index the entry from its name")
	}
	if _, ok := read(c, k, 100, 90, 10); ok {
		t.Fatal("read past a short file succeeded")
	}
	if stats := c.Stats(TierData); stats.Corruptions != 1 || stats.Entries != 0 {
		t.Fatalf("short file stats=%+v, want one corruption and no entries", stats)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("short file not deleted")
	}
}

// TestReplacingWithAnotherSizeDeletesOldFile stores an entry under one size
// and then another: the first file, whose name differs, is deleted.
func TestReplacingWithAnotherSizeDeletesOldFile(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	k := Key{Object: object(23), Kind: KindChunk, Index: 0}
	put(t, c, k, content("a", 100))
	put(t, c, k, content("b", 200))
	if _, err := os.Stat(c.path(k, 100)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("file of the replaced size survived")
	}
	if got, ok := read(c, k, 200, 0, 200); !ok || !bytes.Equal(got, content("b", 200)) {
		t.Fatal("replacement not readable")
	}
	if stats := c.Stats(TierData); stats.Entries != 1 || stats.Bytes != 200 {
		t.Fatalf("stats=%+v, want one entry of 200 bytes", stats)
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
		name := k.name() + ".131072"
		got, size, ok := parseName(name)
		if !ok || got != k || size != 131072 {
			t.Fatalf("parseName(%q) = %v, %d, %t; want %v, 131072", name, got, size, ok, k)
		}
	}
	zero := fmt.Sprintf("%064x", 0)
	for _, bad := range []string{
		"", "abc.meta.1", zero + ".c01.1", zero + ".c.1", zero + ".other.1", fmt.Sprintf("%064X.meta.1", 0xabc),
		zero + ".meta", zero + ".meta.0", zero + ".meta.01", zero + ".meta.-1", zero + ".meta.x", zero + ".c1",
	} {
		if _, _, ok := parseName(bad); ok {
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
