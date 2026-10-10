package diskcache

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

func openCache(t *testing.T, dir string, maxBytes int64) *Cache {
	t.Helper()
	c, err := Open(Options{Dir: dir, MaxBytes: maxBytes})
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
	c := openCache(t, t.TempDir(), 1<<20)
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
	c := openCache(t, t.TempDir(), 1<<20)
	k := Key{Object: object(2), Kind: KindMeta}
	put(t, c, k, content("meta", 500))
	if _, ok := read(c, k, 600, 0, 10); ok {
		t.Fatal("read of a short entry succeeded")
	}
	if stats := c.Stats(TierMeta); stats.Corruptions != 1 || stats.Entries != 0 {
		t.Fatalf("stats = %+v, want the entry dropped as corrupt", stats)
	}
}

func TestLRUEvictionDataFirst(t *testing.T) {
	c := openCache(t, t.TempDir(), 300)
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

	// Metadata makes room by evicting data, then data churn never evicts it.
	meta := Key{Object: object(5), Kind: KindMeta}
	put(t, c, meta, content("m", 200))
	if !c.Contains(meta, 200) || c.Stats(TierData).Bytes != 100 {
		t.Fatalf("meta entry did not evict data: data tier %+v", c.Stats(TierData))
	}
	for i := uint32(10); i < 20; i++ {
		put(t, c, chunk(i), content("e", 100))
	}
	if !c.Contains(meta, 200) {
		t.Fatal("data churn evicted a meta entry")
	}

	// Data that fits only by evicting metadata is not stored.
	put(t, c, chunk(30), content("f", 150))
	if c.Contains(chunk(30), 150) || !c.Contains(meta, 200) || c.Stats(TierData).Bypasses != 1 {
		t.Fatalf("data stored over metadata: data tier %+v", c.Stats(TierData))
	}

	// Metadata evicts metadata when no data is left.
	meta2 := Key{Object: object(6), Kind: KindMeta}
	put(t, c, meta2, content("n", 200))
	if c.Contains(meta, 200) || !c.Contains(meta2, 200) {
		t.Fatal("a new meta entry did not evict the least recently used one")
	}

	// An entry larger than the whole cache is not stored.
	put(t, c, chunk(99), content("big", 301))
	if c.Contains(chunk(99), 301) || c.Stats(TierData).Bypasses != 2 {
		t.Fatal("oversized entry stored")
	}
	if free := c.Free(); free != 100 {
		t.Fatalf("Free = %d, want 100", free)
	}
}

// TestDataRefusedWhenMetadataGrewDuringStore fills the cache with metadata
// while a data Put is between its rename and its commit: the data entry is
// not kept, its file is deleted, and nothing is evicted for it.
func TestDataRefusedWhenMetadataGrewDuringStore(t *testing.T) {
	c := openCache(t, t.TempDir(), 300)
	older := Key{Object: object(39), Kind: KindChunk}
	put(t, c, older, content("old", 100))
	chunk := Key{Object: object(40), Kind: KindChunk}
	meta := Key{Object: object(41), Kind: KindMeta}
	renamed, resume := make(chan struct{}), make(chan struct{})
	c.testHook = func(point string, k Key) {
		if point == "renamed" && k == chunk {
			close(renamed)
			<-resume
		}
	}
	stored := make(chan error, 1)
	go func() { stored <- c.Put(chunk, content("data", 150)) }()
	<-renamed
	put(t, c, meta, content("meta", 200))
	close(resume)
	if err := <-stored; err != nil {
		t.Fatalf("Put: %v", err)
	}
	if c.Contains(chunk, 150) || !c.Contains(meta, 200) {
		t.Fatal("data kept over metadata stored during its write")
	}
	if !c.Contains(older, 100) {
		t.Fatal("a refused data entry evicted other data")
	}
	if _, err := os.Stat(c.path(chunk, 150)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused data entry left its file: %v", err)
	}
	if stats := c.Stats(TierData); stats.Bypasses != 1 || stats.Evictions != 0 {
		t.Fatalf("data tier %+v, want one bypass and no eviction", stats)
	}
}

func TestPutOfStoredEntryIsANoOp(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20)
	k := Key{Object: object(42), Kind: KindChunk}
	put(t, c, k, content("first", 100))
	put(t, c, k, content("second", 100))
	if got, ok := read(c, k, 100, 0, 100); !ok || !bytes.Equal(got, content("first", 100)) {
		t.Fatal("a Put of the same size replaced the stored entry")
	}
	if stats := c.Stats(TierData); stats.Entries != 1 || stats.Bytes != 100 {
		t.Fatalf("stats = %+v, want one entry", stats)
	}
}

func TestFailedWriteCountsAndLeavesNothing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	c := openCache(t, t.TempDir(), 1<<20)
	if err := os.Chmod(c.incoming, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(c.incoming, 0o700) })
	k := Key{Object: object(43), Kind: KindChunk}
	if err := c.Put(k, content("x", 100)); err == nil {
		t.Fatal("Put succeeded with an unwritable directory")
	}
	if c.Contains(k, 100) || c.Stats(TierData).Failures != 1 {
		t.Fatalf("stats = %+v, want one failure and no entry", c.Stats(TierData))
	}
	if left, _ := os.ReadDir(c.incoming); len(left) != 0 {
		t.Fatalf("failed write left %d files behind", len(left))
	}
}

func TestInvalidKeysAndCallsAfterClose(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20)
	k := Key{Object: object(44), Kind: KindChunk}
	put(t, c, k, content("k", 100))
	for _, bad := range []Key{{Kind: kindCount}, {Kind: KindMeta, Index: 1}} {
		if c.Contains(bad, 100) || c.ReadAt(bad, 100, make([]byte, 1), 0) {
			t.Fatalf("invalid key %v found", bad)
		}
		if c.Put(bad, content("x", 10)) == nil {
			t.Fatalf("Put of invalid key %v succeeded", bad)
		}
		if n := c.RemoveAll([]Key{bad}); n != 0 {
			t.Fatalf("RemoveAll of invalid key %v dropped %d", bad, n)
		}
		c.Remove(bad)
	}
	c.Purge(tierCount)
	if stats := c.Stats(tierCount); stats != (Stats{}) {
		t.Fatalf("Stats of an invalid tier = %+v", stats)
	}
	if !c.Contains(k, 100) {
		t.Fatal("calls with invalid keys or tiers dropped a valid entry")
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if c.Contains(k, 100) || c.ReadAt(k, 100, make([]byte, 1), 0) {
		t.Fatal("entry readable after Close")
	}
	if c.Put(Key{Object: object(45), Kind: KindChunk}, content("y", 10)) == nil {
		t.Fatal("Put succeeded after Close")
	}
	if n := c.RemoveAll([]Key{k}); n != 0 {
		t.Fatalf("RemoveAll after Close dropped %d", n)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestRemovePurgeAndVanishedFiles(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20)
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

// TestDropDuringStoreIsNotUndone drops an entry while a Put of it is between
// its rename and its commit: the Put is not kept, so the entry stays gone
// across a restart, and a later Put of it is kept.
func TestDropDuringStoreIsNotUndone(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	k := Key{Object: object(9), Kind: KindChunk, Index: 2}
	renamed, resume := make(chan struct{}), make(chan struct{})
	c.testHook = func(point string, _ Key) {
		if point == "renamed" {
			close(renamed)
			<-resume
		}
	}
	stored := make(chan struct{})
	go func() {
		defer close(stored)
		if err := c.Put(k, content("bad", 4096)); err != nil {
			t.Errorf("Put: %v", err)
		}
	}()
	<-renamed
	c.Remove(k)
	close(resume)
	<-stored
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c = openCache(t, dir, 1<<20)
	if c.Contains(k, 4096) {
		t.Fatal("dropped entry came back from a write in progress")
	}
	good := content("good", 4096)
	put(t, c, k, good)
	if got, ok := read(c, k, 4096, 0, 4096); !ok || !bytes.Equal(got, good) {
		t.Fatal("write after the drop not kept")
	}
}

// TestOtherDropsKeepPut drops other entries while a Put is between its
// rename and its commit: a Remove of another key and a Purge of the other
// tier leave it alone, while a Purge of its own tier discards it.
func TestOtherDropsKeepPut(t *testing.T) {
	cases := []struct {
		name string
		drop func(c *Cache)
		kept bool
	}{
		{"remove_other_key", func(c *Cache) { c.Remove(Key{Object: object(31), Kind: KindChunk}) }, true},
		{"purge_other_tier", func(c *Cache) { c.Purge(TierMeta) }, true},
		{"purge_own_tier", func(c *Cache) { c.Purge(TierData) }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := openCache(t, t.TempDir(), 1<<20)
			k := Key{Object: object(30), Kind: KindChunk}
			renamed, resume := make(chan struct{}), make(chan struct{})
			c.testHook = func(point string, _ Key) {
				if point == "renamed" {
					close(renamed)
					<-resume
				}
			}
			stored := make(chan error, 1)
			go func() { stored <- c.Put(k, content("kept", 100)) }()
			<-renamed
			tc.drop(c)
			close(resume)
			if err := <-stored; err != nil {
				t.Fatalf("Put: %v", err)
			}
			if got := c.Contains(k, 100); got != tc.kept {
				t.Fatalf("entry cached=%t after %s, want %t", got, tc.name, tc.kept)
			}
			if len(c.stores) != 0 {
				t.Fatalf("%d keys still tracked after the Put finished", len(c.stores))
			}
		})
	}
}

// TestFailedReadKeepsReplacedEntry fails a read of an entry whose file
// vanished, while the entry is stored again before the read takes the lock:
// the new entry stays.
func TestFailedReadKeepsReplacedEntry(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20)
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
	c, err := Open(Options{Dir: dir, MaxBytes: 1 << 20})
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
	junkShard := filepath.Join(root, "data", "zz", "file")
	mustWrite(junkShard, []byte("x"))
	strayFile := filepath.Join(root, "data", "stray")
	mustWrite(strayFile, []byte("x"))
	oldLayout := filepath.Join(dir, "v2", "sst", "aa", "file")
	mustWrite(oldLayout, []byte("old"))
	foreign := filepath.Join(dir, "notes.txt")
	mustWrite(foreign, []byte("not ours"))

	c = openCache(t, dir, 1<<20)
	if got, ok := read(c, keep, 300, 0, 300); !ok || !bytes.Equal(got, content("keep", 300)) {
		t.Fatal("chunk lost across restart")
	}
	if !c.Contains(meta, 200) {
		t.Fatal("meta entry lost across restart")
	}
	if stats := c.Stats(TierData); stats.Entries != 1 {
		t.Fatalf("data tier entries=%d after recovery, want 1", stats.Entries)
	}
	for _, gone := range []string{sizeless, wrongTier, unfinished, filepath.Join(root, "data", "zz"), strayFile, filepath.Join(dir, "v2")} {
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
	c, err := Open(Options{Dir: dir, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range uint32(3) {
		put(t, c, Key{Object: object(11), Kind: KindChunk, Index: i}, content("z", 100))
	}
	put(t, c, Key{Object: object(11), Kind: KindMeta}, content("m", 100))
	_ = c.Close()

	// Data is trimmed first: the meta entry and one chunk remain.
	c = openCache(t, dir, 200)
	if stats := c.Stats(TierData); stats.Entries != 1 || stats.Bytes != 100 {
		t.Fatalf("restart kept %d data entries of %d bytes, want 1 of 100", stats.Entries, stats.Bytes)
	}
	if stats := c.Stats(TierMeta); stats.Entries != 1 {
		t.Fatalf("restart kept %d meta entries, want 1", stats.Entries)
	}
	files, err := filepath.Glob(filepath.Join(dir, versionDir, "data", "*", "*"))
	if err != nil || len(files) != 1 {
		t.Fatalf("data files after trim=%v err=%v, want 1", files, err)
	}
}

// TestShortFileDroppedOnRead stores an entry, then truncates its file, as a
// crash before its data reached the disk can: the read past the end fails,
// counts a corruption and drops the entry.
func TestShortFileDroppedOnRead(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MaxBytes: 1 << 20})
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

	c = openCache(t, dir, 1<<20)
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
	c := openCache(t, t.TempDir(), 1<<20)
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

// TestCloseWaitsForPut closes while a Put is in progress: Close refuses new
// Puts at once but returns only after the one in progress is stored, so a
// reopened cache has it.
func TestCloseWaitsForPut(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	k := Key{Object: object(13), Kind: KindChunk, Index: 0}
	renamed, resume := make(chan struct{}), make(chan struct{})
	c.testHook = func(point string, _ Key) {
		if point == "renamed" {
			close(renamed)
			<-resume
		}
	}
	go func() {
		if err := c.Put(k, content("q", 100)); err != nil {
			t.Errorf("Put: %v", err)
		}
	}()
	<-renamed
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	for {
		c.mu.Lock()
		closing := c.closed
		c.mu.Unlock()
		if closing {
			break
		}
		runtime.Gosched()
	}
	if err := c.Put(Key{Object: object(13), Kind: KindChunk, Index: 1}, content("q", 100)); err == nil {
		t.Fatal("Put accepted after Close began")
	}
	select {
	case <-closed:
		t.Fatal("Close returned before the Put in progress finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(resume)
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}

	c = openCache(t, dir, 1<<20)
	if !c.Contains(k, 100) {
		t.Fatal("Put in progress at Close was not kept")
	}
}

// TestDropsAfterCloseLeaveFiles removes and purges after Close: the files
// stay, since the directory may belong to another process by then.
func TestDropsAfterCloseLeaveFiles(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	k := Key{Object: object(32), Kind: KindChunk}
	put(t, c, k, content("stay", 100))
	path := c.path(k, 100)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c.Remove(k)
	c.ReportCorrupt(k)
	c.Purge(TierData)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file removed after Close: %v", err)
	}
}

func TestLockedDirectory(t *testing.T) {
	dir := t.TempDir()
	openCache(t, dir, 1<<20)
	if _, err := Open(Options{Dir: dir, MaxBytes: 1 << 20}); !errors.Is(err, ErrLocked) {
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
	c := openCache(t, t.TempDir(), 8<<10)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				k := Key{Object: object(byte(g)), Kind: KindChunk, Index: uint32(i % 20)}
				data := content(fmt.Sprintf("g%d-%d", g, i%20), 512)
				_ = c.Put(k, data)
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

func TestRemoveAll(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(Options{Dir: dir, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	keep, gone := object(1), object(2)
	keys := []Key{{Object: gone, Kind: KindChunk, Index: 0}, {Object: gone, Kind: KindChunk, Index: 1}, {Object: gone, Kind: KindMeta}}
	for _, k := range append(keys, Key{Object: keep, Kind: KindChunk}) {
		if err := c.Put(k, []byte("data")); err != nil {
			t.Fatal(err)
		}
	}
	c.RemoveAll(append(keys, Key{Object: gone, Kind: KindChunk, Index: 9})) // one never stored
	for _, k := range keys {
		if c.Contains(k, 4) {
			t.Fatalf("%v still cached", k)
		}
		if _, err := os.Stat(c.path(k, 4)); !os.IsNotExist(err) {
			t.Fatalf("%v file still on disk: %v", k, err)
		}
	}
	if !c.Contains(Key{Object: keep, Kind: KindChunk}, 4) {
		t.Fatal("an entry not listed was removed")
	}
	if got := c.Stats(TierData).Bytes; got != 4 {
		t.Fatalf("data tier holds %d bytes, want the one kept entry", got)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c.RemoveAll([]Key{{Object: keep, Kind: KindChunk}})
	if _, err := os.Stat(c.path(Key{Object: keep, Kind: KindChunk}, 4)); err != nil {
		t.Fatalf("RemoveAll after Close deleted a file: %v", err)
	}
}

// TestPutDuringRemoveAllKeepsIndexTrue stores a key again after RemoveAll
// dropped it and before RemoveAll deletes its file, which has the same name:
// the cache must not report the entry while its file is gone.
func TestPutDuringRemoveAllKeepsIndexTrue(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20)
	k := Key{Object: object(46), Kind: KindChunk}
	put(t, c, k, content("old", 100))
	c.testHook = func(point string, _ Key) {
		if point == "unlocked" {
			c.testHook = nil
			put(t, c, k, content("new", 100))
		}
	}
	c.RemoveAll([]Key{k})
	if c.Contains(k, 100) {
		if _, err := os.Stat(c.path(k, 100)); err != nil {
			t.Fatalf("entry indexed but its file is gone: %v", err)
		}
	}
	if stats := c.Stats(TierData); stats.Bypasses != 1 {
		t.Fatalf("stats = %+v, want the Put during the delete bypassed", stats)
	}
	// Once the delete is done, the key is cached again.
	put(t, c, k, content("again", 100))
	if got, ok := read(c, k, 100, 0, 100); !ok || !bytes.Equal(got, content("again", 100)) {
		t.Fatal("key not cached after RemoveAll finished")
	}
}
