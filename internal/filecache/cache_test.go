package filecache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

func descriptor(kind Kind, data []byte) Descriptor {
	sum := sha256.Sum256(data)
	return Descriptor{Kind: kind, Size: int64(len(data)), Checksum: "sha256:" + hex.EncodeToString(sum[:])}
}

func content(seed string, size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = seed[i%len(seed)] + byte(i/len(seed))
	}
	return data
}

func openCache(t *testing.T, dir string, sstMax, bloomMax int64) *Cache {
	t.Helper()
	c, err := Open(Options{Dir: dir, SSTMaxBytes: sstMax, BloomMaxBytes: bloomMax})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// commit writes data through a Writer and returns the committed file's bytes.
func commit(t *testing.T, c *Cache, d Descriptor, data []byte) []byte {
	t.Helper()
	w, err := c.Create(d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	file, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	defer func() { _ = file.Close() }()
	return readAll(t, file, d.Size)
}

func readAll(t *testing.T, file *os.File, size int64) []byte {
	t.Helper()
	data := make([]byte, size)
	if _, err := file.ReadAt(data, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
	return data
}

func openFileBytes(t *testing.T, c *Cache, d Descriptor) ([]byte, bool) {
	t.Helper()
	file, ok := c.OpenFile(d)
	if !ok {
		return nil, false
	}
	defer func() { _ = file.Close() }()
	return readAll(t, file, d.Size), true
}

func incomingFiles(t *testing.T, c *Cache) int {
	t.Helper()
	entries, err := os.ReadDir(c.incoming)
	if err != nil {
		t.Fatalf("read incoming: %v", err)
	}
	return len(entries)
}

func TestCache_SSTRoundTripSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	data := content("sst", 4096)
	d := descriptor(KindSST, data)

	c := openCache(t, dir, 1<<20, 1<<20)
	if got := commit(t, c, d, data); !bytes.Equal(got, data) {
		t.Fatal("committed file differs from written bytes")
	}
	if got, ok := openFileBytes(t, c, d); !ok || !bytes.Equal(got, data) {
		t.Fatalf("OpenFile hit=%t", ok)
	}
	if incomingFiles(t, c) != 0 {
		t.Fatal("temporary file left behind")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openCache(t, dir, 1<<20, 1<<20)
	if got, ok := openFileBytes(t, reopened, d); !ok || !bytes.Equal(got, data) {
		t.Fatalf("OpenFile after reopen hit=%t", ok)
	}
	if stats := reopened.Stats(KindSST); stats.Entries != 1 || stats.Bytes != d.Size || stats.Hits != 1 {
		t.Fatalf("stats after reopen = %+v", stats)
	}
}

func TestCache_BloomRoundTripSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	data := content("bloom", 1000)
	d := descriptor(KindBloom, data)

	c := openCache(t, dir, 1<<20, 1<<20)
	if err := c.Put(d, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got, ok := c.ReadVerified(d); !ok || !bytes.Equal(got, data) {
		t.Fatalf("ReadVerified hit=%t", ok)
	}
	_ = c.Close()

	reopened := openCache(t, dir, 1<<20, 1<<20)
	if got, ok := reopened.ReadVerified(d); !ok || !bytes.Equal(got, data) {
		t.Fatalf("ReadVerified after reopen hit=%t", ok)
	}
}

func TestCache_KindsAreSeparate(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	data := content("same", 100)
	if err := c.Put(descriptor(KindBloom, data), data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if c.Contains(descriptor(KindSST, data)) {
		t.Fatal("Bloom file visible as SST")
	}
}

func TestCache_CommitRejectsMismatchedBytes(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	data := content("sst", 100)
	d := descriptor(KindSST, data)

	t.Run("short", func(t *testing.T) {
		w, err := c.Create(d)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		_, _ = w.Write(data[:50])
		if _, err := w.Commit(); !errors.Is(err, ErrSizeMismatch) {
			t.Fatalf("Commit err=%v, want ErrSizeMismatch", err)
		}
	})
	t.Run("too long", func(t *testing.T) {
		w, err := c.Create(d)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := w.Write(append(data, 'x')); !errors.Is(err, ErrSizeMismatch) {
			t.Fatalf("Write err=%v, want ErrSizeMismatch", err)
		}
		w.Abort()
	})
	t.Run("wrong bytes", func(t *testing.T) {
		w, err := c.Create(d)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		_, _ = w.Write(content("other", 100))
		if _, err := w.Commit(); !errors.Is(err, ErrChecksumMismatch) {
			t.Fatalf("Commit err=%v, want ErrChecksumMismatch", err)
		}
	})
	if c.Contains(d) || incomingFiles(t, c) != 0 {
		t.Fatal("mismatched bytes were cached or left behind")
	}
}

func TestCache_EvictedFileStaysReadableWhileOpen(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("relies on reading a deleted open file")
	}
	c := openCache(t, t.TempDir(), 150, 1<<20)
	first := content("first", 100)
	second := content("second", 100)
	firstDesc := descriptor(KindSST, first)

	commit(t, c, firstDesc, first)
	held, ok := c.OpenFile(firstDesc)
	if !ok {
		t.Fatal("first file missing")
	}
	defer func() { _ = held.Close() }()

	commit(t, c, descriptor(KindSST, second), second) // evicts first
	if c.Contains(firstDesc) {
		t.Fatal("first file was not evicted")
	}
	if got := readAll(t, held, firstDesc.Size); !bytes.Equal(got, first) {
		t.Fatal("open file changed after eviction")
	}
	if stats := c.Stats(KindSST); stats.Evictions != 1 || stats.Entries != 1 || stats.Bytes != 100 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestCache_EvictsLeastRecentlyUsed(t *testing.T) {
	c := openCache(t, t.TempDir(), 250, 1<<20)
	a, b, next := content("a", 100), content("b", 100), content("c", 100)
	commit(t, c, descriptor(KindSST, a), a)
	commit(t, c, descriptor(KindSST, b), b)
	openFileBytes(t, c, descriptor(KindSST, a)) // a is now most recent
	commit(t, c, descriptor(KindSST, next), next)

	if !c.Contains(descriptor(KindSST, a)) || c.Contains(descriptor(KindSST, b)) {
		t.Fatal("expected b, the least recently used, to be evicted")
	}
}

func TestCache_UncachableFileIsStillServed(t *testing.T) {
	c := openCache(t, t.TempDir(), 100, 1<<20)
	data := content("large", 101)
	d := descriptor(KindSST, data)

	if got := commit(t, c, d, data); !bytes.Equal(got, data) {
		t.Fatal("oversized file not served")
	}
	if c.Contains(d) || incomingFiles(t, c) != 0 {
		t.Fatal("oversized file was cached or left behind")
	}
	if stats := c.Stats(KindSST); stats.Bypasses != 1 {
		t.Fatalf("stats = %+v, want one bypass", stats)
	}
}

func TestCache_SameContentCachedOnce(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	data := content("dup", 100)
	d := descriptor(KindSST, data)
	commit(t, c, d, data)
	if got := commit(t, c, d, data); !bytes.Equal(got, data) {
		t.Fatal("second commit not served")
	}
	if stats := c.Stats(KindSST); stats.Entries != 1 || stats.Bytes != 100 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestCache_DamagedFilesBecomeMisses(t *testing.T) {
	cases := []struct {
		name       string
		kind       Kind
		damage     func(path string) error
		corruption bool
	}{
		{
			name: "SST truncated", kind: KindSST, corruption: true,
			damage: func(path string) error { return os.Truncate(path, 10) },
		},
		{
			name: "Bloom byte flipped", kind: KindBloom, corruption: true,
			damage: func(path string) error {
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				data[0] ^= 0xff
				return os.WriteFile(path, data, 0o600)
			},
		},
		{
			name: "SST deleted", kind: KindSST,
			damage: os.Remove,
		},
		{
			name: "Bloom deleted", kind: KindBloom,
			damage: os.Remove,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := openCache(t, t.TempDir(), 1<<20, 1<<20)
			data := content(tc.name, 200)
			d := descriptor(tc.kind, data)
			if err := c.Put(d, data); err != nil {
				t.Fatalf("Put: %v", err)
			}
			sum, _ := d.sum()
			if err := tc.damage(c.path(tc.kind, sum)); err != nil {
				t.Fatalf("damage: %v", err)
			}

			var ok bool
			if tc.kind == KindSST {
				_, ok = openFileBytes(t, c, d)
			} else {
				_, ok = c.ReadVerified(d)
			}
			if ok {
				t.Fatal("damaged file was served")
			}
			stats := c.Stats(tc.kind)
			if stats.Entries != 0 || stats.Misses != 1 {
				t.Fatalf("stats = %+v, want the entry dropped and one miss", stats)
			}
			if got := stats.Corruptions == 1; got != tc.corruption {
				t.Fatalf("corruptions = %d", stats.Corruptions)
			}
		})
	}
}

func TestCache_RemoveAndPurge(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	a, b := content("a", 100), content("b", 100)
	commit(t, c, descriptor(KindSST, a), a)
	commit(t, c, descriptor(KindSST, b), b)

	c.Remove(descriptor(KindSST, a))
	if c.Contains(descriptor(KindSST, a)) || !c.Contains(descriptor(KindSST, b)) {
		t.Fatal("Remove dropped the wrong file")
	}
	c.Purge(KindSST)
	if stats := c.Stats(KindSST); stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatalf("stats after Purge = %+v", stats)
	}
}

func TestOpen_CleansDirectoryAndEnforcesBudget(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir, 1<<20, 1<<20)
	var descs []Descriptor
	for i := range 3 {
		data := content(fmt.Sprintf("file-%d", i), 100)
		d := descriptor(KindSST, data)
		commit(t, c, d, data)
		sum, _ := d.sum()
		// Oldest first: file-0 is the oldest.
		modTime := time.Now().Add(time.Duration(i-10) * time.Minute)
		if err := os.Chtimes(c.path(KindSST, sum), modTime, modTime); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
		descs = append(descs, d)
	}
	_ = c.Close()

	unrelated := filepath.Join(dir, "operator-note")
	if err := os.WriteFile(unrelated, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write unrelated file: %v", err)
	}
	sstDir := filepath.Join(dir, versionDir, "sst")
	junk := []string{
		filepath.Join(dir, "v1", "old"),
		filepath.Join(dir, "CACHEMETA"),
		filepath.Join(dir, "incoming", "sst-partial"),
		filepath.Join(dir, versionDir, incomingName, "sst-partial"),
		filepath.Join(sstDir, "zz", "not-a-checksum"),
		filepath.Join(sstDir, "ab", "not-a-checksum"),
	}
	for _, path := range junk {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte("junk"), 0o600); err != nil {
			t.Fatalf("write junk: %v", err)
		}
	}
	misplaced := filepath.Join(sstDir, "00", hex.EncodeToString(bytes.Repeat([]byte{0xab}, 32)))
	if err := os.MkdirAll(filepath.Dir(misplaced), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(misplaced, []byte("wrong shard"), 0o600); err != nil {
		t.Fatalf("write misplaced: %v", err)
	}
	empty := filepath.Join(sstDir, "cd", "cd"+string(bytes.Repeat([]byte("1"), 62)))
	if err := os.MkdirAll(filepath.Dir(empty), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatalf("write empty: %v", err)
	}

	// A smaller budget keeps only the two newest files.
	reopened := openCache(t, dir, 250, 1<<20)
	if reopened.Contains(descs[0]) || !reopened.Contains(descs[1]) || !reopened.Contains(descs[2]) {
		t.Fatal("expected only the oldest file to be dropped")
	}
	if stats := reopened.Stats(KindSST); stats.Entries != 2 || stats.Bytes != 200 {
		t.Fatalf("stats = %+v", stats)
	}
	for _, path := range append(junk, misplaced, empty) {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was not removed", path)
		}
	}
	if data, err := os.ReadFile(unrelated); err != nil || string(data) != "keep" {
		t.Errorf("unrelated file = %q, %v; want it kept", data, err)
	}
	sum, _ := descs[0].sum()
	if _, err := os.Stat(reopened.path(KindSST, sum)); !errors.Is(err, os.ErrNotExist) {
		t.Error("file dropped for budget was not deleted")
	}
}

func TestOpen_RejectsSecondOwner(t *testing.T) {
	dir := t.TempDir()
	first := openCache(t, dir, 1<<20, 1<<20)
	if _, err := Open(Options{Dir: dir, SSTMaxBytes: 1 << 20, BloomMaxBytes: 1 << 20}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open err=%v, want ErrLocked", err)
	}
	_ = first.Close()
	openCache(t, dir, 1<<20, 1<<20)
}

func TestOpen_RejectsInvalidOptions(t *testing.T) {
	for _, opts := range []Options{
		{SSTMaxBytes: 1, BloomMaxBytes: 1},
		{Dir: t.TempDir(), SSTMaxBytes: 0, BloomMaxBytes: 1},
		{Dir: t.TempDir(), SSTMaxBytes: 1, BloomMaxBytes: -1},
	} {
		if _, err := Open(opts); err == nil {
			t.Errorf("Open(%+v) succeeded", opts)
		}
	}
}

func TestCache_AfterClose(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	data := content("sst", 100)
	d := descriptor(KindSST, data)
	commit(t, c, d, data)

	w, err := c.Create(descriptor(KindSST, content("late", 100)))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write(content("late", 100)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = c.Close()

	if _, ok := c.OpenFile(d); ok {
		t.Fatal("OpenFile hit after Close")
	}
	if _, err := c.Create(d); !errors.Is(err, ErrClosed) {
		t.Fatalf("Create err=%v, want ErrClosed", err)
	}
	// A download finishing after Close is still served, just not cached.
	file, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit after Close: %v", err)
	}
	defer func() { _ = file.Close() }()
	if got := readAll(t, file, 100); !bytes.Equal(got, content("late", 100)) {
		t.Fatal("late commit not served")
	}
}

func TestCache_InvalidDescriptors(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	valid := descriptor(KindSST, content("x", 10))
	invalid := []Descriptor{
		{Kind: kindCount, Size: 10, Checksum: valid.Checksum},
		{Kind: KindSST, Size: 0, Checksum: valid.Checksum},
		{Kind: KindSST, Size: 10, Checksum: "md5:abc"},
	}
	for _, d := range invalid {
		if _, err := c.Create(d); !errors.Is(err, ErrInvalidDescriptor) {
			t.Errorf("Create(%+v) err=%v", d, err)
		}
		if _, ok := c.OpenFile(d); ok {
			t.Errorf("OpenFile(%+v) hit", d)
		}
	}
}

// TestCache_Concurrent mixes writes, reads and removals of a shared set of
// files under a budget that holds only some of them. Every hit must return
// the right bytes, and the budget must hold throughout.
func TestCache_Concurrent(t *testing.T) {
	c := openCache(t, t.TempDir(), 1000, 600)
	const files = 20
	var datas [files][]byte
	var descs [files]Descriptor
	for i := range files {
		datas[i] = content(fmt.Sprintf("concurrent-%02d", i), 100)
		kind := KindSST
		if i%2 == 1 {
			kind = KindBloom
		}
		descs[i] = descriptor(kind, datas[i])
	}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for worker := range 8 {
		wg.Go(func() {
			for round := range 200 {
				i := (worker*31 + round*7) % files
				d := descs[i]
				switch round % 4 {
				case 0:
					if err := c.Put(d, datas[i]); err != nil {
						errs <- err
						return
					}
				case 1:
					if d.Kind == KindBloom {
						if got, ok := c.ReadVerified(d); ok && !bytes.Equal(got, datas[i]) {
							errs <- fmt.Errorf("file %d: wrong bytes", i)
						}
					} else if file, ok := c.OpenFile(d); ok {
						got := make([]byte, d.Size)
						_, err := file.ReadAt(got, 0)
						_ = file.Close()
						if err != nil || !bytes.Equal(got, datas[i]) {
							errs <- fmt.Errorf("file %d: read err=%v", i, err)
						}
					}
				case 2:
					c.Remove(d)
				case 3:
					for _, kind := range []Kind{KindSST, KindBloom} {
						if stats := c.Stats(kind); stats.Bytes > stats.MaxBytes {
							errs <- fmt.Errorf("%s over budget: %+v", kind, stats)
						}
					}
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestFreeBytes(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("only reported on Linux and macOS")
	}
	free, err := FreeBytes(t.TempDir())
	if err != nil || free == 0 {
		t.Fatalf("FreeBytes = %d, %v", free, err)
	}
}

func TestCache_ReportCorruptDropsAndCounts(t *testing.T) {
	c := openCache(t, t.TempDir(), 1<<20, 1<<20)
	data := content("sst", 100)
	d := descriptor(KindSST, data)
	commit(t, c, d, data)

	c.ReportCorrupt(d)
	if c.Contains(d) {
		t.Fatal("reported file still cached")
	}
	if stats := c.Stats(KindSST); stats.Corruptions != 1 || stats.Entries != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

// TestCache_DamagedFileFoundAtOpenIsReplaced damages a cached file while the
// cache is closed, so the next Open indexes it with the wrong size. The
// damaged entry must not block the correct file from being cached again.
func TestCache_DamagedFileFoundAtOpenIsReplaced(t *testing.T) {
	data := content("sst", 200)
	d := descriptor(KindSST, data)
	damage := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		c := openCache(t, dir, 1<<20, 1<<20)
		commit(t, c, d, data)
		sum, _ := d.sum()
		if err := os.Truncate(c.path(KindSST, sum), 50); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		_ = c.Close()
		return dir
	}

	t.Run("lookup drops it", func(t *testing.T) {
		c := openCache(t, damage(t), 1<<20, 1<<20)
		if _, ok := c.OpenFile(d); ok {
			t.Fatal("damaged file was served")
		}
		if stats := c.Stats(KindSST); stats.Entries != 0 || stats.Bytes != 0 || stats.Corruptions != 1 {
			t.Fatalf("stats after lookup = %+v", stats)
		}
		commit(t, c, d, data)
		if got, ok := openFileBytes(t, c, d); !ok || !bytes.Equal(got, data) {
			t.Fatalf("re-downloaded file hit=%t", ok)
		}
	})

	t.Run("download replaces it", func(t *testing.T) {
		c := openCache(t, damage(t), 1<<20, 1<<20)
		commit(t, c, d, data)
		if got, ok := openFileBytes(t, c, d); !ok || !bytes.Equal(got, data) {
			t.Fatalf("replacement hit=%t", ok)
		}
		if stats := c.Stats(KindSST); stats.Entries != 1 || stats.Bytes != d.Size || stats.Corruptions != 1 {
			t.Fatalf("stats after replacement = %+v", stats)
		}
	})

	t.Run("Contains has no side effects", func(t *testing.T) {
		c := openCache(t, damage(t), 1<<20, 1<<20)
		if c.Contains(d) {
			t.Fatal("damaged file reported as cached")
		}
		if stats := c.Stats(KindSST); stats.Entries != 1 || stats.Corruptions != 0 {
			t.Fatalf("Contains changed the cache: %+v", stats)
		}
	})
}

// TestCache_OpenFailuresKeepOrDropEntries checks how a failure to open a
// cached file is handled: a transient failure, such as running out of file
// descriptors, keeps the entry for the next lookup; a device read error drops
// it as corrupt; any other error drops it without counting a corruption.
func TestCache_OpenFailuresKeepOrDropEntries(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		kept        bool
		corruptions int64
	}{
		{name: "too many open files", err: syscall.EMFILE, kept: true},
		{name: "too many open files system-wide", err: syscall.ENFILE, kept: true},
		{name: "out of memory", err: syscall.ENOMEM, kept: true},
		{name: "interrupted", err: syscall.EINTR, kept: true},
		{name: "device read error", err: syscall.EIO, corruptions: 1},
		{name: "permission denied", err: syscall.EACCES},
		{name: "not found", err: syscall.ENOENT},
	}
	for _, tc := range cases {
		for _, kind := range []Kind{KindSST, KindBloom} {
			t.Run(fmt.Sprintf("%s/%s", tc.name, kind), func(t *testing.T) {
				c := openCache(t, t.TempDir(), 1<<20, 1<<20)
				data := content(tc.name, 100)
				d := descriptor(kind, data)
				if err := c.Put(d, data); err != nil {
					t.Fatalf("Put: %v", err)
				}

				realOpen := openFile
				openFile = func(path string) (*os.File, error) {
					return nil, &os.PathError{Op: "open", Path: path, Err: tc.err}
				}
				t.Cleanup(func() { openFile = realOpen })
				lookup := func() bool {
					if kind == KindSST {
						file, ok := c.OpenFile(d)
						if ok {
							_ = file.Close()
						}
						return ok
					}
					_, ok := c.ReadVerified(d)
					return ok
				}
				if lookup() {
					t.Fatal("lookup hit while open fails")
				}
				stats := c.Stats(kind)
				if kept := stats.Entries == 1; kept != tc.kept || stats.Corruptions != tc.corruptions {
					t.Fatalf("stats = %+v, want kept=%t corruptions=%d", stats, tc.kept, tc.corruptions)
				}

				openFile = realOpen
				if got := lookup(); got != tc.kept {
					t.Fatalf("lookup after recovery hit=%t, want %t", got, tc.kept)
				}
			})
		}
	}
}

// TestCache_RemovalsAfterCloseLeaveFiles checks that a closed cache no longer
// deletes files: another owner may already have opened the directory and
// indexed them.
func TestCache_RemovalsAfterCloseLeaveFiles(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir, 1<<20, 1<<20)
	sst, bloom := content("sst", 100), content("bloom", 100)
	sstDesc, bloomDesc := descriptor(KindSST, sst), descriptor(KindBloom, bloom)
	commit(t, c, sstDesc, sst)
	if err := c.Put(bloomDesc, bloom); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_ = c.Close()

	c.Remove(sstDesc)
	c.ReportCorrupt(sstDesc)
	c.Purge(KindSST)
	c.Purge(KindBloom)

	next := openCache(t, dir, 1<<20, 1<<20)
	if got, ok := openFileBytes(t, next, sstDesc); !ok || !bytes.Equal(got, sst) {
		t.Fatalf("SST after closed cache's removals hit=%t", ok)
	}
	if got, ok := next.ReadVerified(bloomDesc); !ok || !bytes.Equal(got, bloom) {
		t.Fatalf("Bloom after closed cache's removals hit=%t", ok)
	}
}

// TestCache_FailedPublishEvictsNothing makes publishing fail after the file
// was verified and checks that the files already cached are all kept: a
// failed rename must not cost cached files.
func TestCache_FailedPublishEvictsNothing(t *testing.T) {
	c := openCache(t, t.TempDir(), 150, 1<<20)
	first := content("first", 100)
	firstDesc := descriptor(KindSST, first)
	commit(t, c, firstDesc, first)

	// Pick a second file stored in a different shard, and block that shard's
	// directory with a regular file so publishing it fails.
	firstSum, _ := firstDesc.sum()
	var second []byte
	var secondSum [sha256.Size]byte
	for i := 0; ; i++ {
		second = content(fmt.Sprintf("second-%d", i), 100)
		secondSum = sha256.Sum256(second)
		if secondSum[0] != firstSum[0] {
			break
		}
	}
	shard := filepath.Dir(c.path(KindSST, secondSum))
	if err := os.WriteFile(shard, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("block shard: %v", err)
	}

	// The download is still served, but not cached.
	secondDesc := descriptor(KindSST, second)
	if got := commit(t, c, secondDesc, second); !bytes.Equal(got, second) {
		t.Fatal("download not served after failed publish")
	}
	if c.Contains(secondDesc) || !c.Contains(firstDesc) {
		t.Fatal("expected the first file kept and the second not cached")
	}
	if stats := c.Stats(KindSST); stats.Failures != 1 || stats.Evictions != 0 || stats.Bytes != 100 {
		t.Fatalf("stats = %+v, want one failure and no evictions", stats)
	}
	if incomingFiles(t, c) != 0 {
		t.Fatal("temporary file left behind")
	}
}

// TestOnRemoveReportsEveryRemoval checks that listeners hear about files
// removed by eviction, Remove, ReportCorrupt and Purge, from outside the
// cache's lock (the listener calls back into the cache), and stop once
// cancelled.
func TestOnRemoveReportsEveryRemoval(t *testing.T) {
	c := openCache(t, t.TempDir(), 300, 1<<20)
	var removed []Descriptor
	byName := map[[sha256.Size]byte]Descriptor{}
	cancel := c.OnRemove(func(kind Kind, sum [sha256.Size]byte) {
		_ = c.Contains(byName[sum]) // would deadlock if called with the lock held
		removed = append(removed, byName[sum])
	})
	put := func(seed string) Descriptor {
		t.Helper()
		data := content(seed, 100)
		d := descriptor(KindSST, data)
		sum, err := d.Sum()
		if err != nil {
			t.Fatalf("Sum: %v", err)
		}
		byName[sum] = d
		if err := c.Put(d, data); err != nil {
			t.Fatalf("Put: %v", err)
		}
		return d
	}
	want := func(step string, ds ...Descriptor) {
		t.Helper()
		if len(removed) != len(ds) {
			t.Fatalf("%s: removed %v, want %v", step, removed, ds)
		}
		for i := range ds {
			if removed[i] != ds[i] {
				t.Fatalf("%s: removed %v, want %v", step, removed, ds)
			}
		}
		removed = nil
	}

	a, b, d3 := put("a"), put("b"), put("c")
	want("fill to budget")
	d4 := put("d") // the budget holds three: a, least recently used, goes
	want("eviction", a)
	c.Remove(b)
	want("Remove", b)
	c.ReportCorrupt(d3)
	want("ReportCorrupt", d3)
	c.Remove(a) // already gone
	want("removing a missing file")
	c.Purge(KindSST)
	want("Purge", d4)

	cancel()
	put("e")
	c.Purge(KindSST)
	want("after cancel")
}
