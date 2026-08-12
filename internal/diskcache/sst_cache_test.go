package diskcache

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSTCache_RefCounting(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSTCache(SSTCacheOptions{
		Dir:     dir,
		MaxSize: 100,
	})
	require.NoError(t, err)
	defer cache.Close()

	data := make([]byte, 50)
	err = cache.Set("protected", data)
	require.NoError(t, err)

	got, ok := cache.Acquire("protected")
	require.True(t, ok)
	require.Equal(t, data, got)

	for i := range 5 {
		key := fmt.Sprintf("key%d", i)
		newData := make([]byte, 30)
		cache.Set(key, newData)
	}

	_, ok = cache.Acquire("protected")
	require.True(t, ok, "protected entry should not be evicted while acquired")
	cache.Release("protected")

	cache.Release("protected")
	cache.Set("final", make([]byte, 80))

	stats := cache.Stats()
	require.LessOrEqual(t, stats.Size, int64(100))
}

func TestSSTCache_OpenReconcilesFilesLeftByPreviousProcess(t *testing.T) {
	dir := t.TempDir()
	orphan := filepath.Join(dir, cacheFileName("sstable/old.sst"))
	temp := filepath.Join(dir, "sst-interrupted-download")
	unrelated := filepath.Join(dir, "keep-me")
	unrelatedDir := filepath.Join(dir, "sst-unrelated-directory")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o644))
	require.NoError(t, os.WriteFile(temp, []byte("partial"), 0o644))
	require.NoError(t, os.WriteFile(unrelated, []byte("unrelated"), 0o644))
	require.NoError(t, os.Mkdir(unrelatedDir, 0o755))

	cache, err := NewSSTCache(SSTCacheOptions{Dir: dir, MaxSize: 1 << 20})
	require.NoError(t, err)
	defer cache.Close()

	_, err = os.Stat(orphan)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(temp)
	require.ErrorIs(t, err, os.ErrNotExist)
	contents, err := os.ReadFile(unrelated)
	require.NoError(t, err)
	require.Equal(t, []byte("unrelated"), contents)
	info, err := os.Stat(unrelatedDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.Equal(t, Stats{MaxSize: 1 << 20}, cache.Stats())
}

func TestSSTCacheArtifactNames(t *testing.T) {
	for name, want := range map[string]bool{
		cacheFileName("key"):               true,
		"sst-12345":                        true,
		"sst-temp-download":                true,
		"keep-me":                          false,
		"ABCDEF0123456789ABCDEF0123456789": false,
		"0000000000000000000000000000000g": false,
	} {
		if got := isSSTCacheArtifact(name); got != want {
			t.Errorf("isSSTCacheArtifact(%q)=%t want=%t", name, got, want)
		}
	}
}

func TestSSTCache_CloseDefersPinnedEntryRemoval(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewSSTCache(SSTCacheOptions{Dir: dir})
	require.NoError(t, err)

	data := []byte("pinned-sst-data")
	require.NoError(t, cache.Set("pinned", data))
	pinned, ok := cache.Acquire("pinned")
	require.True(t, ok)

	require.NoError(t, cache.Close())
	require.Equal(t, data, pinned)
	_, ok = cache.Acquire("pinned")
	require.False(t, ok, "closed cache must reject new pins")
	require.Error(t, cache.Set("new", data))
	require.Equal(t, 1, cache.Stats().EntryCount)

	cache.Release("pinned")
	require.Equal(t, 0, cache.Stats().EntryCount)
	require.Equal(t, int64(0), cache.Stats().Size)
}

func TestSSTCache_ClearDefersPinnedEntryRemoval(t *testing.T) {
	cache, err := NewSSTCache(SSTCacheOptions{Dir: t.TempDir()})
	require.NoError(t, err)
	defer cache.Close()

	data := []byte("pinned-sst-data")
	require.NoError(t, cache.Set("pinned", data))
	pinned, ok := cache.Acquire("pinned")
	require.True(t, ok)

	require.NoError(t, cache.Clear())
	require.Equal(t, data, pinned)
	require.Equal(t, 1, cache.Stats().EntryCount)

	cache.Release("pinned")
	require.Equal(t, 0, cache.Stats().EntryCount)
	require.NoError(t, cache.Set("after-clear", data))
}

func TestSSTCache_PendingEntryWaitsForEveryPin(t *testing.T) {
	cache, err := NewSSTCache(SSTCacheOptions{Dir: t.TempDir()})
	require.NoError(t, err)
	defer cache.Close()

	data := []byte("shared-sst-data")
	require.NoError(t, cache.Set("shared", data))
	first, ok := cache.Acquire("shared")
	require.True(t, ok)
	cache.Remove("shared")

	second, ok := cache.Acquire("shared")
	require.True(t, ok)
	cache.Release("shared")
	require.Equal(t, data, first)
	require.Equal(t, data, second)
	require.Equal(t, 1, cache.Stats().EntryCount)

	cache.Release("shared")
	require.Equal(t, 0, cache.Stats().EntryCount)
}

func TestSSTCache_ConcurrentPinsOnPendingEntry(t *testing.T) {
	cache, err := NewSSTCache(SSTCacheOptions{Dir: t.TempDir()})
	require.NoError(t, err)
	defer cache.Close()

	data := []byte("shared-sst-data")
	require.NoError(t, cache.Set("shared", data))
	_, ok := cache.Acquire("shared")
	require.True(t, ok)
	cache.Remove("shared")

	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			got, acquired := cache.Acquire("shared")
			if !acquired {
				t.Error("pending entry disappeared while guard pin was held")
				return
			}
			if string(got) != string(data) {
				t.Errorf("acquired data=%q, want %q", got, data)
			}
			runtime.Gosched()
			cache.Release("shared")
		}()
	}
	wg.Wait()

	require.Equal(t, 1, cache.Stats().EntryCount)
	cache.Release("shared")
	require.Equal(t, 0, cache.Stats().EntryCount)
}

func TestSSTCache_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory test in short mode")
	}

	dir := t.TempDir()
	cache, err := NewSSTCache(SSTCacheOptions{
		Dir:     dir,
		MaxSize: 200 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer cache.Close()

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	dataSize := 1024 * 1024
	numEntries := 100
	for i := range numEntries {
		key := fmt.Sprintf("key%d", i)
		data := make([]byte, dataSize)
		err := cache.Set(key, data)
		require.NoError(t, err)
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	heapGrowth := m2.HeapAlloc - m1.HeapAlloc
	require.Less(t, heapGrowth, uint64(10*1024*1024),
		"heap grew too much - data may not be off-heap")
}

func TestSSTCache_SetFromFile(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSTCache(SSTCacheOptions{
		Dir:     dir,
		MaxSize: 1024,
	})
	require.NoError(t, err)
	defer cache.Close()

	fb, ok := cache.(FileBackedCache)
	require.True(t, ok, "cache should implement FileBackedCache")

	data := []byte("hello-sst")
	tmpFile, err := os.CreateTemp(dir, "sst-temp-*")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()

	_, err = tmpFile.Write(data)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	err = fb.SetFromFile("sst-key", tmpPath, int64(len(data)))
	require.NoError(t, err)

	_, err = os.Stat(tmpPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	got, ok := cache.Acquire("sst-key")
	require.True(t, ok)
	require.Equal(t, data, got)
	cache.Release("sst-key")

	localPath := filepath.Join(dir, cacheFileName("sst-key"))
	_, err = os.Stat(localPath)
	require.NoError(t, err)
}

func TestSSTCache_EvictOldest_SkipsStaleListNode(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewSSTCache(SSTCacheOptions{
		Dir:     dir,
		MaxSize: 60,
	})
	require.NoError(t, err)
	defer cache.Close()

	require.NoError(t, cache.Set("k1", make([]byte, 30)))
	require.NoError(t, cache.Set("k2", make([]byte, 30)))

	sc := cache.(*sstCache)
	sc.mu.Lock()
	sc.order.PushFront("stale-key")
	sc.mu.Unlock()

	require.NoError(t, cache.Set("k3", make([]byte, 30)))

	stats := cache.Stats()
	require.LessOrEqual(t, stats.Size, int64(60), "cache should still respect max size")

	_, ok := cache.Acquire("k3")
	require.True(t, ok, "newly inserted key should exist")
	cache.Release("k3")
}
