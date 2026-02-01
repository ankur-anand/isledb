package diskcache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type cacheFactory struct {
	name    string
	newFunc func(t *testing.T, maxSize int64) Cache
}

type cacheBenchFactory struct {
	name    string
	newFunc func(b *testing.B, maxSize int64) Cache
}

var cacheFactories = []cacheFactory{
	{
		name: "BlobCache",
		newFunc: func(t *testing.T, maxSize int64) Cache {
			opts := BlobCacheOptions{Dir: t.TempDir()}
			if maxSize > 0 {
				opts.MaxSize = maxSize
			}
			cache, err := NewBlobCache(opts)
			require.NoError(t, err)
			t.Cleanup(func() { cache.Close() })
			return cache
		},
	},
	{
		name: "SSTCache",
		newFunc: func(t *testing.T, maxSize int64) Cache {
			opts := SSTCacheOptions{Dir: t.TempDir()}
			if maxSize > 0 {
				opts.MaxSize = maxSize
			}
			cache, err := NewSSTCache(opts)
			require.NoError(t, err)
			t.Cleanup(func() { cache.Close() })
			return cache
		},
	},
}

var cacheBenchFactories = []cacheBenchFactory{
	{
		name: "BlobCache",
		newFunc: func(b *testing.B, maxSize int64) Cache {
			opts := BlobCacheOptions{Dir: b.TempDir()}
			if maxSize > 0 {
				opts.MaxSize = maxSize
			}
			cache, err := NewBlobCache(opts)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { cache.Close() })
			return cache
		},
	},
	{
		name: "SSTCache",
		newFunc: func(b *testing.B, maxSize int64) Cache {
			opts := SSTCacheOptions{Dir: b.TempDir()}
			if maxSize > 0 {
				opts.MaxSize = maxSize
			}
			cache, err := NewSSTCache(opts)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { cache.Close() })
			return cache
		},
	},
}

func TestCache_Basic(t *testing.T) {
	for _, cf := range cacheFactories {
		t.Run(cf.name, func(t *testing.T) {
			cache := cf.newFunc(t, 0)

			data := []byte("hello world")
			err := cache.Set("key1", data)
			require.NoError(t, err)

			got, ok := cache.Get("key1")
			require.True(t, ok)
			require.Equal(t, data, got)

			_, ok = cache.Get("nonexistent")
			require.False(t, ok)

			cache.Remove("key1")
			_, ok = cache.Get("key1")
			require.False(t, ok)

			stats := cache.Stats()
			require.Equal(t, int64(1), stats.Hits)
			require.Equal(t, int64(2), stats.Misses)
			require.Equal(t, int64(0), stats.Size)
			require.Equal(t, 0, stats.EntryCount)
		})
	}
}

func TestCache_LRUEviction(t *testing.T) {
	for _, cf := range cacheFactories {
		t.Run(cf.name, func(t *testing.T) {
			cache := cf.newFunc(t, 100)

			for i := range 5 {
				key := fmt.Sprintf("key%d", i)
				data := make([]byte, 30)
				for j := range data {
					data[j] = byte(i)
				}
				err := cache.Set(key, data)
				require.NoError(t, err)
			}

			stats := cache.Stats()
			require.LessOrEqual(t, stats.Size, int64(100))

			_, ok := cache.Get("key0")
			require.False(t, ok, "key0 should have been evicted")

			_, ok = cache.Get("key1")
			require.False(t, ok, "key1 should have been evicted")

			_, ok = cache.Get("key4")
			require.True(t, ok, "key4 should still exist")
		})
	}
}

func TestCache_Concurrent(t *testing.T) {
	for _, cf := range cacheFactories {
		t.Run(cf.name, func(t *testing.T) {
			cache := cf.newFunc(t, 10*1024*1024)

			const numGoroutines = 10
			const numOps = 100

			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			for g := range numGoroutines {
				go func(gid int) {
					defer wg.Done()
					for i := range numOps {
						key := fmt.Sprintf("key-%d-%d", gid, i)
						data := fmt.Appendf(nil, "data-%d-%d", gid, i)

						err := cache.Set(key, data)
						if err != nil {
							t.Errorf("Set failed: %v", err)
							return
						}

						got, ok := cache.Get(key)
						if ok {
							if string(got) != string(data) {
								t.Errorf("Got %s, want %s", got, data)
							}
						}
					}
				}(g)
			}

			wg.Wait()

			stats := cache.Stats()
			require.Greater(t, stats.Hits, int64(0))
		})
	}
}

func TestCache_Clear(t *testing.T) {
	for _, cf := range cacheFactories {
		t.Run(cf.name, func(t *testing.T) {
			cache := cf.newFunc(t, 0)

			for i := range 10 {
				key := fmt.Sprintf("key%d", i)
				data := fmt.Appendf(nil, "data%d", i)
				err := cache.Set(key, data)
				require.NoError(t, err)
			}

			stats := cache.Stats()
			require.Equal(t, 10, stats.EntryCount)

			err := cache.Clear()
			require.NoError(t, err)

			stats = cache.Stats()
			require.Equal(t, 0, stats.EntryCount)
			require.Equal(t, int64(0), stats.Size)

			for i := range 10 {
				key := fmt.Sprintf("key%d", i)
				_, ok := cache.Get(key)
				require.False(t, ok)
			}
		})
	}
}

func TestCache_Overwrite(t *testing.T) {
	for _, cf := range cacheFactories {
		t.Run(cf.name, func(t *testing.T) {
			cache := cf.newFunc(t, 0)

			err := cache.Set("key", []byte("initial"))
			require.NoError(t, err)

			got, ok := cache.Get("key")
			require.True(t, ok)
			require.Equal(t, []byte("initial"), got)

			err = cache.Set("key", []byte("updated"))
			require.NoError(t, err)

			got, ok = cache.Get("key")
			require.True(t, ok)
			require.Equal(t, []byte("updated"), got)

			stats := cache.Stats()
			require.Equal(t, 1, stats.EntryCount)
		})
	}
}

func TestCache_EmptyData(t *testing.T) {
	for _, cf := range cacheFactories {
		t.Run(cf.name, func(t *testing.T) {
			cache := cf.newFunc(t, 0)

			err := cache.Set("empty", []byte{})
			require.NoError(t, err)

			got, ok := cache.Get("empty")
			require.True(t, ok)
			require.Empty(t, got)
		})
	}
}

func TestCache_NamespacedKeys(t *testing.T) {
	for _, cf := range cacheFactories {
		t.Run(cf.name, func(t *testing.T) {
			cache := cf.newFunc(t, 0)

			keys := []string{
				"db1/blob/abc123",
				"db1/blob/def456",
				"db2/blob/abc123",
				"/absolute/path/blob.dat",
				"special:chars!@#$%",
			}

			for _, key := range keys {
				data := []byte("data for " + key)
				err := cache.Set(key, data)
				require.NoError(t, err, "failed to set key: %s", key)

				got, ok := cache.Get(key)
				require.True(t, ok, "failed to get key: %s", key)
				require.Equal(t, data, got, "data mismatch for key: %s", key)
			}
		})
	}
}

func BenchmarkCache_Get(b *testing.B) {
	for _, cf := range cacheBenchFactories {
		b.Run(cf.name, func(b *testing.B) {
			cache := cf.newFunc(b, 0)

			data := make([]byte, 4096)
			for i := range 1000 {
				key := fmt.Sprintf("key%d", i)
				if err := cache.Set(key, data); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("key%d", i%1000)
					cache.Get(key)
					i++
				}
			})
		})
	}
}

func BenchmarkCache_Set(b *testing.B) {
	for _, cf := range cacheBenchFactories {
		b.Run(cf.name, func(b *testing.B) {
			cache := cf.newFunc(b, 1<<30)

			data := make([]byte, 4096)

			b.ResetTimer()
			for i := range b.N {
				key := fmt.Sprintf("key%d", i)
				if err := cache.Set(key, data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
