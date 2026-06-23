package cachestore

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/manifest"
)

type mockStorage struct {
	pages     map[string][]byte
	readCount atomic.Int64
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		pages: make(map[string][]byte),
	}
}

func (m *mockStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	return nil, "", manifest.ErrNotFound
}

func (m *mockStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	return "etag", nil
}

func (m *mockStorage) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	return nil, manifest.ErrNotFound
}

func (m *mockStorage) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	return "snapshot/" + id, nil
}

func (m *mockStorage) ReadPage(ctx context.Context, path string) ([]byte, error) {
	m.readCount.Add(1)
	data, ok := m.pages[path]
	if !ok {
		return nil, manifest.ErrNotFound
	}
	return data, nil
}

func (m *mockStorage) WritePage(ctx context.Context, level uint8, id string, data []byte) (string, error) {
	path := m.PagePath(level, id)
	m.pages[path] = data
	return path, nil
}

func (m *mockStorage) PagePath(level uint8, id string) string {
	return "pages/l00/" + id
}

func TestCachingStorage_CacheHit(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorage()

	mock.pages["pages/l00/entry1"] = []byte("data1")

	cs := NewCachingStorage(mock, CachingStorageOptions{CacheSize: 10})

	data, err := cs.ReadPage(ctx, "pages/l00/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "data1" {
		t.Errorf("expected data1, got %s", string(data))
	}

	if mock.readCount.Load() != 1 {
		t.Errorf("expected 1 read, got %d", mock.readCount.Load())
	}

	data, err = cs.ReadPage(ctx, "pages/l00/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "data1" {
		t.Errorf("expected data1, got %s", string(data))
	}

	if mock.readCount.Load() != 1 {
		t.Errorf("expected still 1 read after pageCache hit, got %d", mock.readCount.Load())
	}

	stats := cs.CacheStats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 pageCache hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected 1 pageCache miss, got %d", stats.Misses)
	}
}

func TestCachingStorage_WriteThroughCaching(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorage()

	cs := NewCachingStorage(mock, CachingStorageOptions{CacheSize: 10})

	path, err := cs.WritePage(ctx, 0, "entry1", []byte("data1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := cs.ReadPage(ctx, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "data1" {
		t.Errorf("expected data1, got %s", string(data))
	}

	if mock.readCount.Load() != 0 {
		t.Errorf("expected 0 reads (pageCache hit from write), got %d", mock.readCount.Load())
	}

	stats := cs.CacheStats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 pageCache hit, got %d", stats.Hits)
	}
	if stats.Misses != 0 {
		t.Errorf("expected 0 pageCache misses, got %d", stats.Misses)
	}
}

func TestCachingStorage_ClearCache(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorage()
	mock.pages["pages/l00/entry1"] = []byte("data1")

	cs := NewCachingStorage(mock, CachingStorageOptions{CacheSize: 10})

	_, err := cs.ReadPage(ctx, "pages/l00/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cs.ClearCache()

	_, err = cs.ReadPage(ctx, "pages/l00/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.readCount.Load() != 2 {
		t.Errorf("expected 2 reads after pageCache clear, got %d", mock.readCount.Load())
	}
}

func TestCachingStorage_DelegateMethods(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorage()
	cs := NewCachingStorage(mock, CachingStorageOptions{CacheSize: 10})

	_, _, err := cs.ReadCurrent(ctx)
	if err != manifest.ErrNotFound {
		t.Errorf("expected ErrNotFound from ReadCurrent")
	}

	_, err = cs.WriteCurrentCAS(ctx, []byte("current"), "etag")
	if err != nil {
		t.Errorf("unexpected error from WriteCurrentCAS: %v", err)
	}

	_, err = cs.ReadSnapshot(ctx, "snap1")
	if err != manifest.ErrNotFound {
		t.Errorf("expected ErrNotFound from ReadSnapshot")
	}

	path, err := cs.WriteSnapshot(ctx, "snap1", []byte("snapshot"))
	if err != nil {
		t.Errorf("unexpected error from WriteSnapshot: %v", err)
	}
	if path != "snapshot/snap1" {
		t.Errorf("expected snapshot/snap1, got %s", path)
	}

	pagePath := cs.PagePath(0, "entry1")
	if pagePath != "pages/l00/entry1" {
		t.Errorf("expected pages/l00/entry1, got %s", pagePath)
	}

	_, err = cs.WritePage(ctx, 0, "entry1", []byte("data1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCachingStorage_CustomCache(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorage()
	mock.pages["pages/l00/entry1"] = []byte("data1")

	customCache := NewLRUManifestPageCache(5)
	cs := NewCachingStorage(mock, CachingStorageOptions{PageCache: customCache})

	_, err := cs.ReadPage(ctx, "pages/l00/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := customCache.Stats()
	if stats.MaxEntries != 5 {
		t.Errorf("expected custom pageCache with 5 max entries, got %d", stats.MaxEntries)
	}
	if stats.EntryCount != 1 {
		t.Errorf("expected 1 entry in custom pageCache, got %d", stats.EntryCount)
	}
}

func TestCachingStorage_DefaultCacheSize(t *testing.T) {
	mock := newMockStorage()
	cs := NewCachingStorage(mock, CachingStorageOptions{})

	stats := cs.CacheStats()
	if stats.MaxEntries != DefaultManifestPageCacheSize {
		t.Errorf("expected default pageCache size %d, got %d", DefaultManifestPageCacheSize, stats.MaxEntries)
	}
}
