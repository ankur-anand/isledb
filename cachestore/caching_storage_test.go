package cachestore

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/manifest"
)

type mockStorage struct {
	logs      map[string][]byte
	readCount atomic.Int64
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		logs: make(map[string][]byte),
	}
}

func (m *mockStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	return nil, "", manifest.ErrNotFound
}

func (m *mockStorage) WriteCurrent(ctx context.Context, data []byte) error {
	return nil
}

func (m *mockStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) error {
	return nil
}

func (m *mockStorage) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	return nil, manifest.ErrNotFound
}

func (m *mockStorage) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	return "snapshot/" + id, nil
}

func (m *mockStorage) ReadLog(ctx context.Context, path string) ([]byte, error) {
	m.readCount.Add(1)
	data, ok := m.logs[path]
	if !ok {
		return nil, manifest.ErrNotFound
	}
	return data, nil
}

func (m *mockStorage) WriteLog(ctx context.Context, name string, data []byte) (string, error) {
	path := "logs/" + name
	m.logs[path] = data
	return path, nil
}

func (m *mockStorage) ListLogs(ctx context.Context) ([]string, error) {
	paths := make([]string, 0, len(m.logs))
	for path := range m.logs {
		paths = append(paths, path)
	}
	return paths, nil
}

func (m *mockStorage) LogPath(name string) string {
	return "logs/" + name
}

func TestCachingStorage_CacheHit(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorage()

	mock.logs["logs/entry1"] = []byte("data1")

	cs := NewCachingStorage(mock, CachingStorageOptions{CacheSize: 10})

	data, err := cs.ReadLog(ctx, "logs/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "data1" {
		t.Errorf("expected data1, got %s", string(data))
	}

	if mock.readCount.Load() != 1 {
		t.Errorf("expected 1 read, got %d", mock.readCount.Load())
	}

	data, err = cs.ReadLog(ctx, "logs/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "data1" {
		t.Errorf("expected data1, got %s", string(data))
	}

	if mock.readCount.Load() != 1 {
		t.Errorf("expected still 1 read after manifestCache hit, got %d", mock.readCount.Load())
	}

	stats := cs.CacheStats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 manifestCache hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected 1 manifestCache miss, got %d", stats.Misses)
	}
}

func TestCachingStorage_WriteThroughCaching(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorage()

	cs := NewCachingStorage(mock, CachingStorageOptions{CacheSize: 10})

	path, err := cs.WriteLog(ctx, "entry1", []byte("data1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := cs.ReadLog(ctx, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "data1" {
		t.Errorf("expected data1, got %s", string(data))
	}

	if mock.readCount.Load() != 0 {
		t.Errorf("expected 0 reads (manifestCache hit from write), got %d", mock.readCount.Load())
	}

	stats := cs.CacheStats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 manifestCache hit, got %d", stats.Hits)
	}
	if stats.Misses != 0 {
		t.Errorf("expected 0 manifestCache misses, got %d", stats.Misses)
	}
}

func TestCachingStorage_ClearCache(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorage()
	mock.logs["logs/entry1"] = []byte("data1")

	cs := NewCachingStorage(mock, CachingStorageOptions{CacheSize: 10})

	_, err := cs.ReadLog(ctx, "logs/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cs.ClearCache()

	_, err = cs.ReadLog(ctx, "logs/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.readCount.Load() != 2 {
		t.Errorf("expected 2 reads after manifestCache clear, got %d", mock.readCount.Load())
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

	err = cs.WriteCurrent(ctx, []byte("current"))
	if err != nil {
		t.Errorf("unexpected error from WriteCurrent: %v", err)
	}

	err = cs.WriteCurrentCAS(ctx, []byte("current"), "etag")
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

	logPath := cs.LogPath("entry1")
	if logPath != "logs/entry1" {
		t.Errorf("expected logs/entry1, got %s", logPath)
	}

	_, err = cs.WriteLog(ctx, "entry1", []byte("data1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	logs, err := cs.ListLogs(ctx)
	if err != nil {
		t.Errorf("unexpected error from ListLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(logs))
	}
}

func TestCachingStorage_CustomCache(t *testing.T) {
	ctx := context.Background()
	mock := newMockStorage()
	mock.logs["logs/entry1"] = []byte("data1")

	customCache := NewLRUManifestLogCache(5)
	cs := NewCachingStorage(mock, CachingStorageOptions{ManifestCache: customCache})

	_, err := cs.ReadLog(ctx, "logs/entry1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := customCache.Stats()
	if stats.MaxEntries != 5 {
		t.Errorf("expected custom manifestCache with 5 max entries, got %d", stats.MaxEntries)
	}
	if stats.EntryCount != 1 {
		t.Errorf("expected 1 entry in custom manifestCache, got %d", stats.EntryCount)
	}
}

func TestCachingStorage_DefaultCacheSize(t *testing.T) {
	mock := newMockStorage()
	cs := NewCachingStorage(mock, CachingStorageOptions{})

	stats := cs.CacheStats()
	if stats.MaxEntries != DefaultManifestLogCacheSize {
		t.Errorf("expected default manifestCache size %d, got %d", DefaultManifestLogCacheSize, stats.MaxEntries)
	}
}
