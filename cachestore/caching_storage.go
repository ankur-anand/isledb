package cachestore

import (
	"context"

	"github.com/ankur-anand/isledb/manifest"
)

type ManifestLogCache interface {
	Get(path string) ([]byte, bool)
	Set(path string, data []byte)
	Remove(path string)
	Clear()
	Stats() ManifestLogCacheStats
}

type CachingStorage struct {
	storage       manifest.Storage
	manifestCache ManifestLogCache
}

type CachingStorageOptions struct {
	ManifestCache ManifestLogCache
	CacheSize     int
}

type cacheCandidate[T any] struct {
	enabled bool
	build   func() T
}

func chooseCache[T any](candidates ...cacheCandidate[T]) T {
	for _, candidate := range candidates {
		if candidate.enabled {
			return candidate.build()
		}
	}

	var zero T
	return zero
}

func NewCachingStorage(storage manifest.Storage, opts CachingStorageOptions) *CachingStorage {
	manifestCacheChoose := chooseCache(
		cacheCandidate[ManifestLogCache]{
			enabled: opts.ManifestCache != nil,
			build: func() ManifestLogCache {
				return opts.ManifestCache
			},
		},
		cacheCandidate[ManifestLogCache]{
			enabled: opts.CacheSize > 0,
			build: func() ManifestLogCache {
				return NewLRUManifestLogCache(opts.CacheSize)
			},
		},
		cacheCandidate[ManifestLogCache]{
			enabled: true,
			build: func() ManifestLogCache {
				return NewLRUManifestLogCache(DefaultManifestLogCacheSize)
			},
		},
	)

	return &CachingStorage{
		storage:       storage,
		manifestCache: manifestCacheChoose,
	}
}

func (s *CachingStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	return s.storage.ReadCurrent(ctx)
}

func (s *CachingStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	return s.storage.WriteCurrentCAS(ctx, data, expectedETag)
}

func (s *CachingStorage) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	return s.storage.ReadSnapshot(ctx, path)
}

func (s *CachingStorage) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	return s.storage.WriteSnapshot(ctx, id, data)
}

func (s *CachingStorage) ReadLog(ctx context.Context, path string) ([]byte, error) {
	if data, ok := s.manifestCache.Get(path); ok {
		return data, nil
	}

	data, err := s.storage.ReadLog(ctx, path)
	if err != nil {
		return nil, err
	}

	s.manifestCache.Set(path, data)
	return data, nil
}

func (s *CachingStorage) WriteLog(ctx context.Context, name string, data []byte) (string, error) {
	path, err := s.storage.WriteLog(ctx, name, data)
	if err != nil {
		return "", err
	}

	s.manifestCache.Set(path, data)
	return path, nil
}

func (s *CachingStorage) ListLogs(ctx context.Context) ([]string, error) {
	return s.storage.ListLogs(ctx)
}

func (s *CachingStorage) LogPath(name string) string {
	return s.storage.LogPath(name)
}

func (s *CachingStorage) CacheStats() ManifestLogCacheStats {
	return s.manifestCache.Stats()
}

func (s *CachingStorage) ClearCache() {
	s.manifestCache.Clear()
}
