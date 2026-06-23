package cachestore

import (
	"context"

	"github.com/ankur-anand/isledb/manifest"
)

type ManifestPageCache interface {
	Get(path string) ([]byte, bool)
	Set(path string, data []byte)
	Remove(path string)
	Clear()
	Stats() ManifestPageCacheStats
}

type CachingStorage struct {
	storage   manifest.Storage
	pageCache ManifestPageCache
}

type CachingStorageOptions struct {
	PageCache ManifestPageCache
	CacheSize int
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
	pageCache := chooseCache(
		cacheCandidate[ManifestPageCache]{
			enabled: opts.PageCache != nil,
			build: func() ManifestPageCache {
				return opts.PageCache
			},
		},
		cacheCandidate[ManifestPageCache]{
			enabled: opts.CacheSize > 0,
			build: func() ManifestPageCache {
				return NewLRUManifestPageCache(opts.CacheSize)
			},
		},
		cacheCandidate[ManifestPageCache]{
			enabled: true,
			build: func() ManifestPageCache {
				return NewLRUManifestPageCache(DefaultManifestPageCacheSize)
			},
		},
	)

	return &CachingStorage{
		storage:   storage,
		pageCache: pageCache,
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

func (s *CachingStorage) ReadPage(ctx context.Context, path string) ([]byte, error) {
	if data, ok := s.pageCache.Get(path); ok {
		return data, nil
	}
	pages, ok := s.storage.(manifest.PageStorage)
	if !ok {
		return nil, manifest.ErrNotFound
	}
	data, err := pages.ReadPage(ctx, path)
	if err != nil {
		return nil, err
	}
	s.pageCache.Set(path, data)
	return data, nil
}

func (s *CachingStorage) WritePage(ctx context.Context, level uint8, id string, data []byte) (string, error) {
	pages, ok := s.storage.(manifest.PageStorage)
	if !ok {
		return "", manifest.ErrPreconditionFailed
	}
	path, err := pages.WritePage(ctx, level, id, data)
	if err != nil {
		return "", err
	}
	s.pageCache.Set(path, data)
	return path, nil
}

func (s *CachingStorage) PagePath(level uint8, id string) string {
	if pages, ok := s.storage.(manifest.PageStorage); ok {
		return pages.PagePath(level, id)
	}
	return ""
}

func (s *CachingStorage) CacheStats() ManifestPageCacheStats {
	return s.pageCache.Stats()
}

func (s *CachingStorage) ClearCache() {
	s.pageCache.Clear()
}
