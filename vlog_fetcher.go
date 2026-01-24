package isledb

import (
	"context"
	"fmt"

	"github.com/ankur-anand/isledb/blobstore"
)

const DefaultVLogCacheSize = 50

// VLogFetcher fetches VLog data from object storage on demand.
type VLogFetcher struct {
	store *blobstore.Store
	cache VLogCache
}

// NewVLogFetcherWithCache creates a fetcher with the provided cache.
func NewVLogFetcherWithCache(store *blobstore.Store, cache VLogCache) *VLogFetcher {
	if cache == nil {
		cache = NewLRUVLogCache(DefaultVLogCacheSize)
	}
	return &VLogFetcher{
		store: store,
		cache: cache,
	}
}

// GetValue fetches a value from a VLog, caching the VLog data.
func (f *VLogFetcher) GetValue(ctx context.Context, ptr VLogPointer) ([]byte, error) {
	if f.cache != nil {
		if data, ok := f.cache.Get(ptr.VLogID); ok {
			reader := NewVLogReader(data)
			return reader.ReadValue(ptr)
		}
	}

	path := f.store.VLogPath(ptr.VLogID.String())
	data, _, err := f.store.Read(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("fetch vlog %s: %w", ptr.VLogID.String(), err)
	}

	if f.cache != nil {
		if existing, ok := f.cache.Get(ptr.VLogID); ok {
			data = existing
		} else {
			f.cache.Set(ptr.VLogID, data)
		}
	}

	reader := NewVLogReader(data)
	return reader.ReadValue(ptr)
}

// ClearCache clears the VLog cache.
func (f *VLogFetcher) ClearCache() {
	if f.cache != nil {
		f.cache.Clear()
	}
}
