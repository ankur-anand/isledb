package manifest

import (
	"context"
	"errors"
	"fmt"

	"github.com/ankur-anand/isledb/blobstore"
)

type BlobStoreBackend struct {
	store *blobstore.Store
}

func NewBlobStoreBackend(store *blobstore.Store) *BlobStoreBackend {
	return &BlobStoreBackend{store: store}
}

func (b *BlobStoreBackend) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	data, attr, err := b.store.Read(ctx, b.store.ManifestPath())
	if err != nil {
		return nil, "", b.mapError(err)
	}
	if attr.ETag == "" && attr.Generation == 0 {
		if refreshed, refreshErr := b.store.Attributes(ctx, b.store.ManifestPath()); refreshErr == nil {
			attr = refreshed
		}
	}
	return data, b.etagFromAttrs(attr), nil
}

func (b *BlobStoreBackend) ReadCurrentData(ctx context.Context) ([]byte, error) {
	data, _, err := b.store.Read(ctx, b.store.ManifestPath())
	if err != nil {
		return nil, b.mapError(err)
	}
	return data, nil
}

func (b *BlobStoreBackend) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	attr, err := b.store.WriteIfMatch(ctx, b.store.ManifestPath(), data, expectedETag)
	return b.etagFromAttrs(attr), b.mapError(err)
}

func (b *BlobStoreBackend) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	data, _, err := b.store.Read(ctx, path)
	return data, b.mapError(err)
}

func (b *BlobStoreBackend) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	path := b.store.ManifestSnapshotPath(id)
	_, err := b.store.Write(ctx, path, data)
	return path, b.mapError(err)
}

func (b *BlobStoreBackend) ReadPage(ctx context.Context, path string) ([]byte, error) {
	data, _, err := b.store.Read(ctx, path)
	return data, b.mapError(err)
}

func (b *BlobStoreBackend) WritePage(ctx context.Context, level uint8, id string, data []byte) (string, error) {
	path := b.store.ManifestPagePath(level, id)
	_, err := b.store.WriteIfNotExist(ctx, path, data)
	return path, b.mapError(err)
}

func (b *BlobStoreBackend) PagePath(level uint8, id string) string {
	return b.store.ManifestPagePath(level, id)
}

func (b *BlobStoreBackend) etagFromAttrs(attr blobstore.Attributes) string {
	if attr.Generation > 0 {
		return fmt.Sprintf("%d", attr.Generation)
	}
	return attr.ETag
}

func (b *BlobStoreBackend) mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, blobstore.ErrNotFound) {
		return ErrNotFound
	}
	if errors.Is(err, blobstore.ErrPreconditionFailed) {
		return ErrPreconditionFailed
	}
	return err
}
