package manifest

import (
	"context"
	"errors"

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
	return data, attr.ETag, b.mapError(err)
}

func (b *BlobStoreBackend) WriteCurrent(ctx context.Context, data []byte) error {
	_, err := b.store.Write(ctx, b.store.ManifestPath(), data)
	return b.mapError(err)
}

func (b *BlobStoreBackend) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) error {
	_, err := b.store.WriteIfMatch(ctx, b.store.ManifestPath(), data, expectedETag)
	return b.mapError(err)
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

func (b *BlobStoreBackend) ReadLog(ctx context.Context, path string) ([]byte, error) {
	data, _, err := b.store.Read(ctx, path)
	return data, b.mapError(err)
}

func (b *BlobStoreBackend) WriteLog(ctx context.Context, name string, data []byte) (string, error) {
	path := b.store.ManifestLogPath(name)
	_, err := b.store.Write(ctx, path, data)
	return path, b.mapError(err)
}

func (b *BlobStoreBackend) ListLogs(ctx context.Context) ([]string, error) {
	objects, err := b.store.ListManifestLogs(ctx)
	if err != nil {
		return nil, b.mapError(err)
	}

	var entries []string
	for _, obj := range objects {
		if obj.IsDir {
			continue
		}
		entries = append(entries, obj.Key)
	}
	return entries, nil
}

func (b *BlobStoreBackend) LogPath(name string) string {
	return b.store.ManifestLogPath(name)
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
