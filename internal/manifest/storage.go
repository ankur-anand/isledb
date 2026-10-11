package manifest

import (
	"context"
	"errors"
)

var (
	ErrNotFound           = errors.New("manifest storage: object not found")
	ErrPreconditionFailed = errors.New("manifest storage: precondition failed")
)

type Storage interface {
	ReadCurrent(ctx context.Context) ([]byte, string, error)
	WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error)
	ReadMaintenanceHead(ctx context.Context) ([]byte, string, error)
	WriteMaintenanceHeadCAS(ctx context.Context, data []byte, expectedETag string) (string, error)

	ReadSnapshot(ctx context.Context, path string) ([]byte, error)
	WriteSnapshot(ctx context.Context, id string, data []byte) (string, error)
}

type PageStorage interface {
	ReadPage(ctx context.Context, path string) ([]byte, error)
	WritePage(ctx context.Context, level uint8, id string, data []byte) (string, error)
	PagePath(level uint8, id string) string
}

// SizedReader is a Storage that reads snapshots and pages better when told
// their size, the EncodedBytes of their reference.
type SizedReader interface {
	ReadSnapshotSized(ctx context.Context, path string, size uint64) ([]byte, error)
	ReadPageSized(ctx context.Context, path string, size uint64) ([]byte, error)
}

func readSnapshot(ctx context.Context, storage Storage, ref ObjectRef) ([]byte, error) {
	if sized, ok := storage.(SizedReader); ok && ref.EncodedBytes > 0 {
		return sized.ReadSnapshotSized(ctx, ref.Path, ref.EncodedBytes)
	}
	return storage.ReadSnapshot(ctx, ref.Path)
}

func readPage(ctx context.Context, storage Storage, pages PageStorage, ref ObjectRef) ([]byte, error) {
	if sized, ok := storage.(SizedReader); ok && ref.EncodedBytes > 0 {
		return sized.ReadPageSized(ctx, ref.Path, ref.EncodedBytes)
	}
	return pages.ReadPage(ctx, ref.Path)
}
