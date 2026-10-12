package manifest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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

// RefReader is a Storage that reads snapshots and pages better when given
// their reference: its size, EncodedBytes, and its checksum.
type RefReader interface {
	ReadSnapshotRef(ctx context.Context, ref ObjectRef) ([]byte, error)
	ReadPageRef(ctx context.Context, ref ObjectRef) ([]byte, error)
}

// Matches reports whether data is the object ref describes: its size and
// checksum.
func (ref ObjectRef) Matches(data []byte) bool {
	if uint64(len(data)) != ref.EncodedBytes {
		return false
	}
	sum := sha256.Sum256(data)
	return ref.Checksum == fmt.Sprintf("sha256:%x", sum[:])
}

func readSnapshot(ctx context.Context, storage Storage, ref ObjectRef) ([]byte, error) {
	if byRef, ok := storage.(RefReader); ok && ref.EncodedBytes > 0 {
		return byRef.ReadSnapshotRef(ctx, ref)
	}
	return storage.ReadSnapshot(ctx, ref.Path)
}

func readPage(ctx context.Context, storage Storage, pages PageStorage, ref ObjectRef) ([]byte, error) {
	if byRef, ok := storage.(RefReader); ok && ref.EncodedBytes > 0 {
		return byRef.ReadPageRef(ctx, ref)
	}
	return pages.ReadPage(ctx, ref.Path)
}
