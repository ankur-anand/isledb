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

	ReadSnapshot(ctx context.Context, path string) ([]byte, error)
	WriteSnapshot(ctx context.Context, id string, data []byte) (string, error)
}

type PageStorage interface {
	ReadPage(ctx context.Context, path string) ([]byte, error)
	WritePage(ctx context.Context, level uint8, id string, data []byte) (string, error)
	PagePath(level uint8, id string) string
}
