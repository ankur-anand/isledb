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
	WriteCurrent(ctx context.Context, data []byte) error
	WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) error

	ReadSnapshot(ctx context.Context, path string) ([]byte, error)
	WriteSnapshot(ctx context.Context, id string, data []byte) (string, error)

	ReadLog(ctx context.Context, path string) ([]byte, error)
	WriteLog(ctx context.Context, name string, data []byte) (string, error)
	ListLogs(ctx context.Context) ([]string, error)
}
