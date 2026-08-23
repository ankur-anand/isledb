package blobstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "gocloud.dev/blob/fileblob"
)

func openFile(ctx context.Context, dir, prefix string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory %s: %w", dir, err)
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("absolute path %s: %w", dir, err)
	}

	bucketURL := "file://" + absDir

	return Open(ctx, bucketURL, prefix)
}

func newFileTemp(prefix string) (*Store, string, error) {
	dir, err := os.MkdirTemp("", "isledb-*")
	if err != nil {
		return nil, "", fmt.Errorf("create temp dir: %w", err)
	}

	store, err := openFile(context.Background(), dir, prefix)
	if err != nil {
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remove temp directory %s: %w", dir, removeErr))
		}
		return nil, "", err
	}

	return store, dir, nil
}
