package blobstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	_ "gocloud.dev/blob/fileblob"
)

// NewFile creates a store backed by the local filesystem.
func NewFile(ctx context.Context, dir, prefix string) (*Store, error) {

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

func NewFileTemp(prefix string) (*Store, string, error) {
	dir, err := os.MkdirTemp("", "isledb-*")
	if err != nil {
		return nil, "", fmt.Errorf("create temp dir: %w", err)
	}

	store, err := NewFile(context.Background(), dir, prefix)
	if err != nil {
		os.RemoveAll(dir)
		return nil, "", err
	}

	return store, dir, nil
}
