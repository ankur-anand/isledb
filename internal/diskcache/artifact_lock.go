package diskcache

import (
	"fmt"
	"path/filepath"

	"github.com/gofrs/flock"
)

const artifactCacheLockName = "CACHELOCK"

type artifactDirectoryLock struct {
	lock *flock.Flock
}

func (l *artifactDirectoryLock) close() error {
	if l == nil || l.lock == nil {
		return nil
	}
	err := l.lock.Close()
	l.lock = nil
	return err
}

func acquireArtifactDirectoryLock(dir string) (*artifactDirectoryLock, error) {
	path := filepath.Join(dir, artifactCacheLockName)
	fileLock := flock.New(path, flock.SetPermissions(0o600))
	locked, err := fileLock.TryLock()
	if err != nil {
		_ = fileLock.Close()
		return nil, fmt.Errorf("diskcache: lock artifact cache: %w", err)
	}
	if !locked {
		_ = fileLock.Close()
		return nil, fmt.Errorf("%w: %s", ErrArtifactCacheLocked, dir)
	}

	// The file is intentionally persistent. Ownership is represented only by
	// the OS lock held by fileLock, so process exit or a crash releases it even
	// though CACHELOCK remains in the cache directory.
	return &artifactDirectoryLock{lock: fileLock}, nil
}
