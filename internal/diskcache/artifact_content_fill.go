package diskcache

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// artifactContentFill streams one artifact into a uniquely named incoming
// file while enforcing the size and checksum supplied by the manifest.
type artifactContentFill struct {
	mu sync.Mutex

	address      artifactContentAddress
	expectedSize int64
	file         *os.File
	path         string
	hash         hash.Hash
	written      int64
	done         bool
}

func newArtifactContentFill(
	incomingDir string,
	desc ArtifactDescriptor,
) (*artifactContentFill, error) {
	if err := desc.validate(); err != nil {
		return nil, err
	}
	address, err := artifactContentAddressFor(desc)
	if err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(incomingDir, desc.Key.Kind.dirName()+"-*.part")
	if err != nil {
		return nil, fmt.Errorf("diskcache: create incoming artifact: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		return nil, errors.Join(
			fmt.Errorf("diskcache: secure incoming artifact: %w", err),
			file.Close(), removeArtifactContentFile(file.Name()))
	}
	return &artifactContentFill{
		address:      address,
		expectedSize: desc.Size,
		file:         file,
		path:         file.Name(),
		hash:         sha256.New(),
	}, nil
}

func (fill *artifactContentFill) Write(data []byte) (int, error) {
	if fill == nil {
		return 0, os.ErrInvalid
	}
	fill.mu.Lock()
	defer fill.mu.Unlock()
	if fill.done || fill.file == nil {
		return 0, os.ErrClosed
	}
	remaining := fill.expectedSize - fill.written
	if int64(len(data)) > remaining {
		return 0, fmt.Errorf(
			"%w: write=%d remaining=%d", ErrArtifactSizeMismatch, len(data), remaining)
	}
	written, err := fill.file.Write(data)
	if written > 0 {
		_, _ = fill.hash.Write(data[:written])
		fill.written += int64(written)
	}
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	return written, err
}

// finish verifies the completed download and transfers ownership of its temp
// file to a staged artifact. Verification failures close and remove the temp.
func (fill *artifactContentFill) finish() (*artifactStagedContent, error) {
	if fill == nil {
		return nil, os.ErrInvalid
	}
	fill.mu.Lock()
	if fill.done {
		fill.mu.Unlock()
		return nil, os.ErrClosed
	}
	fill.done = true
	file := fill.file
	path := fill.path
	written := fill.written
	actualChecksum := fill.hash.Sum(nil)
	fill.file = nil
	fill.path = ""
	fill.mu.Unlock()

	cleanupFailure := func(cause error) (*artifactStagedContent, error) {
		return nil, errors.Join(cause, file.Close(), removeArtifactContentFile(path))
	}
	if written != fill.expectedSize {
		return cleanupFailure(fmt.Errorf(
			"%w: got=%d want=%d", ErrArtifactSizeMismatch, written, fill.expectedSize))
	}
	if subtle.ConstantTimeCompare(actualChecksum, fill.address.checksum[:]) != 1 {
		return cleanupFailure(ErrArtifactChecksumMismatch)
	}
	return &artifactStagedContent{
		address: fill.address,
		size:    written,
		file:    file,
		path:    path,
		syncFile: func(file *os.File) error {
			return file.Sync()
		},
	}, nil
}

// abort removes an incomplete download. It is safe to call after finish.
func (fill *artifactContentFill) abort() error {
	if fill == nil {
		return nil
	}
	fill.mu.Lock()
	if fill.done {
		fill.mu.Unlock()
		return nil
	}
	fill.done = true
	file := fill.file
	path := fill.path
	fill.file = nil
	fill.path = ""
	fill.mu.Unlock()

	var err error
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	err = errors.Join(err, removeArtifactContentFile(path))
	return err
}

// artifactStagedContent owns verified bytes at an incoming temp path. It is
// consumed exactly once by successful publication, transient opening, or
// discard.
type artifactStagedContent struct {
	mu sync.Mutex

	address  artifactContentAddress
	size     int64
	file     *os.File
	path     string
	consumed bool
	syncFile func(*os.File) error
}

// publish syncs the completed file and renames it to its derived final path.
// Directory entries are intentionally not synced because cache persistence is
// best-effort. On failure the incoming path remains available for transient
// serving whenever the rename did not succeed.
func (staged *artifactStagedContent) publish(finalPath string) error {
	if staged == nil {
		return os.ErrInvalid
	}
	staged.mu.Lock()
	defer staged.mu.Unlock()
	if staged.consumed || staged.path == "" {
		return os.ErrClosed
	}
	if staged.file != nil {
		if err := staged.syncFile(staged.file); err != nil {
			return fmt.Errorf("diskcache: sync incoming artifact: %w", err)
		}
		if err := staged.file.Close(); err != nil {
			staged.file = nil
			return fmt.Errorf("diskcache: close incoming artifact: %w", err)
		}
		staged.file = nil
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o700); err != nil {
		return fmt.Errorf("diskcache: create artifact directory: %w", err)
	}
	if err := os.Rename(staged.path, finalPath); err != nil {
		return fmt.Errorf("diskcache: publish artifact: %w", err)
	}
	staged.path = ""
	staged.consumed = true
	return nil
}

// openTransient transfers the incoming file to a handle that unconditionally
// removes that unique temp path when closed. It never owns a final derived
// cache path.
func (staged *artifactStagedContent) openTransient() (*artifactTransientHandle, error) {
	if staged == nil {
		return nil, os.ErrInvalid
	}
	staged.mu.Lock()
	defer staged.mu.Unlock()
	if staged.consumed || staged.path == "" {
		return nil, os.ErrClosed
	}
	if staged.file != nil {
		if err := staged.file.Close(); err != nil {
			staged.file = nil
			cleanupErr := removeArtifactContentFile(staged.path)
			staged.path = ""
			staged.consumed = true
			return nil, errors.Join(
				fmt.Errorf("diskcache: close transient artifact: %w", err), cleanupErr)
		}
		staged.file = nil
	}

	path := staged.path
	data, mapped, err := readTransientArtifact(staged.address.kind, path)
	if err != nil {
		cleanupErr := removeArtifactContentFile(path)
		staged.path = ""
		staged.consumed = true
		return nil, errors.Join(err, cleanupErr)
	}
	staged.path = ""
	staged.consumed = true
	return &artifactTransientHandle{data: data, path: path, mapped: mapped}, nil
}

func (staged *artifactStagedContent) discard() error {
	if staged == nil {
		return nil
	}
	staged.mu.Lock()
	if staged.consumed {
		staged.mu.Unlock()
		return nil
	}
	staged.consumed = true
	file := staged.file
	path := staged.path
	staged.file = nil
	staged.path = ""
	staged.mu.Unlock()

	var err error
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	return errors.Join(err, removeArtifactContentFile(path))
}

type artifactTransientHandle struct {
	data   []byte
	path   string
	mapped bool
	once   sync.Once
	err    error
}

func (handle *artifactTransientHandle) bytes() []byte {
	if handle == nil {
		return nil
	}
	return handle.data
}

func (handle *artifactTransientHandle) close() error {
	if handle == nil {
		return nil
	}
	handle.once.Do(func() {
		if handle.mapped {
			handle.err = errors.Join(handle.err, Munmap(handle.data))
		}
		handle.err = errors.Join(handle.err, removeArtifactContentFile(handle.path))
		handle.data = nil
		handle.path = ""
	})
	return handle.err
}

func readTransientArtifact(kind ArtifactKind, path string) ([]byte, bool, error) {
	if kind == ArtifactBloom {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, false, fmt.Errorf("diskcache: read transient Bloom: %w", err)
		}
		return data, false, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false, fmt.Errorf("diskcache: open transient SST: %w", err)
	}
	data, mapErr := MmapFile(file)
	closeErr := file.Close()
	if mapErr != nil {
		return nil, false, errors.Join(
			fmt.Errorf("diskcache: map transient SST: %w", mapErr), closeErr)
	}
	if closeErr != nil {
		return nil, false, errors.Join(
			fmt.Errorf("diskcache: close mapped transient SST: %w", closeErr), Munmap(data))
	}
	return data, true, nil
}

func removeArtifactContentFile(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
