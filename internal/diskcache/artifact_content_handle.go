package diskcache

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"sync"
)

type artifactContentHandle interface {
	bytes() []byte
	close() error
}

// artifactPersistentHandle owns one per-acquisition view of a resident file.
// SST data is mmap-backed; Bloom data is an independently allocated heap copy.
type artifactPersistentHandle struct {
	data       []byte
	mapped     bool
	tier       *artifactContentTier
	entry      *artifactContentIndexEntry
	removeFile func(artifactContentAddress) error
	once       sync.Once
	err        error
}

func (handle *artifactPersistentHandle) bytes() []byte {
	if handle == nil {
		return nil
	}
	return handle.data
}

func (handle *artifactPersistentHandle) close() error {
	if handle == nil {
		return nil
	}
	handle.once.Do(func() {
		if handle.mapped {
			handle.err = errors.Join(handle.err, Munmap(handle.data))
		}
		handle.data = nil
		handle.err = errors.Join(
			handle.err,
			handle.tier.release(handle.entry, handle.removeFile),
		)
	})
	return handle.err
}

// acquire pins a searchable entry before opening its file. Filesystem or
// integrity failures detach the entry and report a miss with a diagnostic
// error; the Reader must treat that error as advisory and continue to origin.
func (tier *artifactContentTier) acquire(
	address artifactContentAddress,
	size int64,
	path string,
	removeFile func(artifactContentAddress) error,
) (*artifactPersistentHandle, bool, error) {
	if removeFile == nil {
		return nil, false, errors.New("diskcache: artifact removal callback is required")
	}
	entry, ok, err := tier.pin(address, size)
	if err != nil {
		if resident, residentOK := tier.probe(address); residentOK {
			_, removeErr := tier.detach(resident, removeFile)
			err = errors.Join(err, removeErr)
		}
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return tier.openPinned(entry, path, removeFile)
}

// openPinned opens an entry whose reference is already owned by the caller.
// Publication uses this to turn its initial refs=1 into the returned handle
// without incrementing the reference a second time.
func (tier *artifactContentTier) openPinned(
	entry *artifactContentIndexEntry,
	path string,
	removeFile func(artifactContentAddress) error,
) (*artifactPersistentHandle, bool, error) {
	if entry == nil || removeFile == nil {
		return nil, false, errors.New("diskcache: invalid pinned artifact open")
	}
	data, mapped, err := openPersistentArtifact(entry.address, entry.size, path)
	if err == nil {
		return &artifactPersistentHandle{
			data:       data,
			mapped:     mapped,
			tier:       tier,
			entry:      entry,
			removeFile: removeFile,
		}, true, nil
	}

	_, detachErr := tier.detach(entry, removeFile)
	releaseErr := tier.release(entry, removeFile)
	return nil, false, errors.Join(err, detachErr, releaseErr)
}

func openPersistentArtifact(
	address artifactContentAddress,
	expectedSize int64,
	path string,
) ([]byte, bool, error) {
	switch address.kind {
	case ArtifactSST:
		return mapPersistentSST(path, expectedSize)
	case ArtifactBloom:
		return readPersistentBloom(path, expectedSize, address.checksum)
	default:
		return nil, false, fmt.Errorf("diskcache: invalid persistent artifact kind %d", address.kind)
	}
}

func mapPersistentSST(path string, expectedSize int64) ([]byte, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, fmt.Errorf("diskcache: open persistent SST: %w", err)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return nil, false, fmt.Errorf("diskcache: stat persistent SST: %w", statErr)
	}
	if info.Size() != expectedSize {
		_ = file.Close()
		return nil, false, fmt.Errorf(
			"%w: got=%d want=%d", ErrArtifactSizeMismatch, info.Size(), expectedSize)
	}
	data, mapErr := MmapFile(file)
	closeErr := file.Close()
	if mapErr != nil {
		return nil, false, errors.Join(
			fmt.Errorf("diskcache: map persistent SST: %w", mapErr), closeErr)
	}
	if closeErr != nil {
		return nil, false, errors.Join(
			fmt.Errorf("diskcache: close mapped persistent SST: %w", closeErr), Munmap(data))
	}
	return data, true, nil
}

func readPersistentBloom(
	path string,
	expectedSize int64,
	expectedChecksum [sha256.Size]byte,
) ([]byte, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false, fmt.Errorf("diskcache: read persistent Bloom: %w", err)
	}
	if int64(len(data)) != expectedSize {
		return nil, false, fmt.Errorf(
			"%w: got=%d want=%d", ErrArtifactSizeMismatch, len(data), expectedSize)
	}
	actualChecksum := sha256.Sum256(data)
	if subtle.ConstantTimeCompare(actualChecksum[:], expectedChecksum[:]) != 1 {
		return nil, false, ErrArtifactChecksumMismatch
	}
	return data, false, nil
}
