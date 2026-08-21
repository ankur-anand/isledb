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
	"time"
)

// ArtifactFill is a verified, crash-recoverable staged write. Call Commit or
// Abort exactly once; both methods are idempotent for cleanup purposes.
type ArtifactFill struct {
	mu sync.Mutex

	cache *ArtifactCache
	desc  ArtifactDescriptor
	file  *os.File
	path  string
	hash  hash.Hash
	size  int64
	done  bool
}

// BeginFill creates a temporary artifact in the cache's incoming directory.
func (c *ArtifactCache) BeginFill(desc ArtifactDescriptor) (*ArtifactFill, error) {
	if err := desc.validate(); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, ErrArtifactCacheClosed
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrArtifactCacheClosed
	}
	c.activeFills++
	c.mu.Unlock()

	file, err := os.CreateTemp(c.incomingDir, desc.Key.Kind.dirName()+"-*.part")
	if err != nil {
		c.finishFill()
		return nil, fmt.Errorf("diskcache: create incoming artifact: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		c.finishFill()
		return nil, fmt.Errorf("diskcache: secure incoming artifact: %w", err)
	}
	return &ArtifactFill{
		cache: c, desc: desc, file: file, path: file.Name(), hash: sha256.New(),
	}, nil
}

func (f *ArtifactFill) Write(data []byte) (int, error) {
	if f == nil {
		return 0, os.ErrInvalid
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.done || f.file == nil {
		return 0, os.ErrClosed
	}
	written, err := f.file.Write(data)
	if written > 0 {
		_, _ = f.hash.Write(data[:written])
		f.size += int64(written)
	}
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	return written, err
}

// Commit verifies and atomically publishes the fill. When resident capacity is
// unavailable, the returned handle owns the verified temporary file and
// removes it on Close.
func (f *ArtifactFill) Commit() (*ArtifactHandle, ArtifactAdmission, error) {
	if f == nil {
		return nil, 0, os.ErrInvalid
	}
	f.mu.Lock()
	if f.done {
		f.mu.Unlock()
		return nil, 0, os.ErrClosed
	}
	f.done = true
	file := f.file
	path := f.path
	size := f.size
	actual := f.hash.Sum(nil)
	f.file = nil
	f.mu.Unlock()

	cleanupFailure := func(err error) (*ArtifactHandle, ArtifactAdmission, error) {
		_ = file.Close()
		_ = os.Remove(path)
		f.cache.finishFill()
		return nil, 0, err
	}

	if size != f.desc.Size {
		return cleanupFailure(fmt.Errorf("%w: got=%d want=%d", ErrArtifactSizeMismatch, size, f.desc.Size))
	}
	expected, err := parseSHA256Checksum(f.desc.Checksum)
	if err != nil {
		return cleanupFailure(err)
	}
	if subtle.ConstantTimeCompare(actual, expected[:]) != 1 {
		return cleanupFailure(ErrArtifactChecksumMismatch)
	}
	if err := file.Sync(); err != nil {
		return cleanupFailure(fmt.Errorf("diskcache: sync incoming artifact: %w", err))
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		f.cache.finishFill()
		return nil, 0, fmt.Errorf("diskcache: close incoming artifact: %w", err)
	}
	return f.cache.publishFill(f.desc, path)
}

// Abort removes an incomplete fill.
func (f *ArtifactFill) Abort() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	if f.done {
		f.mu.Unlock()
		return nil
	}
	f.done = true
	file := f.file
	path := f.path
	f.file = nil
	f.mu.Unlock()

	var err error
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	if path != "" {
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}
	f.cache.finishFill()
	return err
}

// AdmitBytes is a convenience for small artifacts such as Bloom sidecars.
func (c *ArtifactCache) AdmitBytes(
	desc ArtifactDescriptor,
	data []byte,
) (*ArtifactHandle, ArtifactAdmission, error) {
	fill, err := c.BeginFill(desc)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fill.Write(data); err != nil {
		_ = fill.Abort()
		return nil, 0, err
	}
	return fill.Commit()
}

func (c *ArtifactCache) finishFill() {
	c.mu.Lock()
	if c.activeFills > 0 {
		c.activeFills--
	}
	directoryLock := c.takeDirectoryLockIfIdleLocked()
	c.mu.Unlock()
	if directoryLock != nil {
		_ = directoryLock.close()
	}
}

func (c *ArtifactCache) publishFill(
	desc ArtifactDescriptor,
	tempPath string,
) (*ArtifactHandle, ArtifactAdmission, error) {
	id := artifactIDFor(desc.Key)
	finalPath := c.artifactPath(id)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o700); err != nil {
		_ = os.Remove(tempPath)
		c.finishFill()
		return nil, 0, fmt.Errorf("diskcache: create artifact shard: %w", err)
	}

	c.mu.Lock()
	if c.closed {
		c.activeFills--
		directoryLock := c.takeDirectoryLockIfIdleLocked()
		c.mu.Unlock()
		_ = os.Remove(tempPath)
		if directoryLock != nil {
			_ = directoryLock.close()
		}
		return nil, 0, ErrArtifactCacheClosed
	}
	tier := c.tiers[id.kind]
	var cleanups []artifactCleanup

	if existing := tier.entries[id.digest]; existing != nil {
		if existing.state == artifactEntryReady && !existing.pendingRemoval &&
			existing.size == desc.Size && existing.verifiedChecksum == desc.Checksum {
			return c.pinExistingFillLocked(tier, existing, tempPath)
		}
		if existing.refs == 0 && existing.state == artifactEntryReady {
			cleanups = append(cleanups, c.detachEntryLocked(tier, existing, ArtifactRemovalCorrupt))
		} else {
			// A conflicting or transitional resident cannot be replaced safely.
			// The verified fill remains usable through a transient handle.
			tier.stats.AdmissionBypasses++
			return c.publishTransientLocked(tempPath, ArtifactBypassedPinnedCapacity)
		}
	}

	var admission ArtifactAdmission
	var reserveCleanups []artifactCleanup
	reserveCleanups, admission = c.reserveLocked(tier, desc.Size, id.digest)
	cleanups = append(cleanups, reserveCleanups...)
	for _, cleanup := range cleanups {
		if err := cleanup.run(); err != nil {
			c.activeFills--
			directoryLock := c.takeDirectoryLockIfIdleLocked()
			c.mu.Unlock()
			_ = os.Remove(tempPath)
			if directoryLock != nil {
				_ = directoryLock.close()
			}
			return nil, 0, fmt.Errorf("diskcache: remove evicted artifact: %w", err)
		}
	}
	if admission != ArtifactAdmitted {
		return c.publishTransientLocked(tempPath, admission)
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		c.activeFills--
		directoryLock := c.takeDirectoryLockIfIdleLocked()
		c.mu.Unlock()
		_ = os.Remove(tempPath)
		if directoryLock != nil {
			_ = directoryLock.close()
		}
		return nil, 0, fmt.Errorf("diskcache: publish artifact: %w", err)
	}
	if err := syncArtifactDirectory(filepath.Dir(finalPath)); err != nil {
		_ = os.Remove(finalPath)
		c.activeFills--
		directoryLock := c.takeDirectoryLockIfIdleLocked()
		c.mu.Unlock()
		if directoryLock != nil {
			_ = directoryLock.close()
		}
		return nil, 0, err
	}

	now := time.Now()
	entry := &artifactEntry{
		id: id, path: finalPath, size: desc.Size,
		state: artifactEntryOpening, wait: make(chan struct{}),
		verifiedChecksum: desc.Checksum,
		refs:             1, lastAccess: now, lastTouch: now,
	}
	entry.elem = tier.lru.PushBack(entry)
	tier.entries[id.digest] = entry
	tier.residentBytes += entry.size
	tier.pinnedBytes += entry.size
	c.activeFills--
	c.activeHandles++
	c.mu.Unlock()

	return c.finishOpeningFill(tier, entry, ArtifactAdmitted)
}

func (c *ArtifactCache) pinExistingFillLocked(
	tier *artifactTier,
	entry *artifactEntry,
	tempPath string,
) (*ArtifactHandle, ArtifactAdmission, error) {
	entry.refs++
	if entry.refs == 1 {
		tier.pinnedBytes += entry.size
	}
	entry.lastAccess = time.Now()
	if entry.elem != nil {
		tier.lru.MoveToBack(entry.elem)
	}
	c.activeFills--
	c.activeHandles++
	needsOpen := entry.mmap == nil
	if needsOpen {
		entry.state = artifactEntryOpening
		entry.wait = make(chan struct{})
	}
	c.mu.Unlock()
	_ = os.Remove(tempPath)
	if needsOpen {
		return c.finishOpeningFill(tier, entry, ArtifactAlreadyResident)
	}
	return c.newPersistentHandle(tier, entry), ArtifactAlreadyResident, nil
}

func (c *ArtifactCache) finishOpeningFill(
	tier *artifactTier,
	entry *artifactEntry,
	admission ArtifactAdmission,
) (*ArtifactHandle, ArtifactAdmission, error) {
	file, data, err := openMappedArtifact(entry.path)
	c.mu.Lock()
	entry.state = artifactEntryReady
	wait := entry.wait
	entry.wait = nil
	if wait != nil {
		close(wait)
	}
	if err == nil {
		entry.file = file
		entry.mmap = data
		c.openEntries++
		c.mu.Unlock()
		return c.newPersistentHandle(tier, entry), admission, nil
	}

	entry.refs--
	c.activeHandles--
	if entry.refs == 0 {
		tier.pinnedBytes -= entry.size
	}
	cleanup := c.detachEntryLocked(tier, entry, ArtifactRemovalCorrupt)
	directoryLock := c.takeDirectoryLockIfIdleLocked()
	c.mu.Unlock()
	cleanupErr := errors.Join(
		artifactCleanup{data: data, file: file}.run(), cleanup.run())
	if directoryLock != nil {
		cleanupErr = errors.Join(cleanupErr, directoryLock.close())
	}
	return nil, 0, errors.Join(fmt.Errorf("diskcache: open admitted artifact: %w", err), cleanupErr)
}

func (c *ArtifactCache) newPersistentHandle(tier *artifactTier, entry *artifactEntry) *ArtifactHandle {
	return &ArtifactHandle{
		data: entry.mmap, cache: c, tier: tier, entry: entry,
	}
}

func (c *ArtifactCache) publishTransientLocked(
	path string,
	admission ArtifactAdmission,
) (*ArtifactHandle, ArtifactAdmission, error) {
	c.activeFills--
	c.activeHandles++
	c.mu.Unlock()

	file, data, err := openMappedArtifact(path)
	if err != nil {
		removeErr := os.Remove(path)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		c.finishTransientHandle()
		return nil, 0, errors.Join(
			fmt.Errorf("diskcache: open transient artifact: %w", err), removeErr)
	}
	handle := &ArtifactHandle{
		data: data,
		closeFn: func() error {
			err := artifactCleanup{data: data, file: file, path: path, remove: true}.run()
			c.finishTransientHandle()
			return err
		},
	}
	return handle, admission, nil
}

func (c *ArtifactCache) finishTransientHandle() {
	c.mu.Lock()
	if c.activeHandles > 0 {
		c.activeHandles--
	}
	directoryLock := c.takeDirectoryLockIfIdleLocked()
	c.mu.Unlock()
	if directoryLock != nil {
		_ = directoryLock.close()
	}
}

func syncArtifactDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("diskcache: open artifact directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("diskcache: sync artifact directory: %w", err)
	}
	return nil
}
