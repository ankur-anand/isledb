package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/ankur-anand/isledb/blobstore"
	internalchecksum "github.com/ankur-anand/isledb/internal/checksum"
	"github.com/cockroachdb/pebble/v2/objstorage"
)

// stagedCompactionSST keeps a compaction input on local scratch storage. The
// SST reader performs random access against this file while retaining only its
// active blocks in memory.
type stagedCompactionSST struct {
	file *os.File
	path string
	size int64

	closeOnce sync.Once
	closeErr  error
}

const (
	compactionScratchRootPrefix    = "isledb-compaction"
	compactionScratchSessionPrefix = "session-"
	compactionScratchEpochWidth    = 20
	compactionScratchMaintenance   = "maintenance"
	compactionScratchStandalone    = "standalone"
)

// compactionScratchWorkspace owns one fenced maintenance session's local
// files. A higher fence epoch removes abandoned lower-epoch sessions before
// creating its own directory; a delayed older process can never remove a
// newer session.
type compactionScratchWorkspace struct {
	path string

	closeOnce sync.Once
	closeErr  error
}

func openCompactionScratchWorkspace(baseDir, namespace, owner string, fenceEpoch uint64) (*compactionScratchWorkspace, error) {
	if namespace == "" {
		return nil, errors.New("create compaction scratch workspace: empty namespace")
	}
	if owner != compactionScratchMaintenance && owner != compactionScratchStandalone {
		return nil, errors.New("create compaction scratch workspace: invalid owner")
	}
	if baseDir != "" {
		return openCompactionScratchWorkspaceAt(baseDir, namespace, owner, fenceEpoch)
	}

	cacheDir, cacheErr := os.UserCacheDir()
	if cacheErr == nil {
		workspace, err := openCompactionScratchWorkspaceAt(cacheDir, namespace, owner, fenceEpoch)
		if err == nil {
			return workspace, nil
		}
	}

	// Some restricted runtimes report a user cache path but do not allow the
	// process to create it. Fall back to a user-specific directory under the OS
	// temp root so a shared /tmp never becomes one user's 0700 directory.
	userIdentity := cacheDir
	if current, err := user.Current(); err == nil {
		userIdentity = current.Uid + "\x00" + current.Username
	}
	sum := sha256.Sum256([]byte(userIdentity))
	fallback := filepath.Join(os.TempDir(), fmt.Sprintf("isledb-user-%x", sum[:8]))
	return openCompactionScratchWorkspaceAt(fallback, namespace, owner, fenceEpoch)
}

func openCompactionScratchWorkspaceAt(baseDir, namespace, owner string, fenceEpoch uint64) (*compactionScratchWorkspace, error) {
	// Maintenance and the legacy standalone compactor use independent fence
	// counters. Keeping them in separate roots makes epoch comparison local to
	// one counter and prevents either role from deleting the other's session.
	root := filepath.Join(baseDir, compactionScratchRootPrefix, owner, namespace)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create compaction scratch root: %w", err)
	}
	// Abandoned scratch is cleanup, not database availability. A stale path
	// that cannot be removed must not prevent a fresh fenced session opening.
	_ = removeOlderCompactionScratchSessions(root, fenceEpoch)

	prefix := fmt.Sprintf("%s%0*d-", compactionScratchSessionPrefix, compactionScratchEpochWidth, fenceEpoch)
	path, err := os.MkdirTemp(root, prefix)
	if err != nil {
		return nil, fmt.Errorf("create compaction scratch session: %w", err)
	}
	return &compactionScratchWorkspace{path: path}, nil
}

func removeOlderCompactionScratchSessions(root string, currentEpoch uint64) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("read compaction scratch root: %w", err)
	}
	var cleanupErr error
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		epoch, ok := compactionScratchSessionEpoch(entry.Name())
		if !ok || epoch >= currentEpoch {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
			cleanupErr = errors.Join(cleanupErr,
				fmt.Errorf("remove abandoned compaction scratch session %q: %w", entry.Name(), err))
		}
	}
	return cleanupErr
}

func compactionScratchSessionEpoch(name string) (uint64, bool) {
	if !strings.HasPrefix(name, compactionScratchSessionPrefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(name, compactionScratchSessionPrefix)
	if len(rest) <= compactionScratchEpochWidth || rest[compactionScratchEpochWidth] != '-' {
		return 0, false
	}
	epoch, err := strconv.ParseUint(rest[:compactionScratchEpochWidth], 10, 64)
	return epoch, err == nil
}

func (w *compactionScratchWorkspace) Path() string {
	if w == nil {
		return ""
	}
	return w.path
}

func (w *compactionScratchWorkspace) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		w.closeErr = os.RemoveAll(w.path)
	})
	return w.closeErr
}

func stageCompactionSST(
	ctx context.Context,
	store *blobstore.Store,
	meta sstMetadata,
	scratchDir string,
	verifyChecksum bool,
) (_ *stagedCompactionSST, err error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("stage compaction SST: nil store")
	}
	if meta.Size <= 0 {
		return nil, fmt.Errorf("sst %s: missing size in manifest", meta.ID)
	}

	file, err := os.CreateTemp(scratchDir, "isledb-compaction-*.sst.partial")
	if err != nil {
		return nil, fmt.Errorf("create compaction scratch file: %w", err)
	}
	staged := &stagedCompactionSST{file: file, path: file.Name(), size: meta.Size}
	committed := false
	defer func() {
		if !committed {
			err = errors.Join(err, staged.Close())
		}
	}()

	var checksum hash.Hash
	destination := io.Writer(file)
	if verifyChecksum {
		checksum = sha256.New()
		destination = io.MultiWriter(file, checksum)
	}
	if err := copyCompactionSST(ctx, store, meta, destination); err != nil {
		return nil, err
	}
	if verifyChecksum {
		if err := validateCompactionSSTChecksum(meta, checksum.Sum(nil)); err != nil {
			return nil, err
		}
	}

	committed = true
	return staged, nil
}

// verifyCompactionSST streams one immutable SST through the checksum without
// retaining its contents or creating a scratch file. Metadata-only moves need
// integrity verification but never need random access to the object.
func verifyCompactionSST(ctx context.Context, store *blobstore.Store, meta sstMetadata) error {
	checksum := sha256.New()
	if err := copyCompactionSST(ctx, store, meta, checksum); err != nil {
		return err
	}
	return validateCompactionSSTChecksum(meta, checksum.Sum(nil))
}

func copyCompactionSST(ctx context.Context, store *blobstore.Store, meta sstMetadata, destination io.Writer) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if store == nil {
		return errors.New("read compaction SST: nil store")
	}
	if meta.Size <= 0 {
		return fmt.Errorf("sst %s: missing size in manifest", meta.ID)
	}

	source, err := store.ReadRangeStream(ctx, store.SSTPath(meta.ID), 0, meta.Size)
	if err != nil {
		return fmt.Errorf("read sst %s: %w", meta.ID, err)
	}
	written, copyErr := io.Copy(destination, source)
	closeErr := source.Close()
	if copyErr != nil || closeErr != nil {
		return fmt.Errorf("read sst %s: %w", meta.ID, errors.Join(copyErr, closeErr))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if written != meta.Size {
		return fmt.Errorf("sst %s: short read: %d < %d", meta.ID, written, meta.Size)
	}
	return nil
}

func validateCompactionSSTChecksum(meta sstMetadata, actual []byte) error {
	expected, err := internalchecksum.ParseSHA256(meta.Checksum)
	if err != nil {
		return fmt.Errorf("sst %s: unsupported checksum %q", meta.ID, meta.Checksum)
	}
	if !bytes.Equal(expected[:], actual) {
		return fmt.Errorf("sst %s: checksum mismatch", meta.ID)
	}
	return nil
}

func (s *stagedCompactionSST) ReadAt(ctx context.Context, p []byte, off int64) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil || s.file == nil || off < 0 || off > s.size || int64(len(p)) > s.size-off {
		return io.ErrUnexpectedEOF
	}
	n, err := s.file.ReadAt(p, off)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (s *stagedCompactionSST) Size() int64 {
	if s == nil {
		return 0
	}
	return s.size
}

func (s *stagedCompactionSST) NewReadHandle(_ objstorage.ReadBeforeSize) objstorage.ReadHandle {
	handle := objstorage.MakeNoopReadHandle(s)
	return &handle
}

func (s *stagedCompactionSST) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if s.file != nil {
			s.closeErr = s.file.Close()
			s.file = nil
		}
		if s.path != "" {
			removeErr := os.Remove(s.path)
			if errors.Is(removeErr, os.ErrNotExist) {
				removeErr = nil
			}
			s.closeErr = errors.Join(s.closeErr, removeErr)
			s.path = ""
		}
	})
	return s.closeErr
}
