package filecache

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"os"
	"path/filepath"
)

// Writer streams one file into the cache. It is not safe for concurrent use.
type Writer struct {
	cache   *Cache
	desc    Descriptor
	sum     [sha256.Size]byte
	file    *os.File
	hash    hash.Hash
	written int64
	done    bool
}

// Create starts writing the file d describes into a temporary file.
func (c *Cache) Create(d Descriptor) (*Writer, error) {
	sum, err := d.sum()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	file, err := os.CreateTemp(c.incoming, d.Kind.String()+"-*")
	if err != nil {
		return nil, fmt.Errorf("filecache: create temporary file: %w", err)
	}
	return &Writer{cache: c, desc: d, sum: sum, file: file, hash: sha256.New()}, nil
}

// Write appends p, refusing to write past the descriptor's size.
func (w *Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, os.ErrClosed
	}
	if int64(len(p)) > w.desc.Size-w.written {
		return 0, fmt.Errorf("%w: writing %d bytes with %d remaining",
			ErrSizeMismatch, len(p), w.desc.Size-w.written)
	}
	n, err := w.file.Write(p)
	w.hash.Write(p[:n])
	w.written += int64(n)
	return n, err
}

// Commit checks the written bytes against the descriptor and, if they match,
// publishes them into the cache. Whether or not the file could be cached, it
// is returned open for reading (use ReadAt); the caller closes it. Bytes that
// do not match are deleted and an error is returned.
func (w *Writer) Commit() (*os.File, error) {
	if w.done {
		return nil, os.ErrClosed
	}
	w.done = true
	file := w.file
	fail := func(err error) (*os.File, error) {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	if w.written != w.desc.Size {
		return fail(fmt.Errorf("%w: wrote %d bytes, want %d", ErrSizeMismatch, w.written, w.desc.Size))
	}
	var got [sha256.Size]byte
	w.hash.Sum(got[:0])
	if got != w.sum {
		return fail(ErrChecksumMismatch)
	}
	// Syncing before the rename means a cached name always holds complete
	// contents, even after a crash.
	synced := file.Sync() == nil
	w.cache.publish(w.desc.Kind, w.sum, w.desc.Size, file.Name(), synced)
	return file, nil
}

// Abort discards an unfinished file. It does nothing after Commit.
func (w *Writer) Abort() {
	if w.done {
		return
	}
	w.done = true
	_ = w.file.Close()
	_ = os.Remove(w.file.Name())
}

// Put caches data as the file d describes.
func (c *Cache) Put(d Descriptor, data []byte) error {
	w, err := c.Create(d)
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		w.Abort()
		return err
	}
	file, err := w.Commit()
	if err != nil {
		return err
	}
	// The data is cached (or deliberately not); closing the read handle
	// Commit returned cannot change that.
	_ = file.Close()
	return nil
}

// publish moves a verified temporary file into the cache, then evicts the
// least recently used files of its kind to get back within budget, so a
// failed rename never costs cached files. A damaged cached copy of the same
// contents is replaced. The temporary file is deleted instead when the cache
// is closed, the file could not be synced, the file exceeds the whole budget,
// the same contents are already cached, or the rename fails. Readers holding
// it open are unaffected either way.
func (c *Cache) publish(kind Kind, sum [sha256.Size]byte, size int64, tmp string, synced bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tiers[kind]
	c.dropMismatchedLocked(kind, sum, size)
	switch {
	case c.closed:
	case !synced:
		t.stats.Failures++
	case size > t.max:
		t.stats.Bypasses++
	case t.index[sum] != nil:
	default:
		final := c.path(kind, sum)
		err := os.MkdirAll(filepath.Dir(final), 0o700)
		if err == nil {
			err = os.Rename(tmp, final)
		}
		if err != nil {
			t.stats.Failures++
			break
		}
		c.insertLocked(kind, sum, size)
		// The new file is newest, and fits the budget alone, so it is never
		// evicted here.
		for t.bytes > t.max {
			c.removeLocked(kind, t.lru.Front())
			t.stats.Evictions++
		}
		return
	}
	_ = os.Remove(tmp)
}
