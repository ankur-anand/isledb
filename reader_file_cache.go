package isledb

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/internal/filecache"
	"github.com/cockroachdb/pebble/v2/objstorage"
)

// sharedSSTFile owns the file returned by one coalesced SST load. Each waiter
// receives a lease, and the file is closed when the last lease is released.
type sharedSSTFile struct {
	mu   sync.Mutex
	file *os.File
	refs int
}

type sstFileLease struct {
	shared *sharedSSTFile
	file   *os.File
	once   sync.Once
	err    error
}

func newSharedSSTFile(file *os.File) *sharedSSTFile {
	return &sharedSSTFile{file: file, refs: 1}
}

func (s *sharedSSTFile) retainCoalescedLoad() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	s.refs++
	return &sstFileLease{shared: s, file: s.file}
}

func (s *sharedSSTFile) releaseCoalescedLoad() {
	_ = s.release()
}

func (s *sharedSSTFile) release() error {
	s.mu.Lock()
	if s.refs == 0 {
		s.mu.Unlock()
		return nil
	}
	s.refs--
	if s.refs != 0 {
		s.mu.Unlock()
		return nil
	}
	file := s.file
	s.file = nil
	s.mu.Unlock()
	return file.Close()
}

func (l *sstFileLease) Close() error {
	l.once.Do(func() {
		l.err = l.shared.release()
		l.shared = nil
		l.file = nil
	})
	return l.err
}

// sstFileReadable serves Pebble reads of an SST from a local file. The file's
// owner closes it.
type sstFileReadable struct {
	file *os.File
	size int64
	rh   objstorage.NoopReadHandle
}

func newSSTFileReadable(file *os.File, size int64) *sstFileReadable {
	r := &sstFileReadable{file: file, size: size}
	r.rh = objstorage.MakeNoopReadHandle(r)
	return r
}

func (r *sstFileReadable) ReadAt(_ context.Context, p []byte, off int64) error {
	n, err := r.file.ReadAt(p, off)
	if n == len(p) {
		return nil
	}
	if err == nil || err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (r *sstFileReadable) NewReadHandle(objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &r.rh
}

func (*sstFileReadable) Close() error { return nil }

func (r *sstFileReadable) Size() int64 { return r.size }

const readerDiagnosticLogInterval = time.Minute

// readerDiagnosticLimiter bounds diagnostic logs while leaving metrics exact.
// It intentionally has no per-SST map, so a stream of unique corrupt objects
// cannot turn observability into an unbounded memory consumer.
type readerDiagnosticLimiter struct {
	mu         sync.Mutex
	last       time.Time
	suppressed uint64
}

func (limiter *readerDiagnosticLimiter) allow(now time.Time) (bool, uint64) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if !limiter.last.IsZero() && now.Sub(limiter.last) < readerDiagnosticLogInterval {
		limiter.suppressed++
		return false, 0
	}
	suppressed := limiter.suppressed
	limiter.last = now
	limiter.suppressed = 0
	return true, suppressed
}

func sstFileDescriptor(meta sstMetadata) filecache.Descriptor {
	return filecache.Descriptor{Kind: filecache.KindSST, Size: meta.Size, Checksum: meta.Checksum}
}

func bloomFileDescriptor(meta sstMetadata) filecache.Descriptor {
	return filecache.Descriptor{Kind: filecache.KindBloom, Size: meta.Bloom.Length, Checksum: meta.Bloom.Checksum}
}

// acquireSST opens the SST from the local cache.
func (r *Reader) acquireSST(meta sstMetadata) (*os.File, bool) {
	if r.fileCache == nil {
		return nil, false
	}
	return r.fileCache.OpenFile(sstFileDescriptor(meta))
}

// sstResident reports whether the SST is cached locally, without counting a
// cache lookup.
func (r *Reader) sstResident(meta sstMetadata) bool {
	return r.fileCache != nil && r.fileCache.Contains(sstFileDescriptor(meta))
}

// sstResidentByID is the ID-only residency check used by lazy-reader tests.
// Read paths pass metadata directly and avoid this manifest scan.
func (r *Reader) sstResidentByID(id string) bool {
	current := r.currentManifest()
	if current == nil {
		return false
	}
	for _, meta := range current.L0SSTs {
		if meta.ID == id {
			return r.sstResident(meta)
		}
	}
	for _, level := range current.Levels {
		for _, meta := range level.SSTs {
			if meta.ID == id {
				return r.sstResident(meta)
			}
		}
	}
	return false
}

func (r *Reader) removeSST(meta sstMetadata) {
	if r.fileCache != nil {
		r.fileCache.Remove(sstFileDescriptor(meta))
	}
}

// reportCorruptSST drops a cached SST that failed to open or to read, so the
// next read downloads it again, and counts the corruption.
func (r *Reader) reportCorruptSST(meta sstMetadata) {
	if r.fileCache != nil {
		r.fileCache.ReportCorrupt(sstFileDescriptor(meta))
	}
	r.blockCache.evict(meta.ID)
}

func (r *Reader) clearSSTCache() {
	if r.fileCache != nil {
		r.fileCache.Purge(filecache.KindSST)
	}
}

func (r *Reader) clearBloomDiskCache() {
	if r.fileCache != nil {
		r.fileCache.Purge(filecache.KindBloom)
	}
}

// readCachedBloom returns a filter's verified bytes from the local cache.
func (r *Reader) readCachedBloom(meta sstMetadata) ([]byte, bool) {
	if r.fileCache == nil {
		return nil, false
	}
	return r.fileCache.ReadVerified(bloomFileDescriptor(meta))
}

// cacheBloom stores verified filter bytes locally. Caching is best effort.
func (r *Reader) cacheBloom(meta sstMetadata, data []byte) {
	if r.fileCache != nil {
		_ = r.fileCache.Put(bloomFileDescriptor(meta), data)
	}
}

func (r *Reader) observeBloomFilterError(sstID string, err error) {
	if err == nil {
		return
	}
	r.metrics.ObserveBloomFilterError()
	allowed, suppressed := r.bloomDiagnosticLimiter.allow(time.Now())
	if !allowed {
		return
	}
	slog.Warn(
		"isledb: Bloom filter unavailable; continuing with SST lookup",
		"sst_id", sstID,
		"error", err,
		"suppressed_since_last_log", suppressed,
	)
}

func (r *Reader) fileCacheStats(kind filecache.Kind) CacheStats {
	if r.fileCache == nil {
		return CacheStats{}
	}
	stats := r.fileCache.Stats(kind)
	return CacheStats{
		Hits:        stats.Hits,
		Misses:      stats.Misses,
		Bytes:       stats.Bytes,
		MaxBytes:    stats.MaxBytes,
		EntryCount:  stats.Entries,
		Evictions:   stats.Evictions,
		Corruptions: stats.Corruptions,
		Bypasses:    stats.Bypasses,
		Failures:    stats.Failures,
	}
}
