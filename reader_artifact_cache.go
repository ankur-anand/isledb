package isledb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ankur-anand/isledb/internal/diskcache"
)

// sharedSSTArtifact owns the handle returned by one coalesced SST load. Each
// waiter receives a lease, so resident pins and transient files remain valid
// until the final iterator releases them.
type sharedSSTArtifact struct {
	mu     sync.Mutex
	handle *diskcache.ArtifactHandle
	refs   int
}

type sstArtifactLease struct {
	shared *sharedSSTArtifact
	data   []byte
	once   sync.Once
	err    error
}

func newSharedSSTArtifact(handle *diskcache.ArtifactHandle) *sharedSSTArtifact {
	return &sharedSSTArtifact{handle: handle, refs: 1}
}

func (a *sharedSSTArtifact) retainCoalescedLoad() any {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.handle == nil {
		return &sstArtifactLease{}
	}
	a.refs++
	return &sstArtifactLease{shared: a, data: a.handle.Bytes()}
}

func (a *sharedSSTArtifact) releaseCoalescedLoad() {
	_ = a.release()
}

func (a *sharedSSTArtifact) release() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	if a.refs == 0 {
		a.mu.Unlock()
		return nil
	}
	a.refs--
	if a.refs != 0 {
		a.mu.Unlock()
		return nil
	}
	handle := a.handle
	a.handle = nil
	a.mu.Unlock()
	return handle.Close()
}

func (l *sstArtifactLease) Bytes() []byte {
	if l == nil {
		return nil
	}
	return l.data
}

func (l *sstArtifactLease) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		l.err = l.shared.release()
		l.shared = nil
		l.data = nil
	})
	return l.err
}

func sstArtifactDescriptor(meta sstMetadata) diskcache.ArtifactDescriptor {
	return diskcache.ArtifactDescriptor{
		Key: diskcache.ArtifactKey{
			Kind:  diskcache.ArtifactSST,
			SSTID: meta.ID,
		},
		Size:     meta.Size,
		Checksum: meta.Checksum,
	}
}

func bloomArtifactDescriptor(meta sstMetadata) diskcache.ArtifactDescriptor {
	return diskcache.ArtifactDescriptor{
		Key: diskcache.ArtifactKey{
			Kind:  diskcache.ArtifactBloom,
			SSTID: meta.ID,
		},
		Size:     meta.Bloom.Length,
		Checksum: meta.Bloom.Checksum,
	}
}

func (r *Reader) acquireSST(meta sstMetadata) ([]byte, func(), bool, error) {
	handle, ok, err := r.artifactCache.Acquire(sstArtifactDescriptor(meta))
	if err != nil || !ok {
		return nil, nil, ok, err
	}
	return handle.Bytes(), func() { _ = handle.Close() }, true, nil
}

// sstResident is a side-effect-free presence check.
func (r *Reader) sstResident(meta sstMetadata) (bool, error) {
	presence, err := r.artifactCache.Probe(sstArtifactDescriptor(meta))
	return presence != diskcache.ArtifactAbsent, err
}

// sstArtifactResidentByID is the ID-only probe used by lazy-reader tests.
// Runtime read paths pass metadata directly and avoid this manifest scan.
func (r *Reader) sstArtifactResidentByID(id string) bool {
	current := r.currentManifest()
	if current == nil {
		return false
	}
	for _, meta := range current.L0SSTs {
		if meta.ID == id {
			resident, _ := r.sstResident(meta)
			return resident
		}
	}
	for _, level := range current.Levels {
		for _, meta := range level.SSTs {
			if meta.ID == id {
				resident, _ := r.sstResident(meta)
				return resident
			}
		}
	}
	return false
}

func (r *Reader) removeSST(meta sstMetadata, reason diskcache.ArtifactRemovalReason) error {
	return r.artifactCache.Remove(sstArtifactDescriptor(meta).Key, reason)
}

func (r *Reader) clearSSTCache() error {
	return r.artifactCache.Purge(diskcache.ArtifactSST)
}

func (r *Reader) clearBloomDiskCache() error {
	if r.artifactCache == nil {
		return nil
	}
	return r.artifactCache.Purge(diskcache.ArtifactBloom)
}

func (r *Reader) acquireRawBloom(meta sstMetadata) (*diskcache.ArtifactHandle, bool, error) {
	if r.artifactCache == nil || meta.Bloom.Checksum == "" {
		return nil, false, nil
	}
	return r.artifactCache.Acquire(bloomArtifactDescriptor(meta))
}

func (r *Reader) admitRawBloom(
	meta sstMetadata,
	data []byte,
) (*diskcache.ArtifactHandle, error) {
	if r.artifactCache == nil || meta.Bloom.Checksum == "" {
		return nil, nil
	}
	handle, _, err := r.artifactCache.AdmitBytes(bloomArtifactDescriptor(meta), data)
	if err != nil {
		if errors.Is(err, diskcache.ErrArtifactChecksumMismatch) {
			return nil, fmt.Errorf("bloom checksum mismatch: %w", err)
		}
		return nil, err
	}
	return handle, nil
}

func cacheStatsFromArtifact(stats diskcache.ArtifactStats) CacheStats {
	return CacheStats{
		Hits:              stats.Hits,
		Misses:            stats.Misses,
		Bytes:             stats.ResidentBytes,
		MaxBytes:          stats.MaxBytes,
		EntryCount:        stats.ResidentEntries,
		PinnedBytes:       stats.PinnedBytes,
		PinnedEntries:     stats.PinnedEntries,
		Evictions:         stats.CapacityEvictions,
		Corruptions:       stats.Corruptions,
		AdmissionBypasses: stats.AdmissionBypasses,
	}
}
