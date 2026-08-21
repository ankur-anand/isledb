package diskcache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	defaultArtifactSSTBytes   = int64(1 << 30)
	defaultArtifactBloomBytes = int64(64 << 20)
)

var (
	ErrArtifactCacheClosed       = errors.New("diskcache: artifact cache closed")
	ErrArtifactCacheLocked       = errors.New("diskcache: artifact cache directory is locked")
	ErrInvalidArtifactDescriptor = errors.New("diskcache: invalid artifact descriptor")
	ErrArtifactSizeMismatch      = errors.New("diskcache: artifact size mismatch")
	ErrArtifactChecksumMismatch  = errors.New("diskcache: artifact checksum mismatch")
)

// ArtifactKind identifies one independently budgeted persistent cache tier.
type ArtifactKind uint8

const (
	ArtifactSST ArtifactKind = iota + 1
	ArtifactBloom
)

func (k ArtifactKind) valid() bool {
	return k == ArtifactSST || k == ArtifactBloom
}

func (k ArtifactKind) dirName() string {
	switch k {
	case ArtifactSST:
		return "sst"
	case ArtifactBloom:
		return "bloom"
	default:
		return "unknown"
	}
}

func (k ArtifactKind) extension() string {
	switch k {
	case ArtifactSST:
		return ".sst"
	case ArtifactBloom:
		return ".bloom"
	default:
		return ""
	}
}

// ArtifactKey identifies the database object associated with an artifact.
// Persistent cache identity is kind plus the descriptor's full checksum;
// SSTID remains useful for diagnostics and Reader-level download coalescing.
type ArtifactKey struct {
	Kind  ArtifactKind
	SSTID string
}

// ArtifactDescriptor supplies the immutable framing and integrity metadata
// required to admit or recover an artifact.
type ArtifactDescriptor struct {
	Key      ArtifactKey
	Size     int64
	Checksum string
}

func (d ArtifactDescriptor) validate() error {
	if !d.Key.Kind.valid() {
		return fmt.Errorf("%w: kind=%d", ErrInvalidArtifactDescriptor, d.Key.Kind)
	}
	if d.Key.SSTID == "" {
		return fmt.Errorf("%w: empty SST ID", ErrInvalidArtifactDescriptor)
	}
	if d.Size <= 0 {
		return fmt.Errorf("%w: size=%d", ErrInvalidArtifactDescriptor, d.Size)
	}
	if err := validateSHA256Checksum(d.Checksum); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArtifactDescriptor, err)
	}
	return nil
}

func validateSHA256Checksum(checksum string) error {
	const prefix = "sha256:"
	if len(checksum) != len(prefix)+hex.EncodedLen(sha256.Size) ||
		!strings.HasPrefix(checksum, prefix) {
		return fmt.Errorf("unsupported or invalid SHA-256 checksum %q", checksum)
	}
	for index := len(prefix); index < len(checksum); index++ {
		if !isHexDigit(checksum[index]) {
			return fmt.Errorf("invalid SHA-256 checksum %q", checksum)
		}
	}
	return nil
}

func parseSHA256Checksum(checksum string) ([sha256.Size]byte, error) {
	var expected [sha256.Size]byte
	if err := validateSHA256Checksum(checksum); err != nil {
		return expected, err
	}
	_, err := hex.Decode(expected[:], []byte(checksum[len("sha256:"):]))
	if err != nil {
		return expected, fmt.Errorf("invalid SHA-256 checksum %q", checksum)
	}
	return expected, nil
}

func isHexDigit(value byte) bool {
	return value >= '0' && value <= '9' ||
		value >= 'a' && value <= 'f' ||
		value >= 'A' && value <= 'F'
}

// ArtifactPresence is a side-effect-free result from Probe.
type ArtifactPresence uint8

const (
	ArtifactAbsent ArtifactPresence = iota
	ArtifactResidentUnverified
	ArtifactResidentVerified
)

// ArtifactAdmission reports how a completed fill was handled.
type ArtifactAdmission uint8

const (
	ArtifactAdmitted ArtifactAdmission = iota + 1
	ArtifactAlreadyResident
	ArtifactBypassedOversized
	ArtifactBypassedPinnedCapacity
	ArtifactBypassedPublicationFailure
)

// ArtifactRemovalReason explains why a resident entry left the cache.
type ArtifactRemovalReason uint8

const (
	ArtifactRemovalCapacity ArtifactRemovalReason = iota + 1
	ArtifactRemovalCorrupt
	ArtifactRemovalPurge
	ArtifactRemovalRecovery
)

// ArtifactCacheOptions configures the persistent SST and raw-Bloom tiers.
// One live process may own Dir at a time. Dir is also required working space
// for incoming and transient artifacts, whose bytes are additional to the
// searchable resident-byte budgets.
type ArtifactCacheOptions struct {
	Dir string

	SSTMaxBytes   int64
	BloomMaxBytes int64
}

func (o ArtifactCacheOptions) normalize() (ArtifactCacheOptions, error) {
	if o.Dir == "" {
		return ArtifactCacheOptions{}, errors.New("diskcache: artifact cache directory is required")
	}
	if o.SSTMaxBytes < 0 || o.BloomMaxBytes < 0 {
		return ArtifactCacheOptions{}, errors.New("diskcache: artifact cache limits cannot be negative")
	}
	if o.SSTMaxBytes == 0 {
		o.SSTMaxBytes = defaultArtifactSSTBytes
	}
	if o.BloomMaxBytes == 0 {
		o.BloomMaxBytes = defaultArtifactBloomBytes
	}
	return o, nil
}

// ArtifactStats reports one persistent tier's current occupancy and activity.
type ArtifactStats struct {
	Hits              int64
	Misses            int64
	Corruptions       int64
	Evictions         int64
	CapacityEvictions int64
	PurgeRemovals     int64
	RecoveryRemovals  int64
	AdmissionBypasses int64
	RecoveredEntries  int64
	RecoveredBytes    int64

	ResidentEntries int
	ResidentBytes   int64
	PinnedEntries   int
	PinnedBytes     int64
	MaxBytes        int64
}
