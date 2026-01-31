package isledb

import (
	"context"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
)

// CompactorOpenOptions configures background compaction.
type CompactorOpenOptions struct {
	L0CompactionThreshold int
	MinSources            int
	MaxSources            int
	SizeThreshold         int

	BloomBitsPerKey int
	BlockSize       int
	Compression     string
	TargetSSTSize   int64

	CheckInterval     time.Duration
	OnCompactionStart func(CompactionJob)
	OnCompactionEnd   func(CompactionJob, error)
	ManifestStorage   manifest.Storage

	// DisableFencing opts out of single-compactor fencing.
	// By default, fencing is enabled.
	DisableFencing bool
	OwnerID        string
}

// DefaultCompactorOpenOptions returns sane defaults for CompactorOpenOptions.
func DefaultCompactorOpenOptions() CompactorOpenOptions {
	defaults := DefaultCompactorOptions()
	return CompactorOpenOptions{
		L0CompactionThreshold: defaults.L0CompactionThreshold,
		MinSources:            defaults.MinSources,
		MaxSources:            defaults.MaxSources,
		SizeThreshold:         defaults.SizeThreshold,
		BloomBitsPerKey:       defaults.BloomBitsPerKey,
		BlockSize:             defaults.BlockSize,
		Compression:           defaults.Compression,
		TargetSSTSize:         defaults.TargetSSTSize,
		CheckInterval:         defaults.CheckInterval,
	}
}

// OpenCompactor opens a compactor handle.
func OpenCompactor(ctx context.Context, store *blobstore.Store, opts CompactorOpenOptions) (*Compactor, error) {
	copts := CompactorOptions{
		L0CompactionThreshold: opts.L0CompactionThreshold,
		MinSources:            opts.MinSources,
		MaxSources:            opts.MaxSources,
		SizeThreshold:         opts.SizeThreshold,
		BloomBitsPerKey:       opts.BloomBitsPerKey,
		BlockSize:             opts.BlockSize,
		Compression:           opts.Compression,
		TargetSSTSize:         opts.TargetSSTSize,
		CheckInterval:         opts.CheckInterval,
		OnCompactionStart:     opts.OnCompactionStart,
		OnCompactionEnd:       opts.OnCompactionEnd,
		ManifestStorage:       opts.ManifestStorage,
		EnableFencing:         !opts.DisableFencing,
		OwnerID:               opts.OwnerID,
	}
	return newCompactor(ctx, store, copts)
}
