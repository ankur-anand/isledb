package isledb

import (
	"time"

	"github.com/ankur-anand/isledb/internal/cachestore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	defaultSSTCacheSize       = 1 << 30
	defaultBloomDiskCacheSize = 64 << 20
)

const (
	defaultMaxKeyBytes   = 64 * 1024
	defaultMaxValueBytes = 16 * 1024 * 1024
)

type WriterOptions struct {
	// OwnerID is the stable writer identity stored in the writer fence.
	// Empty means a process-local ID is generated.
	OwnerID string

	// Memtable controls in-memory write buffering before SST creation.
	Memtable WriterMemtableOptions

	// Flush controls background flushing. A zero Interval disables auto-flush.
	Flush WriterFlushOptions

	// Maintenance controls discovery of commands staged by a maintenance
	// process using the same object-store prefix.
	Maintenance WriterMaintenanceOptions

	// Values controls key/value limits. Values are stored inline in SSTs.
	Values ValueOptions

	// OnFlushError is called once after the background flush worker stops. The
	// callback may call Writer.Close. The failure makes the writer terminal and
	// is returned by later operations.
	OnFlushError func(error)

	// Metrics receives optional writer observations. Nil disables metrics.
	Metrics *WriterMetrics
}

type WriterMemtableOptions struct {
	// TargetBytes is the approximate active memtable size that triggers rotation.
	// Rotation does not publish data by itself; Flush, background flush, or Close
	// publishes frozen memtables as SSTs. Zero selects the default.
	TargetBytes int64

	// MaxPendingMemtables limits frozen memtables that are queued or currently
	// flushing. When the limit is reached, writes return ErrBackpressure before
	// accepting another mutation. Zero selects the default.
	MaxPendingMemtables int
}

type WriterFlushOptions struct {
	// Interval is the background flush cadence. Zero disables background flush.
	// A background error makes the writer terminal.
	Interval time.Duration
}

type WriterMaintenanceOptions struct {
	// PollInterval is the minimum interval between object-store reads of
	// maintenance/HEAD. Same-process maintenance bypasses this interval through
	// an in-memory notification. Zero selects the default.
	PollInterval time.Duration
}

// ValueOptions controls writer key/value limits. Values are stored inline in
// SSTs. Zero fields select defaults.
type ValueOptions struct {
	// MaxKeyBytes is the largest accepted key size.
	MaxKeyBytes int

	// MaxValueBytes is the largest accepted value size.
	MaxValueBytes int64
}

func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		Memtable: WriterMemtableOptions{
			TargetBytes:         16 * 1024 * 1024,
			MaxPendingMemtables: 4,
		},
		Flush: WriterFlushOptions{
			Interval: time.Second,
		},
		Maintenance: WriterMaintenanceOptions{
			PollInterval: time.Second,
		},
		Values: defaultWriterValueOptions(),
	}
}

func defaultWriterValueOptions() ValueOptions {
	return ValueOptions{
		MaxKeyBytes:   defaultMaxKeyBytes,
		MaxValueBytes: defaultMaxValueBytes,
	}
}

type readerOptions struct {
	// CacheDir is required local working storage for SST downloads as well as
	// the root of the persistent disk caches. It must remain writable while the
	// Reader is open.
	CacheDir string

	// ArtifactCache is an optional pre-created persistent SST/Bloom cache.
	ArtifactCache *diskcache.ArtifactCache

	// SSTCacheSize is the maximum bytes for SST cache (default 1GB).
	SSTCacheSize int64

	// BloomDiskCacheSize is the maximum bytes for verified raw Bloom sidecars.
	BloomDiskCacheSize int64

	// BlockCacheSize is the maximum bytes for the in-memory block cache used
	// when range-reading SSTs. Default 0 disables the block cache.
	BlockCacheSize int64

	// BloomCacheSize is the maximum accounted bytes for decoded SST bloom
	// filters. Zero selects the default (64 MiB).
	BloomCacheSize int64

	// AllowUnverifiedRangeRead permits range-reading SSTs without verifying
	// full-file checksums.
	AllowUnverifiedRangeRead bool

	// RangeReadMinSSTSize is the minimum SST size (bytes) required to use
	// range-read + block cache. Default 0 means no size threshold.
	RangeReadMinSSTSize int64

	ManifestStorage manifest.Storage

	ManifestPageCache        cachestore.ManifestPageCache
	ManifestPageCacheSize    int
	DisableManifestPageCache bool

	// ValidateSSTChecksum verifies SST checksums on read paths that can
	// otherwise skip it. Persistent ArtifactCache admissions always verify.
	ValidateSSTChecksum bool

	ViewPolicy ReaderViewPolicy

	Metrics *ReaderMetrics
}

func defaultReaderOptions() readerOptions {
	return readerOptions{
		SSTCacheSize:       defaultSSTCacheSize,
		BloomDiskCacheSize: defaultBloomDiskCacheSize,
		BloomCacheSize:     defaultBloomCacheSize,
		ViewPolicy: ReaderViewPolicy{
			RefreshAfter: defaultReaderRefreshAfter,
		},
	}
}

type compactorOptions struct {
	// OwnerID is the stable compactor identity stored in the compactor fence.
	OwnerID string

	// InputReadParallelism bounds concurrent source SST reads inside one
	// compaction job. Values <= 0 use the default.
	InputReadParallelism int

	// Trigger controls when compaction work is selected.
	Trigger compactionTriggerOptions

	// Output controls compacted SST file encoding and sizing.
	Output compactionOutputOptions

	// Safety controls source SST validation before compaction.
	Safety compactionSafetyOptions

	OnCompactionStart func(compactionJob)
	OnCompactionEnd   func(compactionJob, error)
}

type compactionTriggerOptions struct {
	// L0SSTCount is the number of L0 SSTs that triggers an L0 merge.
	L0SSTCount int

	// BaseLevelBytes is the target size of L1. Each subsequent level target is
	// multiplied by LevelSizeMultiplier.
	BaseLevelBytes int64

	// LevelSizeMultiplier controls geometric growth of L1..Ln.
	LevelSizeMultiplier int

	// MaxInputSSTs bounds one atomic compaction and its retirement record.
	MaxInputSSTs int

	// MaxInputBytes softly bounds the total source and destination SST bytes in
	// one compaction. One indivisible plan may exceed the limit.
	MaxInputBytes int64
}

type compactionOutputOptions struct {
	// TargetSSTBytes is the target size for compacted SST outputs.
	TargetSSTBytes int64

	// BloomBitsPerKey controls output SST bloom filter size.
	BloomBitsPerKey int

	// BlockBytes is the target output SST data block size.
	BlockBytes int

	// Compression is the output SST compression algorithm.
	Compression string
}

type compactionSafetyOptions struct {
	// ValidateSSTChecksum verifies SST checksums before compaction.
	ValidateSSTChecksum bool
}

func defaultCompactorOptions() compactorOptions {
	return compactorOptions{
		InputReadParallelism: 4,
		Trigger: compactionTriggerOptions{
			L0SSTCount:          8,
			BaseLevelBytes:      512 * 1024 * 1024,
			LevelSizeMultiplier: 8,
			MaxInputSSTs:        manifest.MaxRetiredObjectsPerEntry,
			MaxInputBytes:       512 * 1024 * 1024,
		},
		Output: compactionOutputOptions{
			TargetSSTBytes:  64 * 1024 * 1024,
			BloomBitsPerKey: 10,
			BlockBytes:      4096,
			Compression:     "snappy",
		},
	}
}

type sstWriterOptions struct {
	BloomBitsPerKey int
	BlockSize       int
	Compression     string
}

type IteratorOptions struct {
	MinKey []byte
	MaxKey []byte
}
