package isledb

import (
	"time"

	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/cachestore"
	"github.com/ankur-anand/isledb/internal/config"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	defaultSSTCacheSize  = 1 << 30
	defaultBlobCacheSize = 1 << 30
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

	// SST controls SST file encoding.
	SST WriterSSTOptions

	// Values controls inline-vs-external value storage and key/value limits.
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

type WriterSSTOptions struct {
	// BloomBitsPerKey controls SST bloom filter size. Zero selects the default.
	BloomBitsPerKey int

	// BlockBytes is the target SST data block size. Zero selects the default.
	BlockBytes int

	// Compression is one of "none", "snappy", or "zstd". Empty selects the
	// default.
	Compression string
}

// ValueOptions controls writer key/value limits and external value storage.
// Zero fields select defaults.
type ValueOptions struct {
	// MaxKeyBytes is the largest accepted key size.
	MaxKeyBytes int

	// InlineValueBytes is the external-storage threshold. Values with a size
	// greater than or equal to this value are stored outside the SST.
	InlineValueBytes int

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
		SST: WriterSSTOptions{
			BloomBitsPerKey: 10,
			BlockBytes:      4096,
			Compression:     "snappy",
		},
		Values: defaultWriterValueOptions(),
	}
}

func defaultWriterValueOptions() ValueOptions {
	defaults := config.DefaultValueOptions()
	return ValueOptions{
		MaxKeyBytes:      defaults.MaxKeySize,
		InlineValueBytes: defaults.BlobThreshold,
		MaxValueBytes:    defaults.MaxValueSize,
	}
}

type readerOptions struct {
	// CacheDir is the directory for disk caches.
	CacheDir string

	// SSTCache is an optional pre-created SST cache.
	SSTCache diskcache.RefCountedCache

	// SSTCacheSize is the maximum bytes for SST cache (default 1GB).
	SSTCacheSize int64

	// BlobCache is an optional pre-created blob cache.
	BlobCache internal.BlobCache

	// BlobCacheSize is the maximum bytes for blob cache (default 1GB).
	BlobCacheSize int64

	// BlobCacheMaxItemSize is the maximum size per item in the blob cache.
	// Items larger than this will not be cached. Default 0 means no limit.
	BlobCacheMaxItemSize int64

	// BlockCacheSize is the maximum bytes for the in-memory block cache used
	// when range-reading SSTs. Default 0 disables the block cache.
	BlockCacheSize int64

	// AllowUnverifiedRangeRead permits range-reading SSTs without verifying
	// full-file checksums or signatures.
	AllowUnverifiedRangeRead bool

	// RangeReadMinSSTSize is the minimum SST size (bytes) required to use
	// range-read + block cache. Default 0 means no size threshold.
	RangeReadMinSSTSize int64

	ValueStorageConfig config.ValueStorageConfig
	ManifestStorage    manifest.Storage

	ManifestPageCache        cachestore.ManifestPageCache
	ManifestPageCacheSize    int
	DisableManifestPageCache bool

	// ValidateSSTChecksum verifies SST checksums on first download.
	// If enabled and checksum is missing or mismatched, reads fail.
	ValidateSSTChecksum bool

	// SSTHashVerifier verifies SST signatures when present.
	// If provided and the SST has a signature, verification is enforced.
	SSTHashVerifier SSTHashVerifier

	ViewPolicy ReaderViewPolicy

	Metrics *ReaderMetrics
}

func defaultReaderOptions() readerOptions {
	return readerOptions{
		SSTCacheSize:  defaultSSTCacheSize,
		BlobCacheSize: defaultBlobCacheSize,
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

	OnCompactionStart func(CompactionJob)
	OnCompactionEnd   func(CompactionJob, error)

	GCCursorStorage   manifest.GCCursorStorage
	GCDeleteBatchSize int
}

type compactionTriggerOptions struct {
	// CheckInterval is the background scheduler cadence used by Start.
	CheckInterval time.Duration

	// L0SSTCount is the number of L0 SSTs that triggers an L0 merge.
	L0SSTCount int

	// MaxConsecutiveL0Compactions bounds L0 priority when a lower level is
	// already over budget.
	MaxConsecutiveL0Compactions int

	// BaseLevelBytes is the target size of L1. Each subsequent level target is
	// multiplied by LevelSizeMultiplier.
	BaseLevelBytes int64

	// LevelSizeMultiplier controls geometric growth of L1..Ln.
	LevelSizeMultiplier int

	// MaxInputSSTs bounds one atomic compaction and its retirement record.
	MaxInputSSTs int
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

	// SSTHashVerifier verifies SST signatures when present.
	SSTHashVerifier SSTHashVerifier
}

func defaultCompactorOptions() compactorOptions {
	return compactorOptions{
		InputReadParallelism: 4,
		GCDeleteBatchSize:    defaultSSTSweepBatchSize,
		Trigger: compactionTriggerOptions{
			CheckInterval:               5 * time.Second,
			L0SSTCount:                  8,
			MaxConsecutiveL0Compactions: 4,
			BaseLevelBytes:              512 * 1024 * 1024,
			LevelSizeMultiplier:         8,
			MaxInputSSTs:                manifest.MaxRetiredObjectsPerEntry,
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
	Signer          SSTHashSigner
}

type IteratorOptions struct {
	MinKey []byte
	MaxKey []byte
}
