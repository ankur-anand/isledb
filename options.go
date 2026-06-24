package isledb

import (
	"time"

	"github.com/ankur-anand/isledb/cachestore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/diskcache"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
)

const (
	defaultSSTCacheSize  = 1 << 30
	defaultBlobCacheSize = 1 << 30
)

type WriterOptions struct {
	// OwnerID is the stable writer identity stored in the writer fence.
	OwnerID string

	// Memtable controls in-memory write buffering before SST creation.
	Memtable WriterMemtableOptions

	// Flush controls background flushing. A zero Interval disables auto-flush.
	Flush WriterFlushOptions

	// SST controls SST file encoding.
	SST WriterSSTOptions

	// Values controls inline-vs-blob value storage and value/key limits.
	Values config.ValueStorageConfig

	// ChangeFeed controls whether flushes also publish seq-ordered mutation
	// batches under changes/. Disabled by default for pure KV workloads.
	ChangeFeed ChangeFeedOptions

	OnFlushError func(error)
	Metrics      *WriterMetrics
}

type ChangeFeedOptions struct {
	// Enabled makes Writer.Flush publish one change batch per flushed memtable
	// and attach its metadata to the committed manifest entry.
	Enabled bool
}

type WriterMemtableOptions struct {
	// TargetBytes is the approximate active memtable size that triggers rotation.
	TargetBytes int64

	// MaxFrozen limits full memtables waiting for flush. When the limit is
	// reached, writes return ErrBackpressure. Zero means unbounded.
	MaxFrozen int
}

type WriterFlushOptions struct {
	// Interval is the background flush cadence. Zero disables background flush.
	Interval time.Duration
}

type WriterSSTOptions struct {
	// BloomBitsPerKey controls SST bloom filter size.
	BloomBitsPerKey int

	// BlockBytes is the target SST data block size.
	BlockBytes int

	// Compression is the SST compression algorithm.
	Compression string
}

func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		Memtable: WriterMemtableOptions{
			TargetBytes: 16 * 1024 * 1024,
		},
		Flush: WriterFlushOptions{
			Interval: time.Second,
		},
		SST: WriterSSTOptions{
			BloomBitsPerKey: 10,
			BlockBytes:      4096,
			Compression:     "snappy",
		},
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

	Metrics *ReaderMetrics
}

func defaultReaderOptions() readerOptions {
	return readerOptions{
		SSTCacheSize:  defaultSSTCacheSize,
		BlobCacheSize: defaultBlobCacheSize,
	}
}

type CompactorOptions struct {
	// OwnerID is the stable compactor identity stored in the compactor fence.
	OwnerID string

	// InputReadParallelism bounds concurrent source SST reads inside one
	// compaction job. Values <= 0 use the default.
	InputReadParallelism int

	// Trigger controls when compaction work is selected.
	Trigger CompactionTriggerOptions

	// Output controls compacted SST file encoding and sizing.
	Output CompactionOutputOptions

	// Safety controls source SST validation before compaction.
	Safety CompactionSafetyOptions

	OnCompactionStart func(CompactionJob)
	OnCompactionEnd   func(CompactionJob, error)

	// GCMarkStorage allows using a custom storage backend for GC mark state.
	GCMarkStorage manifest.GCMarkStorage
}

type CompactionTriggerOptions struct {
	// CheckInterval is the background scheduler cadence used by Start.
	CheckInterval time.Duration

	// L0SSTCount is the number of L0 SSTs that triggers an L0 merge.
	L0SSTCount int

	// MaxConsecutiveL0Compactions is the fairness budget for L0 compaction.
	// After this many L0 jobs, an eligible sorted-run merge gets one turn
	// even if L0 is still over threshold. Values <= 0 use the default.
	MaxConsecutiveL0Compactions int

	// MinSources and MaxSources bound consecutive sorted-run merge candidates.
	MinSources int
	MaxSources int

	// SizeRatio is the similarity threshold for consecutive sorted-run merges.
	SizeRatio int
}

type CompactionOutputOptions struct {
	// TargetSSTBytes is the target size for compacted SST outputs.
	TargetSSTBytes int64

	// BloomBitsPerKey controls output SST bloom filter size.
	BloomBitsPerKey int

	// BlockBytes is the target output SST data block size.
	BlockBytes int

	// Compression is the output SST compression algorithm.
	Compression string
}

type CompactionSafetyOptions struct {
	// ValidateSSTChecksum verifies SST checksums before compaction.
	ValidateSSTChecksum bool

	// SSTHashVerifier verifies SST signatures when present.
	SSTHashVerifier SSTHashVerifier
}

func DefaultCompactorOptions() CompactorOptions {
	return CompactorOptions{
		InputReadParallelism: 4,
		Trigger: CompactionTriggerOptions{
			CheckInterval:               5 * time.Second,
			L0SSTCount:                  8,
			MaxConsecutiveL0Compactions: 4,
			MinSources:                  4,
			MaxSources:                  8,
			SizeRatio:                   4,
		},
		Output: CompactionOutputOptions{
			TargetSSTBytes:  64 * 1024 * 1024,
			BloomBitsPerKey: 10,
			BlockBytes:      4096,
			Compression:     "snappy",
		},
	}
}

type SSTWriterOptions struct {
	BloomBitsPerKey int
	BlockSize       int
	Compression     string
	Signer          SSTHashSigner
}

type IteratorOptions struct {
	MinKey []byte
	MaxKey []byte
}
