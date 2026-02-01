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
	MemtableSize    int64
	FlushInterval   time.Duration
	BloomBitsPerKey int
	BlockSize       int
	Compression     string

	OnFlushError    func(error)
	ValueStorage    config.ValueStorageConfig
	ManifestStorage manifest.Storage

	EnableFencing bool
	OwnerID       string
}

func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		MemtableSize:    4 * 1024 * 1024,
		FlushInterval:   time.Second,
		BloomBitsPerKey: 10,
		BlockSize:       4096,
		Compression:     "snappy",
	}
}

type ReaderOptions struct {
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

	// SSTReaderCache caches parsed SST readers (stays in-memory, small footprint).
	SSTReaderCache     SSTReaderCache
	SSTReaderCacheSize int

	ValueStorageConfig config.ValueStorageConfig
	ManifestStorage    manifest.Storage

	ManifestLogCache     cachestore.ManifestLogCache
	ManifestLogCacheSize int
	DisableManifestCache bool
}

func DefaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		SSTCacheSize:       defaultSSTCacheSize,
		BlobCacheSize:      defaultBlobCacheSize,
		SSTReaderCacheSize: DefaultSSTReaderCacheSize,
	}
}

type TailingReaderOptions struct {
	RefreshInterval time.Duration

	OnRefresh      func()
	OnRefreshError func(error)

	ReaderOptions ReaderOptions
}

func DefaultTailingReaderOptions() TailingReaderOptions {
	return TailingReaderOptions{
		RefreshInterval: 100 * time.Millisecond,
		ReaderOptions:   DefaultReaderOptions(),
	}
}

type CompactorOptions struct {
	L0CompactionThreshold int

	MinSources    int
	MaxSources    int
	SizeThreshold int

	BloomBitsPerKey int
	BlockSize       int
	Compression     string
	TargetSSTSize   int64

	CheckInterval     time.Duration
	OnCompactionStart func(CompactionJob)
	OnCompactionEnd   func(CompactionJob, error)
	ManifestStorage   manifest.Storage

	EnableFencing bool
	OwnerID       string
}

func DefaultCompactorOptions() CompactorOptions {
	return CompactorOptions{
		L0CompactionThreshold: 8,
		MinSources:            4,
		MaxSources:            8,
		SizeThreshold:         4,
		BloomBitsPerKey:       10,
		BlockSize:             4096,
		Compression:           "snappy",
		TargetSSTSize:         64 * 1024 * 1024,
		CheckInterval:         time.Second * 5,
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

// TailOptions controls tailing behavior for a TailingReader.
type TailOptions struct {
	// MinKey and MaxKey constrain the tailing range (inclusive bounds).
	MinKey []byte
	MaxKey []byte

	// StartAfterKey resumes tailing from the next key after this value.
	// If set, it overrides MinKey as the lower bound.
	StartAfterKey []byte
	// PollInterval controls how often to check for new keys.
	PollInterval time.Duration
}
