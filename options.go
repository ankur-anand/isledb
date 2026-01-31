package isledb

import (
	"time"

	"github.com/ankur-anand/isledb/cachestore"
	"github.com/ankur-anand/isledb/config"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
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
	SSTCache           SSTCache
	SSTCacheSize       int64
	SSTReaderCache     SSTReaderCache
	SSTReaderCacheSize int

	BlobCache          internal.BlobCache
	BlobCacheSize      int64
	BlobCacheItemSize  int64
	ValueStorageConfig config.ValueStorageConfig
	ManifestStorage    manifest.Storage

	ManifestLogCache     cachestore.ManifestLogCache
	ManifestLogCacheSize int
	DisableManifestCache bool
}

func DefaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		SSTCacheSize:       DefaultSSTCacheSize,
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
