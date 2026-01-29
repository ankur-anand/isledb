package isledb

import "time"

type WriterOptions struct {
	MemtableSize    int64
	FlushInterval   time.Duration
	BloomBitsPerKey int
	BlockSize       int
	Compression     string

	OnFlushError func(error)
	ValueStorage ValueStorageConfig

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

	BlobCache          BlobCache
	BlobCacheSize      int64
	BlobCacheItemSize  int64
	ValueStorageConfig ValueStorageConfig
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
	L0CompactionThreshold   int
	TierCompactionThreshold int
	MaxTiers                int
	LazyLeveling            bool

	BloomBitsPerKey int
	BlockSize       int
	Compression     string

	CheckInterval     time.Duration
	OnCompactionStart func(CompactionJob)
	OnCompactionEnd   func(CompactionJob, error)

	EnableFencing bool
	OwnerID       string
}

func DefaultCompactorOptions() CompactorOptions {
	return CompactorOptions{
		L0CompactionThreshold:   8,
		TierCompactionThreshold: 4,
		MaxTiers:                4,
		LazyLeveling:            true,
		BloomBitsPerKey:         10,
		BlockSize:               4096,
		Compression:             "snappy",
		CheckInterval:           time.Second * 5,
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

type TailOptions struct {
	MinKey []byte
	MaxKey []byte

	StartAfterKey []byte
	PollInterval  time.Duration
}
