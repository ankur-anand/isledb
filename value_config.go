package isledb

import "time"

type ValueStorageConfig struct {
	MaxKeySize int

	BlobThreshold int

	MaxValueSize int64

	TargetSSTSize int64

	BlockSize int

	VerifyBlobsOnRead bool

	BlobGCEnabled bool

	BlobGCInterval time.Duration

	BlobGCMinAge time.Duration
}

func DefaultValueStorageConfig() ValueStorageConfig {
	return ValueStorageConfig{

		MaxKeySize: 64 * 1024,

		BlobThreshold: 256 * 1024,
		MaxValueSize:  256 * 1024 * 1024,

		TargetSSTSize: 64 * 1024 * 1024,
		BlockSize:     64 * 1024,

		VerifyBlobsOnRead: false,

		BlobGCEnabled:  true,
		BlobGCInterval: time.Hour,
		BlobGCMinAge:   10 * time.Minute,
	}
}

func MetadataStoreConfig() ValueStorageConfig {
	cfg := DefaultValueStorageConfig()
	cfg.MaxKeySize = 1024
	cfg.BlobThreshold = 1 * 1024 * 1024
	cfg.MaxValueSize = 1 * 1024 * 1024
	cfg.TargetSSTSize = 16 * 1024 * 1024
	cfg.BlobGCEnabled = false
	return cfg
}

func DocumentStoreConfig() ValueStorageConfig {
	cfg := DefaultValueStorageConfig()
	cfg.BlobThreshold = 64 * 1024
	cfg.MaxValueSize = 16 * 1024 * 1024
	return cfg
}

func MediaStoreConfig() ValueStorageConfig {
	cfg := DefaultValueStorageConfig()
	cfg.BlobThreshold = 64 * 1024
	cfg.MaxValueSize = 2 * 1024 * 1024 * 1024
	cfg.BlobGCInterval = 6 * time.Hour
	return cfg
}

func HighThroughputConfig() ValueStorageConfig {
	cfg := DefaultValueStorageConfig()
	cfg.BlobThreshold = 1 * 1024 * 1024
	cfg.TargetSSTSize = 128 * 1024 * 1024
	cfg.BlockSize = 256 * 1024
	return cfg
}

const BlobIDSize = 32
