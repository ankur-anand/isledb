package config

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
