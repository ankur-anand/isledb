package config

import "time"

type ValueOptions struct {
	MaxKeySize    int
	BlobThreshold int
	MaxValueSize  int64
}

type BlobReadOptions struct {
	VerifyBlobsOnRead bool
}

type BlobGCOptions struct {
	Enabled  bool
	Interval time.Duration
	MinAge   time.Duration
}

type ValueStorageConfig struct {
	ValueOptions
	BlobReadOptions
	BlobGCOptions
}

func DefaultValueOptions() ValueOptions {
	return ValueOptions{
		MaxKeySize:    64 * 1024,
		BlobThreshold: 256 * 1024,
		MaxValueSize:  256 * 1024 * 1024,
	}
}

func DefaultBlobReadOptions() BlobReadOptions {
	return BlobReadOptions{
		VerifyBlobsOnRead: false,
	}
}

func DefaultBlobGCOptions() BlobGCOptions {
	return BlobGCOptions{
		Enabled:  true,
		Interval: time.Hour,
		MinAge:   10 * time.Minute,
	}
}

func DefaultValueStorageConfig() ValueStorageConfig {
	return ValueStorageConfig{
		ValueOptions:    DefaultValueOptions(),
		BlobReadOptions: DefaultBlobReadOptions(),
		BlobGCOptions:   DefaultBlobGCOptions(),
	}
}
