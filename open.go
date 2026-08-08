package isledb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

// ErrInvalidDBOptions is returned when database storage options are invalid.
var ErrInvalidDBOptions = errors.New("invalid database options")

// Open opens a database rooted at opts.Prefix in bucketURL. The returned DB
// owns the bucket connection and closes it from DB.Close.
func Open(ctx context.Context, bucketURL string, opts DBOptions) (*DB, error) {
	policy, err := normalizeStorePolicy(opts.Policy)
	if err != nil {
		return nil, err
	}
	payload, err := normalizeChangeFeedOptions(opts.ChangeFeed)
	if err != nil {
		return nil, err
	}
	sstOutput, err := normalizeSSTOutputOptions(opts.SSTOutput)
	if err != nil {
		return nil, err
	}
	store, err := blobstore.Open(ctx, bucketURL, opts.Prefix)
	if err != nil {
		return nil, err
	}

	db, err := openDB(ctx, store, dbOpenOptions{
		changeFeedPayload: payload,
		storePolicy:       policy,
		sstOutput:         sstOutput,
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	db.closeStore = true
	return db, nil
}

// OpenBucket opens a database over an existing Go Cloud bucket. DB.Close does
// not close bucket; its lifecycle remains owned by the caller.
func OpenBucket(ctx context.Context, bucket *blob.Bucket, bucketName string, opts DBOptions) (*DB, error) {
	if bucket == nil {
		return nil, fmt.Errorf("%w: nil bucket", ErrInvalidDBOptions)
	}
	if bucketName == "" {
		return nil, fmt.Errorf("%w: bucket name is required", ErrInvalidDBOptions)
	}
	policy, err := normalizeStorePolicy(opts.Policy)
	if err != nil {
		return nil, err
	}
	payload, err := normalizeChangeFeedOptions(opts.ChangeFeed)
	if err != nil {
		return nil, err
	}
	sstOutput, err := normalizeSSTOutputOptions(opts.SSTOutput)
	if err != nil {
		return nil, err
	}

	store := blobstore.New(bucket, bucketName, opts.Prefix)
	return openDB(ctx, store, dbOpenOptions{
		changeFeedPayload: payload,
		storePolicy:       policy,
		sstOutput:         sstOutput,
	})
}

func normalizeSSTOutputOptions(opts SSTOutputOptions) (SSTOutputOptions, error) {
	defaults := DefaultSSTOutputOptions()
	l0, err := normalizeSSTEncodingOptions("l0", opts.L0, defaults.L0)
	if err != nil {
		return SSTOutputOptions{}, err
	}
	compacted, err := normalizeSSTEncodingOptions("compacted", opts.Compacted, defaults.Compacted)
	if err != nil {
		return SSTOutputOptions{}, err
	}
	return SSTOutputOptions{L0: l0, Compacted: compacted}, nil
}

func normalizeSSTEncodingOptions(
	class string,
	opts SSTEncodingOptions,
	defaults SSTEncodingOptions,
) (SSTEncodingOptions, error) {
	if opts.BlockBytes < 0 {
		return SSTEncodingOptions{}, fmt.Errorf(
			"%w: sst_output.%s.block_bytes=%d", ErrInvalidDBOptions, class, opts.BlockBytes)
	}
	if opts.BloomBitsPerKey < 0 {
		return SSTEncodingOptions{}, fmt.Errorf(
			"%w: sst_output.%s.bloom_bits_per_key=%d", ErrInvalidDBOptions, class, opts.BloomBitsPerKey)
	}
	if opts.BlockBytes == 0 {
		opts.BlockBytes = defaults.BlockBytes
	}
	if opts.BloomBitsPerKey == 0 {
		opts.BloomBitsPerKey = defaults.BloomBitsPerKey
	}
	opts.Compression = strings.ToLower(strings.TrimSpace(opts.Compression))
	if opts.Compression == "" {
		opts.Compression = defaults.Compression
	}
	switch opts.Compression {
	case "none", "snappy", "zstd":
		return opts, nil
	default:
		return SSTEncodingOptions{}, fmt.Errorf(
			"%w: sst_output.%s.compression=%q", ErrInvalidDBOptions, class, opts.Compression)
	}
}

func normalizeChangeFeedOptions(opts *ChangeFeedOptions) (manifest.ChangeFeedPayload, error) {
	if opts == nil {
		return "", nil
	}
	switch opts.Payload {
	case ChangeFeedKeysOnly:
		return manifest.ChangeFeedPayloadKeysOnly, nil
	case ChangeFeedFullValues:
		return manifest.ChangeFeedPayloadFullValues, nil
	default:
		return "", fmt.Errorf("%w: invalid change feed payload=%d", ErrInvalidDBOptions, opts.Payload)
	}
}

func publicChangeFeedPayload(payload manifest.ChangeFeedPayload) ChangeFeedPayload {
	switch payload {
	case manifest.ChangeFeedPayloadKeysOnly:
		return ChangeFeedKeysOnly
	case manifest.ChangeFeedPayloadFullValues:
		return ChangeFeedFullValues
	default:
		return 0
	}
}

func normalizeStorePolicy(policy StorePolicy) (StorePolicy, error) {
	if policy.MaxPinnedViewAge < 0 {
		return StorePolicy{}, fmt.Errorf("%w: max_pinned_view_age=%s", ErrInvalidDBOptions, policy.MaxPinnedViewAge)
	}
	if policy.MaxPinnedViewAge == 0 {
		policy.MaxPinnedViewAge = DefaultMaxPinnedViewAge
	}
	return policy, nil
}
