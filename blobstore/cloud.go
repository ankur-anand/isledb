package blobstore

import (
	"context"
	"fmt"
	"net/url"

	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

// NewS3 creates a store backed by Amazon S3.
//
// If region is empty, the AWS SDK will try to infer it from environment/config.
// For Options Refer: https://pkg.go.dev/gocloud.dev/blob/s3blob
func NewS3(ctx context.Context, bucket, region, prefix string) (*Store, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	bucketURL := "s3://" + bucket
	if region != "" {
		bucketURL += "?region=" + url.QueryEscape(region)
	}
	return Open(ctx, bucketURL, prefix)
}

// NewGCS creates a store backed by Google Cloud Storage.
//
// Authentication is handled by application default credentials.
// Refer: https://pkg.go.dev/gocloud.dev/blob/gcsblob
func NewGCS(ctx context.Context, bucket, prefix string) (*Store, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	bucketURL := "gs://" + bucket
	return Open(ctx, bucketURL, prefix)
}

// NewAzure creates a store backed by Azure Blob Storage.
//
// Authentication uses AZURE_STORAGE_ACCOUNT/AZURE_STORAGE_KEY or other Azure SDK credentials.
// Refer: https://pkg.go.dev/gocloud.dev/blob/azureblob
func NewAzure(ctx context.Context, container, prefix string) (*Store, error) {
	if container == "" {
		return nil, fmt.Errorf("container is required")
	}
	bucketURL := "azblob://" + container
	return Open(ctx, bucketURL, prefix)
}
