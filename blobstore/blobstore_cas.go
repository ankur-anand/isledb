package blobstore

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// gocloud.dev/blob doesn't provide etag after write.
// which result in calling attribute, under normal circumstances this shouldn't be an issue
// but still it's TOCTOU race.
// To avoid this we will have to use read after write which is two blob API call, even
// though GET is cheap on blob but this pattern will still result in Wasted IO, latency and Cost.
// So for CAS operations we will use native SDK call which returns ETAG or Generation in Write Call
// itself.

func (s *Store) writeWithCAS(ctx context.Context, key string, data []byte, expectedETag string) (Attributes, error) {
	if expectedETag == "" {
		return s.writeIfNotExistWithETag(ctx, key, data)
	}

	kind := s.providerKind()
	switch kind {
	case providerS3, providerGCS, providerAzure:
		if s.bucketName == "" {
			return Attributes{}, ErrBucketNameRequired
		}
		switch kind {
		case providerS3:
			return s.writeIfMatchS3Native(ctx, key, data, expectedETag)
		case providerGCS:
			gen, err := parseGeneration(expectedETag)
			if err != nil {
				return Attributes{}, fmt.Errorf("gcs requires generation match token: %w", err)
			}
			return s.writeIfMatchGCSNative(ctx, key, data, gen)
		default:
			return s.writeIfMatchAzureNative(ctx, key, data, expectedETag)
		}
	default:
		return s.writeIfMatchFallback(ctx, key, data, expectedETag)
	}
}

func (s *Store) writeIfNotExistWithETag(ctx context.Context, key string, data []byte) (Attributes, error) {
	kind := s.providerKind()
	switch kind {
	case providerS3, providerGCS, providerAzure:
		if s.bucketName == "" {
			return Attributes{}, ErrBucketNameRequired
		}
		switch kind {
		case providerS3:
			return s.writeIfNotExistS3Native(ctx, key, data)
		case providerGCS:
			return s.writeIfNotExistGCSNative(ctx, key, data)
		default:
			return s.writeIfNotExistAzureNative(ctx, key, data)
		}
	default:
		if _, err := s.writeIfNotExist(ctx, key, data); err != nil {
			return Attributes{}, err
		}
		attr, err := s.bucket.Attributes(ctx, key)
		if err != nil {
			return Attributes{}, err
		}
		gen := generationFromAttrs(attr)
		return Attributes{
			Size:       attr.Size,
			ETag:       attr.ETag,
			Generation: gen,
		}, nil
	}

}

func (s *Store) writeIfMatchS3Native(ctx context.Context, key string, data []byte, ifMatch string) (Attributes, error) {
	var s3Client *s3.Client
	if !s.bucket.As(&s3Client) {
		return Attributes{}, fmt.Errorf("failed to get S3 client")
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
		IfMatch:     aws.String(ifMatch),
	}

	result, err := s3Client.PutObject(ctx, input)
	if err != nil {
		return Attributes{}, s.mapError(err)
	}

	etag := ""
	if result.ETag != nil {
		etag = *result.ETag
	}

	return Attributes{
		Size: int64(len(data)),
		ETag: etag,
	}, nil
}

func (s *Store) writeIfNotExistS3Native(ctx context.Context, key string, data []byte) (Attributes, error) {
	var s3Client *s3.Client
	if !s.bucket.As(&s3Client) {
		return Attributes{}, fmt.Errorf("failed to get S3 client")
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
		IfNoneMatch: aws.String("*"),
	}

	result, err := s3Client.PutObject(ctx, input)
	if err != nil {
		return Attributes{}, s.mapError(err)
	}

	etag := ""
	if result.ETag != nil {
		etag = *result.ETag
	}

	return Attributes{
		Size: int64(len(data)),
		ETag: etag,
	}, nil
}

func (s *Store) writeIfMatchGCSNative(ctx context.Context, key string, data []byte, generation int64) (Attributes, error) {
	var gcsClient *storage.Client
	if !s.bucket.As(&gcsClient) {
		return Attributes{}, fmt.Errorf("failed to get GCS client")
	}

	obj := gcsClient.Bucket(s.bucketName).Object(key)
	obj = obj.If(storage.Conditions{GenerationMatch: generation})

	w := obj.NewWriter(ctx)
	w.ContentType = "application/octet-stream"

	if _, err := w.Write(data); err != nil {
		w.Close()
		return Attributes{}, s.mapError(err)
	}

	if err := w.Close(); err != nil {
		return Attributes{}, s.mapError(err)
	}

	// Get attributes from the writer after close
	attrs := w.Attrs()

	return Attributes{
		Size:       int64(len(data)),
		ETag:       attrs.Etag,
		Generation: attrs.Generation,
	}, nil
}

func (s *Store) writeIfNotExistGCSNative(ctx context.Context, key string, data []byte) (Attributes, error) {
	var gcsClient *storage.Client
	if !s.bucket.As(&gcsClient) {
		return Attributes{}, fmt.Errorf("failed to get GCS client")
	}

	obj := gcsClient.Bucket(s.bucketName).Object(key)
	// https://docs.cloud.google.com/go/docs/reference/cloud.google.com/go/storage/latest
	// // GenerationMatch specifies that the object must have the given generation
	//	// for the operation to occur.
	//	// If GenerationMatch is zero, it has no effect.
	//	// Use DoesNotExist to specify that the object does not exist in the bucket.
	obj = obj.If(storage.Conditions{DoesNotExist: true})

	w := obj.NewWriter(ctx)
	w.ContentType = "application/octet-stream"

	if _, err := w.Write(data); err != nil {
		w.Close()
		return Attributes{}, s.mapError(err)
	}

	if err := w.Close(); err != nil {
		return Attributes{}, s.mapError(err)
	}

	attrs := w.Attrs()

	return Attributes{
		Size:       int64(len(data)),
		ETag:       attrs.Etag,
		Generation: attrs.Generation,
	}, nil
}

func (s *Store) writeIfMatchAzureNative(ctx context.Context, key string, data []byte, ifMatch string) (Attributes, error) {
	var containerClient *container.Client
	if !s.bucket.As(&containerClient) {
		return Attributes{}, fmt.Errorf("failed to get Azure container client")
	}

	blobClient := containerClient.NewBlockBlobClient(key)

	etag := azcore.ETag(ifMatch)
	opts := &blockblob.UploadOptions{
		AccessConditions: &azblobblob.AccessConditions{
			ModifiedAccessConditions: &azblobblob.ModifiedAccessConditions{
				IfMatch: &etag,
			},
		},
	}

	resp, err := blobClient.Upload(ctx, &nopCloser{bytes.NewReader(data)}, opts)
	if err != nil {
		return Attributes{}, s.mapError(err)
	}

	respETag := ""
	if resp.ETag != nil {
		respETag = string(*resp.ETag)
	}

	return Attributes{
		Size: int64(len(data)),
		ETag: respETag,
	}, nil
}

func (s *Store) writeIfNotExistAzureNative(ctx context.Context, key string, data []byte) (Attributes, error) {
	var containerClient *container.Client
	if !s.bucket.As(&containerClient) {
		return Attributes{}, fmt.Errorf("failed to get Azure container client")
	}

	blobClient := containerClient.NewBlockBlobClient(key)

	noneMatchETag := azcore.ETag("*")
	opts := &blockblob.UploadOptions{
		AccessConditions: &azblobblob.AccessConditions{
			ModifiedAccessConditions: &azblobblob.ModifiedAccessConditions{
				IfNoneMatch: &noneMatchETag,
			},
		},
	}

	resp, err := blobClient.Upload(ctx, &nopCloser{bytes.NewReader(data)}, opts)
	if err != nil {
		return Attributes{}, s.mapError(err)
	}

	respETag := ""
	if resp.ETag != nil {
		respETag = string(*resp.ETag)
	}

	return Attributes{
		Size: int64(len(data)),
		ETag: respETag,
	}, nil
}

// nopCloser wraps an io.ReadSeeker to implement io.ReadSeekCloser.
type nopCloser struct {
	io.ReadSeeker
}

func (nopCloser) Close() error { return nil }
