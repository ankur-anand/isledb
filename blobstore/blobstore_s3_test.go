//go:build integration && s3

package blobstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "gocloud.dev/blob/s3blob"
)

// docker run --rm -it -p 9000:9000 -p 9001:9001 \
//   -e MINIO_ROOT_USER=minioadmin \
//   -e MINIO_ROOT_PASSWORD=minioadmin \
//   quay.io/minio/minio server /data --console-address ":9001"
// go test ./blobstore/... -tags="integration,s3" -v -run TestS3 2>&1

const (
	minioEndpoint  = "http://127.0.0.1:9000"
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
	minioRegion    = "us-east-1"
	minioBucket    = "testbucket"
)

func s3BucketURL() string {
	return fmt.Sprintf("s3://%s?endpoint=http://127.0.0.1:9000&region=%s&use_path_style=true",
		minioBucket, minioRegion)
}

func ensureS3Bucket(t *testing.T) {
	t.Helper()

	os.Setenv("AWS_ACCESS_KEY_ID", minioAccessKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", minioSecretKey)
	os.Setenv("AWS_REGION", minioRegion)
	os.Setenv("AWS_S3_USE_PATH_STYLE", "true")

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(minioRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			minioAccessKey, minioSecretKey, "",
		)),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint)
		o.UsePathStyle = true
	})

	ctx := context.Background()
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(minioBucket),
	})
	if err != nil {
		t.Logf("create bucket (may already exist): %v", err)
	}
}

func TestS3_WriteIfMatch(t *testing.T) {
	ensureS3Bucket(t)
	ctx := context.Background()

	store, err := Open(ctx, s3BucketURL(), "test-prefix")
	if err != nil {
		t.Fatalf("failed to open s3 store: %v", err)
	}
	defer store.Close()

	key := fmt.Sprintf("test-cas-%d", time.Now().UnixNano())

	data1 := []byte(`{"version": 1}`)
	attr1, err := store.Write(ctx, key, data1)
	if err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	data2 := []byte(`{"version": 2}`)
	_, err = store.WriteIfMatch(ctx, key, data2, attr1.ETag)
	if err != nil {
		t.Fatalf("WriteIfMatch failed: %v", err)
	}

	_, err = store.WriteIfMatch(ctx, key, []byte(`{"version": 3}`), attr1.ETag)
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed with stale ETag, got: %v", err)
	}

	content, _, err := store.Read(ctx, key)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(content) != string(data2) {
		t.Errorf("content mismatch: got %q, want %q", content, data2)
	}

	_ = store.Delete(ctx, key)
}

func TestS3_WriteIfNotExist(t *testing.T) {
	ensureS3Bucket(t)
	ctx := context.Background()

	store, err := Open(ctx, s3BucketURL(), "test-prefix")
	if err != nil {
		t.Fatalf("failed to open s3 store: %v", err)
	}
	defer store.Close()

	key := fmt.Sprintf("test-create-%d", time.Now().UnixNano())

	data1 := []byte(`{"version": 1}`)
	_, err = store.WriteIfNotExist(ctx, key, data1)
	if err != nil {
		t.Fatalf("WriteIfNotExist failed: %v", err)
	}

	_, err = store.WriteIfNotExist(ctx, key, []byte(`{"version": 2}`))
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed when file exists, got: %v", err)
	}

	_ = store.Delete(ctx, key)
}

func TestS3_WriteIfMatch_ConcurrentWriters(t *testing.T) {
	ensureS3Bucket(t)
	ctx := context.Background()

	store, err := Open(ctx, s3BucketURL(), "test-prefix")
	if err != nil {
		t.Fatalf("failed to open s3 store: %v", err)
	}
	defer store.Close()

	key := fmt.Sprintf("test-concurrent-%d", time.Now().UnixNano())

	data0 := []byte(`{"version": 0}`)
	attr0, err := store.Write(ctx, key, data0)
	if err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	etagA := attr0.ETag
	etagB := attr0.ETag

	dataA := []byte(`{"version": "A"}`)
	_, err = store.WriteIfMatch(ctx, key, dataA, etagA)
	if err != nil {
		t.Fatalf("writer A failed: %v", err)
	}

	dataB := []byte(`{"version": "B"}`)
	_, err = store.WriteIfMatch(ctx, key, dataB, etagB)
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("writer B should fail with ErrPreconditionFailed, got: %v", err)
	}

	content, _, err := store.Read(ctx, key)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(content) != string(dataA) {
		t.Errorf("content mismatch: got %q, want %q", content, dataA)
	}

	_ = store.Delete(ctx, key)
}
