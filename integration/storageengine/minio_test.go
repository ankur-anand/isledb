//go:build integration && minio

package storageengine

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	isledb "github.com/ankur-anand/isledb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	minioDefaultEndpoint  = "http://127.0.0.1:9000"
	minioDefaultAccessKey = "minioadmin"
	minioDefaultSecretKey = "minioadmin"
	minioDefaultRegion    = "us-east-1"
)

func TestMinIOStorageEngineProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	endpoint := storageEnvOrDefault("ISLEDB_MINIO_ENDPOINT", minioDefaultEndpoint)
	accessKey := storageEnvOrDefault("AWS_ACCESS_KEY_ID", minioDefaultAccessKey)
	secretKey := storageEnvOrDefault("AWS_SECRET_ACCESS_KEY", minioDefaultSecretKey)
	region := storageEnvOrDefault("AWS_REGION", minioDefaultRegion)
	t.Setenv("AWS_ACCESS_KEY_ID", accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
	t.Setenv("AWS_REGION", region)
	t.Setenv("AWS_S3_USE_PATH_STYLE", "true")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey, secretKey, "")),
	)
	if err != nil {
		t.Fatalf("load MinIO client configuration: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	bucket := fmt.Sprintf("isledb-integration-%d", time.Now().UnixNano())
	if err := waitForMinIOBucket(ctx, client, endpoint, bucket); err != nil {
		t.Fatalf("prepare MinIO bucket: %v", err)
	}

	bucketURL := &url.URL{Scheme: "s3", Host: bucket}
	query := bucketURL.Query()
	query.Set("endpoint", endpoint)
	query.Set("region", region)
	query.Set("use_path_style", "true")
	bucketURL.RawQuery = query.Encode()
	for _, payload := range []isledb.ChangeFeedPayload{
		isledb.ChangeFeedKeysOnly,
		isledb.ChangeFeedFullValues,
	} {
		t.Run(payload.String(), func(t *testing.T) {
			runStorageEngineProcessWorkflow(t, bucketURL.String(), payload)
		})
	}
}

func waitForMinIOBucket(ctx context.Context, client *s3.Client, endpoint, bucket string) error {
	var lastErr error
	for {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		if err == nil {
			return nil
		}
		lastErr = err
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			stopStorageProviderTimer(timer)
			return fmt.Errorf("wait for MinIO at %s: %w", endpoint, lastErr)
		}
	}
}

func storageEnvOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
