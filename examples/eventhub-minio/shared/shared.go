package shared

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const (
	defaultMinioAccessKey = "minioadmin"
	defaultMinioSecretKey = "minioadmin"
	defaultMinioRegion    = "us-east-1"
)

type Event struct {
	Seq      uint64    `json:"seq"`
	ID       string    `json:"id"`
	Topic    string    `json:"topic"`
	Producer string    `json:"producer"`
	Payload  string    `json:"payload"`
	At       time.Time `json:"at"`
}

func EnsureAWSDefaults() {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		_ = os.Setenv("AWS_ACCESS_KEY_ID", defaultMinioAccessKey)
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		_ = os.Setenv("AWS_SECRET_ACCESS_KEY", defaultMinioSecretKey)
	}
	if os.Getenv("AWS_REGION") == "" {
		_ = os.Setenv("AWS_REGION", defaultMinioRegion)
	}
}

func DefaultBucketURL(bucket string) string {
	return fmt.Sprintf("s3://%s?endpoint=http://localhost:9000&region=%s&use_path_style=true", bucket, defaultMinioRegion)
}

func EnsureS3Bucket(ctx context.Context, bucketURL string) error {
	u, err := url.Parse(bucketURL)
	if err != nil {
		return fmt.Errorf("parse bucket url: %w", err)
	}
	if u.Host == "" {
		return fmt.Errorf("bucket name missing in url %q", bucketURL)
	}

	endpoint := u.Query().Get("endpoint")
	region := u.Query().Get("region")
	if region == "" {
		region = defaultMinioRegion
	}

	cfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"",
		)),
	}
	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(u.Host),
	})
	if err == nil {
		return nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" || apiErr.ErrorCode() == "BucketAlreadyExists" {
			return nil
		}
	}

	return fmt.Errorf("create bucket %q: %w", u.Host, err)
}

func EventKey(topic string, at time.Time, seq uint64) string {
	return fmt.Sprintf("events/%s/%020d-%020d", topic, at.UTC().UnixNano(), seq)
}

func PrefixUpperBound(prefix string) []byte {
	if prefix == "" {
		return nil
	}

	out := []byte(prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}

	return append(out, 0x00)
}

func LoadLastKey(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func SaveLastKey(path string, key []byte) error {
	if len(key) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, key, 0644)
}
