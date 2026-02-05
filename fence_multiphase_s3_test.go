package isledb

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	_ "gocloud.dev/blob/s3blob"
)

const (
	fakeS3AccessKey = "fakeaccess"
	fakeS3SecretKey = "fakesecret"
	fakeS3Region    = "us-east-1"
	fakeS3Bucket    = "testbucket"
)

func setupFakeS3Store(t *testing.T) *blobstore.Store {
	t.Helper()

	backend := s3mem.New()
	fake := gofakes3.New(backend)
	server := httptest.NewServer(fake.Server())
	t.Cleanup(server.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", fakeS3AccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", fakeS3SecretKey)
	t.Setenv("AWS_REGION", fakeS3Region)
	t.Setenv("AWS_S3_USE_PATH_STYLE", "true")

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(fakeS3Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			fakeS3AccessKey, fakeS3SecretKey, "",
		)),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(fakeS3Bucket),
	})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	store, err := blobstore.Open(context.Background(), s3BucketURL(server.URL), "test-prefix")
	if err != nil {
		t.Fatalf("open s3 store: %v", err)
	}
	return store
}

func s3BucketURL(endpoint string) string {
	return fmt.Sprintf("s3://%s?endpoint=%s&region=%s&use_path_style=true&response_checksum_validation=when_required&request_checksum_calculation=when_required",
		fakeS3Bucket, endpoint, fakeS3Region)
}

func TestReplay_FiltersStaleEntriesAfterNewFenceClaim_MultiPhase_S3(t *testing.T) {
	store := setupFakeS3Store(t)
	defer store.Close()
	runReplayFiltersStaleEntriesAfterNewFenceClaimMultiPhase(t, store)
}
