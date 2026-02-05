package blobstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"gocloud.dev/blob/gcsblob"
)

const (
	fakeGCSBucket = "test-bucket"
)

var fakeGCSServer *fakestorage.Server

func setupFakeGCS(t *testing.T) *Store {
	t.Helper()

	if fakeGCSServer == nil {
		server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
			InitialObjects: []fakestorage.Object{},
			Host:           "127.0.0.1",
			Port:           0,
		})
		if err != nil {
			t.Fatalf("failed to create fake gcs server: %v", err)
		}
		fakeGCSServer = server
		t.Cleanup(func() {
			fakeGCSServer.Stop()
			fakeGCSServer = nil
		})
	}

	fakeGCSServer.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: fakeGCSBucket})

	client := fakeGCSServer.Client()
	ctx := context.Background()
	bucket, err := gcsblob.OpenBucket(ctx, nil, fakeGCSBucket, &gcsblob.Options{
		Client: client,
	})
	if err != nil {
		t.Fatalf("failed to open gcs bucket: %v", err)
	}

	return &Store{
		bucket:     bucket,
		bucketName: fakeGCSBucket,
		prefix:     "test-prefix",
		owns:       true,
	}
}

func TestGCS_WriteIfMatch(t *testing.T) {
	store := setupFakeGCS(t)
	defer store.Close()

	ctx := context.Background()

	key := fmt.Sprintf("test-cas-%d", time.Now().UnixNano())

	data1 := []byte(`{"version": 1}`)
	attr1, err := store.Write(ctx, key, data1)
	if err != nil {
		t.Fatalf("initial write failed: %v", err)
	}
	if attr1.Generation == 0 {
		t.Fatalf("expected non-zero generation for gcs write")
	}

	data2 := []byte(`{"version": 2}`)
	_, err = store.WriteIfMatch(ctx, key, data2, fmt.Sprintf("%d", attr1.Generation))
	if err != nil {
		t.Fatalf("WriteIfMatch failed: %v", err)
	}

	_, err = store.WriteIfMatch(ctx, key, []byte(`{"version": 3}`), fmt.Sprintf("%d", attr1.Generation))
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed with stale generation, got: %v", err)
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

func TestGCS_WriteIfNotExist(t *testing.T) {
	store := setupFakeGCS(t)
	defer store.Close()

	ctx := context.Background()

	key := fmt.Sprintf("test-create-%d", time.Now().UnixNano())

	data1 := []byte(`{"version": 1}`)
	_, err := store.WriteIfNotExist(ctx, key, data1)
	if err != nil {
		t.Fatalf("WriteIfNotExist failed: %v", err)
	}

	_, err = store.WriteIfNotExist(ctx, key, []byte(`{"version": 2}`))
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed when file exists, got: %v", err)
	}

	_ = store.Delete(ctx, key)
}

func TestGCS_WriteIfMatch_ConcurrentWriters(t *testing.T) {
	store := setupFakeGCS(t)
	defer store.Close()

	ctx := context.Background()

	key := fmt.Sprintf("test-concurrent-%d", time.Now().UnixNano())

	data0 := []byte(`{"version": 0}`)
	attr0, err := store.Write(ctx, key, data0)
	if err != nil {
		t.Fatalf("initial write failed: %v", err)
	}
	if attr0.Generation == 0 {
		t.Fatalf("expected non-zero generation for gcs write")
	}

	genA := attr0.Generation
	genB := attr0.Generation

	dataA := []byte(`{"version": "A"}`)
	_, err = store.WriteIfMatch(ctx, key, dataA, fmt.Sprintf("%d", genA))
	if err != nil {
		t.Fatalf("writer A failed: %v", err)
	}

	dataB := []byte(`{"version": "B"}`)
	_, err = store.WriteIfMatch(ctx, key, dataB, fmt.Sprintf("%d", genB))
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
