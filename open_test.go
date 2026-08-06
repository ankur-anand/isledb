package isledb

import (
	"context"
	"errors"
	"testing"

	"gocloud.dev/blob/memblob"
)

func TestOpenOwnsBucketAndSupportsReadWrite(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, "file://"+t.TempDir(), DBOptions{Prefix: "open-owned"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := writer.Put(ctx, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	value, found, err := reader.Get(ctx, []byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(value) != "value" {
		t.Fatalf("Get = (%q, %v), want (%q, true)", value, found, "value")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}
	if _, _, err := db.store.Read(ctx, db.store.ManifestPath()); err == nil {
		t.Fatal("owned bucket remained usable after DB.Close")
	}
}

func TestOpenBucketBorrowsBucket(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{Prefix: "borrowed"})
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}

	writer, err := bucket.NewWriter(ctx, "still-open", nil)
	if err != nil {
		t.Fatalf("borrowed bucket NewWriter after DB.Close: %v", err)
	}
	if _, err := writer.Write([]byte("value")); err != nil {
		t.Fatalf("borrowed bucket Write after DB.Close: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("borrowed bucket writer Close: %v", err)
	}
}

func TestOpenBucketValidatesRequiredInputs(t *testing.T) {
	ctx := context.Background()
	if _, err := OpenBucket(ctx, nil, "bucket", DBOptions{}); !errors.Is(err, ErrInvalidDBOptions) {
		t.Fatalf("OpenBucket(nil) error=%v, want %v", err, ErrInvalidDBOptions)
	}

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	if _, err := OpenBucket(ctx, bucket, "", DBOptions{}); !errors.Is(err, ErrInvalidDBOptions) {
		t.Fatalf("OpenBucket(empty name) error=%v, want %v", err, ErrInvalidDBOptions)
	}
}
