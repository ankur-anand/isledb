package blobstore

import (
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

// NewMemory creates an in-memory store for testing.
func NewMemory(prefix string) *Store {
	bkt := memblob.OpenBucket(nil)
	return &Store{
		bucket: bkt,
		prefix: prefix,
		owns:   true,
	}
}

// NewMemoryFromBucket creates an in-memory store from an existing memblob bucket.
func NewMemoryFromBucket(bkt *blob.Bucket, prefix string) *Store {
	return New(bkt, prefix)
}
