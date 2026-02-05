package blobstore

import (
	"strings"

	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

// NewMemory creates an in-memory store for testing.
func NewMemory(prefix string) *Store {
	bkt := memblob.OpenBucket(nil)
	return &Store{
		bucket: bkt,
		prefix: strings.TrimSuffix(prefix, "/"),
		owns:   true,
	}
}

func newMemoryFromBucket(bkt *blob.Bucket, prefix string) *Store {
	return New(bkt, "", prefix)
}
