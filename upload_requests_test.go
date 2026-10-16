package isledb

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// TestWriterUploadsAnSSTInOneRequest: a memtable's SST above S3's 5 MiB
// default part goes up in one request, not one per part.
func TestWriterUploadsAnSSTInOneRequest(t *testing.T) {
	var sstWrites atomic.Int32
	url := setupFakeS3BucketURLWithObserver(t, func(r *http.Request) {
		if (r.Method == http.MethodPut || r.Method == http.MethodPost) && strings.Contains(r.URL.Path, "/sstable/") {
			sstWrites.Add(1)
		}
	})
	ctx := context.Background()
	db, err := Open(ctx, url, DBOptions{Prefix: "uploads"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	opts := DefaultWriterOptions()
	opts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close(ctx) }()

	// 8 MiB of random values: they don't compress, so the SST stays above
	// 5 MiB.
	for i := range 128 {
		value := make([]byte, 64<<10)
		_, _ = rand.Read(value)
		if _, err := writer.Put(ctx, []byte(fmt.Sprintf("key-%03d", i)), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n := sstWrites.Load(); n != 1 {
		t.Fatalf("an 8 MiB SST took %d write requests, want 1", n)
	}
}
