package blobstore

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"gocloud.dev/blob"
)

// countingS3 is a fake S3 server that counts the write requests it is sent.
type countingS3 struct {
	mu     sync.Mutex
	writes int
}

func (c *countingS3) reset() {
	c.mu.Lock()
	c.writes = 0
	c.mu.Unlock()
}

func (c *countingS3) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

func TestUploadInOneRequestSendsOneRequest(t *testing.T) {
	ensureS3Bucket(t)
	counter := &countingS3{}
	fake := gofakes3.New(s3mem.New()).Server()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			counter.mu.Lock()
			counter.writes++
			counter.mu.Unlock()
		}
		fake.ServeHTTP(w, r)
	}))
	defer server.Close()

	ctx := context.Background()
	url := fmt.Sprintf("s3://%s?endpoint=%s&region=%s&use_path_style=true", fakeS3Bucket, server.URL, fakeS3Region)
	s, err := Open(ctx, url, "uploads")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	setup := httptest.NewRecorder()
	fake.ServeHTTP(setup, httptest.NewRequest(http.MethodPut, "/"+fakeS3Bucket, nil))

	data := bytes.Repeat([]byte("0123456789abcdef"), 12<<20/16) // 12 MiB
	for _, c := range []struct {
		name string
		opts *blob.WriterOptions
		want func(int) bool
	}{
		{"default parts", nil, func(n int) bool { return n > 1 }},
		{"one request", UploadInOneRequest(int64(len(data))), func(n int) bool { return n == 1 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			counter.reset()
			key := s.SSTPath(c.name)
			if _, err := s.WriteReader(ctx, key, bytes.NewReader(data), c.opts); err != nil {
				t.Fatal(err)
			}
			if n := counter.count(); !c.want(n) {
				t.Fatalf("a 12 MiB upload made %d write requests", n)
			}
			got, _, err := s.Read(ctx, key)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("read back %d bytes, err %v; want the %d written", len(got), err, len(data))
			}
		})
	}
}

func TestUploadInOneRequestKeepsS3sSmallestPart(t *testing.T) {
	if got := UploadInOneRequest(1).BufferSize; got != minUploadBufferBytes {
		t.Fatalf("buffer for a 1-byte object is %d, want S3's smallest part, %d", got, minUploadBufferBytes)
	}
	if got := UploadInOneRequest(96 << 20).BufferSize; got != 96<<20+1 {
		t.Fatalf("buffer for 96 MiB is %d", got)
	}
}
