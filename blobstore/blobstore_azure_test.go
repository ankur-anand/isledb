//go:build integration && azure

package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
)

// docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite
// go test ./blobstore/... -tags="integration,azure" -v -run TestAzure 2>&1

const (
	azuriteAccount   = "devstoreaccount1"
	azuriteKey       = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azuriteConnStr   = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
	azuriteContainer = "testcontainer"
	azuriteDomain    = "127.0.0.1:10000"
)

func azureBucketURL() string {
	return fmt.Sprintf("azblob://%s?protocol=http&domain=%s&localemu=true", azuriteContainer, azuriteDomain)
}

func ensureAzureContainer(t *testing.T) {
	t.Helper()

	os.Setenv("AZURE_STORAGE_ACCOUNT", azuriteAccount)
	os.Setenv("AZURE_STORAGE_KEY", azuriteKey)

	client, err := azblob.NewClientFromConnectionString(azuriteConnStr, nil)
	if err != nil {
		t.Fatalf("failed to create azure client: %v", err)
	}

	ctx := context.Background()
	_, err = client.CreateContainer(ctx, azuriteContainer, nil)
	if err != nil {
		t.Logf("create container (may already exist): %v", err)
	}
}

func TestAzure_WriteIfMatch(t *testing.T) {
	ensureAzureContainer(t)
	ctx := context.Background()

	store, err := Open(ctx, azureBucketURL(), "test-prefix")
	if err != nil {
		t.Fatalf("failed to open azure store: %v", err)
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

func TestAzure_WriteIfNotExist(t *testing.T) {
	ensureAzureContainer(t)
	ctx := context.Background()

	store, err := Open(ctx, azureBucketURL(), "test-prefix")
	if err != nil {
		t.Fatalf("failed to open azure store: %v", err)
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

func TestAzure_WriteIfMatch_ConcurrentWriters(t *testing.T) {
	ensureAzureContainer(t)
	ctx := context.Background()

	store, err := Open(ctx, azureBucketURL(), "test-prefix")
	if err != nil {
		t.Fatalf("failed to open azure store: %v", err)
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

func TestAzure_UploadInOneRequestSendsOneRequest(t *testing.T) {
	ensureAzureContainer(t)
	ctx := context.Background()

	// Record the write requests Azurite is sent: Put Blob has no comp
	// parameter, Put Block is comp=block and Put Block List comp=blocklist.
	var mu sync.Mutex
	var puts []string
	target, _ := url.Parse("http://" + azuriteDomain)
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "uploads") {
			mu.Lock()
			puts = append(puts, r.URL.Query().Get("comp"))
			mu.Unlock()
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()

	bucketURL := fmt.Sprintf("azblob://%s?protocol=http&domain=%s&localemu=true",
		azuriteContainer, strings.TrimPrefix(server.URL, "http://"))
	store, err := Open(ctx, bucketURL, "uploads")
	if err != nil {
		t.Fatalf("failed to open azure store: %v", err)
	}
	defer store.Close()

	big := bytes.Repeat([]byte("0123456789abcdef"), 12<<20/16) // 12 MiB
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	for _, c := range []struct {
		name string
		data []byte
		opts *blob.WriterOptions
		want []string
	}{
		{"default blocks", big, nil, []string{"block", "block", "blocklist"}},
		{"one request", big, UploadInOneRequest(int64(len(big))), []string{""}},
		{"small default", []byte(`{"v":1}`), nil, []string{""}},
	} {
		t.Run(c.name, func(t *testing.T) {
			mu.Lock()
			puts = nil
			mu.Unlock()
			key := store.SSTPath(c.name + "-" + run)
			if _, err := store.WriteReader(ctx, key, bytes.NewReader(c.data), c.opts); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			got := append([]string(nil), puts...)
			mu.Unlock()
			if !slices.Equal(got, c.want) {
				t.Fatalf("write requests (comp=) %q, want %q", got, c.want)
			}
			read, _, err := store.Read(ctx, key)
			if err != nil || !bytes.Equal(read, c.data) {
				t.Fatalf("read back %d bytes, err %v; want the %d written", len(read), err, len(c.data))
			}
			_ = store.Delete(ctx, key)
		})
	}
}
