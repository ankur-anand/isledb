//go:build integration && azure

package storageengine

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	isledb "github.com/ankur-anand/isledb"
)

const (
	azuriteDefaultAccount = "devstoreaccount1"
	azuriteDefaultKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azuriteDefaultURL     = "http://127.0.0.1:10000"
)

func TestAzuriteStorageEngineProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	account := storageAzureEnvOrDefault("AZURE_STORAGE_ACCOUNT", azuriteDefaultAccount)
	key := storageAzureEnvOrDefault("AZURE_STORAGE_KEY", azuriteDefaultKey)
	endpoint := strings.TrimRight(
		storageAzureEnvOrDefault("ISLEDB_AZURITE_ENDPOINT", azuriteDefaultURL), "/")
	t.Setenv("AZURE_STORAGE_ACCOUNT", account)
	t.Setenv("AZURE_STORAGE_KEY", key)

	parsedEndpoint, err := url.Parse(endpoint)
	if err != nil || parsedEndpoint.Scheme == "" || parsedEndpoint.Host == "" {
		t.Fatalf("invalid Azurite endpoint %q: %v", endpoint, err)
	}
	connection := fmt.Sprintf(
		"DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;",
		parsedEndpoint.Scheme, account, key, endpoint, account)
	client, err := azblob.NewClientFromConnectionString(connection, nil)
	if err != nil {
		t.Fatalf("create Azurite client: %v", err)
	}
	container := fmt.Sprintf("isledbintegration%d", time.Now().UnixNano())
	if err := waitForAzuriteContainer(ctx, client, endpoint, container); err != nil {
		t.Fatalf("prepare Azurite container: %v", err)
	}

	bucketURL := &url.URL{Scheme: "azblob", Host: container}
	query := bucketURL.Query()
	query.Set("protocol", parsedEndpoint.Scheme)
	query.Set("domain", parsedEndpoint.Host)
	query.Set("localemu", "true")
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

func waitForAzuriteContainer(ctx context.Context, client *azblob.Client, endpoint, container string) error {
	var lastErr error
	for {
		_, err := client.CreateContainer(ctx, container, nil)
		if err == nil {
			return nil
		}
		lastErr = err
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			stopStorageProviderTimer(timer)
			return fmt.Errorf("wait for Azurite at %s: %w", endpoint, lastErr)
		}
	}
}

func storageAzureEnvOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
