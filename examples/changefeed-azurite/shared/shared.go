package shared

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/isledb"
)

const (
	defaultAccount = "devstoreaccount1"
	defaultKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

func ConfigureEnvironment() error {
	for key, value := range map[string]string{
		"AZURE_STORAGE_ACCOUNT": defaultAccount,
		"AZURE_STORAGE_KEY":     defaultKey,
	} {
		if os.Getenv(key) == "" {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("set %s: %w", key, err)
			}
		}
	}
	return nil
}

func ContainerName() string {
	return Getenv("ISLEDB_AZBLOB_CONTAINER", "isledb")
}

func BucketURL(containerName string) string {
	return Getenv("ISLEDB_AZBLOB_URL", "azblob://"+containerName+"?protocol=http&domain=localhost:10000")
}

func DatabasePrefix() string {
	return Getenv("ISLEDB_PREFIX", "changefeed")
}

func Getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func EnsureContainer(ctx context.Context, name string) error {
	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var lastErr error
	for {
		if err := createContainer(startupCtx, name); err == nil {
			return nil
		} else {
			lastErr = err
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-timer.C:
		case <-startupCtx.Done():
			timer.Stop()
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("wait for Azurite: %w", lastErr)
		}
	}
}

func createContainer(ctx context.Context, name string) error {
	account := Getenv("AZURE_STORAGE_ACCOUNT", defaultAccount)
	key := Getenv("AZURE_STORAGE_KEY", defaultKey)
	endpoint := strings.TrimRight(Getenv("ISLEDB_AZURITE_ENDPOINT", "http://localhost:10000"), "/")

	credential, err := azblob.NewSharedKeyCredential(account, key)
	if err != nil {
		return fmt.Errorf("create Azurite credential: %w", err)
	}
	client, err := container.NewClientWithSharedKeyCredential(
		fmt.Sprintf("%s/%s/%s", endpoint, account, name), credential, nil)
	if err != nil {
		return fmt.Errorf("create container client: %w", err)
	}
	if _, err := client.Create(ctx, nil); err != nil {
		var responseErr *azcore.ResponseError
		if errors.As(err, &responseErr) && responseErr.ErrorCode == "ContainerAlreadyExists" {
			return nil
		}
		return fmt.Errorf("create container %q: %w", name, err)
	}
	return nil
}

func LoadCursor(path string) (isledb.ChangeCursor, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return isledb.ChangeCursor{}, nil
		}
		return isledb.ChangeCursor{}, fmt.Errorf("read cursor: %w", err)
	}
	cursor, err := isledb.ParseChangeCursor(strings.TrimSpace(string(data)))
	if err != nil {
		return isledb.ChangeCursor{}, fmt.Errorf("parse cursor: %w", err)
	}
	return cursor, nil
}

// SaveCursor atomically replaces the local checkpoint after a page has been
// processed. A crash leaves either the previous cursor or the complete new one.
func SaveCursor(path string, cursor isledb.ChangeCursor) (retErr error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create cursor directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".consumer-cursor-*")
	if err != nil {
		return fmt.Errorf("create temporary cursor: %w", err)
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, tmp.Close())
		}
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("set cursor permissions: %w", err)
	}
	if _, err := fmt.Fprintln(tmp, cursor.String()); err != nil {
		return fmt.Errorf("write cursor: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync cursor: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close cursor: %w", err)
	}
	closed = true
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace cursor: %w", err)
	}
	return nil
}
