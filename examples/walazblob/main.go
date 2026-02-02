package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
)

// This example can be run with the local azure emulator
// https://github.com/Azure/Azurite
// docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite

// https://github.com/Azure/Azurite?tab=readme-ov-file#usage-with-azure-storage-sdks-or-tools
// Default Storage Account
// Azurite V3 provides support for a default storage account as General Storage Account V2 and associated features.
//
// Account name: devstoreaccount1
// Account key: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
const azuriteDefaultKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func ensureAzuriteEnv() {
	if os.Getenv("AZURE_STORAGE_ACCOUNT") == "" {
		_ = os.Setenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
	}
	if os.Getenv("AZURE_STORAGE_KEY") == "" {
		_ = os.Setenv("AZURE_STORAGE_KEY", azuriteDefaultKey)
	}
}

func ensureAzuriteContainer(ctx context.Context, name string) error {
	account := getenvDefault("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
	key := getenvDefault("AZURE_STORAGE_KEY", azuriteDefaultKey)

	cred, err := azblob.NewSharedKeyCredential(account, key)
	if err != nil {
		return fmt.Errorf("create azurite credential: %w", err)
	}

	containerURL := fmt.Sprintf("http://localhost:10000/%s/%s", account, name)
	client, err := container.NewClientWithSharedKeyCredential(containerURL, cred, nil)
	if err != nil {
		return fmt.Errorf("create container client: %w", err)
	}

	_, err = client.Create(ctx, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.ErrorCode == "ContainerAlreadyExists" {
			return nil
		}
		return fmt.Errorf("create container: %w", err)
	}
	return nil
}

func main() {
	ctx := context.Background()

	container := getenvDefault("ISLEDB_AZBLOB_CONTAINER", "isledb")
	prefix := "db1"

	bucketURL := os.Getenv("ISLEDB_AZBLOB_URL")
	if bucketURL == "" {
		ensureAzuriteEnv()
		if err := ensureAzuriteContainer(ctx, container); err != nil {
			log.Fatal(err)
		}
		bucketURL = "azblob://" + container + "?protocol=http&domain=localhost:10000"
	}

	store, err := blobstore.Open(ctx, bucketURL, prefix)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	cacheDir, err := os.MkdirTemp("", "isledb-cache-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(cacheDir)

	db, err := isledb.OpenDB(ctx, store, isledb.DBOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	walPrefix := "wal/"
	walMax := prefixUpperBound(walPrefix)
	walSeq := 0
	checkpointPath := filepath.Join(cacheDir, "wal_checkpoint")
	lastKey, err := loadLastKey(checkpointPath)
	if err != nil {
		log.Fatal(err)
	}

	writer, err := db.OpenWriter(ctx, isledb.DefaultWriterOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	for i := 0; i < 10; i++ {
		dataKey := fmt.Sprintf("data/%03d", i)
		value := fmt.Sprintf("value-%03d", i)
		if err := writer.Put([]byte(dataKey), []byte(value)); err != nil {
			log.Fatal(err)
		}
		if err := writeWALEvent(writer, walPrefix, &walSeq, "put", dataKey); err != nil {
			log.Fatal(err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		log.Fatal(err)
	}

	reader, err := isledb.OpenReader(ctx, store, isledb.ReaderOpenOptions{
		CacheDir: cacheDir,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	value, ok, err := reader.Get(ctx, []byte("data/000"))
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		log.Printf("read: data/000=%s", value)
	}

	tr, err := isledb.OpenTailingReader(ctx, store, isledb.TailingReaderOpenOptions{
		RefreshInterval: 200 * time.Millisecond,
		ReaderOptions: isledb.ReaderOpenOptions{
			CacheDir: cacheDir,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer tr.Close()

	if err := tr.Start(); err != nil {
		log.Fatal(err)
	}
	tailCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- tr.Tail(tailCtx, isledb.TailOptions{
			MinKey:        []byte(walPrefix),
			MaxKey:        walMax,
			StartAfterKey: lastKey,
			PollInterval:  200 * time.Millisecond,
		}, func(kv isledb.KV) error {
			log.Printf("tail: %s=%s", kv.Key, kv.Value)
			return saveLastKey(checkpointPath, kv.Key)
		})
	}()

	if err := writer.Put([]byte("data/tail"), []byte("tail-value")); err != nil {
		log.Fatal(err)
	}
	if err := writeWALEvent(writer, walPrefix, &walSeq, "put", "data/tail"); err != nil {
		log.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	batch := 0
	for time.Since(start) < time.Second {
		for i := 0; i < 5; i++ {
			dataKey := fmt.Sprintf("data/burst-%03d-%03d", batch, i)
			value := fmt.Sprintf("value-%03d-%03d", batch, i)
			if err := writer.Put([]byte(dataKey), []byte(value)); err != nil {
				log.Fatal(err)
			}
			if err := writeWALEvent(writer, walPrefix, &walSeq, "put", dataKey); err != nil {
				log.Fatal(err)
			}
		}
		batch++
		time.Sleep(200 * time.Millisecond)
	}
	if err := writer.Flush(ctx); err != nil {
		log.Fatal(err)
	}

	if err := writer.Delete([]byte("data/005")); err != nil {
		log.Fatal(err)
	}
	if err := writeWALEvent(writer, walPrefix, &walSeq, "delete", "data/005"); err != nil {
		log.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		log.Fatal(err)
	}

	if err := <-done; err != nil && !errors.Is(err, context.DeadlineExceeded) {
		log.Fatal(err)
	}

	if err := reader.Refresh(ctx); err != nil {
		log.Fatal(err)
	}
	live, err := reader.Scan(ctx, []byte("data/"), prefixUpperBound("data/"))
	if err != nil {
		log.Fatal(err)
	}
	for _, kv := range live {
		log.Printf("live Key=Value: %s=%s", kv.Key, kv.Value)
	}
}

func getenvDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

type walEvent struct {
	Key  string `json:"key"`
	Op   string `json:"op"`
	Time string `json:"time"`
}

func writeWALEvent(writer *isledb.Writer, prefix string, seq *int, op, key string) error {
	*seq = *seq + 1
	event := walEvent{
		Key:  key,
		Op:   op,
		Time: time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	walKey := fmt.Sprintf("%s%s-%06d", prefix, time.Now().UTC().Format("20060102T150405.000000000Z"), *seq)
	return writer.Put([]byte(walKey), data)
}

func prefixUpperBound(prefix string) []byte {
	if prefix == "" {
		return nil
	}

	out := []byte(prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}

	return append(out, 0x00)
}

func loadLastKey(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func saveLastKey(path string, key []byte) error {
	if len(key) == 0 {
		return nil
	}
	return os.WriteFile(path, key, 0644)
}
