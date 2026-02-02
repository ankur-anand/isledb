package main

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
)

func main() {
	ctx := context.Background()

	store, dir, err := blobstore.NewFileTemp("db1")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	defer store.Close()

	cacheDir := filepath.Join(dir, "cache")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Fatal(err)
	}

	db, err := isledb.OpenDB(ctx, store, isledb.DBOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	writer, err := db.OpenWriter(ctx, isledb.DefaultWriterOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	if err := writer.Put([]byte("hello"), []byte("world")); err != nil {
		log.Fatal(err)
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

	value, ok, err := reader.Get(ctx, []byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		log.Printf("read: hello=%s", value)
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
			PollInterval: 200 * time.Millisecond,
		}, func(kv isledb.KV) error {
			log.Printf("tail: %s=%s", kv.Key, kv.Value)
			return nil
		})
	}()

	for i := 0; i < 10; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		value := []byte("value-" + strconv.Itoa(i))
		if err := writer.Put(key, value); err != nil {
			log.Fatal(err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		log.Fatal(err)
	}

	if err := <-done; err != nil && !errors.Is(err, context.DeadlineExceeded) {
		log.Fatal(err)
	}
}
