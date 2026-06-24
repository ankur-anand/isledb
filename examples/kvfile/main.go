package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "isledb-*")
	if err != nil {
		log.Fatal(err)
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		log.Fatal(err)
	}
	store, err := blobstore.Open(ctx, "file://"+absDir, "db1")
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
	defer writer.Close(ctx)

	if err := writer.Put(ctx, []byte("hello"), []byte("world")); err != nil {
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

	for i := 0; i < 10; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		value := []byte("value-" + strconv.Itoa(i))
		if err := writer.Put(ctx, key, value); err != nil {
			log.Fatal(err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		log.Fatal(err)
	}

	if err := reader.Refresh(ctx); err != nil {
		log.Fatal(err)
	}
	rows, err := reader.ScanLimit(ctx, []byte("key-"), []byte("key."), 20)
	if err != nil {
		log.Fatal(err)
	}
	for _, kv := range rows {
		log.Printf("scan: %s=%s", kv.Key, kv.Value)
	}
}
