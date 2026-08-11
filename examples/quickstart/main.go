package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/ankur-anand/isledb"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (retErr error) {
	var (
		dataDir  = flag.String("data-dir", filepath.Join(os.TempDir(), "isledb-quickstart-data"), "object-store directory")
		cacheDir = flag.String("cache-dir", filepath.Join(os.TempDir(), "isledb-quickstart-cache"), "reader cache directory")
	)
	flag.Parse()

	ctx := context.Background()
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	absDataDir, err := filepath.Abs(*dataDir)
	if err != nil {
		return fmt.Errorf("resolve data directory: %w", err)
	}

	db, err := isledb.Open(ctx, "file://"+absDataDir, isledb.DBOptions{Prefix: "quickstart"})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()

	writerOpts := isledb.DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		return fmt.Errorf("open writer: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, writer.Close(ctx)) }()

	for i, name := range []string{"Ada", "Grace", "Linus"} {
		key := fmt.Appendf(nil, "accounts/%03d", i+1)
		if err := writer.Put(ctx, key, []byte(name)); err != nil {
			return fmt.Errorf("put %q: %w", key, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		return fmt.Errorf("flush writes: %w", err)
	}

	reader, err := db.OpenReader(ctx, isledb.DefaultReaderOpenOptions(*cacheDir))
	if err != nil {
		return fmt.Errorf("open reader: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, reader.Close()) }()

	value, found, err := reader.Get(ctx, []byte("accounts/001"))
	if err != nil {
		return fmt.Errorf("get account: %w", err)
	}
	if !found {
		return errors.New("accounts/001 was not found")
	}
	log.Printf("get accounts/001=%s", value)

	keyRange := isledb.PrefixRange([]byte("accounts/"))
	rows, err := reader.ScanLimit(ctx, keyRange.Min, keyRange.Max, 100)
	if err != nil {
		return fmt.Errorf("scan accounts: %w", err)
	}
	for _, row := range rows {
		log.Printf("scan %s=%s", row.Key, row.Value)
	}
	return nil
}
