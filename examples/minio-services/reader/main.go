package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/examples/minio-services/shared"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (retErr error) {
	var (
		bucketURL    = flag.String("bucket-url", shared.BucketURL(), "S3/MinIO bucket URL")
		prefix       = flag.String("prefix", shared.DatabasePrefix(), "IsleDB prefix")
		cacheDir     = flag.String("cache-dir", filepath.Join(os.TempDir(), "isledb-minio-reader-cache"), "reader cache directory")
		limit        = flag.Int("limit", 20, "maximum accounts per query")
		pollInterval = flag.Duration("poll-interval", time.Second, "delay between queries")
		once         = flag.Bool("once", false, "run one query and exit")
	)
	flag.Parse()
	if *limit <= 0 || *pollInterval <= 0 {
		return errors.New("limit and poll-interval must be > 0")
	}
	if err := shared.ConfigureEnvironment(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := isledb.Open(ctx, *bucketURL, isledb.DBOptions{Prefix: *prefix})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()

	reader, err := db.OpenReader(ctx, isledb.DefaultReaderOpenOptions(*cacheDir))
	if err != nil {
		return fmt.Errorf("open reader: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, reader.Close()) }()

	log.Printf("reader started bucket_url=%s prefix=%s", *bucketURL, *prefix)
	for {
		if err := query(ctx, reader, *limit); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if *once {
			return nil
		}

		timer := time.NewTimer(*pollInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			log.Printf("reader stopped")
			return nil
		}
	}
}

func query(ctx context.Context, reader *isledb.Reader, limit int) error {
	if err := reader.Refresh(ctx); err != nil {
		return fmt.Errorf("refresh reader: %w", err)
	}
	keyRange := isledb.PrefixRange([]byte("accounts/"))
	rows, err := reader.ScanLimit(ctx, keyRange.Min, keyRange.Max, limit)
	if err != nil {
		return fmt.Errorf("scan accounts: %w", err)
	}
	log.Printf("query returned %d accounts", len(rows))
	for i, row := range rows {
		if i == 3 {
			log.Printf("... %d more", len(rows)-i)
			break
		}
		account, err := shared.DecodeAccount(row.Value)
		if err != nil {
			return fmt.Errorf("decode %q: %w", row.Key, err)
		}
		log.Printf("account id=%d revision=%d updated_at=%s", account.ID, account.Revision, account.UpdatedAt.Format(time.RFC3339Nano))
	}
	return nil
}
