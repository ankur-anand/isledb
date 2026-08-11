package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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
		count        = flag.Int("count", 0, "updates to write; 0 runs until interrupted")
		accountCount = flag.Int("accounts", 100, "number of account keys to update")
		flushEvery   = flag.Int("flush-every", 25, "commit after this many updates")
		interval     = flag.Duration("interval", 100*time.Millisecond, "delay between updates")
	)
	flag.Parse()
	if *count < 0 || *accountCount <= 0 || *flushEvery <= 0 || *interval < 0 {
		return errors.New("count must be >= 0, accounts and flush-every must be > 0, and interval must be >= 0")
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

	writerOpts := isledb.DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		return fmt.Errorf("open writer: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, writer.Close(closeCtx))
	}()

	log.Printf("writer started bucket_url=%s prefix=%s", *bucketURL, *prefix)
	var revision uint64
	pending := 0
	for *count == 0 || revision < uint64(*count) {
		if err := ctx.Err(); err != nil {
			log.Printf("writer stopping after %d updates", revision)
			return nil
		}

		revision++
		id := int((revision-1)%uint64(*accountCount)) + 1
		account := shared.Account{
			ID:        id,
			Name:      fmt.Sprintf("account-%06d", id),
			Revision:  revision,
			UpdatedAt: time.Now().UTC(),
		}
		value, err := json.Marshal(account)
		if err != nil {
			return fmt.Errorf("encode account: %w", err)
		}
		if err := writer.Put(ctx, shared.AccountKey(id), value); err != nil {
			return fmt.Errorf("put account %d: %w", id, err)
		}
		pending++
		if pending == *flushEvery {
			if err := writer.Flush(ctx); err != nil {
				return fmt.Errorf("flush accounts: %w", err)
			}
			log.Printf("committed through revision=%d", revision)
			pending = 0
		}

		if *interval > 0 {
			timer := time.NewTimer(*interval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		}
	}

	if pending > 0 {
		if err := writer.Flush(ctx); err != nil {
			return fmt.Errorf("flush final accounts: %w", err)
		}
	}
	log.Printf("writer completed %d updates", revision)
	return nil
}
