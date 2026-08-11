package main

import (
	"context"
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
		bucketURL = flag.String("bucket-url", shared.BucketURL(), "S3/MinIO bucket URL")
		prefix    = flag.String("prefix", shared.DatabasePrefix(), "IsleDB prefix")
	)
	flag.Parse()
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

	opts := isledb.DefaultMaintenanceOptions()
	opts.OnError = func(err error) { log.Printf("maintenance cycle failed: %v", err) }
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		return fmt.Errorf("open maintenance: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, maintenance.Close(closeCtx))
	}()

	log.Printf("maintenance started bucket_url=%s prefix=%s", *bucketURL, *prefix)
	err = maintenance.Run(ctx)
	if errors.Is(err, context.Canceled) {
		log.Printf("maintenance stopped")
		return nil
	}
	if err != nil {
		return fmt.Errorf("run maintenance: %w", err)
	}
	return nil
}
