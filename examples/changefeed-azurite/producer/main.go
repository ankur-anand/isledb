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
	"github.com/ankur-anand/isledb/examples/changefeed-azurite/shared"
)

type customer struct {
	ID        int       `json:"id"`
	Revision  uint64    `json:"revision"`
	UpdatedAt time.Time `json:"updated_at"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (retErr error) {
	containerName := shared.ContainerName()
	var (
		bucketURL       = flag.String("bucket-url", "", "Azure Blob/Azurite bucket URL; defaults to the selected container")
		containerFlag   = flag.String("container", containerName, "Azurite container to create")
		prefix          = flag.String("prefix", shared.DatabasePrefix(), "IsleDB prefix")
		count           = flag.Int("count", 100, "puts to write; 0 runs until interrupted")
		customerCount   = flag.Int("customers", 25, "number of customer keys to update")
		flushEvery      = flag.Int("flush-every", 20, "commit after this many mutations")
		interval        = flag.Duration("interval", 100*time.Millisecond, "delay between puts")
		ensureContainer = flag.Bool("ensure-container", true, "create the local Azurite container if missing")
	)
	flag.Parse()
	if *bucketURL == "" {
		*bucketURL = shared.BucketURL(*containerFlag)
	}
	if *count < 0 || *customerCount <= 0 || *flushEvery <= 0 || *interval < 0 {
		return errors.New("count must be >= 0, customers and flush-every must be > 0, and interval must be >= 0")
	}
	if err := shared.ConfigureEnvironment(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *ensureContainer {
		if err := shared.EnsureContainer(ctx, *containerFlag); err != nil {
			return err
		}
	}

	db, err := isledb.Open(ctx, *bucketURL, isledb.DBOptions{
		Prefix: *prefix,
		ChangeFeed: &isledb.ChangeFeedOptions{
			Payload: isledb.ChangeFeedFullValues,
		},
	})
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

	log.Printf("producer started bucket_url=%s prefix=%s payload=%s", *bucketURL, *prefix, isledb.ChangeFeedFullValues)
	var revision uint64
	pending := 0
	for *count == 0 || revision < uint64(*count) {
		if err := ctx.Err(); err != nil {
			log.Printf("producer stopping after %d puts", revision)
			return nil
		}

		revision++
		id := int((revision-1)%uint64(*customerCount)) + 1
		value, err := json.Marshal(customer{ID: id, Revision: revision, UpdatedAt: time.Now().UTC()})
		if err != nil {
			return fmt.Errorf("encode customer: %w", err)
		}
		key := fmt.Appendf(nil, "customers/%06d", id)
		if err := writer.Put(ctx, key, value); err != nil {
			return fmt.Errorf("put customer %d: %w", id, err)
		}
		pending++

		if revision%10 == 0 {
			deleteID := int((revision/10-1)%uint64(*customerCount)) + 1
			if err := writer.Delete(ctx, fmt.Appendf(nil, "customers/%06d", deleteID)); err != nil {
				return fmt.Errorf("delete customer %d: %w", deleteID, err)
			}
			pending++
		}
		if pending >= *flushEvery {
			if err := writer.Flush(ctx); err != nil {
				return fmt.Errorf("flush mutations: %w", err)
			}
			log.Printf("committed through producer revision=%d", revision)
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
			return fmt.Errorf("flush final mutations: %w", err)
		}
	}
	log.Printf("producer completed %d puts", revision)
	return nil
}
