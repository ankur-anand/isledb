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
	"github.com/ankur-anand/isledb/examples/changefeed-azurite/shared"
)

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
		cursorFile      = flag.String("cursor-file", filepath.Join(os.TempDir(), "isledb-changefeed-azurite", "consumer.cursor"), "durable consumer cursor file")
		startAt         = flag.String("start", "oldest", "initial position: oldest or head")
		onExpired       = flag.String("on-expired", "fail", "expired cursor policy: fail or oldest")
		maxChanges      = flag.Int("max-changes", 128, "maximum changes in one read")
		maxBytes        = flag.Int64("max-bytes", 4<<20, "approximate value bytes in one read")
		pollInterval    = flag.Duration("poll-interval", 500*time.Millisecond, "delay after catching up")
		once            = flag.Bool("once", false, "exit after reaching the current feed head")
		ensureContainer = flag.Bool("ensure-container", true, "create the local Azurite container if missing")
	)
	flag.Parse()
	if *bucketURL == "" {
		*bucketURL = shared.BucketURL(*containerFlag)
	}
	if (*startAt != "oldest" && *startAt != "head") || (*onExpired != "fail" && *onExpired != "oldest") {
		return errors.New("start must be oldest or head, and on-expired must be fail or oldest")
	}
	if *maxChanges <= 0 || *maxBytes <= 0 || *pollInterval <= 0 {
		return errors.New("max-changes, max-bytes, and poll-interval must be > 0")
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
	db, err := isledb.Open(ctx, *bucketURL, isledb.DBOptions{Prefix: *prefix})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()

	reader, err := db.OpenChangeReader(ctx)
	if errors.Is(err, isledb.ErrChangeFeedDisabled) {
		return errors.New("change feed is disabled; start the producer once before starting this consumer")
	}
	if err != nil {
		return fmt.Errorf("open change reader: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, reader.Close()) }()

	cursor, err := shared.LoadCursor(*cursorFile)
	if err != nil {
		return err
	}
	if cursor.IsZero() {
		bounds, err := reader.Bounds(ctx)
		if err != nil {
			return fmt.Errorf("load change-feed bounds: %w", err)
		}
		cursor = bounds.Oldest
		if *startAt == "head" {
			cursor = bounds.Head
		}
		if err := shared.SaveCursor(*cursorFile, cursor); err != nil {
			return err
		}
	}

	readOpts := isledb.ChangeReadOptions{MaxChanges: *maxChanges, MaxBytes: *maxBytes}
	log.Printf("consumer started bucket_url=%s prefix=%s cursor=%s", *bucketURL, *prefix, cursor.String())
	for {
		page, err := reader.Read(ctx, cursor, readOpts)
		if errors.Is(err, context.Canceled) {
			log.Printf("consumer stopped")
			return nil
		}
		if errors.Is(err, isledb.ErrChangeCursorExpired) {
			if *onExpired == "fail" {
				return fmt.Errorf("cursor %q expired; choose a recovery policy or restart with -on-expired=oldest: %w", cursor.String(), err)
			}
			bounds, boundsErr := reader.Bounds(ctx)
			if boundsErr != nil {
				return fmt.Errorf("reload bounds after cursor expiry: %w", boundsErr)
			}
			cursor = bounds.Oldest
			if err := shared.SaveCursor(*cursorFile, cursor); err != nil {
				return err
			}
			log.Printf("expired cursor reset to oldest=%s", cursor.String())
			continue
		}
		if err != nil {
			return fmt.Errorf("read changes: %w", err)
		}

		for _, change := range page.Changes {
			if change.HasValue {
				log.Printf("change sequence=%d operation=%s key=%s value=%s", change.Sequence, change.Operation, change.Key, change.Value)
			} else {
				log.Printf("change sequence=%d operation=%s key=%s", change.Sequence, change.Operation, change.Key)
			}
		}
		// Advance only after every change in the page has been processed.
		if page.Next != cursor {
			if err := shared.SaveCursor(*cursorFile, page.Next); err != nil {
				return err
			}
		}
		cursor = page.Next
		if !page.CaughtUp() {
			continue
		}
		if *once {
			log.Printf("consumer caught up at cursor=%s", cursor.String())
			return nil
		}

		timer := time.NewTimer(*pollInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			log.Printf("consumer stopped")
			return nil
		}
	}
}
