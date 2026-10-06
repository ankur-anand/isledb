package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
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
		grace        = flag.Duration("grace", 25*time.Second, "time to commit pending writes on shutdown")
		readyAddr    = flag.String("ready-addr", "localhost:8081", "address serving /ready; empty disables it")
	)
	flag.Parse()
	if *count < 0 || *accountCount <= 0 || *flushEvery <= 0 || *interval < 0 || *grace <= 0 {
		return errors.New("count must be >= 0, accounts, flush-every and grace must be > 0, and interval must be >= 0")
	}
	if err := shared.ConfigureEnvironment(); err != nil {
		return err
	}

	// ctx ends on Ctrl-C or SIGTERM: the signal to stop taking work.
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
	// Shutdown runs however the loop ends, before db.Close.
	defer func() { retErr = errors.Join(retErr, shutdown(writer, *grace)) }()

	if *readyAddr != "" {
		server := &http.Server{Addr: *readyAddr, Handler: readiness(writer), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("readiness server: %v", err)
			}
		}()
		defer func() { _ = server.Close() }()
		log.Printf("readiness on http://%s/ready", *readyAddr)
	}

	log.Printf("writer started bucket_url=%s prefix=%s", *bucketURL, *prefix)
	var revision uint64
	pending := 0
	for *count == 0 || revision < uint64(*count) {
		if ctx.Err() != nil {
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
		if _, err := writer.Put(ctx, shared.AccountKey(id), value); err != nil {
			return fmt.Errorf("put account %d: %w", id, err)
		}
		pending++
		if pending == *flushEvery {
			switch err := writer.Flush(ctx); {
			case err == nil:
				log.Printf("committed through revision=%d", revision)
				pending = 0
			case ctx.Err() != nil:
				// Interrupted: shutdown commits what is pending.
			case errors.Is(err, isledb.ErrFenced):
				return fmt.Errorf("another writer took over: %w", err)
			default:
				// The writes stay queued; the next Flush or the shutdown
				// drain retries them.
				log.Printf("flush failed, will retry: %v", err)
			}
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
	log.Printf("writer completed %d updates", revision)
	return nil
}

// shutdown stops the writer within grace, the way a service should on
// SIGTERM: Drain stops new writes at once (readiness turns 503) and commits
// everything accepted, retrying until most of grace is spent; Close then
// finishes the writer with the rest. Close's error names any writes it could
// not confirm.
func shutdown(writer *isledb.Writer, grace time.Duration) error {
	deadline := time.Now().Add(grace)
	state := writer.State()
	log.Printf("shutting down: %d writes pending (%d bytes), grace %s",
		state.Accepted-state.Committed, state.PendingBytes, grace)

	drainCtx, cancel := context.WithDeadline(context.Background(), deadline.Add(-grace/5))
	defer cancel()
	if err := writer.Drain(drainCtx); err != nil {
		log.Printf("drain: %v", err)
	} else {
		log.Printf("drained: everything through sequence %d is committed", writer.CommittedSequence())
	}

	closeCtx, cancelClose := context.WithDeadline(context.Background(), deadline)
	defer cancelClose()
	if err := writer.Close(closeCtx); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	log.Printf("writer closed")
	return nil
}

// readiness serves the writer's State: 200 while it takes writes, 503 from
// the moment shutdown starts, so a load balancer stops sending work before
// writes are refused.
func readiness(writer *isledb.Writer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		state := writer.State()
		w.Header().Set("Content-Type", "application/json")
		if state.Status != isledb.WriterOpen {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":             state.Status.String(),
			"accepted":           state.Accepted,
			"committed":          state.Committed,
			"pending_memtables":  state.PendingMemtables,
			"pending_bytes":      state.PendingBytes,
			"oldest_uncommitted": state.OldestUncommitted,
		})
	})
}
