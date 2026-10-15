package writerrole

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
	"github.com/ankur-anand/isledb/internal/writehttp"
)

// Role runs the writer.
var Role = service.Role{
	Name:      "writer",
	Summary:   "holds the writer fence for one database and serves writes over HTTP",
	NewConfig: NewConfig,
}

const (
	// writeSlack is a write's time to send its response beyond its wait for
	// the commit.
	writeSlack = 10 * time.Second
	// shutdownFlush bounds the commit of in-flight writes when shutdown
	// starts; drainTimeout and closeTimeout bound the final commit.
	shutdownFlush = 10 * time.Second
	drainTimeout  = 20 * time.Second
	closeTimeout  = 10 * time.Second
)

func (c *Config) Start(ctx context.Context, common service.Common, log *slog.Logger) (svc *service.Service, err error) {
	var closers []func() error
	closeAll := func() error {
		var errs []error
		for i := len(closers) - 1; i >= 0; i-- {
			errs = append(errs, closers[i]())
		}
		return errors.Join(errs...)
	}
	defer func() {
		if err != nil {
			_ = closeAll()
		}
	}()

	db, err := isledb.Open(ctx, common.BucketURL, isledb.DBOptions{
		Prefix:     c.prefix(),
		ChangeFeed: c.changeFeed(),
		Policy:     isledb.StorePolicy{MaxPinnedViewAge: time.Duration(c.Policy.MaxPinnedViewAge)},
	})
	if err != nil {
		return nil, err
	}
	closers = append(closers, db.Close)

	owner := c.OwnerID
	if owner == "" {
		if owner, err = os.Hostname(); err != nil {
			return nil, fmt.Errorf("owner_id is empty and the host name is unknown: %w", err)
		}
	}
	metrics, collectors := newWriterMetrics()
	opts := isledb.DefaultWriterOptions()
	opts.OwnerID = owner
	opts.Flush.Interval = time.Duration(c.Flush.Interval)
	opts.Memtable.TargetBytes = int64(c.Memtable.TargetBytes)
	opts.Memtable.MaxPendingMemtables = c.Memtable.MaxPending
	opts.Values.MaxKeyBytes = c.Values.MaxKeyBytes
	opts.Values.MaxValueBytes = int64(c.Values.MaxValueBytes)
	opts.Metrics = metrics
	opts.OnFlushError = func(err error) { log.Warn("commit failed; retrying", "error", err) }
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() error { return closeWriter(writer, log) })
	log.Info("writer open", "prefix", c.Prefix, "owner", owner, "epoch", writer.Epoch())

	handler, err := writehttp.NewHandler(writer, writehttp.Options{
		CommitWait:       time.Duration(c.Writes.CommitWait),
		MaxValueBytes:    int64(c.Values.MaxValueBytes),
		MaxBodyBytes:     int64(c.Writes.MaxRequestBytes),
		MaxInflightBytes: int64(c.Writes.MaxInflightBytes),
		BodyReadTimeout:  time.Duration(c.Writes.BodyReadTimeout),
	})
	if err != nil {
		return nil, err
	}

	return &service.Service{
		// No http.ServeMux in front: it would clean key paths.
		API:        service.WithWriteDeadline(handler, time.Duration(c.Writes.BodyReadTimeout+c.Writes.CommitWait)+writeSlack),
		Route:      writehttp.Route,
		Ready:      func() bool { return writer.State().Status == isledb.WriterOpen },
		Collectors: append(collectors, &collector{writer: writer, handler: handler}),
		OnShutdown: func() {
			// Refuse new writes, and commit the ones waiting so their
			// requests are answered before the server stops.
			writer.StopWrites()
			flushCtx, cancel := context.WithTimeout(context.Background(), shutdownFlush)
			defer cancel()
			if err := writer.Flush(flushCtx); err != nil {
				log.Warn("commit at shutdown", "error", err)
			}
		},
		Close: closeAll,
	}, nil
}

// closeWriter commits everything the writer accepted, then closes it.
func closeWriter(writer *isledb.Writer, log *slog.Logger) error {
	drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := writer.Drain(drainCtx); err != nil {
		log.Warn("drain", "error", err)
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), closeTimeout)
	defer cancelClose()
	return writer.Close(closeCtx)
}
