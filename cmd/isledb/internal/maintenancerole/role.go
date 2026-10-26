package maintenancerole

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
)

// Role runs maintenance.
var Role = service.Role{
	Name:      "maintenance",
	Summary:   "compacts one database and deletes what it no longer needs; run one per database",
	NewConfig: NewConfig,
}

// closeTimeout bounds stopping a cycle in progress at shutdown.
const closeTimeout = 30 * time.Second

// shapeEvery is how often the L0 and level metrics are read from the
// manifest: often enough to watch L0 grow, while a long compaction runs too.
var shapeEvery = 10 * time.Second

// watchShape keeps the L0 and level metrics current until ctx ends. A
// failed read keeps the last values; shape_timestamp_seconds shows their age.
func watchShape(ctx context.Context, maintenance *isledb.Maintenance, m *metrics, log *slog.Logger) {
	failing := false
	for {
		shape, err := maintenance.Shape(ctx)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			if !failing {
				log.Warn("reading the L0 and level metrics failed; retrying", "error", err)
			}
			failing = true
		default:
			failing = false
			m.shape(shape, time.Now())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(shapeEvery):
		}
	}
}

func (c *Config) Start(ctx context.Context, common service.Common, log *slog.Logger) (svc *service.Service, err error) {
	db, err := isledb.Open(ctx, common.BucketURL, isledb.DBOptions{Prefix: c.prefix()})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	m := newMetrics()
	opts := c.options()
	opts.OnCycle = m.cycle
	opts.OnReclamationCycle = m.reclamation
	opts.OnError = func(err error) {
		m.cycleErrors.Inc()
		log.Warn("maintenance cycle failed; retrying", "error", err)
	}
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		return nil, err
	}
	log.Info("maintenance open", "prefix", c.Prefix, "change_feed_retain_for", time.Duration(c.ChangeFeed.RetainFor))

	// Run returns only when maintenance closes, or when another process
	// takes the maintenance fence: this one then stays up, not ready, so it
	// does not take the fence back.
	var running atomic.Bool
	running.Store(true)
	m.running.Set(1)
	shapeCtx, stopShape := context.WithCancel(context.Background())
	shapeDone := make(chan struct{})
	go func() {
		defer close(shapeDone)
		watchShape(shapeCtx, maintenance, m, log)
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := maintenance.Run(context.Background())
		running.Store(false)
		m.running.Set(0)
		switch {
		case errors.Is(err, isledb.ErrFenced):
			log.Error("another maintenance process took over this database; this one stays not ready and does no more work", "error", err)
		case err != nil:
			log.Error("maintenance stopped", "error", err)
		}
	}()

	return &service.Service{
		Ready:      running.Load,
		Collectors: m.collectors(),
		Close: func() error {
			stopShape()
			<-shapeDone
			closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			defer cancel()
			if err := maintenance.Close(closeCtx); err != nil {
				// The process is exiting: closing the database would wait
				// for maintenance again, and a round stopped partway is
				// safe.
				return err
			}
			<-done
			return db.Close()
		},
	}, nil
}
