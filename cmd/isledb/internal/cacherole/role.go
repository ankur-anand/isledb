package cacherole

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/ankur-anand/isledb/internal/readcache"
	"github.com/prometheus/client_golang/prometheus"
)

// Role runs the cache.
var Role = service.Role{
	Name:      "cache",
	Summary:   "regional read cache: relays CURRENT and serves immutable objects",
	NewConfig: NewConfig,
}

// Write deadlines beyond the waits a request can make: a long poll's hold,
// or an object's wait for read budget and its storage read.
const (
	longPollWriteSlack = 10 * time.Second
	storageReadTimeout = 30 * time.Second
	objectWriteSlack   = 5 * time.Minute
)

func (c *Config) Start(_ context.Context, common service.Common, log *slog.Logger) (svc *service.Service, err error) {
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

	store, err := blobstore.Open(context.Background(), common.BucketURL, "")
	if err != nil {
		return nil, err
	}
	closers = append(closers, store.Close)
	disk, err := diskcache.Open(diskcache.Options{Dir: c.Disk.Dir, MaxBytes: int64(c.Disk.Bytes)})
	if err != nil {
		return nil, err
	}
	closers = append(closers, disk.Close)
	objects, err := readcache.NewObjects(store, disk, readcache.ObjectsOptions{
		MaxReadBytes: int64(c.Reads.MaxBytes),
		MaxReadWait:  time.Duration(c.Reads.MaxWait),
	})
	if err != nil {
		return nil, err
	}

	relayCtx, stopRelays := context.WithCancel(context.Background())
	var relaysDone sync.WaitGroup
	closers = append(closers, func() error {
		stopRelays()
		relaysDone.Wait()
		return nil
	})
	relays := map[string]*readcache.Relay{}
	names := map[string]string{}
	for _, db := range c.Databases {
		poll := time.Duration(c.Current.Poll)
		if db.CurrentPoll > 0 {
			poll = time.Duration(db.CurrentPoll)
		}
		relay := readcache.NewRelay(manifest.NewBlobStoreBackend(blobstore.New(store.Bucket(), "", db.prefix())), poll)
		relays[db.prefix()] = relay
		names[db.prefix()] = db.name()
		relaysDone.Go(func() { relay.Run(relayCtx, log.With("db", db.name())) })
	}
	handler, err := readcache.NewHandler(objects, relays, time.Duration(c.Current.LongPoll))
	if err != nil {
		return nil, err
	}
	responses := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "isledb_cache_object_responses_total", Help: "Object responses, by Isledb-Cache: hit, shared or miss.",
	}, []string{"status"})

	api := http.NewServeMux()
	api.Handle("/v1/current", service.WithWriteDeadline(handler, time.Duration(c.Current.LongPoll)+longPollWriteSlack))
	api.Handle("/v1/object/", service.WithWriteDeadline(countResponses(handler, responses),
		time.Duration(c.Reads.MaxWait)+storageReadTimeout+objectWriteSlack))

	return &service.Service{
		API:   api,
		Route: route,
		Ready: func() bool {
			for _, relay := range relays {
				if _, ok := relay.Current(); !ok {
					return false
				}
			}
			return true
		},
		Collectors: []prometheus.Collector{responses,
			&collector{objects: objects, disk: disk, relays: relays, names: names, handler: handler}},
		OnShutdown: handler.StopLongPolls,
		Close:      closeAll,
	}, nil
}

func route(r *http.Request) string {
	switch {
	case r.URL.Path == "/v1/current" && r.URL.Query().Has("after"):
		return "current_long_poll"
	case r.URL.Path == "/v1/current":
		return "current"
	case strings.HasPrefix(r.URL.Path, "/v1/object/"):
		return "object"
	default:
		return "other"
	}
}

// countResponses counts object responses by how the cache served them.
func countResponses(next http.Handler, responses *prometheus.CounterVec) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if status := w.Header().Get(readcache.HeaderCache); status != "" {
			responses.WithLabelValues(status).Inc()
		}
	})
}
