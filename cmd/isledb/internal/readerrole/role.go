package readerrole

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
	"github.com/ankur-anand/isledb/internal/readhttp"
)

// Role runs the reader.
var Role = service.Role{
	Name:      "reader",
	Summary:   "serves reads of one database over HTTP; replicas share nothing",
	NewConfig: NewConfig,
}

const (
	// readDeadline bounds one read, objects fetched from storage included;
	// writeSlack is the time beyond it to send the answer.
	readDeadline = time.Minute
	writeSlack   = 10 * time.Second
	// readyTimeout bounds the readiness check, which may refresh an expired
	// view.
	readyTimeout = time.Second
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

	db, err := isledb.Open(ctx, common.BucketURL, isledb.DBOptions{Prefix: c.prefix()})
	if err != nil {
		return nil, err
	}
	closers = append(closers, db.Close)

	metrics := isledb.DefaultReaderMetrics(nil)
	opts := isledb.DefaultReaderOpenOptions(c.CacheDir)
	opts.DiskCacheSize = int64(c.Caches.DiskBytes)
	opts.BlockCacheSize = int64(c.Caches.BlockBytes)
	opts.BloomCacheSize = int64(c.Caches.BloomBytes)
	opts.Views.RefreshAfter = time.Duration(c.Views.RefreshAfter)
	opts.Metrics = metrics
	if c.ReadCache.URL != "" {
		opts.ReadCache.URL = c.ReadCache.URL
		if c.ReadCache.TokenFile != "" {
			if opts.ReadCache.Token, err = readCacheToken(c.ReadCache.TokenFile); err != nil {
				return nil, err
			}
		} else if c.ReadCache.Cert == "" {
			log.Warn("read_cache has no token_file or cert: only a cache run with auth.insecure accepts it; any other answers 401 and every read goes to storage")
		}
		client, err := readCacheClient(c.ReadCache)
		if err != nil {
			return nil, err
		}
		if client != nil {
			opts.ReadCache.HTTPClient = client
			closers = append(closers, func() error { client.CloseIdleConnections(); return nil })
		}
	}
	// Opening loads the first view: the reader can serve once it returns.
	reader, err := db.OpenReader(ctx, opts)
	if err != nil {
		return nil, err
	}
	closers = append(closers, reader.Close)
	log.Info("reader open", "prefix", c.Prefix, "position", reader.ViewPosition(), "read_cache", c.ReadCache.URL != "")

	// The feed opens on its first read, so a reader may start before the
	// writer turns it on; its head is the reader's view.
	changes := readhttp.NewChanges(func(ctx context.Context) (*isledb.ChangeReader, error) {
		return db.OpenChangeReader(ctx, isledb.ChangeReaderOptions{ReadCache: opts.ReadCache})
	})
	closers = append(closers, changes.Close)

	handler, err := readhttp.NewHandler(reader, readhttp.Options{MaxConcurrent: c.Reads.MaxConcurrent, ReadTimeout: readDeadline, Changes: changes})
	if err != nil {
		return nil, err
	}
	return &service.Service{
		API:        service.WithWriteDeadline(handler, readDeadline+writeSlack),
		Route:      readhttp.Route,
		Ready:      func() bool { return hasView(reader) },
		Collectors: append(readerCollectors(metrics), &collector{reader: reader, handler: handler}),
		Close:      closeAll,
	}, nil
}

// readCacheToken reads the one token in file, laid out like a token file
// the cache reads: blank lines and lines starting with # are skipped.
func readCacheToken(file string) (string, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read_cache.token_file: %w", err)
	}
	var tokens []string
	for line := range strings.Lines(string(raw)) {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			tokens = append(tokens, line)
		}
	}
	if len(tokens) != 1 {
		return "", fmt.Errorf("read_cache.token_file: %s holds %d tokens, want 1", file, len(tokens))
	}
	return tokens[0], nil
}

// readCacheClient returns the client for a read cache with a private CA or a
// client certificate, or nil to let the reader use its own.
func readCacheClient(c ReadCache) (*http.Client, error) {
	if c.CAFile == "" && c.Cert == "" {
		return nil, nil
	}
	conf := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read_cache.ca_file: %w", err)
		}
		conf.RootCAs = x509.NewCertPool()
		if !conf.RootCAs.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("read_cache.ca_file: %s holds no certificates", c.CAFile)
		}
	}
	if c.Cert != "" {
		cert, err := tls.LoadX509KeyPair(c.Cert, c.Key)
		if err != nil {
			return nil, fmt.Errorf("read_cache.cert and key: %w", err)
		}
		conf.Certificates = []tls.Certificate{cert}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 128 // as the reader's own client: many reads share the cache
	transport.TLSClientConfig = conf
	return &http.Client{Transport: transport}, nil
}

// hasView reports whether reader has a view to read from, refreshing an
// expired one first as a read would.
func hasView(reader *isledb.Reader) bool {
	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	snap, err := reader.Snapshot(ctx)
	if err != nil {
		return false
	}
	_ = snap.Close()
	return true
}
