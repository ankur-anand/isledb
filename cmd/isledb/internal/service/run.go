package service

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const shutdownTimeout = 30 * time.Second

// Role is one way to run isledb.
type Role struct {
	Name, Summary string
	// NewConfig returns the role's configuration with its defaults.
	NewConfig func() Config
}

// Config is a role's section of the configuration.
type Config interface {
	// Settings are the values flags and the environment can set.
	Settings() []Setting
	Validate() error
	ServerConfig() *ServerConfig
	// Start builds the role's service; Serve then runs it.
	Start(ctx context.Context, common Common, log *slog.Logger) (*Service, error)
}

// CommonDefaults is a Config with defaults that depend on the common
// settings, such as the bucket: Load calls ApplyCommon once every source is
// read, before Validate.
type CommonDefaults interface {
	ApplyCommon(common Common)
}

// Service is what a started role serves.
type Service struct {
	// API is served on the API listener; nil for a role without one.
	API http.Handler
	// Route names a request's route for metrics.
	Route func(*http.Request) string
	// Ready reports whether the role can serve; /readyz also reports not
	// ready while shutting down.
	Ready      func() bool
	Collectors []prometheus.Collector
	// OnShutdown runs once the role reports not ready and has drained,
	// before requests in flight are waited for.
	OnShutdown func()
	// Close releases the role after its listeners have stopped.
	Close func() error
}

// ServerConfig configures the listeners every role has. A role embeds it in
// its configuration inline.
type ServerConfig struct {
	Listen        string     `yaml:"listen"`
	AdminListen   string     `yaml:"admin_listen"`
	Auth          AuthConfig `yaml:"auth"`
	TLS           TLSConfig  `yaml:"tls"`
	ShutdownDrain Duration   `yaml:"shutdown_drain"`
}

type AuthConfig struct {
	TokenFile string `yaml:"token_file,omitempty"`
	Insecure  bool   `yaml:"insecure,omitempty"`
}

type TLSConfig struct {
	Cert     string `yaml:"cert,omitempty"`
	Key      string `yaml:"key,omitempty"`
	ClientCA string `yaml:"client_ca,omitempty"`
}

// DefaultServer is the server configuration with the role's addresses.
func DefaultServer(listen, adminListen string) ServerConfig {
	return ServerConfig{Listen: listen, AdminListen: adminListen, ShutdownDrain: Duration(5 * time.Second)}
}

// Settings are the server's flags.
func (s *ServerConfig) Settings() []Setting {
	str := func(p *string) func(string) error { return func(v string) error { *p = v; return nil } }
	return []Setting{
		{Flag: "listen", Path: "listen", Usage: "API address; empty for none", Set: str(&s.Listen)},
		{Flag: "admin-listen", Path: "admin_listen", Usage: "address of /metrics, /healthz, /readyz and /debug/pprof; empty for none", Set: str(&s.AdminListen)},
		{Flag: "auth-token-file", Path: "auth.token_file", Usage: "file of bearer tokens, one per line; reread on SIGHUP", Set: str(&s.Auth.TokenFile)},
		{Flag: "insecure", Path: "auth.insecure", Usage: "serve the API without authentication, for development", Bool: true, Set: func(v string) error {
			b, err := strconv.ParseBool(v)
			s.Auth.Insecure = b
			return err
		}},
		{Flag: "tls-cert", Path: "tls.cert", Usage: "TLS certificate file", Set: str(&s.TLS.Cert)},
		{Flag: "tls-key", Path: "tls.key", Usage: "TLS key file", Set: str(&s.TLS.Key)},
		{Flag: "tls-client-ca", Path: "tls.client_ca", Usage: "CA file; clients must present a certificate it signed", Set: str(&s.TLS.ClientCA)},
		{Flag: "shutdown-drain", Path: "shutdown_drain", Usage: "how long /readyz reports not ready before shutting down", Set: s.ShutdownDrain.Parse},
	}
}

func (s *ServerConfig) Validate() error {
	var errs []error
	if s.Listen != "" && s.Auth.TokenFile == "" && s.TLS.ClientCA == "" && !s.Auth.Insecure {
		errs = append(errs, errors.New("no authentication: set auth.token_file or tls.client_ca, or auth.insecure for development"))
	}
	if (s.TLS.Cert == "") != (s.TLS.Key == "") {
		errs = append(errs, errors.New("tls.cert and tls.key go together"))
	}
	if s.TLS.ClientCA != "" && s.TLS.Cert == "" {
		errs = append(errs, errors.New("tls.client_ca needs tls.cert and tls.key"))
	}
	if s.ShutdownDrain < 0 {
		errs = append(errs, errors.New("shutdown_drain is negative"))
	}
	return errors.Join(errs...)
}

// ErrUsage reports that the command line names no known role.
var ErrUsage = errors.New("usage")

// Usage describes the roles.
func Usage(roles []Role) string {
	var b strings.Builder
	b.WriteString("usage: isledb <role> [flags]\n\nroles:\n")
	for _, r := range roles {
		fmt.Fprintf(&b, "  %-12s %s\n", r.Name, r.Summary)
	}
	b.WriteString("\nRun \"isledb <role> -h\" for a role's flags, \"isledb version\" for the version.\n")
	return b.String()
}

// Main runs the role args names until SIGINT or SIGTERM. A second signal
// during shutdown ends the process at once.
func Main(roles []Role, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return ErrUsage
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		_, err := io.WriteString(stdout, Usage(roles))
		return err
	}
	var role *Role
	var names []string
	for i := range roles {
		names = append(names, roles[i].Name)
		if roles[i].Name == args[0] {
			role = &roles[i]
		}
	}
	if role == nil {
		return fmt.Errorf("%w: unknown role %q", ErrUsage, args[0])
	}
	cfg := role.NewConfig()
	common, check, err := Load(role.Name, names, cfg, args[1:], os.LookupEnv)
	if err != nil {
		return err
	}
	if check {
		out, err := Resolved(role.Name, common, cfg)
		if err != nil {
			return err
		}
		_, err = stdout.Write(out)
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: common.Level()}))
	slog.SetDefault(log)
	keepHangups()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(ctx, stop)
	return Serve(ctx, *role, cfg, common, log, nil)
}

// Serve starts the role and serves it until ctx ends, then shuts down:
// /readyz reports not ready, the drain passes, OnShutdown runs, requests in
// flight finish, and the role closes. started, if not nil, gets the
// addresses served once listening; either may be nil.
func Serve(ctx context.Context, role Role, cfg Config, common Common, log *slog.Logger, started func(api, admin net.Addr)) (err error) {
	sc := cfg.ServerConfig()

	// Everything that can fail on its own, credentials and addresses, is
	// ready before Start: a role such as the writer takes over its database
	// in Start, and must not do so only to fail here afterwards.
	var (
		tlsConf        *tls.Config
		tokens         *TokenAuth
		apiLn, adminLn net.Listener
	)
	defer func() {
		if err != nil {
			for _, ln := range []net.Listener{apiLn, adminLn} {
				if ln != nil {
					_ = ln.Close()
				}
			}
		}
	}()
	if sc.Listen != "" {
		if tlsConf, err = serverTLS(sc.TLS); err != nil {
			return err
		}
		if sc.Auth.TokenFile != "" {
			if tokens, err = NewTokenAuth(sc.Auth.TokenFile); err != nil {
				return err
			}
		}
		if apiLn, err = net.Listen("tcp", sc.Listen); err != nil {
			return err
		}
	}
	if sc.AdminListen != "" {
		if adminLn, err = net.Listen("tcp", sc.AdminListen); err != nil {
			return err
		}
	}

	svc, err := cfg.Start(ctx, common, log)
	if err != nil {
		return err
	}
	if svc.Close != nil {
		defer func() {
			if err := svc.Close(); err != nil {
				log.Warn("isledb: close", "role", role.Name, "error", err)
			}
		}()
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(svc.Collectors...)

	var stopping atomic.Bool
	ready := func() bool { return !stopping.Load() && (svc.Ready == nil || svc.Ready()) }

	var servers []*http.Server
	var addrs [2]net.Addr
	serveErr := make(chan error, 2)
	serve := func(i int, ln net.Listener, srv *http.Server, tlsCerts bool) {
		addrs[i] = ln.Addr()
		servers = append(servers, srv)
		go func() {
			if tlsCerts {
				serveErr <- srv.ServeTLS(ln, "", "")
			} else {
				serveErr <- srv.Serve(ln)
			}
		}()
	}
	shutdown := func() error {
		stopCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		var errs []error
		for _, srv := range servers {
			errs = append(errs, srv.Shutdown(stopCtx))
		}
		return errors.Join(errs...)
	}

	if apiLn != nil && svc.API == nil {
		_ = apiLn.Close()
		apiLn = nil
	}
	if apiLn != nil {
		handler := svc.API
		if tokens != nil {
			go reloadOnHangup(ctx, tokens, log)
			handler = tokens.Wrap(handler)
		}
		handler = newHTTPMetrics(registry, role.Name, svc.Route).wrap(handler)
		serve(0, apiLn, &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       2 * time.Minute,
			TLSConfig:         tlsConf,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		}, tlsConf != nil)
	}
	if adminLn != nil {
		serve(1, adminLn, &http.Server{Handler: adminHandler(registry, ready), ReadHeaderTimeout: 10 * time.Second}, false)
	}
	log.Info("isledb: serving", "role", role.Name, "listen", addrs[0], "admin", addrs[1])
	if started != nil {
		started(addrs[0], addrs[1])
	}

	var failed error
	select {
	case <-ctx.Done():
	case failed = <-serveErr:
		log.Error("isledb: server stopped", "role", role.Name, "error", failed)
	}
	log.Info("isledb: shutting down", "role", role.Name)
	stopping.Store(true)
	if failed == nil {
		time.Sleep(time.Duration(sc.ShutdownDrain))
	}
	if svc.OnShutdown != nil {
		svc.OnShutdown()
	}
	return errors.Join(failed, shutdown())
}

// WithWriteDeadline gives each request d to write its response.
func WithWriteDeadline(next http.Handler, d time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(d))
		next.ServeHTTP(w, r)
	})
}

// adminHandler serves metrics, health and profiles, without authentication:
// it belongs on an address only operators reach.
func adminHandler(registry *prometheus.Registry, ready func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// keepHangups stops SIGHUP ending the process, its default, for the whole
// run: without a token file to reload, during start, and during shutdown.
// The channel is never read; a hangup sent to it is dropped.
func keepHangups() {
	signal.Notify(make(chan os.Signal, 1), syscall.SIGHUP)
}

func reloadOnHangup(ctx context.Context, tokens *TokenAuth, log *slog.Logger) {
	hangups := make(chan os.Signal, 1)
	signal.Notify(hangups, syscall.SIGHUP)
	defer signal.Stop(hangups)
	for {
		select {
		case <-ctx.Done():
			return
		case <-hangups:
			if err := tokens.Reload(); err != nil {
				log.Warn("isledb: reload tokens; keeping those in use", "error", err)
			} else {
				log.Info("isledb: tokens reloaded")
			}
		}
	}
}
