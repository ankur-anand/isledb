package runner

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Role is one isledb process of the cluster.
type Role struct {
	Name    string // writer, maintenance, reader-N
	Section string // its section of the configuration
	Proxy   string // the latency proxy it goes through, "" for none
	Listen  string // its API, "" for maintenance
	Admin   string
	config  string
	bin     Binary
	proc    *Proc
}

// Cluster is the isledb roles and latency proxies of one run.
type Cluster struct {
	spec    *Spec
	dir     string
	prefix  string
	latency string // the latproxy binary
	log     *slog.Logger
	Roles   map[string]*Role
	Proxies map[string]*Proxy // by role group: writer, maintenance, reader
}

// Proxy is a latency proxy in front of storage.
type Proxy struct {
	Group string
	URL   string
	proc  *Proc
}

// stopTimeout is how long a role gets to stop before it is killed: longer
// than any role's own shutdown.
const stopTimeout = 3 * time.Minute

func newCluster(spec *Spec, dir, prefix, latproxy string, bin Binary, log *slog.Logger) (*Cluster, error) {
	c := &Cluster{spec: spec, dir: dir, prefix: prefix, latency: latproxy, log: log, Roles: map[string]*Role{}, Proxies: map[string]*Proxy{}}
	add := func(name, section, group string) error {
		admin, err := freePort()
		if err != nil {
			return err
		}
		r := &Role{Name: name, Section: section, Proxy: group, Admin: "127.0.0.1:" + strconv.Itoa(admin), bin: bin,
			config: filepath.Join(dir, "roles", name+".yaml")}
		if section != "maintenance" {
			api, err := freePort()
			if err != nil {
				return err
			}
			r.Listen = "127.0.0.1:" + strconv.Itoa(api)
		}
		c.Roles[name] = r
		return nil
	}
	if err := add("writer", "writer", "writer"); err != nil {
		return nil, err
	}
	if err := add("maintenance", "maintenance", "maintenance"); err != nil {
		return nil, err
	}
	for i := range spec.Isledb.Readers {
		if err := add("reader-"+strconv.Itoa(i+1), "reader", "reader"); err != nil {
			return nil, err
		}
	}
	for _, sub := range []string{"roles", "logs", "cache", "scratch"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// bucketURL returns the bucket as group reaches it.
func (c *Cluster) bucketURL(group string) string {
	u := c.spec.Storage.BucketURL
	if !strings.Contains(u, "{endpoint}") {
		return u
	}
	endpoint := c.spec.Storage.Endpoint
	if p := c.Proxies[group]; p != nil {
		endpoint = p.URL
	}
	return strings.ReplaceAll(u, "{endpoint}", endpoint)
}

// startProxies starts one latency proxy per role group.
func (c *Cluster) startProxies() error {
	lat := c.spec.Storage.Latency
	if lat == nil {
		return nil
	}
	for _, group := range []string{"writer", "maintenance", "reader"} {
		port, err := freePort()
		if err != nil {
			return err
		}
		addr := "127.0.0.1:" + strconv.Itoa(port)
		p, err := StartProc("latproxy-"+group, c.latency,
			[]string{"-listen", addr, "-target", c.spec.Storage.Endpoint, "-min", lat.Min.D().String(), "-max", lat.Max.D().String()},
			filepath.Join(c.dir, "logs", "latproxy-"+group+".log"), nil)
		if err != nil {
			return err
		}
		c.Proxies[group] = &Proxy{Group: group, URL: "http://" + addr, proc: p}
	}
	for _, p := range c.Proxies {
		if err := waitReady(context.Background(), p.proc, p.URL+"/_latproxy/stats", 30*time.Second); err != nil {
			return err
		}
	}
	return nil
}

// render writes r's configuration file.
func (c *Cluster) render(r *Role) error {
	section := map[string]any{}
	maps.Copy(section, c.spec.Isledb.Config[r.Section])
	set := func(k string, v any) error {
		if _, ok := section[k]; ok {
			return fmt.Errorf("isledb.config.%s.%s is set by the runner", r.Section, k)
		}
		section[k] = v
		return nil
	}
	settings := map[string]any{"prefix": c.prefix, "admin_listen": r.Admin}
	switch r.Section {
	case "writer":
		settings["listen"] = r.Listen
		settings["auth"] = map[string]any{"insecure": true}
		settings["owner_id"] = "stress-" + r.Name
	case "reader":
		settings["listen"] = r.Listen
		settings["auth"] = map[string]any{"insecure": true}
		settings["cache_dir"] = filepath.Join(c.dir, "cache", r.Name)
	case "maintenance":
		settings["scratch_dir"] = filepath.Join(c.dir, "scratch")
	}
	for k, v := range settings {
		if err := set(k, v); err != nil {
			return err
		}
	}
	doc := map[string]any{"bucket_url": c.bucketURL(r.Proxy), r.Section: section}
	b, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	return os.WriteFile(r.config, b, 0o644)
}

// start renders r's configuration, checks it with --check, starts r and
// waits until it is ready.
func (c *Cluster) start(ctx context.Context, r *Role) error {
	if err := c.render(r); err != nil {
		return err
	}
	args := []string{r.Section, "--config", r.config}
	if out, err := output(ctx, "", r.bin.Path, append(args, "--check")...); err != nil {
		return fmt.Errorf("%s configuration refused by isledb %s: %w\n%s", r.Name, r.bin.Version, err, out)
	}
	p, err := StartProc(r.Name, r.bin.Path, args, filepath.Join(c.dir, "logs", r.Name+".log"), nil)
	if err != nil {
		return err
	}
	r.proc = p
	c.log.Info("started", "role", r.Name, "isledb", r.bin.Version, "api", r.Listen, "admin", r.Admin)
	return waitReady(ctx, p, "http://"+r.Admin+"/readyz", 2*time.Minute)
}

// Start starts the proxies, then the writer, which creates the database,
// then maintenance and the readers.
func (c *Cluster) Start(ctx context.Context) error {
	if err := c.startProxies(); err != nil {
		return err
	}
	if err := c.start(ctx, c.Roles["writer"]); err != nil {
		return err
	}
	for name, r := range c.Roles {
		if name == "writer" {
			continue
		}
		if err := c.start(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// Restart stops a role, by SIGTERM or SIGKILL, keeps it down for down, and
// starts it again, at bin when given.
func (c *Cluster) Restart(ctx context.Context, name string, kill bool, down time.Duration, bin *Binary) error {
	r := c.Roles[name]
	if r == nil {
		return fmt.Errorf("no role %q", name)
	}
	how := "stopped"
	if kill {
		r.proc.Kill()
		how = "killed"
	} else if err := r.proc.Stop(stopTimeout); err != nil {
		c.log.Warn("stop", "role", name, "error", err)
	}
	c.log.Info(how, "role", name, "down", down)
	if down > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(down):
		}
	}
	if bin != nil {
		r.bin = *bin
	}
	return c.start(ctx, r)
}

// Exited returns the first role that has exited unasked.
func (c *Cluster) Exited() (string, error) {
	for name, r := range c.Roles {
		if r.proc == nil {
			continue
		}
		if exited, err := r.proc.Exited(); exited {
			return name, fmt.Errorf("%s exited: %v; see %s", name, err, filepath.Join(c.dir, "logs", name+".log"))
		}
	}
	return "", nil
}

// Stop stops the readers, maintenance, the writer, then the proxies.
func (c *Cluster) Stop() {
	order := []string{}
	for name := range c.Roles {
		if strings.HasPrefix(name, "reader-") {
			order = append(order, name)
		}
	}
	order = append(order, "maintenance", "writer")
	for _, name := range order {
		if r := c.Roles[name]; r != nil && r.proc != nil {
			if err := r.proc.Stop(stopTimeout); err != nil {
				c.log.Warn("stop", "role", name, "error", err)
			}
		}
	}
	for _, p := range c.Proxies {
		_ = p.proc.Stop(10 * time.Second)
	}
}

// ReaderURLs returns the readers' API addresses.
func (c *Cluster) ReaderURLs() []string {
	var out []string
	for i := range c.spec.Isledb.Readers {
		out = append(out, "http://"+c.Roles["reader-"+strconv.Itoa(i+1)].Listen)
	}
	return out
}
