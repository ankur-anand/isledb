// Package service runs an isledb role as a service: configuration from a
// file, the environment and flags; the API and admin listeners;
// authentication; metrics; and shutdown. A role supplies its configuration
// and its handlers.
package service

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Setting is one configuration value a role takes. Path is its key in the
// role's section of the file, such as "disk.bytes"; its environment variable
// is ISLEDB_<ROLE>_<PATH>, upper case, with dots as underscores. Flag is its
// flag name, without the role, as the subcommand names it.
type Setting struct {
	Flag, Path, Usage string
	// Bool marks a flag that takes no value.
	Bool bool
	// List marks a repeatable flag. In the environment the values are
	// comma-separated, and empty ones are skipped. Values from flags or the
	// environment replace the file's rather than add to them.
	List  bool
	Set   func(v string) error
	Reset func()
}

// EnvName is the environment variable of a setting at path in role's
// section, or of a common setting when role is empty.
func EnvName(role, path string) string {
	name := strings.ToUpper(strings.ReplaceAll(path, ".", "_"))
	if role != "" {
		name = strings.ToUpper(role) + "_" + name
	}
	return "ISLEDB_" + name
}

// Common is the configuration every role shares, at the top of the file.
type Common struct {
	BucketURL string `yaml:"bucket_url"`
	LogLevel  string `yaml:"log_level"`
}

func (c *Common) settings() []Setting {
	return []Setting{
		{Flag: "bucket-url", Path: "bucket_url", Usage: "bucket holding the databases", Set: func(v string) error { c.BucketURL = v; return nil }},
		{Flag: "log-level", Path: "log_level", Usage: "debug, info, warn or error", Set: func(v string) error { c.LogLevel = v; return nil }},
	}
}

func (c *Common) validate() error {
	var errs []error
	if c.BucketURL == "" {
		errs = append(errs, errors.New("bucket_url is required"))
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.LogLevel)); err != nil {
		errs = append(errs, fmt.Errorf("log_level: %w", err))
	}
	return errors.Join(errs...)
}

// Level is the log level, which validate has checked.
func (c *Common) Level() slog.Level {
	var level slog.Level
	_ = level.UnmarshalText([]byte(c.LogLevel))
	return level
}

// Load resolves role's configuration into cfg from defaults (already in
// cfg), the YAML file named by --config or ISLEDB_CONFIG, the environment,
// and args, each overriding the one before, and validates it. The file's
// top level holds the common settings and one section per role; roles names
// every role, so that another role's section is not taken for a mistake.
func Load(role string, roles []string, cfg Config, args []string, lookupEnv func(string) (string, bool)) (common Common, check bool, err error) {
	common = Common{LogLevel: "info"}
	fs := flag.NewFlagSet("isledb "+role, flag.ContinueOnError)
	configPath := fs.String("config", "", "YAML configuration file (environment: ISLEDB_CONFIG)")
	fs.BoolVar(&check, "check", false, "print the resolved configuration and exit")

	type scoped struct {
		Setting
		env string
	}
	var all []scoped
	for _, st := range common.settings() {
		all = append(all, scoped{st, EnvName("", st.Path)})
	}
	for _, st := range cfg.Settings() {
		all = append(all, scoped{st, EnvName(role, st.Path)})
	}
	type given struct {
		st    scoped
		value string
	}
	var fromFlags []given
	for _, st := range all {
		record := func(v string) error {
			fromFlags = append(fromFlags, given{st, v})
			return nil
		}
		usage := fmt.Sprintf("%s (environment: %s)", st.Usage, st.env)
		if st.Bool {
			fs.BoolFunc(st.Flag, usage, record)
		} else {
			fs.Func(st.Flag, usage, record)
		}
	}
	if err := fs.Parse(args); err != nil {
		return common, false, err
	}
	if fs.NArg() > 0 {
		return common, false, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	path := *configPath
	if path == "" {
		path, _ = lookupEnv("ISLEDB_CONFIG")
	}
	if path != "" {
		if err := loadFile(path, role, roles, &common, cfg); err != nil {
			return common, false, err
		}
	}
	onFlags := map[string]bool{}
	for _, g := range fromFlags {
		onFlags[g.st.Flag] = true
	}
	for _, st := range all {
		v, ok := lookupEnv(st.env)
		if !ok || onFlags[st.Flag] {
			continue
		}
		if err := setFromEnv(st.Setting, v); err != nil {
			return common, false, fmt.Errorf("%s: %w", st.env, err)
		}
	}
	reset := map[string]bool{}
	for _, g := range fromFlags {
		if g.st.List && !reset[g.st.Flag] {
			g.st.Reset()
			reset[g.st.Flag] = true
		}
		if err := g.st.Set(g.value); err != nil {
			return common, false, fmt.Errorf("-%s: %w", g.st.Flag, err)
		}
	}
	return common, check, errors.Join(common.validate(), cfg.Validate())
}

// setFromEnv sets st from an environment variable's value: for a list,
// comma-separated values that replace the list, skipping empty ones.
func setFromEnv(st Setting, v string) error {
	if !st.List {
		return st.Set(v)
	}
	st.Reset()
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			if err := st.Set(part); err != nil {
				return err
			}
		}
	}
	return nil
}

func loadFile(path, role string, roles []string, common *Common, cfg Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if len(doc.Content) == 0 {
		return nil
	}
	top := doc.Content[0]
	if top.Kind != yaml.MappingNode {
		return fmt.Errorf("%s: want a mapping at the top", path)
	}
	known := map[string]bool{}
	for _, r := range roles {
		known[r] = true
	}
	for i := 0; i+1 < len(top.Content); i += 2 {
		key, value := top.Content[i].Value, top.Content[i+1]
		switch {
		case key == "bucket_url":
			err = value.Decode(&common.BucketURL)
		case key == "log_level":
			err = value.Decode(&common.LogLevel)
		case key == role:
			err = decodeStrict(value, cfg)
		case known[key]:
		default:
			err = fmt.Errorf("line %d: unknown key %q", top.Content[i].Line, key)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	return nil
}

// decodeStrict decodes node into out, refusing keys out does not have.
func decodeStrict(node *yaml.Node, out any) error {
	data, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	return dec.Decode(out)
}

// Resolved renders the configuration as the file would hold it.
func Resolved(role string, common Common, cfg Config) ([]byte, error) {
	return yaml.Marshal(map[string]any{"bucket_url": common.BucketURL, "log_level": common.LogLevel, role: cfg})
}

// ByteSize is a size written as a number of bytes or with a binary unit:
// 4096, 512KiB, 256MiB, 200GiB, 2TiB.
type ByteSize int64

var byteUnits = []struct {
	suffix string
	shift  uint
}{{"TiB", 40}, {"GiB", 30}, {"MiB", 20}, {"KiB", 10}, {"B", 0}}

func (b *ByteSize) Parse(s string) error {
	s = strings.TrimSpace(s)
	shift := uint(0)
	for _, u := range byteUnits {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			s, shift = strings.TrimSpace(n), u.shift
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || n > (1<<63-1)>>shift {
		return fmt.Errorf("invalid size %q: want a number of bytes or one with KiB, MiB, GiB or TiB", s)
	}
	*b = ByteSize(n << shift)
	return nil
}

func (b ByteSize) String() string {
	for _, u := range byteUnits {
		if u.shift > 0 && b != 0 && int64(b)%(1<<u.shift) == 0 {
			return strconv.FormatInt(int64(b)>>u.shift, 10) + u.suffix
		}
	}
	return strconv.FormatInt(int64(b), 10)
}

func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error { return b.Parse(node.Value) }
func (b ByteSize) MarshalYAML() (any, error)            { return b.String(), nil }

// Duration is a time.Duration written as Go writes one: 500ms, 30s, 2m.
type Duration time.Duration

func (d *Duration) Parse(s string) error {
	v, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) String() string                       { return time.Duration(d).String() }
func (d *Duration) UnmarshalYAML(node *yaml.Node) error { return d.Parse(node.Value) }
func (d Duration) MarshalYAML() (any, error)            { return d.String(), nil }
