// Package runner runs a stress test from a spec: it starts the isledb roles
// of the version under test, the latency proxies in front of storage, the
// load and the readers, runs the phases, and writes what it measured.
package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
	"go.yaml.in/yaml/v3"
)

// Spec is a run, as its YAML file gives it.
type Spec struct {
	Name     string       `yaml:"name"`
	Seed     uint64       `yaml:"seed"`
	Workload WorkloadSpec `yaml:"workload"`
	Readers  []ReaderSpec `yaml:"readers"`
	Storage  StorageSpec  `yaml:"storage"`
	Isledb   IsledbSpec   `yaml:"isledb"`
	Phases   []PhaseSpec  `yaml:"phases"`
	// SampleEvery is how often a row of measurements is written.
	SampleEvery Duration `yaml:"sample_every"`
	// Checkpoints are the ingest sizes at which the summary records every
	// measurement, so runs can be compared at the same points. Empty takes
	// the steps of Ladder up to what the run writes.
	Checkpoints []Size `yaml:"checkpoints"`
}

// Ladder is the checkpoints every run records unless its spec names its
// own, so runs of any size share their points.
var Ladder = []Size{1 << 30, 5 << 30, 10 << 30, 20 << 30, 50 << 30, 100 << 30, 250 << 30, 500 << 30, 1 << 40, 2 << 40, 5 << 40}

// Overrides change a spec from the command line, so one spec runs at any
// size: Target replaces the workload's keys or target, Rate its rate.
type Overrides struct {
	Target Size
	Rate   *Size
}

type WorkloadSpec struct {
	// Shape is random or sequential.
	Shape  string `yaml:"shape"`
	Values string `yaml:"values"`
	// Keys is the number of keys; Target the key and value bytes the insert
	// round writes, from which the number of keys follows. Give one.
	Keys      uint64 `yaml:"keys"`
	Target    Size   `yaml:"target"`
	BatchKeys uint64 `yaml:"batch_keys"`
	// DeletePermille is the share of keys, in thousandths, each overwrite
	// round deletes.
	DeletePermille uint64 `yaml:"delete_permille"`
	Rate           Size   `yaml:"rate"` // bytes a second; 0 unlimited
	// Workers is the batches in flight at once. Each waits for its commit,
	// so a rate needs about rate × commit latency ÷ batch bytes of them:
	// 12MiB/s of 40KB batches at 300ms is about 100.
	Workers int `yaml:"workers"`
}

type ReaderSpec struct {
	Name      string  `yaml:"name"`
	Rate      float64 `yaml:"rate"` // operations a second; 0 unlimited
	Workers   int     `yaml:"workers"`
	ScanRatio float64 `yaml:"scan_ratio"`
	ScanLimit int     `yaml:"scan_limit"`
	Dist      string  `yaml:"dist"`
}

type StorageSpec struct {
	// BucketURL is the bucket. In an s3:// URL, {endpoint} is replaced by
	// each role's latency proxy, or by Endpoint when there are none.
	BucketURL string `yaml:"bucket_url"`
	// Endpoint is where the proxies forward: the S3-compatible server.
	Endpoint string `yaml:"endpoint"`
	// Latency, when set, puts a latency proxy in front of storage for each
	// role, which also counts the requests and bytes of each.
	Latency *LatencySpec `yaml:"latency"`
	// Disk, when set, pauses the load while the disk holding Path is fuller
	// than MaxUsedPct, so a long run never fills the disk under storage.
	Disk *DiskSpec `yaml:"disk"`
	// ListEvery is how often the bucket is listed for its size, 5m when
	// unset. A listing reads every object name, so it is spaced out.
	ListEvery Duration `yaml:"list_every"`
}

type DiskSpec struct {
	Path       string  `yaml:"path"`
	MaxUsedPct float64 `yaml:"max_used_pct"`
}

type LatencySpec struct {
	Min Duration `yaml:"min"`
	Max Duration `yaml:"max"`
}

type IsledbSpec struct {
	Readers int `yaml:"readers"`
	// Config is merged into each role's section of the isledb configuration:
	// writer, reader and maintenance. The runner sets prefix, addresses,
	// directories and auth itself.
	Config map[string]map[string]any `yaml:"config"`
}

// PhaseSpec is one phase; exactly one of its fields is set.
type PhaseSpec struct {
	// Load runs the insert round to the end.
	Load *WritePhase `yaml:"load"`
	// Update runs Rounds overwrite rounds.
	Update *WritePhase `yaml:"update"`
	// Settle stops writing and waits for compaction to go quiet.
	Settle *SettlePhase `yaml:"settle"`
	// Verify reads every key and checks it.
	Verify *VerifyPhase `yaml:"verify"`
	// Restart stops a role and starts it again, maybe at another version.
	Restart *RestartEvent `yaml:"restart"`
}

type WritePhase struct {
	Rounds uint32 `yaml:"rounds"` // update only; default 1
	// Rate overrides the workload's for this phase.
	Rate *Size `yaml:"rate"`
	// Events happen while the phase runs.
	Events []RestartEvent `yaml:"events"`
	// Faults change what a latency proxy injects while the phase runs.
	Faults []FaultEvent `yaml:"faults"`
}

// FaultEvent sets a latency proxy's faults, After the phase starts. The
// phase clears them when it ends.
type FaultEvent struct {
	After Duration `yaml:"after"`
	// Proxy is writer, maintenance or reader.
	Proxy string `yaml:"proxy"`
	// Set is the faults; empty clears them.
	Set Faults `yaml:"set"`
}

// Faults are what a latency proxy injects into the requests it matches:
// those whose method is in Methods and kind of object in Kinds, either
// empty for all.
type Faults struct {
	Methods []string `yaml:"methods" json:"methods,omitempty"`
	Kinds   []string `yaml:"kinds" json:"kinds,omitempty"`
	// ErrorRate is the share refused without being forwarded.
	ErrorRate float64 `yaml:"error_rate" json:"error_rate,omitempty"`
	// DropRate is the share forwarded and then answered with an error: they
	// took effect, but the client cannot tell.
	DropRate   float64  `yaml:"drop_rate" json:"drop_rate,omitempty"`
	ExtraDelay Duration `yaml:"extra_delay" json:"extra_delay,omitempty"`
}

func (f Faults) validate() error {
	var errs []error
	for _, m := range f.Methods {
		if !slices.Contains([]string{"GET", "HEAD", "PUT", "POST", "DELETE"}, m) {
			errs = append(errs, fmt.Errorf("method %q: want GET, HEAD, PUT, POST or DELETE", m))
		}
	}
	for _, k := range f.Kinds {
		if !slices.Contains([]string{"sst", "manifest", "current", "other"}, k) {
			errs = append(errs, fmt.Errorf("kind %q: want sst, manifest, current or other", k))
		}
	}
	if f.ErrorRate < 0 || f.DropRate < 0 || f.ErrorRate+f.DropRate > 1 {
		errs = append(errs, errors.New("error_rate and drop_rate must be at least 0 and add up to at most 1"))
	}
	if f.ExtraDelay < 0 {
		errs = append(errs, errors.New("extra_delay is negative"))
	}
	return errors.Join(errs...)
}

type SettlePhase struct {
	// Quiet is how long maintenance must upload nothing to count as settled.
	Quiet Duration `yaml:"quiet"`
	Max   Duration `yaml:"max"`
}

type VerifyPhase struct {
	Parts int `yaml:"parts"`
	Limit int `yaml:"limit"`
}

type RestartEvent struct {
	After Duration `yaml:"after"`
	// Role is writer, maintenance, or reader-N.
	Role string `yaml:"role"`
	// Kill stops the role with SIGKILL instead of SIGTERM.
	Kill bool `yaml:"kill"`
	// Down is how long the role stays stopped.
	Down Duration `yaml:"down"`
	// Isledb, when set, is the version the role restarts at.
	Isledb string `yaml:"isledb"`
}

// LoadSpec reads and checks a spec file.
func LoadSpec(path string) (*Spec, error) { return LoadSpecWith(path, Overrides{}) }

// LoadSpecWith reads a spec file, applies o, and checks the result.
func LoadSpecWith(path string, o Overrides) (*Spec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Spec
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("spec %s: %w", path, err)
	}
	if o.Target > 0 {
		s.Workload.Keys, s.Workload.Target = 0, o.Target
	}
	if o.Rate != nil {
		s.Workload.Rate = *o.Rate
	}
	if err := s.normalize(); err != nil {
		return nil, fmt.Errorf("spec %s: %w", path, err)
	}
	return &s, nil
}

func (s *Spec) normalize() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if s.Name == "" {
		fail("name is required")
	}
	if s.Seed == 0 {
		s.Seed = 1
	}
	w := &s.Workload
	if w.Shape == "" {
		w.Shape = "random"
	}
	if w.Values == "" {
		w.Values = "mixed"
	}
	if w.BatchKeys == 0 {
		w.BatchKeys = 16
	}
	if w.Workers == 0 {
		w.Workers = 128
	}
	switch {
	case w.Keys == 0 && w.Target == 0:
		fail("workload: give keys or target")
	case w.Keys != 0 && w.Target != 0:
		fail("workload: give keys or target, not both")
	}
	for i := range s.Readers {
		r := &s.Readers[i]
		if r.Name == "" {
			r.Name = "reader-" + strconv.Itoa(i+1)
		}
	}
	if s.Storage.BucketURL == "" {
		fail("storage.bucket_url is required")
	}
	if s.Storage.Latency != nil && s.Storage.Endpoint == "" {
		fail("storage.latency needs storage.endpoint, the server the proxies forward to")
	}
	if d := s.Storage.Disk; d != nil && (d.Path == "" || d.MaxUsedPct <= 0 || d.MaxUsedPct >= 100) {
		fail("storage.disk needs a path and a max_used_pct between 0 and 100")
	}
	if s.Isledb.Readers == 0 {
		s.Isledb.Readers = 1
	}
	for role := range s.Isledb.Config {
		switch role {
		case "writer", "reader", "maintenance":
		default:
			fail("isledb.config.%s: want writer, reader or maintenance", role)
		}
	}
	if len(s.Phases) == 0 {
		fail("no phases")
	}
	roles := []string{"writer", "maintenance"}
	for i := range s.Isledb.Readers {
		roles = append(roles, "reader-"+strconv.Itoa(i+1))
	}
	checkRestart := func(where string, ev RestartEvent) {
		if !slices.Contains(roles, ev.Role) {
			fail("%s: role %q: want one of %s", where, ev.Role, strings.Join(roles, ", "))
		}
		if ev.After < 0 || ev.Down < 0 {
			fail("%s: after and down must not be negative", where)
		}
	}
	for i, p := range s.Phases {
		n := 0
		for _, set := range []bool{p.Load != nil, p.Update != nil, p.Settle != nil, p.Verify != nil, p.Restart != nil} {
			if set {
				n++
			}
		}
		if n != 1 {
			fail("phase %d: give exactly one of load, update, settle, verify, restart", i+1)
		}
		if p.Restart != nil {
			checkRestart(fmt.Sprintf("phase %d", i+1), *p.Restart)
		}
		for _, w := range []*WritePhase{p.Load, p.Update} {
			if w == nil {
				continue
			}
			for j, ev := range w.Events {
				checkRestart(fmt.Sprintf("phase %d, event %d", i+1, j+1), ev)
			}
			for _, f := range w.Faults {
				if err := f.Set.validate(); err != nil {
					fail("phase %d: faults for %s: %w", i+1, f.Proxy, err)
				}
				if s.Storage.Latency == nil {
					fail("phase %d: faults need storage.latency, which starts the proxies", i+1)
				}
				switch f.Proxy {
				case "writer", "maintenance", "reader":
				default:
					fail("phase %d: faults proxy %q: want writer, maintenance or reader", i+1, f.Proxy)
				}
			}
		}
	}
	if s.SampleEvery == 0 {
		s.SampleEvery = Duration(10 * time.Second)
	}
	if s.Storage.ListEvery <= 0 {
		s.Storage.ListEvery = Duration(5 * time.Minute)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if len(s.Checkpoints) == 0 {
		planned := s.Planned()
		for _, c := range Ladder {
			if c <= planned {
				s.Checkpoints = append(s.Checkpoints, c)
			}
		}
	}
	return nil
}

// Inserted returns about the key and value bytes the insert round writes.
func (s *Spec) Inserted() Size {
	if s.Workload.Target > 0 {
		return s.Workload.Target
	}
	d, err := workload.ParseSizeDist(s.Workload.Values)
	if err != nil {
		return 0
	}
	return Size(float64(s.Workload.Keys) * (16 + d.Mean()))
}

// Planned returns about the key and value bytes the whole run writes: the
// insert round and every overwrite round, deletes not counted.
func (s *Spec) Planned() Size {
	rounds := Size(1)
	for _, p := range s.Phases {
		if p.Update != nil {
			rounds += Size(max(p.Update.Rounds, 1))
		}
	}
	return s.Inserted() * rounds
}

// SizeLabel names the run's size for its directory: the target, as 20GiB or
// 1TiB, or the number of keys.
func (s *Spec) SizeLabel() string {
	if s.Workload.Target > 0 {
		return s.Workload.Target.Label()
	}
	return strconv.FormatUint(s.Workload.Keys, 10) + "keys"
}

// Label writes z in the largest binary unit at most z, as 20GiB or 1TiB,
// with one decimal when it is not a whole number of it, as 1.5GiB.
func (z Size) Label() string {
	for _, u := range units[:4] {
		if n := int64(u.scale); int64(z) >= n {
			if int64(z)%n == 0 {
				return strconv.FormatInt(int64(z)/n, 10) + u.suffix
			}
			if u.suffix != "KiB" {
				return strconv.FormatFloat(float64(z)/u.scale, 'f', 1, 64) + u.suffix
			}
		}
	}
	return strconv.FormatInt(int64(z), 10) + "B"
}

// Size is a byte count written as 64MiB, 1TiB, 12MB or a plain number.
type Size int64

var units = []struct {
	suffix string
	scale  float64
}{
	{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
	{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1},
}

func ParseSize(s string) (Size, error) {
	s = strings.TrimSpace(s)
	scale := 1.0
	for _, u := range units {
		if rest, ok := strings.CutSuffix(s, u.suffix); ok {
			s, scale = strings.TrimSpace(rest), u.scale
			break
		}
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%q is not a size such as 64MiB", s)
	}
	return Size(v * scale), nil
}

func (z *Size) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseSize(n.Value)
	*z = v
	return err
}

// Duration is a time.Duration written as 250ms or 2m.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := time.ParseDuration(n.Value)
	*d = Duration(v)
	return err
}

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}

// Spec lists every isledb version the spec restarts a role at.
func (s *Spec) Versions() []string {
	var out []string
	add := func(ev RestartEvent) {
		if ev.Isledb != "" && !slices.Contains(out, ev.Isledb) {
			out = append(out, ev.Isledb)
		}
	}
	for _, p := range s.Phases {
		if p.Restart != nil {
			add(*p.Restart)
		}
		for _, w := range []*WritePhase{p.Load, p.Update} {
			if w != nil {
				for _, ev := range w.Events {
					add(ev)
				}
			}
		}
	}
	return out
}
