package runner

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func loadSpec(t *testing.T, body string) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spec.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadSpec(path)
	return err
}

const baseSpec = `
name: t
workload: {keys: 10}
storage: {bucket_url: "file:///b", endpoint: "http://s3", latency: {min: 1ms, max: 2ms}}
`

func TestSpecMistakesFailWhenLoaded(t *testing.T) {
	for _, c := range []struct{ name, body, want string }{
		{"no keys", "name: t\nworkload: {}\nstorage: {bucket_url: x}\nphases: [{verify: {}}]", "give keys or target\n"},
		{"both", "name: t\nworkload: {keys: 1, target: 1MiB}\nstorage: {bucket_url: x}\nphases: [{verify: {}}]", "not both"},
		{"event role", baseSpec + "phases: [{load: {events: [{after: 2h, role: reader-2}]}}]", `role "reader-2"`},
		{"restart role", baseSpec + "phases: [{restart: {role: cache}}]", `role "cache"`},
		{"fault typo", baseSpec + "phases: [{load: {faults: [{proxy: writer, set: {drop_rat: 0.1}}]}}]", "drop_rat"},
		{"fault kind", baseSpec + "phases: [{load: {faults: [{proxy: writer, set: {kinds: [blob]}}]}}]", `kind "blob"`},
		{"fault rates", baseSpec + "phases: [{load: {faults: [{proxy: writer, set: {error_rate: 0.7, drop_rate: 0.7}}]}}]", "add up to at most 1"},
		{"fault proxy", baseSpec + "phases: [{load: {faults: [{proxy: cache, set: {}}]}}]", `proxy "cache"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := loadSpec(t, c.body)
			if err == nil || !strings.Contains(err.Error()+"\n", c.want) {
				t.Fatalf("error %v, want one naming %q", err, c.want)
			}
		})
	}
	if err := loadSpec(t, baseSpec+`phases:
  - load: {events: [{after: 1s, role: reader-1}], faults: [{proxy: writer, set: {methods: [PUT], kinds: [sst], drop_rate: 0.1, extra_delay: 5ms}}]}
  - restart: {role: maintenance, isledb: main}
`); err != nil {
		t.Fatalf("a good spec: %v", err)
	}
}

func TestSizeOverridesAndTheLadder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spec.yaml")
	if err := os.WriteFile(path, []byte(baseSpec+"phases: [{load: {}}, {update: {rounds: 2}}, {verify: {}}]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rate := Size(1 << 20)
	s, err := LoadSpecWith(path, Overrides{Target: 20 << 30, Rate: &rate})
	if err != nil {
		t.Fatal(err)
	}
	if s.Workload.Keys != 0 || s.Workload.Target != 20<<30 || s.Workload.Rate != 1<<20 {
		t.Fatalf("overrides not applied: %+v", s.Workload)
	}
	if s.SizeLabel() != "20GiB" || s.Planned() != 60<<30 {
		t.Fatalf("label %s, planned %s", s.SizeLabel(), s.Planned())
	}
	// Three rounds of 20 GiB: the ladder up to 60 GiB.
	want := []Size{1 << 30, 5 << 30, 10 << 30, 20 << 30, 50 << 30}
	if !slices.Equal(s.Checkpoints, want) {
		t.Fatalf("checkpoints %v, want %v", s.Checkpoints, want)
	}
	for z, want := range map[Size]string{1 << 40: "1TiB", 512 << 20: "512MiB", 3 << 29: "1.5GiB", 100: "100B"} {
		if got := z.Label(); got != want {
			t.Errorf("Label(%d) = %s, want %s", z, got, want)
		}
	}
	if s, err := LoadSpec(path); err != nil || s.SizeLabel() != "10keys" {
		t.Fatalf("a spec with keys: %v %v", s, err)
	}
	if err := loadSpec(t, baseSpec+"phases: [{verify: {}}]\n"+`checkpoints: [3MiB]`+"\n"); err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(baseSpec, "latency:", "disk: {path: /, max_used_pct: 100}, latency:", 1)
	if err := loadSpec(t, bad+"phases: [{verify: {}}]\n"); err == nil || !strings.Contains(err.Error(), "max_used_pct") {
		t.Fatalf("a disk limit of 100%%: %v", err)
	}
}
