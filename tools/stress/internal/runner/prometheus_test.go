package runner

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/tools/stress/internal/client"
	"github.com/ankur-anand/isledb/tools/stress/internal/ledger"
	"github.com/ankur-anand/isledb/tools/stress/internal/load"
	"github.com/ankur-anand/isledb/tools/stress/internal/read"
	"github.com/ankur-anand/isledb/tools/stress/internal/verify"
	"github.com/ankur-anand/isledb/tools/stress/internal/workload"
)

func TestCheckLoopback(t *testing.T) {
	for _, ok := range []string{"127.0.0.1:9400", "[::1]:9400", "localhost:9400", "127.0.0.1:0"} {
		if err := CheckLoopback(ok); err != nil {
			t.Errorf("%s refused: %v", ok, err)
		}
	}
	for _, bad := range []string{":9400", "0.0.0.0:9400", "10.0.0.5:9400", "[::]:9400", "example.com:9400", "127.0.0.1", "127.0.0.1:x"} {
		if err := CheckLoopback(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func testRunState(t *testing.T) *runState {
	t.Helper()
	d, _ := workload.ParseSizeDist("fixed:64")
	s, _ := workload.NewSchedule(100, 10, 1, 0, d)
	shape, _ := workload.NewShape("random", 100, 1)
	l := ledger.New()
	a, _ := l.Begin(0)
	_ = a.Committed(3)
	st := &runState{
		spec:     &Spec{Name: "default", Workload: WorkloadSpec{Target: 20 << 30}, Phases: []PhaseSpec{{Load: &WritePhase{}}}},
		info:     RunInfo{Name: "default-20GiB-v1-x", Isledb: Binary{Version: "v1", Commit: "abc"}},
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		sched:    s,
		shape:    shape,
		ledger:   l,
		counters: &load.Counters{},
		groups:   map[string]*read.Reader{},
		cluster:  &Cluster{Roles: map[string]*Role{"writer": {Name: "writer", Admin: "127.0.0.1:1001"}, "reader-1": {Name: "reader-1", Admin: "127.0.0.1:1002"}}, Proxies: map[string]*Proxy{}},
		phase:    "load",
		started:  time.Now(),
	}
	st.verifier = verify.New(&verify.Model{Schedule: s, Ledger: l}, shape, nil)
	st.counters.Bytes.Store(1 << 20)
	st.counters.Commit.Observe(30 * time.Millisecond)
	st.counters.Commit.Observe(400 * time.Millisecond)
	r, err := read.New(read.Config{Name: "uniform", Schedule: s, Shape: shape, Ledger: l, Verifier: st.verifier,
		Readers: []*client.Client{client.New("http://127.0.0.1:1", "")}})
	if err != nil {
		t.Fatal(err)
	}
	r.C.Gets.Add(5)
	r.C.Get.Observe(2 * time.Millisecond)
	st.groups["uniform"] = r
	u := BucketUsage{Time: time.Now(), Objects: map[string]int64{"sst": 4}, Bytes: map[string]int64{"sst": 3 << 20}}
	st.usage.Store(&u)
	st.counters.Live.Store(1 << 20)
	st.lastDisk = &DiskSample{Path: "/data", UsedPct: 42, Free: 1 << 40}
	return st
}

var promLine = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})? (-?[0-9.e+-]+|\+Inf|NaN)$`)

func TestExpositionIsValidPrometheusText(t *testing.T) {
	st := testRunState(t)
	out := string(st.exposition(context.Background()))
	help, typ := map[string]int{}, map[string]string{}
	bucketsBy := map[string][]float64{}
	countOf := map[string]float64{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		switch {
		case strings.HasPrefix(line, "# HELP "):
			help[strings.Fields(line)[2]]++
		case strings.HasPrefix(line, "# TYPE "):
			f := strings.Fields(line)
			typ[f[2]] = f[3]
		default:
			m := promLine.FindStringSubmatch(line)
			if m == nil {
				t.Fatalf("not a Prometheus sample: %q", line)
			}
			name := m[1]
			family := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_sum"), "_count")
			if typ[family] == "" && typ[name] == "" {
				t.Fatalf("%s sampled before its TYPE", name)
			}
			v, _ := strconv.ParseFloat(m[3], 64)
			series := family + regexp.MustCompile(`,?le="[^"]*"`).ReplaceAllString(m[2], "")
			switch {
			case strings.HasSuffix(name, "_bucket"):
				bucketsBy[series] = append(bucketsBy[series], v)
			case strings.HasSuffix(name, "_count") && typ[family] == "histogram":
				countOf[series] = v
			}
		}
	}
	for name, n := range help {
		if n != 1 {
			t.Errorf("%s has %d HELP lines", name, n)
		}
	}
	if typ["stress_commit_seconds"] != "histogram" || len(bucketsBy) == 0 {
		t.Fatalf("no commit histogram:\n%s", out)
	}
	for series, bs := range bucketsBy {
		for i := 1; i < len(bs); i++ {
			if bs[i] < bs[i-1] {
				t.Fatalf("%s buckets fall: %v", series, bs)
			}
		}
		if inf := bs[len(bs)-1]; inf != countOf[strings.Replace(series, "{}", "", 1)] && inf != countOf[series] {
			t.Fatalf("%s: +Inf bucket %v, count %v", series, inf, countOf[series])
		}
	}
	for _, want := range []string{
		`stress_run_info{spec="default",size="20GiB",commit="abc"} 1`,
		`stress_phase{phase="load"} 1`,
		`stress_written_bytes_total 1.048576e+06`,
		`stress_commit_seconds_bucket{le="0.05"} 1`,
		`stress_commit_seconds_count 2`,
		`stress_writes_total{outcome="committed"}`,
		`stress_verify_failures_total{kind="missing"} 0`,
		`stress_ledger_max_revision 3`,
		`stress_space_amplification 3`,
		`stress_disk_used_ratio{path="/data"} 0.42`,
		`stress_disk_held{path="/data"} 0`,
		`stress_reads_total{group="uniform",op="get",result="ok"} 5`,
		`stress_read_seconds_bucket{group="uniform",op="get",le="0.0025"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition lacks %s", want)
		}
	}
	if strings.Contains(out, `role=`) {
		t.Error("a metric carries role, which the scrape target's label holds")
	}
}

func TestTargetsFileListsEveryRoleAndGoesAway(t *testing.T) {
	st := testRunState(t)
	dir := t.TempDir()
	path, err := st.writeTargets(dir, "127.0.0.1:9400")
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ts []promTarget
	if err := json.Unmarshal(b, &ts); err != nil {
		t.Fatal(err)
	}
	roles := map[string]string{}
	for _, tg := range ts {
		if tg.Labels["run"] != st.info.Name || tg.Labels["version"] != "v1" || len(tg.Targets) != 1 {
			t.Fatalf("target %+v", tg)
		}
		roles[tg.Labels["role"]] = tg.Targets[0]
	}
	if roles["runner"] != "127.0.0.1:9400" || roles["writer"] != "127.0.0.1:1001" || roles["reader-1"] != "127.0.0.1:1002" {
		t.Fatalf("targets %v", roles)
	}
	if filepath.Dir(path) != dir {
		t.Fatalf("written to %s", path)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(left) != 0 {
		t.Fatalf("left %v", left)
	}
}
