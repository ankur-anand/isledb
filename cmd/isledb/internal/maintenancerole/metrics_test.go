package maintenancerole

import (
	"testing"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestShapeMetrics(t *testing.T) {
	m := newMetrics()
	at := time.Unix(1700000000, 0)
	m.shape(isledb.LSMShape{
		L0:      isledb.LevelShape{Files: 40, Bytes: 2 << 30},
		L0Depth: 17,
		Levels: []isledb.LevelShape{
			{Level: 1, Files: 60, Bytes: 4 << 30, TargetBytes: 4 << 30},
			{Level: 2, Files: 200, Bytes: 14 << 30, TargetBytes: 32 << 30},
		},
	}, at)
	for name, c := range map[string]struct{ got, want float64 }{
		"l0 files":  {testutil.ToFloat64(m.l0Files), 40},
		"l0 bytes":  {testutil.ToFloat64(m.l0Bytes), 2 << 30},
		"l0 depth":  {testutil.ToFloat64(m.l0Depth), 17},
		"read at":   {testutil.ToFloat64(m.shapeTime), 1700000000},
		"L1 files":  {testutil.ToFloat64(m.levelFiles.WithLabelValues("1")), 60},
		"L2 bytes":  {testutil.ToFloat64(m.levelBytes.WithLabelValues("2")), 14 << 30},
		"L2 target": {testutil.ToFloat64(m.levelTarget.WithLabelValues("2")), 32 << 30},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", name, c.got, c.want)
		}
	}
	// L2 merged away: it reads 0, not its last size.
	m.shape(isledb.LSMShape{Levels: []isledb.LevelShape{{Level: 1, Files: 3, Bytes: 9, TargetBytes: 4 << 30}}}, at)
	if testutil.ToFloat64(m.levelFiles.WithLabelValues("2")) != 0 || testutil.ToFloat64(m.levelBytes.WithLabelValues("2")) != 0 {
		t.Fatal("a level no longer there kept its old size")
	}
}

func TestCompactionCyclesAreTimedAndMarked(t *testing.T) {
	m := newMetrics()
	compaction := isledb.MaintenanceScheduleStats{Selected: isledb.MaintenanceTaskSSTCompaction, CompactionCritical: true}
	m.cycle(isledb.MaintenanceStats{Scheduling: compaction, SSTCompaction: isledb.SSTCompactionStats{Jobs: 1, RewrittenBytes: 1,
		Runs: []isledb.CompactionRun{{DestinationLevel: 1, Duration: 42 * time.Second}}}})
	if testutil.ToFloat64(m.critical) != 1 || testutil.CollectAndCount(m.jobSeconds) != 1 {
		t.Fatalf("critical %v, histograms %d", testutil.ToFloat64(m.critical), testutil.CollectAndCount(m.jobSeconds))
	}
	// A cycle that only waits keeps the last job's mark.
	m.cycle(isledb.MaintenanceStats{State: isledb.MaintenanceWaitingForWriter})
	if testutil.ToFloat64(m.critical) != 1 {
		t.Fatal("a cycle without a job changed compaction_critical")
	}
	compaction.CompactionCritical = false
	m.cycle(isledb.MaintenanceStats{Scheduling: compaction, SSTCompaction: isledb.SSTCompactionStats{Jobs: 1, RewrittenBytes: 1}})
	if testutil.ToFloat64(m.critical) != 0 {
		t.Fatal("a job that wasn't urgent left compaction_critical at 1")
	}
}

func TestCompactionJobsAndBlocksByLevel(t *testing.T) {
	m := newMetrics()
	fromL2 := isledb.MaintenanceScheduleStats{Selected: isledb.MaintenanceTaskSSTCompaction, CompactionSourceLevel: 2,
		Blocked: []isledb.CompactionBlocked{{SourceLevel: 3}}}
	fromL2Run := []isledb.CompactionRun{{SourceLevel: 2, DestinationLevel: 3}}
	m.cycle(isledb.MaintenanceStats{Scheduling: fromL2, SSTCompaction: isledb.SSTCompactionStats{Jobs: 1, RewrittenBytes: 1, Runs: fromL2Run}})
	m.cycle(isledb.MaintenanceStats{Scheduling: fromL2, SSTCompaction: isledb.SSTCompactionStats{Jobs: 1, RewrittenBytes: 1, Runs: fromL2Run}})
	// A checkpoint ran instead: no job, but L3 was still blocked.
	m.cycle(isledb.MaintenanceStats{Scheduling: isledb.MaintenanceScheduleStats{Selected: isledb.MaintenanceTaskManifestCheckpoint,
		Blocked: []isledb.CompactionBlocked{{SourceLevel: 3}}}})
	for name, c := range map[string]struct{ got, want float64 }{
		"jobs from L2": {testutil.ToFloat64(m.levelJobs.WithLabelValues("2")), 2},
		"L3 blocked":   {testutil.ToFloat64(m.blocked.WithLabelValues("3")), 3},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", name, c.got, c.want)
		}
	}
	if n := testutil.CollectAndCount(m.levelJobs); n != 1 {
		t.Errorf("jobs counted for %d levels, want only L2", n)
	}
}

func TestJobKind(t *testing.T) {
	for want, run := range map[string]isledb.CompactionRun{
		"l0_drain": {SourceLevel: 0, DestinationLevel: 1},
		"l0_merge": {SourceLevel: 0, DestinationLevel: 0},
		"move":     {SourceLevel: 2, DestinationLevel: 3, MetadataOnly: true},
		"level":    {SourceLevel: 2, DestinationLevel: 3},
	} {
		if got := jobKind(run); got != want {
			t.Errorf("jobKind = %s, want %s", got, want)
		}
	}
}

// Jobs run beside the cycle are counted one by one, each by its own level
// and duration, however many end in one cycle.
func TestCompactionRunsCountOneByOne(t *testing.T) {
	m := newMetrics()
	m.cycle(isledb.MaintenanceStats{SSTCompaction: isledb.SSTCompactionStats{Jobs: 2, Runs: []isledb.CompactionRun{
		{SourceLevel: 0, DestinationLevel: 1, Duration: time.Minute},
		{SourceLevel: 2, DestinationLevel: 3, Duration: 2 * time.Minute},
	}}})
	if testutil.ToFloat64(m.levelJobs.WithLabelValues("0")) != 1 || testutil.ToFloat64(m.levelJobs.WithLabelValues("2")) != 1 {
		t.Fatal("each job was not counted by its own level")
	}
	if n := testutil.CollectAndCount(m.jobSeconds); n != 2 {
		t.Fatalf("job durations for %d kinds, want l0_drain and level", n)
	}
}
