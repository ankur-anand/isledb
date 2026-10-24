package maintenancerole

import (
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	cycles, cycleErrors                   prometheus.Counter
	running, lastCycle, waitingForWriter  prometheus.Gauge
	waitingSince                          prometheus.Gauge
	jobs, l0Merges, movedJobs, checkpoint prometheus.Counter
	readBytes, rewrittenBytes, movedBytes prometheus.Counter
	oversizedJobs, rejectedCommands       prometheus.Counter
	rejectedBytes                         prometheus.Counter
	deleted, deleteFailures               *prometheus.CounterVec
}

func newMetrics() *metrics {
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: "isledb_maintenance_" + name, Help: help})
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Name: "isledb_maintenance_" + name, Help: help})
	}
	byKind := func(name, help string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: "isledb_maintenance_" + name, Help: help}, []string{"kind"})
	}
	return &metrics{
		cycles:           counter("cycles_total", "Maintenance cycles that completed."),
		cycleErrors:      counter("cycle_errors_total", "Maintenance cycles that failed; they are retried."),
		running:          gauge("running", "1 while maintenance works on the database, 0 after another process took it over."),
		lastCycle:        gauge("last_cycle_timestamp_seconds", "Unix time the last cycle completed."),
		waitingForWriter: gauge("waiting_for_writer", "1 while a command waits for the writer to apply it."),
		waitingSince:     gauge("waiting_for_writer_since_timestamp_seconds", "Unix time the command the writer has yet to answer was staged; 0 when none waits."),
		jobs:             counter("compaction_jobs_total", "Compaction jobs, metadata-only moves and L0 merges included, counted when run; rejected ones included."),
		l0Merges:         counter("compaction_l0_merges_total", "Compaction jobs that merged L0 files with each other."),
		movedJobs:        counter("compaction_moves_total", "Compaction jobs that moved files to another level without rewriting them."),
		readBytes:        counter("compaction_read_bytes_total", "Bytes compaction read from storage."),
		rewrittenBytes:   counter("compaction_rewritten_bytes_total", "Bytes compaction wrote to storage, rejected jobs included."),
		movedBytes:       counter("compaction_moved_bytes_total", "Bytes of files moved without being rewritten."),
		checkpoint:       counter("checkpoints_total", "Manifest checkpoints prepared for the writer to publish."),
		oversizedJobs:    counter("compaction_oversized_jobs_total", "Compaction jobs over the input size limit, run because the destination level could not be drained first."),
		rejectedCommands: counter("commands_rejected_total", "Commands the writer rejected; the writer logs why."),
		rejectedBytes:    counter("compaction_rejected_bytes_total", "Bytes compaction wrote for jobs the writer then rejected."),
		deleted:          byKind("deleted_objects_total", "Objects deleted from storage, by kind: sst, change_batch, snapshot or page."),
		deleteFailures:   byKind("delete_failures_total", "Deletes that failed, by kind; they are retried."),
	}
}

func (m *metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.cycles, m.cycleErrors, m.running, m.lastCycle, m.waitingForWriter, m.waitingSince, m.jobs, m.l0Merges,
		m.movedJobs, m.readBytes, m.rewrittenBytes, m.movedBytes, m.checkpoint, m.oversizedJobs, m.rejectedCommands,
		m.rejectedBytes, m.deleted, m.deleteFailures}
}

func (m *metrics) cycle(s isledb.MaintenanceStats) {
	m.cycles.Inc()
	m.lastCycle.Set(float64(time.Now().UnixNano()) / 1e9)
	waiting := 0.0
	if s.State == isledb.MaintenanceWaitingForWriter {
		waiting = 1
	}
	m.waitingForWriter.Set(waiting)
	since := 0.0
	if !s.WaitingSince.IsZero() {
		since = float64(s.WaitingSince.UnixNano()) / 1e9
	}
	m.waitingSince.Set(since)
	c := s.SSTCompaction
	m.jobs.Add(float64(c.Jobs))
	m.l0Merges.Add(float64(c.L0Merges))
	m.movedJobs.Add(float64(c.MovedJobs))
	m.readBytes.Add(float64(c.ReadBytes))
	m.rewrittenBytes.Add(float64(c.RewrittenBytes))
	m.movedBytes.Add(float64(c.MovedBytes))
	m.oversizedJobs.Add(float64(c.OversizedJobs))
	m.rejectedBytes.Add(float64(c.RejectedBytes))
	m.rejectedCommands.Add(float64(s.RejectedCommands))
	if s.ManifestCheckpoint.Staged {
		m.checkpoint.Inc()
	}
}

func (m *metrics) reclamation(s isledb.ReclamationCycleStats) {
	add := func(kind string, deleted, failed int) {
		m.deleted.WithLabelValues(kind).Add(float64(deleted))
		m.deleteFailures.WithLabelValues(kind).Add(float64(failed))
	}
	switch s.Family {
	case isledb.ReclamationSST:
		add("sst", s.SST.SSTsDeleted+s.SST.OrphansDeleted, s.SST.Failures)
	case isledb.ReclamationChangeFeed:
		add("change_batch", s.ChangeFeed.BatchesDeleted, s.ChangeFeed.FailedDeletes)
	case isledb.ReclamationManifest:
		add("snapshot", s.Manifest.Snapshots.SnapshotsDeleted, s.Manifest.Snapshots.Failures)
		add("page", s.Manifest.Pages.PagesDeleted, s.Manifest.Pages.Failures)
	}
}
