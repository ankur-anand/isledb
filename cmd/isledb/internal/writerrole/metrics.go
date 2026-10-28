package writerrole

import (
	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/internal/writehttp"
	"github.com/prometheus/client_golang/prometheus"
)

// newWriterMetrics returns the writer's own observations and their
// collectors.
func newWriterMetrics() (*isledb.WriterMetrics, []prometheus.Collector) {
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: "isledb_writer_" + name, Help: help})
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Name: "isledb_writer_" + name, Help: help})
	}
	m := &isledb.WriterMetrics{
		FlushTotal:  counter("commits_total", "Commit attempts."),
		FlushErrors: counter("commit_errors_total", "Commit attempts that failed; they are retried."),
		FlushLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "isledb_writer_commit_seconds", Help: "Time a commit attempt took.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
		}),
		FlushBytes: counter("commit_bytes_total", "Bytes of SSTs committed."),
		// PutErrors is left unset: writes arrive as batches, whose refusals
		// isledb_writer_http_requests_total counts by status.
		PutTotal:            counter("puts_total", "Puts accepted."),
		BackPressureTotal:   counter("backpressure_total", "Writes refused because too many memtables waited to commit."),
		DeleteTotal:         counter("deletes_total", "Deletes accepted."),
		CommittedSequence:   gauge("committed_sequence", "Highest sequence committed to storage."),
		OldestUncommitted:   gauge("oldest_uncommitted_timestamp_seconds", "Unix time the oldest write not yet committed was accepted, 0 when none is waiting."),
		L0MergeTotal:        counter("l0_merges_total", "Merged L0 files the writer uploaded."),
		L0MergeErrors:       counter("l0_merge_errors_total", "L0 merge attempts that failed; they are retried."),
		L0MergeBytes:        counter("l0_merge_bytes_total", "Bytes of merged L0 files uploaded."),
		L0MergeSkippedBytes: counter("l0_merge_skipped_bytes_total", "Bytes of flushes committed without a held copy because the held limit was reached."),
		L0MergeHeldBytes:    gauge("l0_merge_held_bytes", "Bytes of committed flushes held in memory for merging."),
		L0MergePublished:    counter("l0_merges_published_total", "Merged L0 files that replaced their inputs in L0."),
		L0MergeRefused:      counter("l0_merges_refused_total", "Merges refused because their inputs were no longer a run in L0."),
		L0MergeDropped:      counter("l0_merges_dropped_total", "Held runs and unpublished merges given up because maintenance took their files."),
		MaintenancePublishDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "isledb_writer_maintenance_publish_delay_seconds", Help: "Time from when maintenance staged a command to when the writer published it, applied or rejected.",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600},
		}),
		MaintenanceApplyLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "isledb_writer_maintenance_apply_seconds", Help: "Time an attempt to publish a maintenance command took, by outcome: applied, rejected or failed.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
		}, []string{"outcome"}),
	}
	return m, []prometheus.Collector{m.FlushTotal, m.FlushErrors, m.FlushLatency, m.FlushBytes, m.PutTotal,
		m.BackPressureTotal, m.DeleteTotal, m.CommittedSequence, m.OldestUncommitted,
		m.L0MergeTotal, m.L0MergeErrors, m.L0MergeBytes, m.L0MergeSkippedBytes, m.L0MergeHeldBytes,
		m.L0MergePublished, m.L0MergeRefused, m.L0MergeDropped, m.MaintenancePublishDelay, m.MaintenanceApplyLatency}
}

// collector reports the writer's state as it is at each scrape.
type collector struct {
	writer  *isledb.Writer
	handler *writehttp.Handler
}

var (
	descStatus           = prometheus.NewDesc("isledb_writer_status", "1 for the writer's status: open, stopped, closed or fenced.", []string{"status"}, nil)
	descEpoch            = prometheus.NewDesc("isledb_writer_epoch", "The writer's fence epoch.", nil, nil)
	descAccepted         = prometheus.NewDesc("isledb_writer_accepted_sequence", "Highest sequence handed to a write, committed or not.", nil, nil)
	descPendingMemtables = prometheus.NewDesc("isledb_writer_pending_memtables", "Memtables not yet committed, the active one included.", nil, nil)
	descPendingBytes     = prometheus.NewDesc("isledb_writer_pending_bytes", "Approximate bytes of writes not yet committed.", nil, nil)
	descInflightBytes    = prometheus.NewDesc("isledb_writer_http_inflight_body_bytes", "Request body bytes reserved by writes being read or applied.", nil, nil)
)

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descStatus, descEpoch, descAccepted, descPendingMemtables, descPendingBytes, descInflightBytes} {
		ch <- d
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	state := c.writer.State()
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	for _, s := range []isledb.WriterStatus{isledb.WriterOpen, isledb.WriterStopped, isledb.WriterClosed, isledb.WriterFenced} {
		v := 0.0
		if state.Status == s {
			v = 1
		}
		gauge(descStatus, v, s.String())
	}
	gauge(descEpoch, float64(state.Epoch))
	gauge(descAccepted, float64(state.Accepted))
	gauge(descPendingMemtables, float64(state.PendingMemtables))
	gauge(descPendingBytes, float64(state.PendingBytes))
	gauge(descInflightBytes, float64(c.handler.InflightBytes()))
}
