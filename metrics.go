package isledb

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type WriterMetrics struct {
	FlushTotal        prometheus.Counter
	FlushErrors       prometheus.Counter
	FlushLatency      prometheus.Histogram
	FlushBytes        prometheus.Counter
	PutTotal          prometheus.Counter
	PutErrors         prometheus.Counter
	BackPressureTotal prometheus.Counter
	DeleteTotal       prometheus.Counter

	// CommittedSequence is the highest change sequence committed to object
	// storage and visible to readers. A writer sets it on open from the
	// manifest, then after each memtable it commits.
	CommittedSequence prometheus.Gauge

	// OldestUncommitted is the Unix time the oldest write not yet committed
	// was accepted, or 0 when every write is committed: time() minus it is how
	// long writes have waited to become durable.
	OldestUncommitted prometheus.Gauge

	// L0MergeTotal counts merged L0 files the writer uploaded; L0MergeErrors
	// counts merge attempts that failed and are retried.
	L0MergeTotal  prometheus.Counter
	L0MergeErrors prometheus.Counter
	// L0MergeBytes is the bytes of merged L0 files uploaded: the writer's
	// share of write amplification.
	L0MergeBytes prometheus.Counter
	// L0MergeSkippedBytes is the bytes of flushes committed without a held
	// copy because the writer already held as much as it may.
	L0MergeSkippedBytes prometheus.Counter
	// L0MergeHeldBytes is the bytes of committed flushes the writer holds
	// for merging.
	L0MergeHeldBytes prometheus.Gauge
}

func (m *WriterMetrics) incCounter(counter prometheus.Counter) {
	if m == nil || counter == nil {
		return
	}
	counter.Inc()
}

func (m *WriterMetrics) addCounter(counter prometheus.Counter, value float64) {
	if m == nil || counter == nil || value == 0 {
		return
	}
	counter.Add(value)
}

func (m *WriterMetrics) observeHistogram(histogram prometheus.Histogram, value float64) {
	if m == nil || histogram == nil {
		return
	}
	histogram.Observe(value)
}

func (m *WriterMetrics) ObservePut(err error) {
	if m == nil {
		return
	}
	m.incCounter(m.PutTotal)
	if err != nil {
		m.incCounter(m.PutErrors)
	}
}

func (m *WriterMetrics) ObserveBackpressure() {
	if m == nil {
		return
	}
	m.incCounter(m.BackPressureTotal)
}

func (m *WriterMetrics) ObserveDelete() {
	if m == nil {
		return
	}
	m.incCounter(m.DeleteTotal)
}

// ObserveOldestUncommitted records when the oldest uncommitted write was
// accepted; the zero time means none is uncommitted.
func (m *WriterMetrics) ObserveOldestUncommitted(at time.Time) {
	if m == nil || m.OldestUncommitted == nil {
		return
	}
	if at.IsZero() {
		m.OldestUncommitted.Set(0)
		return
	}
	m.OldestUncommitted.Set(float64(at.UnixNano()) / 1e9)
}

// ObserveCommittedSequence records the highest change sequence committed.
func (m *WriterMetrics) ObserveCommittedSequence(seq uint64) {
	if m == nil || m.CommittedSequence == nil {
		return
	}
	m.CommittedSequence.Set(float64(seq))
}

func (m *WriterMetrics) ObserveFlushBytes(sizeBytes int64) {
	if m == nil {
		return
	}
	if sizeBytes > 0 {
		m.addCounter(m.FlushBytes, float64(sizeBytes))
	}
}

// ObserveL0Merge records one attempt to merge held L0 files: the output's
// size when it uploaded, or the error.
func (m *WriterMetrics) ObserveL0Merge(outputBytes int64, err error) {
	if m == nil {
		return
	}
	if err != nil {
		m.incCounter(m.L0MergeErrors)
		return
	}
	m.incCounter(m.L0MergeTotal)
	m.addCounter(m.L0MergeBytes, float64(outputBytes))
}

// ObserveL0MergeSkipped records a flush committed without a held copy.
func (m *WriterMetrics) ObserveL0MergeSkipped(sizeBytes int64) {
	if m == nil {
		return
	}
	m.addCounter(m.L0MergeSkippedBytes, float64(sizeBytes))
}

// ObserveL0MergeHeld records how many bytes the writer holds for merging.
func (m *WriterMetrics) ObserveL0MergeHeld(heldBytes int64) {
	if m == nil || m.L0MergeHeldBytes == nil {
		return
	}
	m.L0MergeHeldBytes.Set(float64(heldBytes))
}

func (m *WriterMetrics) ObserveFlush(d time.Duration, err error) {
	if m == nil {
		return
	}
	m.incCounter(m.FlushTotal)
	m.observeHistogram(m.FlushLatency, d.Seconds())
	if err != nil {
		m.incCounter(m.FlushErrors)
	}
}

func DefaultWriterMetrics(constLabels prometheus.Labels) *WriterMetrics {
	return &WriterMetrics{
		OldestUncommitted: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "oldest_uncommitted_timestamp_seconds",
			Help:        "Unix time the oldest write not yet committed was accepted; 0 when all are committed.",
			ConstLabels: constLabels,
		}),
		CommittedSequence: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "committed_sequence",
			Help:        "Highest change sequence committed to object storage and visible to readers.",
			ConstLabels: constLabels,
		}),
		FlushTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "flush_total",
			Help:        "Total number of memtable flushes.",
			ConstLabels: constLabels,
		}),
		FlushErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "flush_errors",
			Help:        "Number of errors encountered during flush",
			ConstLabels: constLabels,
		}),
		FlushLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "flush_latency_seconds",
			Help:        "Histogram of memtable flush latency in seconds",
			ConstLabels: constLabels,
		}),
		FlushBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "flush_bytes_total",
			Help:        "Total number of bytes written to object Storage",
			ConstLabels: constLabels,
		}),
		PutTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "put_total",
			Help:        "Total Put Operations.",
			ConstLabels: constLabels,
		}),
		PutErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "put_errors_total",
			Help:        "Total Put Errors.",
			ConstLabels: constLabels,
		}),
		BackPressureTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "backpressure_total",
			Help:        "Total times ErrBackPressure was returned.",
			ConstLabels: constLabels,
		}),
		DeleteTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "delete_total",
			Help:        "Total Delete Operations.",
			ConstLabels: constLabels,
		}),
		L0MergeTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "l0_merges_total",
			Help:        "Merged L0 files the writer uploaded.",
			ConstLabels: constLabels,
		}),
		L0MergeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "l0_merge_errors_total",
			Help:        "L0 merge attempts that failed; they are retried.",
			ConstLabels: constLabels,
		}),
		L0MergeBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "l0_merge_bytes_total",
			Help:        "Bytes of merged L0 files uploaded.",
			ConstLabels: constLabels,
		}),
		L0MergeSkippedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "l0_merge_skipped_bytes_total",
			Help:        "Bytes of flushes committed without a held copy because the held limit was reached.",
			ConstLabels: constLabels,
		}),
		L0MergeHeldBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "l0_merge_held_bytes",
			Help:        "Bytes of committed flushes held in memory for merging.",
			ConstLabels: constLabels,
		}),
	}
}

type ReaderMetrics struct {
	RefreshTotal   prometheus.Counter
	RefreshErrors  prometheus.Counter
	RefreshLatency prometheus.Histogram
	// StaleReads counts reads answered from a view past its refresh time
	// because refreshing it failed.
	StaleReads prometheus.Counter
	// ViewLoaded is the Unix time the published manifest view was loaded;
	// time() minus it is the view's age.
	ViewLoaded prometheus.Gauge
	// ViewOutdatedSince is the Unix time the published view first became
	// outdated in Manual mode, the load time of a newer view not yet
	// published, or 0 when it is the newest loaded: time() minus it climbing
	// toward MaxLag means preparation is falling behind.
	ViewOutdatedSince prometheus.Gauge
	// NextViewPublishes counts NextView.Publish calls by result: published,
	// changed or expired.
	NextViewPublishes *prometheus.CounterVec
	// SafetyPublishes counts views the Reader published itself after MaxLag.
	SafetyPublishes prometheus.Counter
	// DeadSSTsPending is how many SSTs a publish retired that are not yet
	// deleted from the disk cache; one that keeps rising means deletes fall
	// behind compactions.
	DeadSSTsPending prometheus.Gauge

	GetTotal   prometheus.Counter
	GetErrors  prometheus.Counter
	GetHits    prometheus.Counter
	GetMisses  prometheus.Counter
	GetLatency prometheus.Histogram

	ScanTotal   prometheus.Counter
	ScanErrors  prometheus.Counter
	ScanLatency prometheus.Histogram
	ScanResults prometheus.Counter

	ScanLimitTotal   prometheus.Counter
	ScanLimitErrors  prometheus.Counter
	ScanLimitLatency prometheus.Histogram
	ScanLimitResults prometheus.Counter

	BloomFilterErrors prometheus.Counter
	BloomFilterSkips  prometheus.Counter

	SSTRangeReadTotal   prometheus.Counter
	SSTRangeReadErrors  prometheus.Counter
	SSTRangeReadLatency prometheus.Histogram
	SSTRangeReadBytes   prometheus.Counter

	// ReadCacheReads counts reads of SSTs and manifest pages and snapshots
	// the read cache answered; the SSTRangeRead metrics count only SST reads
	// from storage.
	ReadCacheReads prometheus.Counter
	// ReadCacheFallbacks counts those reads that went to storage because the
	// read cache failed, by reason: timeout, unavailable, not_found,
	// size_mismatch, refused or invalid; bypassed, sent straight to storage
	// because the cache had stopped answering; checksum_mismatch, bytes from
	// the cache that failed their checksum and were read again from storage
	// (also counted when the object itself is corrupt, which storage then
	// shows too);
	// or after_damage, a read of an SST found damaged in the last ten
	// minutes, sent to storage.
	ReadCacheFallbacks *prometheus.CounterVec
	// ReadCacheCurrent counts refreshes that asked the read cache for
	// CURRENT, by outcome: relayed, its copy used; too_old or behind, its
	// copy older than MaxCurrentAge or behind the published view; or failed,
	// no answer. All but relayed read CURRENT from storage instead.
	ReadCacheCurrent *prometheus.CounterVec
	// ReadCacheCurrentAge is the age, in seconds, of the last copy of
	// CURRENT the read cache gave.
	ReadCacheCurrentAge prometheus.Gauge
	// ReadCacheDoorbells counts answers to the long poll on the read cache,
	// by result: rang, a newer position, for which a refresh was requested;
	// quiet, none before the cache's timeout; or error.
	ReadCacheDoorbells *prometheus.CounterVec
}

func (m *ReaderMetrics) incCounter(counter prometheus.Counter) {
	if m == nil || counter == nil {
		return
	}
	counter.Inc()
}

func (m *ReaderMetrics) addCounter(counter prometheus.Counter, value float64) {
	if m == nil || counter == nil || value == 0 {
		return
	}
	counter.Add(value)
}

func (m *ReaderMetrics) observeHistogram(histogram prometheus.Histogram, value float64) {
	if m == nil || histogram == nil {
		return
	}
	histogram.Observe(value)
}

func (m *ReaderMetrics) ObserveRefresh(d time.Duration, err error) {
	if m == nil {
		return
	}
	m.incCounter(m.RefreshTotal)
	m.observeHistogram(m.RefreshLatency, d.Seconds())
	if err != nil {
		m.incCounter(m.RefreshErrors)
	}
}

func (m *ReaderMetrics) ObserveStaleRead() {
	if m == nil {
		return
	}
	m.incCounter(m.StaleReads)
}

func (m *ReaderMetrics) ObserveViewLoaded(at time.Time) {
	if m == nil || m.ViewLoaded == nil {
		return
	}
	m.ViewLoaded.Set(float64(at.UnixNano()) / 1e9)
}

// ObserveViewOutdatedSince records when the published view became outdated;
// the zero time means it is not.
func (m *ReaderMetrics) ObserveViewOutdatedSince(at time.Time) {
	if m == nil || m.ViewOutdatedSince == nil {
		return
	}
	if at.IsZero() {
		m.ViewOutdatedSince.Set(0)
		return
	}
	m.ViewOutdatedSince.Set(float64(at.UnixNano()) / 1e9)
}

func (m *ReaderMetrics) ObserveNextViewPublish(err error) {
	if m == nil || m.NextViewPublishes == nil {
		return
	}
	switch {
	case err == nil:
		m.NextViewPublishes.WithLabelValues("published").Inc()
	case errors.Is(err, ErrViewChanged):
		m.NextViewPublishes.WithLabelValues("changed").Inc()
	case errors.Is(err, ErrNextViewExpired):
		m.NextViewPublishes.WithLabelValues("expired").Inc()
	}
}

func (m *ReaderMetrics) ObserveSafetyPublish() {
	if m == nil {
		return
	}
	m.incCounter(m.SafetyPublishes)
}

func (m *ReaderMetrics) ObserveDeadSSTsPending(n int) {
	if m == nil || m.DeadSSTsPending == nil {
		return
	}
	m.DeadSSTsPending.Set(float64(n))
}

func (m *ReaderMetrics) ObserveGet(d time.Duration, found bool, err error) {
	if m == nil {
		return
	}
	m.incCounter(m.GetTotal)
	m.observeHistogram(m.GetLatency, d.Seconds())
	if err != nil {
		m.incCounter(m.GetErrors)
		return
	}
	if found {
		m.incCounter(m.GetHits)
		return
	}
	m.incCounter(m.GetMisses)
}

func (m *ReaderMetrics) ObserveScan(d time.Duration, resultCount int, err error) {
	if m == nil {
		return
	}
	m.incCounter(m.ScanTotal)
	m.observeHistogram(m.ScanLatency, d.Seconds())
	if err != nil {
		m.incCounter(m.ScanErrors)
		return
	}
	if resultCount > 0 {
		m.addCounter(m.ScanResults, float64(resultCount))
	}
}

func (m *ReaderMetrics) ObserveScanLimit(d time.Duration, resultCount int, err error) {
	if m == nil {
		return
	}
	m.incCounter(m.ScanLimitTotal)
	m.observeHistogram(m.ScanLimitLatency, d.Seconds())
	if err != nil {
		m.incCounter(m.ScanLimitErrors)
		return
	}
	if resultCount > 0 {
		m.addCounter(m.ScanLimitResults, float64(resultCount))
	}
}

func (m *ReaderMetrics) ObserveBloomFilterError() {
	if m == nil {
		return
	}
	m.incCounter(m.BloomFilterErrors)
}

func (m *ReaderMetrics) ObserveBloomFilterSkip() {
	if m == nil {
		return
	}
	m.incCounter(m.BloomFilterSkips)
}

func (m *ReaderMetrics) ObserveSSTRangeRead(d time.Duration, sizeBytes int64, err error) {
	if m == nil {
		return
	}
	m.incCounter(m.SSTRangeReadTotal)
	m.observeHistogram(m.SSTRangeReadLatency, d.Seconds())
	if err != nil {
		m.incCounter(m.SSTRangeReadErrors)
		return
	}
	if sizeBytes > 0 {
		m.addCounter(m.SSTRangeReadBytes, float64(sizeBytes))
	}
}

// ObserveReadCache records an SST read sent to the read cache: answered
// when reason is empty, else fallen back to storage for reason.
func (m *ReaderMetrics) ObserveReadCache(reason string) {
	if m == nil {
		return
	}
	if reason == "" {
		m.incCounter(m.ReadCacheReads)
		return
	}
	if m.ReadCacheFallbacks != nil {
		m.ReadCacheFallbacks.WithLabelValues(reason).Inc()
	}
}

// ObserveReadCacheCurrent records a refresh's request for CURRENT to the
// read cache, and the age of the copy it gave unless it failed.
func (m *ReaderMetrics) ObserveReadCacheCurrent(outcome string, age time.Duration) {
	if m == nil {
		return
	}
	if m.ReadCacheCurrent != nil {
		m.ReadCacheCurrent.WithLabelValues(outcome).Inc()
	}
	if outcome != "failed" && m.ReadCacheCurrentAge != nil {
		m.ReadCacheCurrentAge.Set(age.Seconds())
	}
}

// ObserveReadCacheDoorbell records an answer to the long poll on the read
// cache.
func (m *ReaderMetrics) ObserveReadCacheDoorbell(result string) {
	if m != nil && m.ReadCacheDoorbells != nil {
		m.ReadCacheDoorbells.WithLabelValues(result).Inc()
	}
}

func DefaultReaderMetrics(constLabels prometheus.Labels) *ReaderMetrics {
	return &ReaderMetrics{
		RefreshTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "refresh_total",
			Help:        "Total number of manifest refresh operations.",
			ConstLabels: constLabels,
		}),
		RefreshErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "refresh_errors_total",
			Help:        "Total number of manifest refresh errors.",
			ConstLabels: constLabels,
		}),
		RefreshLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "refresh_latency_seconds",
			Help:        "Histogram of reader manifest refresh latency in seconds.",
			ConstLabels: constLabels,
		}),
		StaleReads: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "stale_reads_total",
			Help:        "Reads answered from a manifest view past its refresh time because refreshing it failed.",
			ConstLabels: constLabels,
		}),
		ViewLoaded: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "view_loaded_timestamp_seconds",
			Help:        "Unix time the published manifest view was loaded.",
			ConstLabels: constLabels,
		}),
		ViewOutdatedSince: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "view_outdated_since_timestamp_seconds",
			Help:        "Unix time the published view became outdated in manual mode, or 0 when it is the newest loaded.",
			ConstLabels: constLabels,
		}),
		NextViewPublishes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "next_view_publishes_total",
			Help:        "NextView.Publish calls by result: published, changed or expired.",
			ConstLabels: constLabels,
		}, []string{"result"}),
		SafetyPublishes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "view_safety_publishes_total",
			Help:        "Views the reader published itself after MaxLag in manual mode.",
			ConstLabels: constLabels,
		}),
		DeadSSTsPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "dead_ssts_pending",
			Help:        "SSTs retired by a publish and not yet deleted from the disk cache.",
			ConstLabels: constLabels,
		}),
		GetTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "get_total",
			Help:        "Total number of Get operations.",
			ConstLabels: constLabels,
		}),
		GetErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "get_errors_total",
			Help:        "Total number of Get operation errors.",
			ConstLabels: constLabels,
		}),
		GetHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "get_hits_total",
			Help:        "Total number of successful Get lookups.",
			ConstLabels: constLabels,
		}),
		GetMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "get_misses_total",
			Help:        "Total number of Get lookups that missed.",
			ConstLabels: constLabels,
		}),
		GetLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "get_latency_seconds",
			Help:        "Histogram of Get latency in seconds.",
			ConstLabels: constLabels,
		}),
		ScanTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "scan_total",
			Help:        "Total number of Scan operations.",
			ConstLabels: constLabels,
		}),
		ScanErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "scan_errors_total",
			Help:        "Total number of Scan operation errors.",
			ConstLabels: constLabels,
		}),
		ScanLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "scan_latency_seconds",
			Help:        "Histogram of Scan latency in seconds.",
			ConstLabels: constLabels,
		}),
		ScanResults: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "scan_results_total",
			Help:        "Total number of key/value results returned by Scan.",
			ConstLabels: constLabels,
		}),
		ScanLimitTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "scan_limit_total",
			Help:        "Total number of ScanLimit operations.",
			ConstLabels: constLabels,
		}),
		ScanLimitErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "scan_limit_errors_total",
			Help:        "Total number of ScanLimit operation errors.",
			ConstLabels: constLabels,
		}),
		ScanLimitLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "scan_limit_latency_seconds",
			Help:        "Histogram of ScanLimit latency in seconds.",
			ConstLabels: constLabels,
		}),
		ScanLimitResults: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "scan_limit_results_total",
			Help:        "Total number of key/value results returned by ScanLimit.",
			ConstLabels: constLabels,
		}),
		BloomFilterErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "bloom_filter_errors_total",
			Help:        "Total Bloom-filter loading, verification, or decoding errors; operations either recover from origin or continue to the SST.",
			ConstLabels: constLabels,
		}),
		BloomFilterSkips: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "bloom_filter_skips_total",
			Help:        "Total point lookups that read an SST without its Bloom filter because the filter was still loading in the background.",
			ConstLabels: constLabels,
		}),
		SSTRangeReadTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_range_read_total",
			Help:        "Total number of SST range-read object-store fetch attempts.",
			ConstLabels: constLabels,
		}),
		SSTRangeReadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_range_read_errors_total",
			Help:        "Total number of SST range-read object-store fetch errors.",
			ConstLabels: constLabels,
		}),
		SSTRangeReadLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_range_read_latency_seconds",
			Help:        "Histogram of SST range-read object-store fetch latency in seconds.",
			ConstLabels: constLabels,
		}),
		SSTRangeReadBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_range_read_bytes_total",
			Help:        "Total number of SST bytes fetched through range-read requests.",
			ConstLabels: constLabels,
		}),
		ReadCacheReads: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "read_cache_reads_total",
			Help:        "Reads of SSTs and manifest pages and snapshots the read cache answered.",
			ConstLabels: constLabels,
		}),
		ReadCacheFallbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "read_cache_fallbacks_total",
			Help:        "Reads of SSTs and manifest pages and snapshots that went to storage because the read cache failed, by reason.",
			ConstLabels: constLabels,
		}, []string{"reason"}),
		ReadCacheCurrent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "read_cache_current_total",
			Help:        "Refreshes that asked the read cache for CURRENT, by outcome: relayed, too_old, behind or failed.",
			ConstLabels: constLabels,
		}, []string{"outcome"}),
		ReadCacheCurrentAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "read_cache_current_age_seconds",
			Help:        "Age of the last copy of CURRENT the read cache gave.",
			ConstLabels: constLabels,
		}),
		ReadCacheDoorbells: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "read_cache_doorbells_total",
			Help:        "Answers to the long poll on the read cache, by result: rang, quiet or error.",
			ConstLabels: constLabels,
		}, []string{"result"}),
	}
}
