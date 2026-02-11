package isledb

import (
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
	PutBlobTotal      prometheus.Counter
	PutBlobErrors     prometheus.Counter
	PutBlobLatency    prometheus.Histogram
	BlobBytesTotal    prometheus.Counter
	DeleteTotal       prometheus.Counter
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

func (m *WriterMetrics) ObservePutBlob(sizeBytes int, d time.Duration, err error) {
	if m == nil {
		return
	}
	m.incCounter(m.PutBlobTotal)
	m.observeHistogram(m.PutBlobLatency, d.Seconds())
	if err != nil {
		m.incCounter(m.PutBlobErrors)
		return
	}
	if sizeBytes > 0 {
		m.addCounter(m.BlobBytesTotal, float64(sizeBytes))
	}
}

func (m *WriterMetrics) ObserveFlushBytes(sizeBytes int64) {
	if m == nil {
		return
	}
	if sizeBytes > 0 {
		m.addCounter(m.FlushBytes, float64(sizeBytes))
	}
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
		PutBlobTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "put_blob_total",
			Help:        "Total PutBlob Operations.",
			ConstLabels: constLabels,
		}),
		PutBlobErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "put_blob_errors_total",
			Help:        "Total PutBlob Errors.",
			ConstLabels: constLabels,
		}),
		PutBlobLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "put_blob_latency_seconds",
			Help:        "Histogram of memtable put blob latency in seconds",
			ConstLabels: constLabels,
		}),
		BlobBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "blob_bytes_total",
			Help:        "Total number of large blob bytes written to object Storage",
			ConstLabels: constLabels,
		}),
		DeleteTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "writer",
			Name:        "delete_total",
			Help:        "Total Delete Operations.",
			ConstLabels: constLabels,
		}),
	}
}

type ReaderMetrics struct {
	RefreshTotal   prometheus.Counter
	RefreshErrors  prometheus.Counter
	RefreshLatency prometheus.Histogram

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

	BlobFetchTotal   prometheus.Counter
	BlobFetchErrors  prometheus.Counter
	BlobFetchLatency prometheus.Histogram
	BlobCacheHits    prometheus.Counter
	BlobCacheMisses  prometheus.Counter
	BlobBytesTotal   prometheus.Counter

	SSTCacheHits       prometheus.Counter
	SSTCacheMisses     prometheus.Counter
	SSTDownloadTotal   prometheus.Counter
	SSTDownloadErrors  prometheus.Counter
	SSTDownloadLatency prometheus.Histogram
	SSTDownloadBytes   prometheus.Counter

	SSTRangeBlockCacheHits   prometheus.Counter
	SSTRangeBlockCacheMisses prometheus.Counter
	SSTRangeReadTotal        prometheus.Counter
	SSTRangeReadErrors       prometheus.Counter
	SSTRangeReadLatency      prometheus.Histogram
	SSTRangeReadBytes        prometheus.Counter
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

func (m *ReaderMetrics) ObserveBlobFetch(d time.Duration, sizeBytes int, cacheHit bool, err error) {
	if m == nil {
		return
	}
	m.incCounter(m.BlobFetchTotal)
	m.observeHistogram(m.BlobFetchLatency, d.Seconds())
	if cacheHit {
		m.incCounter(m.BlobCacheHits)
	} else {
		m.incCounter(m.BlobCacheMisses)
	}
	if err != nil {
		m.incCounter(m.BlobFetchErrors)
		return
	}
	if sizeBytes > 0 {
		m.addCounter(m.BlobBytesTotal, float64(sizeBytes))
	}
}

func (m *ReaderMetrics) ObserveSSTCacheLookup(hit bool) {
	if m == nil {
		return
	}
	if hit {
		m.incCounter(m.SSTCacheHits)
		return
	}
	m.incCounter(m.SSTCacheMisses)
}

func (m *ReaderMetrics) ObserveSSTDownload(d time.Duration, sizeBytes int64, err error) {
	if m == nil {
		return
	}
	m.incCounter(m.SSTDownloadTotal)
	m.observeHistogram(m.SSTDownloadLatency, d.Seconds())
	if err != nil {
		m.incCounter(m.SSTDownloadErrors)
		return
	}
	if sizeBytes > 0 {
		m.addCounter(m.SSTDownloadBytes, float64(sizeBytes))
	}
}

func (m *ReaderMetrics) ObserveSSTRangeBlockCacheLookup(hit bool) {
	if m == nil {
		return
	}
	if hit {
		m.incCounter(m.SSTRangeBlockCacheHits)
		return
	}
	m.incCounter(m.SSTRangeBlockCacheMisses)
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
		BlobFetchTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "blob_fetch_total",
			Help:        "Total number of blob fetch operations.",
			ConstLabels: constLabels,
		}),
		BlobFetchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "blob_fetch_errors_total",
			Help:        "Total number of blob fetch errors.",
			ConstLabels: constLabels,
		}),
		BlobFetchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "blob_fetch_latency_seconds",
			Help:        "Histogram of blob fetch latency in seconds.",
			ConstLabels: constLabels,
		}),
		BlobCacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "blob_cache_hits_total",
			Help:        "Total number of blob cache hits.",
			ConstLabels: constLabels,
		}),
		BlobCacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "blob_cache_misses_total",
			Help:        "Total number of blob cache misses.",
			ConstLabels: constLabels,
		}),
		BlobBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "blob_bytes_total",
			Help:        "Total blob payload bytes returned by reader fetches.",
			ConstLabels: constLabels,
		}),
		SSTCacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_cache_hits_total",
			Help:        "Total number of SST cache hits.",
			ConstLabels: constLabels,
		}),
		SSTCacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_cache_misses_total",
			Help:        "Total number of SST cache misses.",
			ConstLabels: constLabels,
		}),
		SSTDownloadTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_download_total",
			Help:        "Total number of SST download attempts.",
			ConstLabels: constLabels,
		}),
		SSTDownloadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_download_errors_total",
			Help:        "Total number of SST download errors.",
			ConstLabels: constLabels,
		}),
		SSTDownloadLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_download_latency_seconds",
			Help:        "Histogram of SST download latency in seconds.",
			ConstLabels: constLabels,
		}),
		SSTDownloadBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_download_bytes_total",
			Help:        "Total number of SST bytes downloaded.",
			ConstLabels: constLabels,
		}),
		SSTRangeBlockCacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_range_block_cache_hits_total",
			Help:        "Total number of SST range-read block cache hits.",
			ConstLabels: constLabels,
		}),
		SSTRangeBlockCacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "isledb",
			Subsystem:   "reader",
			Name:        "sst_range_block_cache_misses_total",
			Help:        "Total number of SST range-read block cache misses.",
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
	}
}
