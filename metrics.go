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
