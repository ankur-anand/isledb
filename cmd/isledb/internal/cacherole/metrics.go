package cacherole

import (
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/readcache"
	"github.com/prometheus/client_golang/prometheus"
)

// collector reports the cache's counters as they are at each scrape.
type collector struct {
	objects *readcache.Objects
	disk    *diskcache.Cache
	relays  map[string]*readcache.Relay
	// prefetchers holds the databases with prefetch on, by prefix.
	prefetchers map[string]*readcache.Prefetcher
	names       map[string]string // prefix to its label: "/" for the root
	handler     *readcache.Handler
}

var (
	descChunks         = prometheus.NewDesc("isledb_cache_chunks_total", "Chunks served, by where they came from: disk, storage (read by this request), shared (read by another request in flight) or bridged (on disk, read again inside a run).", []string{"source"}, nil)
	descSizeLookups    = prometheus.NewDesc("isledb_cache_size_lookups_total", "Object sizes asked of storage, for requests without a size.", nil, nil)
	descStorageReads   = prometheus.NewDesc("isledb_cache_storage_reads_total", "Range reads sent to storage.", nil, nil)
	descStorageBytes   = prometheus.NewDesc("isledb_cache_storage_read_bytes_total", "Bytes read from storage.", nil, nil)
	descStorageQueued  = prometheus.NewDesc("isledb_cache_storage_reads_queued_total", "Storage reads that waited for the read budget.", nil, nil)
	descStorageReading = prometheus.NewDesc("isledb_cache_storage_bytes_reading", "Bytes of storage reads in progress.", nil, nil)
	descPosition       = prometheus.NewDesc("isledb_cache_current_position", "Log position of the relayed CURRENT.", []string{"db"}, nil)
	descAge            = prometheus.NewDesc("isledb_cache_current_age_seconds", "Time since the relayed CURRENT was read from storage.", []string{"db"}, nil)
	descPollErrors     = prometheus.NewDesc("isledb_cache_current_poll_errors_total", "Failed reads of CURRENT.", []string{"db"}, nil)
	descLongPolls      = prometheus.NewDesc("isledb_cache_long_polls_held", "Long polls waiting now.", nil, nil)
	descDiskBytes      = prometheus.NewDesc("isledb_cache_disk_bytes", "Bytes of chunks on disk.", nil, nil)
	descDiskMaxBytes   = prometheus.NewDesc("isledb_cache_disk_max_bytes", "Disk budget.", nil, nil)
	descDiskEntries    = prometheus.NewDesc("isledb_cache_disk_entries", "Chunks on disk.", nil, nil)
	descDiskEvictions  = prometheus.NewDesc("isledb_cache_disk_evictions_total", "Chunks evicted from disk.", nil, nil)
	descDiskFailures   = prometheus.NewDesc("isledb_cache_disk_failures_total", "Chunks that could not be written to disk.", nil, nil)
	descPrefetchPaused = prometheus.NewDesc("isledb_cache_prefetch_paused", "1 while prefetch is paused because the database is larger than the disk.", []string{"db"}, nil)
	descPrefetchBytes  = prometheus.NewDesc("isledb_cache_prefetch_read_bytes_total", "Bytes prefetch read from storage.", []string{"db"}, nil)
	descPrefetchFails  = prometheus.NewDesc("isledb_cache_prefetch_failures_total", "Data files prefetch could not read, and manifest replays that failed; retried.", []string{"db"}, nil)
)

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descChunks, descSizeLookups, descStorageReads, descStorageBytes,
		descStorageQueued, descStorageReading, descPosition, descAge, descPollErrors, descLongPolls,
		descDiskBytes, descDiskMaxBytes, descDiskEntries, descDiskEvictions, descDiskFailures,
		descPrefetchPaused, descPrefetchBytes, descPrefetchFails} {
		ch <- d
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	counter := func(d *prometheus.Desc, v int64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), labels...)
	}
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	s := c.objects.Stats()
	counter(descChunks, s.ChunkHits, "disk")
	counter(descChunks, s.ChunkMisses, "storage")
	counter(descChunks, s.ChunkShared, "shared")
	counter(descChunks, s.ChunkBridged, "bridged")
	counter(descSizeLookups, s.SizeLookups)
	counter(descStorageReads, s.StorageReads)
	counter(descStorageBytes, s.StorageBytes)
	counter(descStorageQueued, s.StorageQueued)
	gauge(descStorageReading, float64(s.StorageBytesReading))
	for prefix, relay := range c.relays {
		db := c.names[prefix]
		if current, ok := relay.Current(); ok {
			gauge(descPosition, float64(current.Position), db)
			gauge(descAge, current.Age.Seconds(), db)
		}
		counter(descPollErrors, relay.PollErrors(), db)
	}
	for prefix, p := range c.prefetchers {
		db := c.names[prefix]
		paused := 0.0
		if p.Paused() {
			paused = 1
		}
		gauge(descPrefetchPaused, paused, db)
		counter(descPrefetchBytes, p.ReadBytes(), db)
		counter(descPrefetchFails, p.Failures(), db)
	}
	gauge(descLongPolls, float64(c.handler.LongPollsHeld()))
	d := c.disk.Stats(diskcache.TierData)
	gauge(descDiskBytes, float64(d.Bytes))
	gauge(descDiskMaxBytes, float64(d.MaxBytes))
	gauge(descDiskEntries, float64(d.Entries))
	counter(descDiskEvictions, d.Evictions)
	counter(descDiskFailures, d.Failures)
}
