package isledb

import (
	"log/slog"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/internal/diskcache"
)

const readerDiagnosticLogInterval = time.Minute

// readerDiagnosticLimiter bounds diagnostic logs while leaving metrics exact.
// It intentionally has no per-SST map, so a stream of unique corrupt objects
// cannot turn observability into an unbounded memory consumer.
type readerDiagnosticLimiter struct {
	mu         sync.Mutex
	last       time.Time
	suppressed uint64
}

func (limiter *readerDiagnosticLimiter) allow(now time.Time) (bool, uint64) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if !limiter.last.IsZero() && now.Sub(limiter.last) < readerDiagnosticLogInterval {
		limiter.suppressed++
		return false, 0
	}
	suppressed := limiter.suppressed
	limiter.last = now
	limiter.suppressed = 0
	return true, suppressed
}

// dropSST forgets everything held of an SST whose read failed on damage (see
// damaged): its open reader, its cached blocks and what the disk cache holds
// of it, so the next read fetches it again.
func (r *Reader) dropSST(meta sstMetadata) {
	r.sstDrops.Add(1)
	r.openSSTs.remove(meta.ID)
	r.blockCache.forget(meta.ID)
	r.fetcher.dropObject(r.fetcher.object(meta))
}

// clearDiskCache drops everything the disk cache holds; tests and benchmarks
// use it to start cold.
func (r *Reader) clearDiskCache() {
	if r.diskCache != nil {
		r.diskCache.Purge(diskcache.TierMeta)
		r.diskCache.Purge(diskcache.TierData)
	}
}

func (r *Reader) observeBloomFilterError(sstID string, err error) {
	if err == nil {
		return
	}
	r.metrics.ObserveBloomFilterError()
	allowed, suppressed := r.bloomDiagnosticLimiter.allow(time.Now())
	if !allowed {
		return
	}
	slog.Warn(
		"isledb: Bloom filter unavailable; continuing with SST lookup",
		"sst_id", sstID,
		"error", err,
		"suppressed_since_last_log", suppressed,
	)
}
