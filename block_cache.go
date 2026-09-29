package isledb

import (
	"cmp"
	"strconv"
	"strings"

	"github.com/dgraph-io/ristretto/v2"
)

const defaultBlockSize = 4 << 10

// initBlockCache returns the range-read block cache, or nil when range reads
// are disabled.
func initBlockCache(opts readerOptions) (*ristretto.Cache[string, []byte], error) {
	if !opts.RangeRead {
		return nil, nil
	}
	maxCost := cmp.Or(opts.BlockCacheSize, defaultBlockCacheSize)
	return ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters:        blockCacheCounters(maxCost),
		MaxCost:            maxCost,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
}

func blockCacheCounters(maxCost int64) int64 {
	if maxCost <= 0 {
		return 0
	}
	entries := maxCost / defaultBlockSize
	if entries < 1 {
		entries = 1
	}
	counters := entries * 10
	if counters < 1024 {
		counters = 1024
	}
	return counters
}

func blockCacheKey(sstID string, off int64, length int) string {
	var b strings.Builder
	b.Grow(len(sstID) + 1 + 20 + 1 + 10)
	b.WriteString(sstID)
	b.WriteByte(':')
	b.WriteString(strconv.FormatInt(off, 10))
	b.WriteByte(':')
	b.WriteString(strconv.Itoa(length))
	return b.String()
}
