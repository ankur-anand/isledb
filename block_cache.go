package isledb

import (
	"strconv"
	"strings"

	"github.com/dgraph-io/ristretto/v2"
)

const defaultBlockSize = 4 << 10

func initBlockCache(opts ReaderOptions) (*ristretto.Cache[string, []byte], bool, error) {
	if opts.BlockCacheSize <= 0 {
		return nil, false, nil
	}

	numCounters := blockCacheCounters(opts.BlockCacheSize)
	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters:        numCounters,
		MaxCost:            opts.BlockCacheSize,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		return nil, false, err
	}
	return cache, true, nil
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
