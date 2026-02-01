package isledb

import "github.com/ankur-anand/isledb/diskcache"

// SSTCache is an alias to diskcache.RefCountedCache for caching SST file data.
type SSTCache = diskcache.RefCountedCache

// SSTCacheStats is an alias to diskcache.Stats for SST cache statistics.
type SSTCacheStats = diskcache.Stats
