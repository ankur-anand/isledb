package readcache

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"golang.org/x/sync/singleflight"
)

// Chunking matches the reader's, so one reader miss is one cache miss.
const (
	chunkSize       = 128 << 10
	maxRunChunks    = 32
	maxBridgeChunks = 8
	fetchTimeout    = 30 * time.Second
	maxKnownSizes   = 1 << 20
)

// ObjectSource reads immutable objects from storage. *blobstore.Store
// satisfies it.
type ObjectSource interface {
	Attributes(ctx context.Context, key string) (blobstore.Attributes, error)
	ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
}

// ObjectStats counts chunks served and storage reads made.
type ObjectStats struct {
	ChunkHits    int64 // read from disk
	ChunkMisses  int64 // read from storage by this request
	ChunkShared  int64 // read from storage by another request in flight
	StorageReads int64
	StorageBytes int64
}

// Objects serves byte ranges of immutable objects from 128 KiB chunks kept
// on disk, reading missing chunks from storage. Missing chunks next to each
// other are read in one request, and concurrent requests for the same chunk
// share one read. Objects only serves keys that are immutable; see
// Immutable.
type Objects struct {
	source ObjectSource
	disk   *diskcache.Cache

	sizeCalls singleflight.Group

	mu       sync.Mutex
	sizes    map[string]int64
	inflight map[diskcache.Key]*chunkFetch

	chunkHits, chunkMisses, chunkShared atomic.Int64
	storageReads, storageBytes          atomic.Int64
}

type chunkFetch struct {
	done chan struct{}
	data []byte
	err  error
}

func NewObjects(source ObjectSource, disk *diskcache.Cache) *Objects {
	return &Objects{
		source:   source,
		disk:     disk,
		sizes:    make(map[string]int64),
		inflight: make(map[diskcache.Key]*chunkFetch),
	}
}

func (o *Objects) Stats() ObjectStats {
	return ObjectStats{
		ChunkHits:    o.chunkHits.Load(),
		ChunkMisses:  o.chunkMisses.Load(),
		ChunkShared:  o.chunkShared.Load(),
		StorageReads: o.storageReads.Load(),
		StorageBytes: o.storageBytes.Load(),
	}
}

// Immutable reports whether key names an object that is never rewritten:
// an SST, a change batch, a manifest snapshot or a manifest page.
func Immutable(key string) bool {
	parts := strings.Split(key, "/")
	n := len(parts)
	if n < 3 || parts[n-1] == "" {
		return false
	}
	name, dir, parent := parts[n-1], parts[n-2], parts[n-3]
	switch {
	case parent == "sstable":
		return dir == blobstore.SSTBucket(name)
	case parent == "changes":
		return dir == blobstore.ChangeBatchBucket(name)
	case parent == "manifest" && dir == "snapshots":
		return len(name) > len(".manifest.zst") && strings.HasSuffix(name, ".manifest.zst")
	case n >= 4 && parts[n-4] == "manifest" && parent == "pages" && isLevelDir(dir):
		return len(name) > len(".page.zst") && strings.HasSuffix(name, ".page.zst")
	}
	return false
}

func isLevelDir(s string) bool {
	return len(s) == 3 && s[0] == 'l' && s[1] >= '0' && s[1] <= '9' && s[2] >= '0' && s[2] <= '9'
}

// Size returns the object's size, read from storage once and then
// remembered.
func (o *Objects) Size(ctx context.Context, key string) (int64, error) {
	o.mu.Lock()
	size, ok := o.sizes[key]
	o.mu.Unlock()
	if ok {
		return size, nil
	}
	v, err, _ := o.sizeCalls.Do(key, func() (any, error) {
		attrs, err := o.source.Attributes(ctx, key)
		if err != nil {
			return int64(0), err
		}
		o.mu.Lock()
		if len(o.sizes) >= maxKnownSizes {
			clear(o.sizes)
		}
		o.sizes[key] = attrs.Size
		o.mu.Unlock()
		return attrs.Size, nil
	})
	if err != nil {
		return 0, err
	}
	return v.(int64), nil
}

// WriteRange writes length bytes of key at offset to w, one run of chunks
// at a time. size must be the object's size. With fill false, chunks read
// from storage are served but not kept, as for a scan.
func (o *Objects) WriteRange(ctx context.Context, w io.Writer, key string, size, offset, length int64, fill bool) error {
	if offset < 0 || length < 0 || offset+length > size {
		return fmt.Errorf("readcache: range [%d, %d) outside %s of size %d", offset, offset+length, key, size)
	}
	end := offset + length
	object := sha256.Sum256([]byte(key))
	for first := offset / chunkSize; first*chunkSize < end; first += maxRunChunks {
		last := min(first+maxRunChunks, (end+chunkSize-1)/chunkSize) - 1
		chunks, err := o.chunks(ctx, key, object, size, first, last, fill)
		if err != nil {
			return err
		}
		for i, data := range chunks {
			start := (first + int64(i)) * chunkSize
			lo, hi := max(offset, start)-start, min(end, start+int64(len(data)))-start
			if _, err := w.Write(data[lo:hi]); err != nil {
				return err
			}
		}
	}
	return nil
}

// chunks returns chunks first..last of the object.
func (o *Objects) chunks(ctx context.Context, key string, object [32]byte, size, first, last int64, fill bool) ([][]byte, error) {
	n := int(last - first + 1)
	keys := make([]diskcache.Key, n)
	lengths := make([]int64, n)
	shared := make([]*chunkFetch, n)
	cached := make([]bool, n)
	var missing []int

	o.mu.Lock()
	for i := range n {
		index := first + int64(i)
		keys[i] = diskcache.Key{Object: object, Kind: diskcache.KindChunk, Index: uint32(index)}
		lengths[i] = min(chunkSize, size-index*chunkSize)
		if f := o.inflight[keys[i]]; f != nil {
			shared[i] = f
			continue
		}
		if o.disk.Contains(keys[i], lengths[i]) {
			cached[i] = true
			continue
		}
		missing = append(missing, i)
		if fill {
			o.inflight[keys[i]] = &chunkFetch{done: make(chan struct{})}
		}
	}
	o.mu.Unlock()

	out := make([][]byte, n)
	fetched := o.fetchRuns(key, first, missing, cached, keys, lengths, fill)
	for i := range n {
		switch {
		case fetched[i] != nil:
			if fetched[i].err != nil {
				return nil, fetched[i].err
			}
			out[i] = fetched[i].data
		case shared[i] != nil:
			select {
			case <-shared[i].done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if shared[i].err != nil {
				return nil, shared[i].err
			}
			o.chunkShared.Add(1)
			out[i] = shared[i].data
		default:
			data, err := o.cachedChunk(ctx, key, keys[i], first+int64(i), lengths[i], fill)
			if err != nil {
				return nil, err
			}
			out[i] = data
		}
	}
	return out, nil
}

// fetchRuns reads the missing chunks from storage, one request per run of
// neighbours. A run also spans up to maxBridgeChunks cached chunks between
// missing ones, as one request costs less than two. It returns a result for
// every chunk read, by position, after storing and releasing the ones this
// request claimed.
func (o *Objects) fetchRuns(key string, first int64, missing []int, cached []bool, keys []diskcache.Key, lengths []int64, fill bool) []*chunkFetch {
	results := make([]*chunkFetch, len(keys))
	if len(missing) == 0 {
		return results
	}
	var wg sync.WaitGroup
	for start := 0; start < len(missing); {
		end := start
		for end+1 < len(missing) && bridgeable(cached, missing[end], missing[end+1]) {
			end++
		}
		run := missing[start : end+1]
		wg.Go(func() {
			o.fetchRun(key, first, run, keys, lengths, fill, results)
		})
		start = end + 1
	}
	wg.Wait()
	return results
}

// bridgeable reports whether chunks a and b, both missing, can be read in
// one request: everything between them is cached and the gap is short.
func bridgeable(cached []bool, a, b int) bool {
	if b-a-1 > maxBridgeChunks {
		return false
	}
	for i := a + 1; i < b; i++ {
		if !cached[i] {
			return false
		}
	}
	return true
}

func (o *Objects) fetchRun(key string, first int64, run []int, keys []diskcache.Key, lengths []int64, fill bool, results []*chunkFetch) {
	lo, hi := run[0], run[len(run)-1]
	// Not the request's context: other requests may be waiting on these
	// chunks, and they should not fail because this client went away.
	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()
	offset := (first + int64(lo)) * chunkSize
	length := (first+int64(hi))*chunkSize + lengths[hi] - offset
	data, err := o.source.ReadRange(ctx, key, offset, length)
	o.storageReads.Add(1)
	o.storageBytes.Add(int64(len(data)))
	if err == nil && int64(len(data)) != length {
		err = fmt.Errorf("readcache: read %d bytes of %s at %d, want %d", len(data), key, offset, length)
	}
	for _, i := range run {
		f := &chunkFetch{err: err}
		if err == nil {
			start := int64(i-lo) * chunkSize
			f.data = data[start : start+lengths[i] : start+lengths[i]]
			o.chunkMisses.Add(1)
			if fill {
				_ = o.disk.Put(keys[i], f.data)
			}
		}
		results[i] = f
		if fill {
			o.release(keys[i], f)
		}
	}
	if err == nil {
		for i := lo + 1; i < hi; i++ {
			if results[i] == nil {
				start := int64(i-lo) * chunkSize
				results[i] = &chunkFetch{data: data[start : start+lengths[i] : start+lengths[i]]}
				o.chunkHits.Add(1)
			}
		}
	}
}

func (o *Objects) release(k diskcache.Key, result *chunkFetch) {
	o.mu.Lock()
	f := o.inflight[k]
	delete(o.inflight, k)
	o.mu.Unlock()
	f.data, f.err = result.data, result.err
	close(f.done)
}

// cachedChunk reads a chunk from disk, or from storage if it was evicted
// since it was found.
func (o *Objects) cachedChunk(ctx context.Context, key string, k diskcache.Key, index, length int64, fill bool) ([]byte, error) {
	data := make([]byte, length)
	if o.disk.ReadAt(k, length, data, 0) {
		o.chunkHits.Add(1)
		return data, nil
	}
	data, err := o.source.ReadRange(ctx, key, index*chunkSize, length)
	o.storageReads.Add(1)
	o.storageBytes.Add(int64(len(data)))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("readcache: read %d bytes of %s at %d, want %d", len(data), key, index*chunkSize, length)
	}
	o.chunkMisses.Add(1)
	if fill {
		_ = o.disk.Put(k, data)
	}
	return data, nil
}
