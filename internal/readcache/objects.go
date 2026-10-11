package readcache

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"gocloud.dev/blob"
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

// ErrSizeMismatch reports that an object's size in storage is not the size
// a request gave for it.
var ErrSizeMismatch = errors.New("readcache: object size mismatch")

// CacheStatus is how a request's first run of chunks was served.
type CacheStatus string

const (
	// StatusHit: every chunk came from disk.
	StatusHit CacheStatus = "hit"
	// StatusShared: no chunk was read by this request, at least one by
	// another request in flight.
	StatusShared CacheStatus = "shared"
	// StatusMiss: this request read at least one chunk from storage.
	StatusMiss CacheStatus = "miss"
)

// ObjectSource reads immutable objects from storage. *blobstore.Store
// satisfies it.
type ObjectSource interface {
	Attributes(ctx context.Context, key string) (blobstore.Attributes, error)
	ReadRangeStream(ctx context.Context, key string, offset, length int64) (*blob.Reader, error)
}

// ObjectStats counts chunks served and storage requests made.
type ObjectStats struct {
	ChunkHits    int64 // read from disk
	ChunkMisses  int64 // read from storage by this request
	ChunkShared  int64 // read from storage by another request in flight
	ChunkBridged int64 // on disk, but read again inside a run from storage
	SizeLookups  int64
	StorageReads int64
	StorageBytes int64
}

// Objects serves byte ranges of immutable objects from 128 KiB chunks kept
// on disk, reading missing chunks from storage. Missing chunks next to each
// other are read in one request, and concurrent requests for the same chunk
// share one read. Entries are keyed by object key and size, so a request
// with a wrong size never reads or stores another request's chunks.
type Objects struct {
	source ObjectSource
	disk   *diskcache.Cache

	sizeCalls singleflight.Group

	mu       sync.Mutex
	sizes    map[string]int64
	inflight map[diskcache.Key]*chunkFetch

	chunkHits, chunkMisses, chunkShared, chunkBridged atomic.Int64
	sizeLookups, storageReads, storageBytes           atomic.Int64
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
		ChunkBridged: o.chunkBridged.Load(),
		SizeLookups:  o.sizeLookups.Load(),
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
// remembered, for requests that do not give it.
func (o *Objects) Size(ctx context.Context, key string) (int64, error) {
	o.mu.Lock()
	size, ok := o.sizes[key]
	o.mu.Unlock()
	if ok {
		return size, nil
	}
	results := o.sizeCalls.DoChan(key, func() (any, error) {
		// Not the caller's context: other requests may share this lookup.
		ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
		defer cancel()
		o.sizeLookups.Add(1)
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
	select {
	case r := <-results:
		if r.Err != nil {
			return 0, r.Err
		}
		return r.Val.(int64), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// WriteRange writes length bytes of key at offset to w, up to maxRunChunks
// chunks at a time. size is the object's size; a read from storage checks it
// and fails with ErrSizeMismatch if it is wrong. ready, if not nil, is
// called once before anything is written, with how the first run was
// served; an error returned before ready means nothing was written. With
// fill false, chunks read from storage are served but not kept.
func (o *Objects) WriteRange(ctx context.Context, w io.Writer, key string, size, offset, length int64, fill bool, ready func(CacheStatus)) error {
	if offset < 0 || length < 0 || offset+length > size {
		return fmt.Errorf("readcache: range [%d, %d) outside %s of size %d", offset, offset+length, key, size)
	}
	if ready == nil {
		ready = func(CacheStatus) {}
	}
	if length == 0 {
		ready(StatusHit)
		return nil
	}
	end := offset + length
	object := objectID(key, size)
	for first := offset / chunkSize; first*chunkSize < end; first += maxRunChunks {
		last := min(first+maxRunChunks, (end+chunkSize-1)/chunkSize) - 1
		pieces, status, err := o.chunks(ctx, key, object, size, first, last, offset, end, fill)
		if err != nil {
			return err
		}
		if first == offset/chunkSize {
			ready(status)
		}
		for _, piece := range pieces {
			if _, err := w.Write(piece); err != nil {
				return err
			}
		}
	}
	return nil
}

func objectID(key string, size int64) [32]byte {
	return sha256.Sum256([]byte(key + "\x00" + strconv.FormatInt(size, 10)))
}

// chunks returns the parts of chunks first..last inside [lo, hi), and how
// they were served. Any error fails all of them.
func (o *Objects) chunks(ctx context.Context, key string, object [32]byte, size, first, last, lo, hi int64, fill bool) ([][]byte, CacheStatus, error) {
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

	status := StatusHit
	if len(missing) > 0 {
		status = StatusMiss
	}
	fetched := o.fetchRuns(key, size, first, missing, cached, keys, lengths, fill)
	pieces := make([][]byte, n)
	for i := range n {
		start := (first + int64(i)) * chunkSize
		pieceLo, pieceHi := max(lo, start)-start, min(hi, start+lengths[i])-start
		switch {
		case fetched[i] != nil:
			if fetched[i].err != nil {
				return nil, "", fetched[i].err
			}
			pieces[i] = fetched[i].data[pieceLo:pieceHi]
		case shared[i] != nil:
			select {
			case <-shared[i].done:
			case <-ctx.Done():
				return nil, "", ctx.Err()
			}
			if shared[i].err != nil {
				return nil, "", shared[i].err
			}
			o.chunkShared.Add(1)
			if status == StatusHit {
				status = StatusShared
			}
			pieces[i] = shared[i].data[pieceLo:pieceHi]
		default:
			piece, fromStorage, err := o.cachedPiece(ctx, key, size, keys[i], first+int64(i), lengths[i], pieceLo, pieceHi, fill)
			if err != nil {
				return nil, "", err
			}
			if fromStorage {
				status = StatusMiss
			}
			pieces[i] = piece
		}
	}
	return pieces, status, nil
}

// fetchRuns reads the missing chunks from storage, one request per run of
// neighbours. A run also spans up to maxBridgeChunks cached chunks between
// missing ones, as one request costs less than two. It returns a result for
// every chunk read, by position, after storing and releasing the ones this
// request claimed.
func (o *Objects) fetchRuns(key string, size, first int64, missing []int, cached []bool, keys []diskcache.Key, lengths []int64, fill bool) []*chunkFetch {
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
			o.fetchRun(key, size, first, run, keys, lengths, fill, results)
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

func (o *Objects) fetchRun(key string, size, first int64, run []int, keys []diskcache.Key, lengths []int64, fill bool, results []*chunkFetch) {
	lo, hi := run[0], run[len(run)-1]
	// Not the request's context: other requests may be waiting on these
	// chunks, and they should not fail because this client went away.
	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()
	offset := (first + int64(lo)) * chunkSize
	data, err := o.read(ctx, key, size, offset, (first+int64(hi))*chunkSize+lengths[hi]-offset)
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
				o.chunkBridged.Add(1)
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

// read reads [offset, offset+length) of key from storage, checking that
// the object is size bytes.
func (o *Objects) read(ctx context.Context, key string, size, offset, length int64) ([]byte, error) {
	o.storageReads.Add(1)
	r, err := o.source.ReadRangeStream(ctx, key, offset, length)
	if err != nil {
		return nil, err
	}
	if r.Size() != size {
		_ = r.Close()
		return nil, fmt.Errorf("%w: %s is %d bytes, not %d", ErrSizeMismatch, key, r.Size(), size)
	}
	data := make([]byte, length)
	n, err := io.ReadFull(r, data)
	o.storageBytes.Add(int64(n))
	if err = errors.Join(err, r.Close()); err != nil {
		return nil, err
	}
	return data, nil
}

// cachedPiece reads [pieceLo, pieceHi) of a cached chunk from disk, or, if
// it was evicted since it was found, the whole chunk from storage. It
// reports whether it went to storage.
func (o *Objects) cachedPiece(ctx context.Context, key string, size int64, k diskcache.Key, index, length, pieceLo, pieceHi int64, fill bool) ([]byte, bool, error) {
	piece := make([]byte, pieceHi-pieceLo)
	if o.disk.ReadAt(k, length, piece, pieceLo) {
		o.chunkHits.Add(1)
		return piece, false, nil
	}
	data, err := o.read(ctx, key, size, index*chunkSize, length)
	if err != nil {
		return nil, true, err
	}
	o.chunkMisses.Add(1)
	if fill {
		_ = o.disk.Put(k, data)
	}
	return data[pieceLo:pieceHi], true, nil
}
