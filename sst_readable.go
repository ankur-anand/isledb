package isledb

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/checksum"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/cockroachdb/pebble/v2/objstorage"
)

const (
	// sstChunkSize is the unit in which an SST's data region is fetched and
	// cached: aligned, so an entry is either wholly present or absent.
	sstChunkSize = 128 << 10
	// maxReadAheadChunks caps how many chunks a scan fetches in one request.
	maxReadAheadChunks = 32
	// smallSSTBytes is the largest SST fetched and cached whole, together
	// with its Bloom sidecar, in one request.
	smallSSTBytes = 4 << 20
	// maxBridgeChunks is the longest run of cached chunks a request fetches
	// again to reach a missing chunk after it. 8 chunks (1 MiB) cost about
	// 10 ms at 100 MB/s, half a typical request.
	maxBridgeChunks = 8
)

// sstObject describes where an SST's parts lie in its object and how the
// disk cache names them. The SST occupies [0, Size), with its metadata in
// [MetaOffset, Size); the Bloom sidecar follows it.
type sstObject struct {
	id            string
	path          string
	size          int64
	metaOffset    int64
	bloomOffset   int64
	bloomLength   int64
	checksum      string
	bloomChecksum string
	key           [32]byte
	// whole reports that the SST is fetched and cached whole: it is small, or
	// its metadata offset is unknown (an SST from before offsets were recorded).
	whole bool
}

func newSSTObject(store *blobstore.Store, meta sstMetadata, smallLimit int64) sstObject {
	o := sstObject{
		id:         meta.ID,
		path:       store.SSTPath(meta.ID),
		size:       meta.Size,
		metaOffset: meta.MetaOffset,
		checksum:   meta.Checksum,
		key: sha256.Sum256([]byte(meta.ID + "\x00" + meta.Checksum + "\x00" +
			strconv.FormatInt(meta.Size, 10))),
	}
	if hasUsableBloom(meta) {
		o.bloomOffset, o.bloomLength, o.bloomChecksum = meta.Bloom.Offset, meta.Bloom.Length, meta.Bloom.Checksum
	}
	if o.metaOffset >= o.size {
		o.metaOffset = 0
	}
	o.whole = o.size <= smallLimit || o.metaOffset <= 0
	return o
}

func (o sstObject) small() bool { return o.whole }

func (o sstObject) numChunks() uint32 {
	return uint32((o.metaOffset + sstChunkSize - 1) / sstChunkSize)
}

// chunkSpan returns chunk i's byte range, the last chunk cut at MetaOffset.
func (o sstObject) chunkSpan(i uint32) (int64, int64) {
	start := int64(i) * sstChunkSize
	return start, min(start+sstChunkSize, o.metaOffset)
}

func (o sstObject) entry(kind diskcache.Kind, index uint32) diskcache.Key {
	return diskcache.Key{Object: o.key, Kind: kind, Index: index}
}

// privateReadsKey marks a context whose reads fill no cache: the long tail of
// a scan.
type privateReadsKey struct{}

func withPrivateReads(ctx context.Context) context.Context {
	return context.WithValue(ctx, privateReadsKey{}, true)
}

func fillsCaches(ctx context.Context) bool {
	private, _ := ctx.Value(privateReadsKey{}).(bool)
	return !private
}

// sstFetcher reads SST parts from the disk cache or object storage, and
// stores what it fetches. One serves all of a Reader's SSTs, so concurrent
// reads of the same part share one request.
type sstFetcher struct {
	store *blobstore.Store
	disk  *diskcache.Cache // nil: nothing is cached on disk
	m     *ReaderMetrics
	// smallLimit is the largest SST fetched whole; tests lower it to read
	// small fixtures in chunks.
	smallLimit int64
	// loads coalesces fetches of whole entries: metadata, Bloom, small SSTs.
	loads coalescedLoadGroup

	mu       sync.Mutex
	inflight map[diskcache.Key]*chunkFetch
}

// chunkFetch is one request for a run of chunks, shared by every reader that
// needs one of them while it is in flight.
type chunkFetch struct {
	done  chan struct{}
	start int64
	end   int64
	data  []byte
	err   error
	// stored reports that the requester stored the chunks; a private scan's
	// request does not, leaving it to a waiter that caches.
	stored bool
}

func newSSTFetcher(store *blobstore.Store, disk *diskcache.Cache, metrics *ReaderMetrics) *sstFetcher {
	f := &sstFetcher{
		store: store, disk: disk, m: metrics, smallLimit: smallSSTBytes,
		inflight: make(map[diskcache.Key]*chunkFetch),
	}
	if disk != nil {
		// A whole SST that the data tier could never hold is read in chunks.
		f.smallLimit = min(f.smallLimit, disk.Stats(diskcache.TierData).MaxBytes)
	}
	return f
}

func (f *sstFetcher) object(meta sstMetadata) sstObject {
	return newSSTObject(f.store, meta, f.smallLimit)
}

func (f *sstFetcher) close() { f.loads.Close(ErrReaderClosed) }

func (f *sstFetcher) diskRead(k diskcache.Key, size int64, p []byte, off int64) bool {
	return f.disk != nil && f.disk.ReadAt(k, size, p, off)
}

func (f *sstFetcher) diskHas(k diskcache.Key, size int64) bool {
	return f.disk != nil && f.disk.Contains(k, size)
}

// diskStore stores an entry before returning. A failure only means a later
// read fetches it again.
func (f *sstFetcher) diskStore(k diskcache.Key, data []byte) {
	if f.disk != nil && len(data) > 0 {
		_ = f.disk.Put(k, data)
	}
}

// dropObject removes every cached part of a damaged SST, so the next read
// fetches it again. The Bloom sidecar stays: it is checksummed on every load.
func (f *sstFetcher) dropObject(o sstObject) {
	if f.disk == nil {
		return
	}
	f.disk.Remove(o.entry(diskcache.KindWhole, 0))
	f.disk.Remove(o.entry(diskcache.KindMeta, 0))
	for i := range o.numChunks() {
		f.disk.Remove(o.entry(diskcache.KindChunk, i))
	}
}

// fetchError is a failed or invalid fetch from object storage. It says nothing
// about the SST's cached bytes; see damaged.
type fetchError struct{ err error }

func (e *fetchError) Error() string { return e.err.Error() }
func (e *fetchError) Unwrap() error { return e.err }

// damaged reports whether err from reading an SST means its cached bytes are
// bad. Every error is, except a failed fetch or an ended context: those leave
// the cache as it was, and the next read tries again.
func damaged(err error) bool {
	// Checked first: fetchErr escapes, so declaring it allocates, and nearly
	// every call is on a successful read.
	if err == nil {
		return false
	}
	var fetchErr *fetchError
	return !errors.As(err, &fetchErr) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func (f *sstFetcher) readRange(ctx context.Context, path string, off, length int64) ([]byte, error) {
	start := time.Now()
	reader, err := f.store.ReadRangeStream(ctx, path, off, length)
	if err != nil {
		f.m.ObserveSSTRangeRead(time.Since(start), 0, err)
		return nil, &fetchError{err}
	}
	data := make([]byte, length)
	n, readErr := io.ReadFull(reader, data)
	err = errors.Join(readErr, reader.Close())
	f.m.ObserveSSTRangeRead(time.Since(start), int64(n), err)
	if err != nil {
		return nil, &fetchError{err}
	}
	return data, nil
}

// load runs fetch once for concurrent callers of the same key.
func (f *sstFetcher) load(ctx context.Context, k diskcache.Key, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	name := fmt.Sprintf("%x/%d/%d", k.Object, k.Kind, k.Index)
	value, err := f.loads.Do(ctx, name, func(ctx context.Context) (any, error) { return fetch(ctx) })
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
}

// whole returns a small SST's object through the end of its Bloom sidecar,
// fetched in one request, with both parts verified and stored.
func (f *sstFetcher) whole(ctx context.Context, o sstObject) ([]byte, error) {
	return f.load(ctx, o.entry(diskcache.KindWhole, 0), func(ctx context.Context) ([]byte, error) {
		end := o.size
		if o.bloomLength > 0 && o.bloomOffset >= o.size {
			end = o.bloomOffset + o.bloomLength
		}
		data, err := f.readRange(ctx, o.path, 0, end)
		if err != nil {
			return nil, fmt.Errorf("read sst %s: %w", o.id, err)
		}
		if o.checksum != "" {
			want, err := checksum.ParseSHA256(o.checksum)
			if err != nil || sha256.Sum256(data[:o.size]) != want {
				// Checked before anything is stored: the object itself is bad.
				return nil, &fetchError{fmt.Errorf("validate sst %s: checksum mismatch", o.id)}
			}
		}
		f.diskStore(o.entry(diskcache.KindWhole, 0), data[:o.size])
		if end > o.size {
			if bloom := data[o.bloomOffset:end]; validateBloomChecksum(o.bloomChecksum, bloom) == nil {
				f.diskStore(o.entry(diskcache.KindBloom, 0), bloom)
			}
		}
		return data, nil
	})
}

// meta returns a large SST's metadata region, [MetaOffset, Size), fetched in
// one request and stored.
func (f *sstFetcher) meta(ctx context.Context, o sstObject) ([]byte, error) {
	return f.load(ctx, o.entry(diskcache.KindMeta, 0), func(ctx context.Context) ([]byte, error) {
		data, err := f.readRange(ctx, o.path, o.metaOffset, o.size-o.metaOffset)
		if err != nil {
			return nil, fmt.Errorf("read sst %s metadata: %w", o.id, err)
		}
		f.diskStore(o.entry(diskcache.KindMeta, 0), data)
		return data, nil
	})
}

// bloom returns an SST's verified Bloom sidecar: from disk, else with the
// whole object for a small SST that is not cached either, else with one
// request of its own.
func (f *sstFetcher) bloom(ctx context.Context, o sstObject) ([]byte, error) {
	if data, ok := f.diskBloom(o); ok {
		return data, nil
	}
	k := o.entry(diskcache.KindBloom, 0)
	var data []byte
	if o.small() && !f.diskHas(o.entry(diskcache.KindWhole, 0), o.size) {
		object, err := f.whole(ctx, o)
		if err != nil {
			return nil, err
		}
		if int64(len(object)) < o.bloomOffset+o.bloomLength {
			return nil, fmt.Errorf("read bloom %s: missing from object", o.id)
		}
		// Copy it out: the filter keeps its bytes, and must not keep the
		// whole object alive.
		data = append([]byte(nil), object[o.bloomOffset:o.bloomOffset+o.bloomLength]...)
	} else {
		var err error
		data, err = f.load(ctx, k, func(ctx context.Context) ([]byte, error) {
			data, err := f.readRange(ctx, o.path, o.bloomOffset, o.bloomLength)
			if err == nil && validateBloomChecksum(o.bloomChecksum, data) == nil {
				f.diskStore(k, data)
			}
			return data, err
		})
		if err != nil {
			return nil, fmt.Errorf("read bloom %s: %w", o.id, err)
		}
	}
	if err := validateBloomChecksum(o.bloomChecksum, data); err != nil {
		return nil, fmt.Errorf("validate bloom %s: %w", o.id, err)
	}
	return data, nil
}

// diskBloom returns an SST's filter from the disk cache, verified.
func (f *sstFetcher) diskBloom(o sstObject) ([]byte, bool) {
	k := o.entry(diskcache.KindBloom, 0)
	data := make([]byte, o.bloomLength)
	if !f.diskRead(k, o.bloomLength, data, 0) {
		return nil, false
	}
	if validateBloomChecksum(o.bloomChecksum, data) != nil {
		f.disk.ReportCorrupt(k)
		return nil, false
	}
	return data, true
}

// resident reports whether every part of an SST is on disk.
func (f *sstFetcher) resident(o sstObject) bool {
	if o.bloomLength > 0 && !f.diskHas(o.entry(diskcache.KindBloom, 0), o.bloomLength) {
		return false
	}
	if o.small() {
		return f.diskHas(o.entry(diskcache.KindWhole, 0), o.size)
	}
	if !f.diskHas(o.entry(diskcache.KindMeta, 0), o.size-o.metaOffset) {
		return false
	}
	for i := range o.numChunks() {
		if !f.diskHas(o.entry(diskcache.KindChunk, i), chunkLen(o, i)) {
			return false
		}
	}
	return true
}

// prefetch caches every part of an SST on disk, fetching only what is
// missing, and reports the bytes fetched.
func (f *sstFetcher) prefetch(ctx context.Context, o sstObject) (int64, error) {
	if f.disk == nil {
		return 0, nil
	}
	var fetched int64
	if o.small() {
		if f.resident(o) {
			return 0, nil
		}
		if f.diskHas(o.entry(diskcache.KindWhole, 0), o.size) {
			// Only the Bloom sidecar is missing: fetch its range alone.
			data, err := f.bloom(ctx, o)
			return int64(len(data)), err
		}
		data, err := f.whole(ctx, o)
		return int64(len(data)), err
	}
	if !f.diskHas(o.entry(diskcache.KindMeta, 0), o.size-o.metaOffset) {
		data, err := f.meta(ctx, o)
		if err != nil {
			return fetched, err
		}
		fetched += int64(len(data))
	}
	if o.bloomLength > 0 && !f.diskHas(o.entry(diskcache.KindBloom, 0), o.bloomLength) {
		if _, err := f.bloom(ctx, o); err != nil {
			return fetched, err
		}
		fetched += o.bloomLength
	}
	for i := uint32(0); i < o.numChunks(); {
		if f.diskHas(o.entry(diskcache.KindChunk, i), chunkLen(o, i)) {
			i++
			continue
		}
		start, data, err := f.fetchChunks(ctx, o, i, i, min(i+maxReadAheadChunks, o.numChunks()), true)
		if err != nil {
			return fetched, err
		}
		// A run shared with another reader can start before chunk i, at
		// chunks already counted.
		end := start + int64(len(data))
		fetched += end - max(start, int64(i)*sstChunkSize)
		i = uint32((end + sstChunkSize - 1) / sstChunkSize)
	}
	return fetched, nil
}

// chunksOnDisk fills p, which covers [off, off+len(p)) of the data region,
// from cached chunks, and reports whether all of it was there.
func (f *sstFetcher) chunksOnDisk(o sstObject, p []byte, off int64) bool {
	for len(p) > 0 {
		i := uint32(off / sstChunkSize)
		start, end := o.chunkSpan(i)
		n := min(int64(len(p)), end-off)
		if !f.diskRead(o.entry(diskcache.KindChunk, i), end-start, p[:n], off-start) {
			return false
		}
		p, off = p[n:], off+n
	}
	return true
}

// fetchChunks fetches, in one request, chunks first through last, extended
// toward want (exclusive) up to the last missing chunk, and returns where the
// run starts and its bytes. Up to maxBridgeChunks cached chunks in a row are
// fetched again rather than split the run. The run stops before a chunk in
// flight; a caller needing such a chunk waits for its request instead. With
// store, the chunks are on disk when it returns.
func (f *sstFetcher) fetchChunks(ctx context.Context, o sstObject, first, last, want uint32, store bool) (int64, []byte, error) {
	waited := false
	for {
		f.mu.Lock()
		if fl := f.inflight[o.entry(diskcache.KindChunk, first)]; fl != nil && !waited {
			f.mu.Unlock()
			select {
			case <-fl.done:
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			}
			waited = true
			lastEnd := min(int64(last+1)*sstChunkSize, o.metaOffset)
			if fl.err == nil && fl.start <= int64(first)*sstChunkSize && fl.end >= lastEnd {
				if store && !fl.stored {
					f.storeChunks(o, fl.start, fl.data)
				}
				return fl.start, fl.data, nil
			}
			continue // the request failed or did not cover the run: fetch it here
		}
		lastMissing, cached := last, 0
		for i := last + 1; i < max(want, last+1); i++ {
			if f.inflight[o.entry(diskcache.KindChunk, i)] != nil {
				break
			}
			if f.diskHas(o.entry(diskcache.KindChunk, i), chunkLen(o, i)) {
				if cached++; cached > maxBridgeChunks {
					break
				}
				continue
			}
			lastMissing, cached = i, 0
		}
		end := lastMissing + 1
		start, _ := o.chunkSpan(first)
		_, stop := o.chunkSpan(end - 1)
		fl := &chunkFetch{done: make(chan struct{}), start: start, end: stop}
		for i := first; i < end; i++ {
			if k := o.entry(diskcache.KindChunk, i); f.inflight[k] == nil {
				f.inflight[k] = fl
			}
		}
		f.mu.Unlock()

		fl.data, fl.err = f.readRange(ctx, o.path, start, stop-start)
		if fl.err == nil && store {
			// Stored before the request leaves inflight, so a reader that
			// no longer finds it in flight finds it on disk.
			f.storeChunks(o, start, fl.data)
			fl.stored = true
		}
		f.mu.Lock()
		for i := first; i < end; i++ {
			if k := o.entry(diskcache.KindChunk, i); f.inflight[k] == fl {
				delete(f.inflight, k)
			}
		}
		f.mu.Unlock()
		close(fl.done)
		if fl.err != nil {
			return 0, nil, fmt.Errorf("read sst %s: %w", o.id, fl.err)
		}
		return start, fl.data, nil
	}
}

// storeChunks stores the chunks in data, which holds whole chunks from start.
func (f *sstFetcher) storeChunks(o sstObject, start int64, data []byte) {
	for off := start; off < start+int64(len(data)); {
		i := uint32(off / sstChunkSize)
		s, e := o.chunkSpan(i)
		f.diskStore(o.entry(diskcache.KindChunk, i), data[s-start:e-start])
		off = e
	}
}

func chunkLen(o sstObject, i uint32) int64 {
	start, end := o.chunkSpan(i)
	return end - start
}

// sstReadable serves Pebble's block cache misses for one SST, from the disk
// cache or object storage. It keeps no fetched bytes in memory.
type sstReadable struct {
	f *sstFetcher
	o sstObject

	mu sync.Mutex
	// While an open holds the metadata (holding), its metadata reads are
	// served from held, read from disk once rather than once per read.
	holding bool
	held    []byte
}

// holdMetadata keeps the metadata region in memory until releaseMetadata,
// for the duration of an open.
func (r *sstReadable) holdMetadata() {
	r.mu.Lock()
	r.holding = true
	r.mu.Unlock()
}

func (r *sstReadable) releaseMetadata() {
	r.mu.Lock()
	r.holding, r.held = false, nil
	r.mu.Unlock()
}

// heldMetadata returns the metadata region while an open holds it, reading
// it on first use, or nil when nothing holds it.
func (r *sstReadable) heldMetadata(ctx context.Context) ([]byte, error) {
	o, f := r.o, r.f
	r.mu.Lock()
	holding, held := r.holding, r.held
	r.mu.Unlock()
	if !holding || o.metaOffset <= 0 || held != nil {
		return held, nil
	}
	region := make([]byte, o.size-o.metaOffset)
	var err error
	if o.small() {
		err = r.readEntry(ctx, o.entry(diskcache.KindWhole, 0), o.size, region, o.metaOffset,
			func(ctx context.Context) ([]byte, error) { return f.whole(ctx, o) })
	} else {
		err = r.readEntry(ctx, o.entry(diskcache.KindMeta, 0), int64(len(region)), region, 0,
			func(ctx context.Context) ([]byte, error) { return f.meta(ctx, o) })
	}
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.holding {
		r.held = region
	}
	r.mu.Unlock()
	return region, nil
}

// readEntry fills p from the whole-object or metadata entry k of the given
// size, at off within it: from disk, else by fetching it.
func (r *sstReadable) readEntry(ctx context.Context, k diskcache.Key, size int64, p []byte, off int64,
	fetch func(context.Context) ([]byte, error)) error {
	if r.f.diskRead(k, size, p, off) {
		return nil
	}
	data, err := fetch(ctx)
	if err != nil {
		return err
	}
	copy(p, data[off:])
	return nil
}

func (f *sstFetcher) readable(o sstObject) *sstReadable { return &sstReadable{f: f, o: o} }

func (r *sstReadable) ReadAt(ctx context.Context, p []byte, off int64) error {
	return (&sstReadHandle{r: r, nextOff: -1}).ReadAt(ctx, p, off)
}

func (*sstReadable) Close() error { return nil }

func (r *sstReadable) Size() int64 { return r.o.size }

func (r *sstReadable) NewReadHandle(objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &sstReadHandle{r: r, nextOff: -1}
}

// sstReadHandle serves one iterator's reads; Pebble never calls a handle
// concurrently. A lookup's read fetches the aligned chunk holding its block;
// a scan's reads ahead, doubling each request up to maxReadAheadChunks.
type sstReadHandle struct {
	r *sstReadable
	// nextOff is where the previous data read ended, or -1.
	nextOff int64
	// chunks is how many chunks the last read-ahead fetched, or zero.
	chunks      uint32
	ahead       []byte
	aheadOffset int64
}

func (h *sstReadHandle) ReadAt(ctx context.Context, p []byte, off int64) error {
	if h.r == nil {
		return io.ErrClosedPipe
	}
	o, f := h.r.o, h.r.f
	end := off + int64(len(p))
	if off < 0 || end > o.size {
		return io.ErrUnexpectedEOF
	}
	if len(p) == 0 {
		return nil
	}
	if o.metaOffset > 0 && off >= o.metaOffset {
		region, err := h.r.heldMetadata(ctx)
		if err != nil {
			return err
		}
		if region != nil {
			copy(p, region[off-o.metaOffset:])
			return nil
		}
	}
	if o.small() {
		return h.r.readEntry(ctx, o.entry(diskcache.KindWhole, 0), o.size, p, off,
			func(ctx context.Context) ([]byte, error) { return f.whole(ctx, o) })
	}
	if off >= o.metaOffset {
		return h.r.readEntry(ctx, o.entry(diskcache.KindMeta, 0), o.size-o.metaOffset, p, off-o.metaOffset,
			func(ctx context.Context) ([]byte, error) { return f.meta(ctx, o) })
	}
	if end > o.metaOffset {
		// Pebble never reads across the data and metadata regions; serve it
		// as two reads all the same.
		split := o.metaOffset - off
		if err := h.ReadAt(ctx, p[:split], off); err != nil {
			return err
		}
		return h.ReadAt(ctx, p[split:], o.metaOffset)
	}
	return h.readData(ctx, p, off)
}

// readData fills p, at off in the data region, chunk by chunk: from the
// read-ahead buffer, else from disk, else by fetching a run that covers the
// rest of the read (see fetchChunks).
func (h *sstReadHandle) readData(ctx context.Context, p []byte, off int64) error {
	o, f := h.r.o, h.r.f
	end := off + int64(len(p))
	sequential := off == h.nextOff
	h.nextOff = end
	if !sequential {
		h.chunks = 0
	}
	store := fillsCaches(ctx)
	for pos := off; pos < end; {
		i := uint32(pos / sstChunkSize)
		chunkStart, chunkEnd := o.chunkSpan(i)
		piece := p[pos-off : min(end, chunkEnd)-off]
		switch {
		case len(h.ahead) > 0 && pos >= h.aheadOffset && pos+int64(len(piece)) <= h.aheadOffset+int64(len(h.ahead)):
			copy(piece, h.ahead[pos-h.aheadOffset:])
		case f.diskRead(o.entry(diskcache.KindChunk, i), chunkEnd-chunkStart, piece, pos-chunkStart):
		default:
			// Each request of a continuing scan reads twice as far ahead.
			if sequential {
				h.chunks = min(max(2*h.chunks, 1), maxReadAheadChunks)
			}
			// end is at most MetaOffset, so its chunk exists.
			want := max(min(i+max(h.chunks, 1), o.numChunks()), uint32((end-1)/sstChunkSize)+1)
			start, data, err := f.fetchChunks(ctx, o, i, i, want, store)
			if err != nil {
				return err
			}
			h.ahead, h.aheadOffset = data, start
			copy(piece, data[pos-start:])
		}
		pos += int64(len(piece))
	}
	return nil
}

func (h *sstReadHandle) Close() error {
	h.r = nil
	h.ahead = nil
	return nil
}

func (*sstReadHandle) SetupForCompaction() {}

// RecordCacheHit keeps a scan's position when Pebble serves a block from its
// own cache, so the next miss is recognised as continuing the scan.
func (h *sstReadHandle) RecordCacheHit(_ context.Context, off, length int64) {
	if h.r != nil && off+length <= h.r.o.metaOffset {
		h.nextOff = off + length
	}
}
