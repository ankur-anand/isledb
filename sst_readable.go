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
	// whole reports that the SST is fetched and cached whole: it is no larger
	// than the fetcher's small-SST limit, or its metadata offset is unknown.
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

// object describes an SST's object for this fetcher.
func (f *sstFetcher) object(meta sstMetadata) sstObject {
	return newSSTObject(f.store, meta, f.smallLimit)
}

func (f *sstFetcher) close() { f.loads.Close(ErrReaderClosed) }

// storeMode says how fetched bytes are cached on disk.
type storeMode uint8

const (
	storeNone  storeMode = iota // a scan's private tail
	storeAsync                  // reads: queued, dropped if the queue is full
	storeSync                   // prefetch: written before returning
)

func (f *sstFetcher) diskRead(k diskcache.Key, size int64, p []byte, off int64) bool {
	return f.disk != nil && f.disk.ReadAt(k, size, p, off)
}

func (f *sstFetcher) diskHas(k diskcache.Key, size int64) bool {
	return f.disk != nil && f.disk.Contains(k, size)
}

func (f *sstFetcher) diskStore(k diskcache.Key, data []byte, mode storeMode) {
	if f.disk == nil || len(data) == 0 {
		return
	}
	switch mode {
	case storeAsync:
		f.disk.Write(k, data)
	case storeSync:
		_ = f.disk.Put(k, data)
	}
}

// dropObject removes every cached part of an SST whose contents proved
// damaged, so the next read fetches them again; which part was damaged is not
// known, so each counts as a corruption. Its Bloom sidecar has its own
// checksum and is left alone.
func (f *sstFetcher) dropObject(o sstObject) {
	if f.disk == nil {
		return
	}
	f.disk.ReportCorrupt(o.entry(diskcache.KindWhole, 0))
	f.disk.ReportCorrupt(o.entry(diskcache.KindMeta, 0))
	for i := range o.numChunks() {
		f.disk.ReportCorrupt(o.entry(diskcache.KindChunk, i))
	}
}

// dropMetadata removes the cached parts an SST open reads, after the open
// found them damaged: the metadata entry, and the whole-object entry a small
// SST is read from. Data chunks are left alone; a damaged block fails its own
// checksum when read.
func (f *sstFetcher) dropMetadata(o sstObject) {
	if f.disk == nil {
		return
	}
	f.disk.ReportCorrupt(o.entry(diskcache.KindWhole, 0))
	f.disk.ReportCorrupt(o.entry(diskcache.KindMeta, 0))
}

func (f *sstFetcher) readRange(ctx context.Context, path string, off, length int64) ([]byte, error) {
	start := time.Now()
	reader, err := f.store.ReadRangeStream(ctx, path, off, length)
	if err != nil {
		f.m.ObserveSSTRangeRead(time.Since(start), 0, err)
		return nil, err
	}
	data := make([]byte, length)
	n, readErr := io.ReadFull(reader, data)
	err = errors.Join(readErr, reader.Close())
	f.m.ObserveSSTRangeRead(time.Since(start), int64(n), err)
	if err != nil {
		return nil, err
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

// whole returns a small SST's object from its start through the end of its
// Bloom sidecar, fetched in one request. It verifies both parts and caches
// each as its own entry.
func (f *sstFetcher) whole(ctx context.Context, o sstObject, mode storeMode) ([]byte, error) {
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
				return nil, fmt.Errorf("validate sst %s: checksum mismatch", o.id)
			}
		}
		f.diskStore(o.entry(diskcache.KindWhole, 0), data[:o.size], mode)
		if end > o.size {
			if bloom := data[o.bloomOffset:end]; validateBloomChecksum(o.bloomChecksum, bloom) == nil {
				f.diskStore(o.entry(diskcache.KindBloom, 0), bloom, mode)
			}
		}
		return data, nil
	})
}

// meta returns a large SST's metadata region, [MetaOffset, Size), fetched in
// one request and cached as one entry.
func (f *sstFetcher) meta(ctx context.Context, o sstObject, mode storeMode) ([]byte, error) {
	return f.load(ctx, o.entry(diskcache.KindMeta, 0), func(ctx context.Context) ([]byte, error) {
		data, err := f.readRange(ctx, o.path, o.metaOffset, o.size-o.metaOffset)
		if err != nil {
			return nil, fmt.Errorf("read sst %s metadata: %w", o.id, err)
		}
		f.diskStore(o.entry(diskcache.KindMeta, 0), data, mode)
		return data, nil
	})
}

// bloom returns an SST's verified Bloom sidecar: from disk, else with the
// whole object for a small SST, else with one request of its own.
func (f *sstFetcher) bloom(ctx context.Context, o sstObject, mode storeMode) ([]byte, error) {
	k := o.entry(diskcache.KindBloom, 0)
	data := make([]byte, o.bloomLength)
	if f.diskRead(k, o.bloomLength, data, 0) {
		if err := validateBloomChecksum(o.bloomChecksum, data); err == nil {
			return data, nil
		}
		f.disk.ReportCorrupt(k)
	}
	if o.small() {
		object, err := f.whole(ctx, o, mode)
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
			return f.readRange(ctx, o.path, o.bloomOffset, o.bloomLength)
		})
		if err != nil {
			return nil, fmt.Errorf("read bloom %s: %w", o.id, err)
		}
	}
	if err := validateBloomChecksum(o.bloomChecksum, data); err != nil {
		return nil, fmt.Errorf("validate bloom %s: %w", o.id, err)
	}
	f.diskStore(k, data, mode)
	return data, nil
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
		data, err := f.whole(ctx, o, storeSync)
		return int64(len(data)), err
	}
	if !f.diskHas(o.entry(diskcache.KindMeta, 0), o.size-o.metaOffset) {
		data, err := f.meta(ctx, o, storeSync)
		if err != nil {
			return fetched, err
		}
		fetched += int64(len(data))
	}
	if o.bloomLength > 0 && !f.diskHas(o.entry(diskcache.KindBloom, 0), o.bloomLength) {
		if _, err := f.bloom(ctx, o, storeSync); err != nil {
			return fetched, err
		}
		fetched += o.bloomLength
	}
	for i := uint32(0); i < o.numChunks(); {
		if f.diskHas(o.entry(diskcache.KindChunk, i), chunkLen(o, i)) {
			i++
			continue
		}
		start, data, err := f.fetchChunks(ctx, o, i, i, min(i+maxReadAheadChunks, o.numChunks()), storeSync)
		if err != nil {
			return fetched, err
		}
		fetched += int64(len(data))
		i = uint32((start + int64(len(data)) + sstChunkSize - 1) / sstChunkSize)
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

// fetchChunks returns a run of whole chunks covering chunks first through
// last, extended toward want (exclusive) but stopping before a chunk that is
// already cached or being fetched, so no request repeats another's bytes. A
// reader needing a chunk already in flight waits for that request instead.
// It returns where the run starts and its bytes.
func (f *sstFetcher) fetchChunks(ctx context.Context, o sstObject, first, last, want uint32, mode storeMode) (int64, []byte, error) {
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
				return fl.start, fl.data, nil
			}
			continue // the request failed or did not cover the run: fetch it here
		}
		end := max(want, last+1)
		for i := last + 1; i < end; i++ {
			if f.inflight[o.entry(diskcache.KindChunk, i)] != nil ||
				f.diskHas(o.entry(diskcache.KindChunk, i), chunkLen(o, i)) {
				end = i
				break
			}
		}
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
		if fl.err == nil {
			for i := first; i < end; i++ {
				s, e := o.chunkSpan(i)
				f.diskStore(o.entry(diskcache.KindChunk, i), fl.data[s-start:e-start], mode)
			}
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

func chunkLen(o sstObject, i uint32) int64 {
	start, end := o.chunkSpan(i)
	return end - start
}

// sstReadable serves Pebble's reads of one SST, through the disk cache, from
// object storage. Decoded blocks are cached above it by Pebble's block cache,
// so a read here is a block cache miss.
//
// It keeps the metadata region, or a small SST's whole object, that it
// fetched until the disk cache serves it, so an entry the cache could not
// store, or dropped from a full write queue, is not fetched again by every
// read of this open SST.
type sstReadable struct {
	f *sstFetcher
	o sstObject

	mu     sync.Mutex
	pinned []byte
	// holding, while the SST is being opened, makes metadata reads come from
	// held, the whole metadata region read once, so the open's several
	// metadata reads cost one disk read rather than one each. Both are
	// released when the open finishes.
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
			func(ctx context.Context) ([]byte, error) { return f.whole(ctx, o, storeAsync) })
	} else {
		err = r.readEntry(ctx, o.entry(diskcache.KindMeta, 0), int64(len(region)), region, 0,
			func(ctx context.Context) ([]byte, error) { return f.meta(ctx, o, storeAsync) })
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
// size, at off within it: from disk, else from what this readable fetched,
// else by fetching it.
func (r *sstReadable) readEntry(ctx context.Context, k diskcache.Key, size int64, p []byte, off int64,
	fetch func(context.Context) ([]byte, error)) error {
	if r.f.diskRead(k, size, p, off) {
		r.mu.Lock()
		r.pinned = nil
		r.mu.Unlock()
		return nil
	}
	r.mu.Lock()
	pinned := r.pinned
	r.mu.Unlock()
	if pinned == nil {
		data, err := fetch(ctx)
		if err != nil {
			return err
		}
		pinned = data
		r.mu.Lock()
		r.pinned = data
		r.mu.Unlock()
	}
	copy(p, pinned[off:])
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

// sstReadHandle serves one iterator's reads. Pebble gives each iterator its
// own handle and does not call a handle concurrently.
//
// A data read that does not continue the previous one, as a lookup's, fetches
// the aligned chunk holding its block. A read that does, as a scan's, reads
// ahead: each request fetches twice as many chunks as the last, up to
// maxReadAheadChunks, into a buffer only this iterator uses. Either way the
// request stops before chunks already cached or in flight, and chunks already
// cached are read from disk without any read-ahead.
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
			func(ctx context.Context) ([]byte, error) { return f.whole(ctx, o, storeAsync) })
	}
	if off >= o.metaOffset {
		return h.r.readEntry(ctx, o.entry(diskcache.KindMeta, 0), o.size-o.metaOffset, p, off-o.metaOffset,
			func(ctx context.Context) ([]byte, error) { return f.meta(ctx, o, storeAsync) })
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

// readData fills p, at off in the data region, chunk by chunk: from this
// iterator's read-ahead buffer, else from a chunk on disk, else by fetching a
// run of chunks from the first one missing. A run reads ahead as far as the
// read-ahead allows but stops before a chunk already cached or in flight, so
// a block crossing into a cached chunk fetches only its missing part.
func (h *sstReadHandle) readData(ctx context.Context, p []byte, off int64) error {
	o, f := h.r.o, h.r.f
	end := off + int64(len(p))
	sequential := off == h.nextOff
	h.nextOff = end
	if !sequential {
		h.chunks = 0
	}
	mode := storeNone
	if fillsCaches(ctx) {
		mode = storeAsync
	}
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
			want := min(i+max(h.chunks, 1), o.numChunks())
			start, data, err := f.fetchChunks(ctx, o, i, i, want, mode)
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
