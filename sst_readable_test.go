package isledb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/diskcache"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// byteRange is one ranged GET of an SST object.
type byteRange struct{ start, end int64 } // [start, end)

// rangeRecorder records the ranged GETs fake S3 serves for SST objects.
type rangeRecorder struct {
	mu     sync.Mutex
	ranges []byteRange
}

func (r *rangeRecorder) observe(request *http.Request) {
	header := request.Header.Get("Range")
	if request.Method != http.MethodGet || header == "" || !strings.Contains(request.URL.Path, ".sst") {
		return
	}
	var first, last int64
	if _, err := fmt.Sscanf(header, "bytes=%d-%d", &first, &last); err != nil {
		return
	}
	r.mu.Lock()
	r.ranges = append(r.ranges, byteRange{first, last + 1})
	r.mu.Unlock()
}

func (r *rangeRecorder) take() []byteRange {
	r.mu.Lock()
	defer r.mu.Unlock()
	taken := r.ranges
	r.ranges = nil
	return taken
}

// readableTestFixture is one SST in fake S3, its manifest, and a reader whose
// ranged GETs are recorded. Values are incompressible, so the data region
// spans many chunks.
type readableTestFixture struct {
	ctx     context.Context
	reader  *Reader
	entries []internal.MemEntry
	state   *manifestState
	meta    sstMetadata
	ranges  *rangeRecorder
}

func newReadableTestFixture(t *testing.T, n int, chunked bool) *readableTestFixture {
	t.Helper()
	ctx := context.Background()
	ranges := &rangeRecorder{}
	bucketURL := setupFakeS3BucketURLWithObserver(t, ranges.observe)
	store, err := blobstore.Open(ctx, bucketURL, fmt.Sprintf("readable-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	reader := newBlockCacheTestReader(t, ctx, store, readerOptions{}, chunked)

	rng := rand.New(rand.NewSource(1))
	entries := make([]internal.MemEntry, n)
	for i := range entries {
		value := make([]byte, 200)
		rng.Read(value)
		entries[i] = internal.MemEntry{Key: kvLeveledBenchmarkKey(i), Seq: uint64(i + 1), Kind: internal.OpPut, Value: value}
	}
	result, err := writeSST(ctx, &sliceSSTIter{entries: entries},
		sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 12, Compression: "snappy"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
		t.Fatalf("store SST: %v", err)
	}
	meta := result.Meta
	meta.Level = 1
	ranges.take()
	return &readableTestFixture{
		ctx: ctx, reader: reader, entries: entries, meta: meta, ranges: ranges,
		state: &manifestState{Levels: []manifest.Level{{Number: 1, SSTs: []manifest.SSTMeta{meta}}}},
	}
}

func (f *readableTestFixture) get(t *testing.T, i int) {
	t.Helper()
	value, found, err := f.reader.getWithManifest(f.ctx, f.state, f.entries[i].Key)
	if err != nil || !found {
		t.Fatalf("Get(%d) found=%t err=%v", i, found, err)
	}
	_ = value
}

func (f *readableTestFixture) scan(t *testing.T, from, limit int) {
	t.Helper()
	kvs, err := f.reader.scanInternalWithManifest(f.ctx, f.state, f.entries[from].Key, nil, limit)
	if err != nil || len(kvs) != min(limit, len(f.entries)-from) {
		t.Fatalf("scan from %d rows=%d err=%v", from, len(kvs), err)
	}
	for j, kv := range kvs {
		if !bytes.Equal(kv.Key, f.entries[from+j].Key) {
			t.Fatalf("scan row %d = %q, want %q", j, kv.Key, f.entries[from+j].Key)
		}
	}
}

// forgetMemory drops the reader's open SSTs and blocks, so the next read
// starts from the disk cache.
func (f *readableTestFixture) forgetMemory() {
	f.reader.diskCache.Sync()
	f.reader.openSSTs.clear()
	f.reader.blockCache.clear()
}

func (f *readableTestFixture) object() sstObject { return f.reader.fetcher.object(f.meta) }

// dataRanges keeps the GETs that read the data region.
func (f *readableTestFixture) dataRanges(ranges []byteRange) []byteRange {
	var data []byteRange
	for _, r := range ranges {
		if r.start < f.meta.MetaOffset {
			data = append(data, r)
		}
	}
	return data
}

// TestSSTReadable_LookupFetchesAlignedChunk checks a cold lookup's requests:
// the metadata region and Bloom sidecar exactly, and one aligned chunk of
// data; a lookup of another key in that chunk then needs no request.
func TestSSTReadable_LookupFetchesAlignedChunk(t *testing.T) {
	f := newReadableTestFixture(t, 20_000, true)
	o := f.object()
	if o.numChunks() < 8 {
		t.Fatalf("fixture data region spans %d chunks, want several", o.numChunks())
	}

	f.get(t, 10_000)
	ranges := f.ranges.take()
	var meta, bloom int
	for _, r := range ranges {
		switch {
		case r == byteRange{f.meta.MetaOffset, f.meta.Size}:
			meta++
		case r == byteRange{f.meta.Bloom.Offset, f.meta.Bloom.Offset + f.meta.Bloom.Length}:
			bloom++
		}
	}
	data := f.dataRanges(ranges)
	if meta != 1 || bloom != 1 || len(data) != 1 || len(ranges) != 3 {
		t.Fatalf("cold lookup ranges=%v, want metadata, Bloom and one data chunk", ranges)
	}
	chunk := data[0]
	if chunk.start%sstChunkSize != 0 || (chunk.end%sstChunkSize != 0 && chunk.end != f.meta.MetaOffset) ||
		chunk.end-chunk.start > 2*sstChunkSize {
		t.Fatalf("data request %v is not an aligned chunk", chunk)
	}

	// A neighbour in the same chunk, after memory forgets the SST: served
	// from disk.
	f.forgetMemory()
	for i := 10_001; i < 10_400; i++ {
		start, _ := o.chunkSpan(uint32(chunk.start / sstChunkSize))
		_ = start
	}
	f.get(t, 10_001)
	if got := f.dataRanges(f.ranges.take()); len(got) != 0 {
		t.Fatalf("lookup in a cached chunk made data requests %v", got)
	}
	if stats := f.reader.DiskCacheStats(); stats.Data.Hits == 0 || stats.Meta.Hits == 0 {
		t.Fatalf("lookup did not read the disk cache: %+v", stats)
	}
}

// TestSSTReadable_SmallSSTIsOneRequest reads a small SST: one request fetches
// the SST and its Bloom sidecar, cached as two entries; nothing else is
// fetched afterwards.
func TestSSTReadable_SmallSSTIsOneRequest(t *testing.T) {
	f := newReadableTestFixture(t, 2_000, false)
	f.get(t, 100)
	ranges := f.ranges.take()
	want := byteRange{0, f.meta.Bloom.Offset + f.meta.Bloom.Length}
	if len(ranges) != 1 || ranges[0] != want {
		t.Fatalf("small SST ranges=%v, want one request %v", ranges, want)
	}
	f.forgetMemory()
	f.reader.bloomCache.clear()
	f.get(t, 1_500)
	f.scan(t, 0, 500)
	if got := f.ranges.take(); len(got) != 0 {
		t.Fatalf("reads of a cached small SST made requests %v", got)
	}
	if stats := f.reader.DiskCacheStats(); stats.Data.EntryCount != 1 || stats.Meta.EntryCount != 1 {
		t.Fatalf("disk entries %+v, want the whole SST and its Bloom filter", stats)
	}
}

// TestSSTReadable_ScanStopsBeforeCachedChunks caches one chunk with a lookup,
// then scans across it from cold memory: no request covers the cached chunk.
func TestSSTReadable_ScanStopsBeforeCachedChunks(t *testing.T) {
	f := newReadableTestFixture(t, 20_000, true)
	o := f.object()
	f.get(t, 10_000)
	cached := f.dataRanges(f.ranges.take())[0]
	f.forgetMemory()

	f.scan(t, 5_000, 10_000)
	for _, r := range f.dataRanges(f.ranges.take()) {
		if r.start < cached.end && cached.start < r.end {
			t.Fatalf("scan request %v overlaps cached chunk %v", r, cached)
		}
	}
	_ = o
}

// TestSSTReadable_LongScanStoresOnlyItsStart scans a whole chunked SST from
// cold: its requests grow, so they are few, and only the chunks read while
// filling, at most two, are stored on disk.
func TestSSTReadable_LongScanStoresOnlyItsStart(t *testing.T) {
	f := newReadableTestFixture(t, 20_000, true)
	o := f.object()
	f.scan(t, 0, len(f.entries))
	data := f.dataRanges(f.ranges.take())
	var covered int64
	for _, r := range data {
		covered += r.end - r.start
	}
	if covered < f.meta.MetaOffset {
		t.Fatalf("scan requests covered %d bytes, want the data region %d", covered, f.meta.MetaOffset)
	}
	if limit := 12; len(data) > limit {
		t.Fatalf("scan of %d chunks made %d data requests, want at most %d", o.numChunks(), len(data), limit)
	}
	f.reader.diskCache.Sync()
	if got := f.reader.DiskCacheStats().Data.EntryCount; got == 0 || got > 2 {
		t.Fatalf("long scan stored %d chunks, want its first one or two", got)
	}
}

// chunkCached reports whether the disk cache holds data chunk i of f's SST.
func (f *readableTestFixture) chunkCached(i uint32) bool {
	o := f.object()
	start, end := o.chunkSpan(i)
	return f.reader.diskCache.Contains(o.entry(diskcache.KindChunk, i), end-start)
}

// TestSSTReadable_FailedOpenKeepsCache fails an SST open for a reason other
// than corruption, a canceled metadata fetch: what the disk cache holds of the
// SST stays, and nothing counts as a corruption.
func TestSSTReadable_FailedOpenKeepsCache(t *testing.T) {
	f := newReadableTestFixture(t, 20_000, true)
	f.get(t, 10_000)
	chunk := uint32(f.dataRanges(f.ranges.take())[0].start / sstChunkSize)
	f.forgetMemory()
	// Evicted metadata makes the next open fetch it.
	f.reader.diskCache.Remove(f.object().entry(diskcache.KindMeta, 0))

	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, _, err := f.reader.openSSTIterBounded(ctx, f.meta, nil, nil, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("open with canceled context err=%v, want context.Canceled", err)
	}
	if !f.chunkCached(chunk) {
		t.Fatal("failed open dropped a cached chunk")
	}
	if stats := f.reader.DiskCacheStats(); stats.SSTDrops != 0 || stats.Meta.Corruptions != 0 || stats.Data.Corruptions != 0 {
		t.Fatalf("failed open counted damage: %+v", stats)
	}

	f.ranges.take()
	f.get(t, 10_000)
	if got := f.ranges.take(); len(got) != 1 || got[0] != (byteRange{f.meta.MetaOffset, f.meta.Size}) {
		t.Fatalf("lookup after failed open ranges=%v, want only the metadata region", got)
	}
}

// TestSSTReadable_DamagedMetadataHealsWithinLookup damages the cached
// metadata entry in ways Pebble finds at different points: the whole entry
// (the open fails on the footer), only the index block (the open succeeds and
// the iterator fails), and only the footer's metaindex handle, made to point
// past the end of the SST (the read fails without a corruption error). Each
// time the lookup still succeeds, dropping the SST and fetching it again.
func TestSSTReadable_DamagedMetadataHealsWithinLookup(t *testing.T) {
	cases := []struct {
		name   string
		damage func(t *testing.T, meta []byte, layout *sstable.Layout, metaOffset int64)
	}{
		{"whole_entry", func(t *testing.T, meta []byte, _ *sstable.Layout, _ int64) {
			for i := range meta {
				meta[i] ^= 0xff
			}
		}},
		{"index_block", func(t *testing.T, meta []byte, layout *sstable.Layout, metaOffset int64) {
			index := layout.TopIndex
			if index.Length == 0 {
				index = layout.Index[0]
			}
			at := int64(index.Offset) - metaOffset + int64(index.Length)/2
			meta[at] ^= 0xff
		}},
		{"footer_handle_out_of_range", func(t *testing.T, meta []byte, layout *sstable.Layout, metaOffset int64) {
			// The footer starts with a checksum type byte, then the metaindex
			// handle: varint offset, varint length.
			footer := meta[int64(layout.Footer.Offset)-metaOffset:]
			_, n1 := binary.Uvarint(footer[1:])
			_, n2 := binary.Uvarint(footer[1+n1:])
			handle := binary.AppendUvarint(nil, uint64(metaOffset)+uint64(len(meta))+1<<20)
			handle = binary.AppendUvarint(handle, 64)
			if len(handle) > n1+n2 {
				t.Fatalf("new handle is %d bytes, room for %d", len(handle), n1+n2)
			}
			// Shift the index handle back to follow the new metaindex handle.
			rest := append([]byte(nil), footer[1+n1+n2:]...)
			copy(footer[1:], handle)
			copy(footer[1+len(handle):], rest)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReadableTestFixture(t, 20_000, true)
			f.get(t, 10_000)
			sst := f.reader.openSSTs.acquire(f.meta.ID)
			layout, err := sst.reader.Layout()
			sst.unref()
			if err != nil {
				t.Fatalf("layout: %v", err)
			}
			f.forgetMemory()
			f.ranges.take()

			paths, err := filepath.Glob(filepath.Join(f.reader.cacheDir, "artifacts", "v4", "meta", "*",
				fmt.Sprintf("%x.meta.*", f.object().key)))
			if err != nil || len(paths) != 1 {
				t.Fatalf("metadata file matches=%v err=%v", paths, err)
			}
			meta, err := os.ReadFile(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			tc.damage(t, meta, layout, f.meta.MetaOffset)
			if err := os.WriteFile(paths[0], meta, 0o600); err != nil {
				t.Fatal(err)
			}

			f.get(t, 10_000)
			if stats := f.reader.DiskCacheStats(); stats.SSTDrops != 1 {
				t.Fatalf("damaged metadata not counted once: %+v", stats)
			}
			var refetched bool
			for _, r := range f.ranges.take() {
				refetched = refetched || r == (byteRange{f.meta.MetaOffset, f.meta.Size})
			}
			if !refetched {
				t.Fatal("damaged metadata was not fetched again")
			}
			// The SST is healthy again: a lookup in memory needs nothing.
			f.get(t, 10_000)
			if got := f.ranges.take(); len(got) != 0 {
				t.Fatalf("lookup after healing ranges=%v, want none", got)
			}
		})
	}
}

// TestSSTReadable_OriginChecksumMismatchIsNotDamage reads a small SST whose
// object does not match its manifest checksum: the read fails without
// dropping anything or retrying, so each lookup fetches the object once for
// its Bloom filter and once for its open.
func TestSSTReadable_OriginChecksumMismatchIsNotDamage(t *testing.T) {
	f := newReadableTestFixture(t, 2_000, false)
	sum := []byte(f.meta.Checksum)
	last := len(sum) - 1
	if sum[last] == '0' {
		sum[last] = '1'
	} else {
		sum[last] = '0'
	}
	f.meta.Checksum = string(sum)
	f.state.Levels[0].SSTs[0].Checksum = f.meta.Checksum
	if !f.object().small() {
		t.Fatal("fixture SST is not read whole")
	}

	for range 2 {
		if _, _, err := f.reader.getWithManifest(f.ctx, f.state, f.entries[1_000].Key); err == nil ||
			!strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("Get err=%v, want a checksum mismatch", err)
		}
		if got := f.ranges.take(); len(got) != 2 {
			t.Fatalf("lookup ranges=%v, want the object fetched for the Bloom filter and the open", got)
		}
	}
	if stats := f.reader.DiskCacheStats(); stats.SSTDrops != 0 || stats.Data.EntryCount != 0 {
		t.Fatalf("origin mismatch counted as damage or cached: %+v", stats)
	}
}

// TestDamaged classifies read errors: failed fetches and ended contexts leave
// the cache alone; every other error means damaged bytes.
func TestDamaged(t *testing.T) {
	fetch := &fetchError{errors.New("connection reset")}
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fetch, false},
		{fmt.Errorf("read block: %w", fetch), false},
		{errors.Join(errors.New("open"), fetch), false},
		{context.Canceled, false},
		{fmt.Errorf("read: %w", context.DeadlineExceeded), false},
		{pebble.ErrCorruption, true},
		{io.ErrUnexpectedEOF, true},
		{errors.New("anything else"), true},
	}
	for _, tc := range cases {
		if got := damaged(tc.err); got != tc.want {
			t.Errorf("damaged(%v) = %t, want %t", tc.err, got, tc.want)
		}
	}
}

// flipFile inverts every byte of the file at path.
func flipFile(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		data[i] ^= 0xff
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestSSTReadable_CorruptChunkHealsWithinLookup damages a cached chunk on
// disk: the next lookup through it still succeeds, fetching it again.
func TestSSTReadable_CorruptChunkHealsWithinLookup(t *testing.T) {
	f := newReadableTestFixture(t, 20_000, true)
	f.get(t, 10_000)
	cached := f.dataRanges(f.ranges.take())[0]
	f.forgetMemory()

	name := (f.object().entry(diskcache.KindChunk, uint32(cached.start/sstChunkSize))).Object
	paths, err := filepath.Glob(filepath.Join(f.reader.cacheDir, "artifacts", "v4", "data", "*",
		fmt.Sprintf("%x.c%d.*", name, cached.start/sstChunkSize)))
	if err != nil || len(paths) != 1 {
		t.Fatalf("chunk file matches=%v err=%v", paths, err)
	}
	flipFile(t, paths[0])

	f.get(t, 10_000)
	if stats := f.reader.DiskCacheStats(); stats.SSTDrops != 1 || stats.Data.Corruptions != 0 {
		t.Fatalf("damaged chunk not counted once as a damaged SST: %+v", stats)
	}
	if got := f.dataRanges(f.ranges.take()); len(got) == 0 {
		t.Fatal("damaged chunk was not fetched again")
	}
}

// TestSSTReadable_ReadAheadDoublesPerRequest scans a chunked SST from cold:
// once the scan's tail reads privately, each request fetches twice as many
// chunks as the one before, up to the maximum.
func TestSSTReadable_ReadAheadDoublesPerRequest(t *testing.T) {
	f := newReadableTestFixture(t, 40_000, true)
	f.scan(t, 0, len(f.entries))
	data := f.dataRanges(f.ranges.take())
	var sizes []int64
	for _, r := range data {
		sizes = append(sizes, (r.end-r.start+sstChunkSize-1)/sstChunkSize)
	}
	// The last request is cut at the end of the data region.
	for i := 1; i < len(sizes)-1; i++ {
		if sizes[i] != 1 && sizes[i] != min(2*sizes[i-1], maxReadAheadChunks) {
			t.Fatalf("request sizes in chunks %v: %d does not double %d", sizes, sizes[i], sizes[i-1])
		}
	}
	if largest := sizes[len(sizes)-2]; largest < 8 {
		t.Fatalf("request sizes in chunks %v never grew", sizes)
	}
}

// TestSSTReadable_BloomMissFetchesOnlyBloom evicts a small SST's Bloom
// filter while the SST stays cached whole: the next lookup fetches the Bloom
// range alone, not the whole object again.
func TestSSTReadable_BloomMissFetchesOnlyBloom(t *testing.T) {
	f := newReadableTestFixture(t, 2_000, false)
	if !f.object().small() || f.meta.Bloom.Length == 0 {
		t.Fatal("fixture SST is not small with a Bloom filter")
	}
	f.get(t, 1_000)
	f.forgetMemory()
	f.reader.bloomCache.clear()
	f.reader.diskCache.Remove(f.object().entry(diskcache.KindBloom, 0))
	f.ranges.take()

	f.get(t, 1_000)
	bloom := byteRange{f.meta.Bloom.Offset, f.meta.Bloom.Offset + f.meta.Bloom.Length}
	if got := f.ranges.take(); len(got) != 1 || got[0] != bloom {
		t.Fatalf("lookup after Bloom eviction ranges=%v, want only the Bloom range %v", got, bloom)
	}
}

// TestSSTReadable_WaiterStoresSharedChunks has a caller that caches wait on
// a chunk request made by one that does not, as a prefetch or lookup waiting
// on a scan's private read: the waiter stores the chunks it receives.
func TestSSTReadable_WaiterStoresSharedChunks(t *testing.T) {
	f := newReadableTestFixture(t, 20_000, true)
	fetcher, o := f.reader.fetcher, f.object()
	start, end := o.chunkSpan(3)
	data := make([]byte, end-start)
	if err := (&sstReadHandle{r: fetcher.readable(o), nextOff: -1}).readData(f.ctx, data, start); err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	f.reader.diskCache.Sync()
	f.reader.diskCache.Purge(diskcache.TierData)
	if f.chunkCached(3) {
		t.Fatal("chunk still cached after purge")
	}

	// A finished private request for chunk 3, still registered in flight.
	k := o.entry(diskcache.KindChunk, 3)
	fl := &chunkFetch{done: make(chan struct{}), start: start, end: end, data: data}
	close(fl.done)
	fetcher.mu.Lock()
	fetcher.inflight[k] = fl
	fetcher.mu.Unlock()
	got, gotData, err := fetcher.fetchChunks(f.ctx, o, 3, 3, 4, storeSync)
	fetcher.mu.Lock()
	delete(fetcher.inflight, k)
	fetcher.mu.Unlock()
	if err != nil || got != start || !bytes.Equal(gotData, data) {
		t.Fatalf("fetchChunks start=%d err=%v, want the shared request's bytes", got, err)
	}
	if len(f.ranges.take()) != 1 {
		t.Fatal("waiter made a request of its own")
	}
	if !f.chunkCached(3) {
		t.Fatal("waiter did not store the chunks it received")
	}
}
