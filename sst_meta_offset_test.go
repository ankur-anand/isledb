package isledb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func metaOffsetTestEntries(n int) []internal.MemEntry {
	entries := make([]internal.MemEntry, n)
	for i := range entries {
		entries[i] = internal.MemEntry{
			Key:   []byte(fmt.Sprintf("key-%08d", i)),
			Seq:   uint64(i + 1),
			Kind:  internal.OpPut,
			Value: []byte(fmt.Sprintf("value-%08d-padding-padding-padding", i)),
		}
		if i%7 == 0 {
			entries[i].Kind = internal.OpDelete
			entries[i].Value = nil
		}
	}
	return entries
}

// requireMetaOffsetAtFirstMetaBlock checks that MetaOffset is exactly where
// the first non-data block starts, so [MetaOffset, Size) covers every block
// Pebble reads besides data.
func requireMetaOffsetAtFirstMetaBlock(t *testing.T, meta sstMetadata, payload []byte) {
	t.Helper()
	reader, err := sstable.NewReader(context.Background(), newMemReadable(payload), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("open SST: %v", err)
	}
	defer func() { _ = reader.Close() }()
	layout, err := reader.Layout()
	if err != nil {
		t.Fatalf("SST layout: %v", err)
	}

	first := uint64(len(payload))
	note := func(offset, length uint64) {
		if length > 0 && offset < first {
			first = offset
		}
	}
	for _, h := range layout.Index {
		note(h.Offset, h.Length)
	}
	note(layout.TopIndex.Offset, layout.TopIndex.Length)
	for _, h := range layout.Filter {
		note(h.Offset, h.Length)
	}
	note(layout.RangeDel.Offset, layout.RangeDel.Length)
	note(layout.RangeKey.Offset, layout.RangeKey.Length)
	for _, h := range layout.ValueBlock {
		note(h.Offset, h.Length)
	}
	note(layout.ValueIndex.Offset, layout.ValueIndex.Length)
	note(layout.Properties.Offset, layout.Properties.Length)
	note(layout.MetaIndex.Offset, layout.MetaIndex.Length)
	note(layout.Footer.Offset, layout.Footer.Length)

	if meta.MetaOffset <= 0 || uint64(meta.MetaOffset) != first {
		t.Fatalf("MetaOffset=%d, first metadata block at %d (size %d)",
			meta.MetaOffset, first, len(payload))
	}
}

func TestWriteSST_RecordsMetaOffset(t *testing.T) {
	for _, n := range []int{1, 50, 5_000} {
		t.Run(fmt.Sprintf("entries=%d", n), func(t *testing.T) {
			result, err := writeSST(context.Background(),
				&sliceSSTIter{entries: metaOffsetTestEntries(n)},
				sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 10, Compression: "snappy"}, 1)
			if err != nil {
				t.Fatalf("writeSST: %v", err)
			}
			requireMetaOffsetAtFirstMetaBlock(t, result.Meta, sstPayload(t, result.Meta, result.SSTData))
		})
	}
}

func TestWriteSSTStreaming_RecordsMetaOffset(t *testing.T) {
	var uploaded []byte
	result, err := writeSSTStreaming(context.Background(),
		&sliceSSTIter{entries: metaOffsetTestEntries(5_000)},
		sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 10, Compression: "snappy"},
		testSSTStreamIdentity(1, 1, 5_000),
		func(_ context.Context, _ string, r io.Reader) error {
			var err error
			uploaded, err = io.ReadAll(r)
			return err
		})
	if err != nil {
		t.Fatalf("writeSSTStreaming: %v", err)
	}
	requireMetaOffsetAtFirstMetaBlock(t, result.Meta, sstPayload(t, result.Meta, uploaded))
}

func TestWriteMultipleSSTsStreaming_RecordsMetaOffset(t *testing.T) {
	var mu sync.Mutex
	uploads := map[string][]byte{}
	results, err := writeMultipleSSTsStreaming(context.Background(),
		&sliceSSTIter{entries: metaOffsetTestEntries(5_000)},
		sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 10, Compression: "snappy"},
		testSSTStreamSetIdentity(1), 16<<10,
		func(_ context.Context, id string, r io.Reader) error {
			data, err := io.ReadAll(r)
			mu.Lock()
			uploads[id] = data
			mu.Unlock()
			return err
		})
	if err != nil {
		t.Fatalf("writeMultipleSSTsStreaming: %v", err)
	}
	if len(results) < 2 {
		t.Fatalf("got %d SSTs, want several to cover the rollover path", len(results))
	}
	for _, result := range results {
		requireMetaOffsetAtFirstMetaBlock(t, result.Meta,
			sstPayload(t, result.Meta, uploads[result.Meta.ID]))
	}
}

// TestSSTRangeReadable_MetaRegionServesPebbleOpen opens an SST the way the
// reader does and checks that every metadata read comes from one fetch of
// [MetaOffset, Size), which later opens find in the metadata cache.
func TestSSTRangeReadable_MetaRegionServesPebbleOpen(t *testing.T) {
	ctx := context.Background()
	result, err := writeSST(ctx, &sliceSSTIter{entries: metaOffsetTestEntries(5_000)},
		sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 10, Compression: "snappy"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}

	var gets atomic.Int64
	var lastRange atomic.Value
	bucketURL := setupFakeS3BucketURLWithObserver(t, func(request *http.Request) {
		if request.Method == http.MethodGet && request.Header.Get("Range") != "" {
			gets.Add(1)
			lastRange.Store(request.Header.Get("Range"))
		}
	})
	store, err := blobstore.Open(ctx, bucketURL, "meta-region")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	path := store.SSTPath(result.Meta.ID)
	if _, err := store.Write(ctx, path, result.SSTData); err != nil {
		t.Fatalf("write SST: %v", err)
	}

	metaCache := newSSTMetaCache(16 << 20)
	open := func() (*sstable.Reader, error) {
		readable := newSSTRangeReadable(store, path, result.Meta.ID, result.Meta.Size,
			&coalescedLoadGroup{}, DefaultReaderMetrics(nil))
		readable.useMetaRegion(result.Meta.MetaOffset)
		readable.useMetaCache(metaCache)
		return sstable.NewReader(ctx, readable, sstable.ReaderOptions{})
	}

	reader, err := open()
	if err != nil {
		t.Fatalf("open SST: %v", err)
	}
	iter, err := reader.NewIter(sstable.NoTransforms, []byte("key-00002500"), nil, sstable.AssertNoBlobHandles)
	if err != nil {
		t.Fatalf("new iter: %v", err)
	}
	kv := iter.First()
	if kv == nil || string(kv.K.UserKey) != "key-00002500" {
		t.Fatalf("First() = %v, err=%v", kv, iter.Error())
	}
	_ = iter.Close()
	_ = reader.Close()

	// One GET for the metadata region, one for the data block.
	if got := gets.Load(); got != 2 {
		t.Fatalf("cold open + seek issued %d GETs, want 2", got)
	}

	gets.Store(0)
	reader, err = open()
	if err != nil {
		t.Fatalf("reopen SST: %v", err)
	}
	_ = reader.Close()
	if got := gets.Load(); got != 0 {
		t.Fatalf("reopen issued %d GETs (last %v), want 0 from cached metadata region",
			got, lastRange.Load())
	}
}

func TestSSTRangeReadable_UseMetaRegionIgnoresUnusableOffsets(t *testing.T) {
	cases := []struct {
		name   string
		size   int64
		offset int64
		want   int64
	}{
		{name: "unknown", size: 1000, offset: 0, want: 0},
		{name: "negative", size: 1000, offset: -1, want: 0},
		{name: "at end", size: 1000, offset: 1000, want: 0},
		{name: "past end", size: 1000, offset: 2000, want: 0},
		{name: "large region", size: 64 << 20, offset: 1, want: 1},
		{name: "usable", size: 1000, offset: 900, want: 900},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newSSTRangeReadable(nil, "", "sst", tc.size, nil, nil)
			r.useMetaRegion(tc.offset)
			if r.metaOffset != tc.want {
				t.Fatalf("metaOffset=%d, want %d", r.metaOffset, tc.want)
			}
		})
	}
}

// TestSSTWriters_OmitPebbleFilter checks that new SSTs carry only the isledb
// Bloom sidecar. Pebble's filter block is never read, and writing it would
// double the metadata region readers fetch.
func TestSSTWriters_OmitPebbleFilter(t *testing.T) {
	ctx := context.Background()
	opts := sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 10, Compression: "snappy"}

	requireNoPebbleFilter := func(t *testing.T, meta sstMetadata, data []byte) {
		t.Helper()
		if meta.Bloom.Length == 0 {
			t.Fatalf("SST %s has no Bloom sidecar", meta.ID)
		}
		reader, err := sstable.NewReader(ctx, newMemReadable(sstPayload(t, meta, data)), sstable.ReaderOptions{})
		if err != nil {
			t.Fatalf("open SST: %v", err)
		}
		defer func() { _ = reader.Close() }()
		layout, err := reader.Layout()
		if err != nil {
			t.Fatalf("SST layout: %v", err)
		}
		if len(layout.Filter) != 0 {
			t.Fatalf("SST has Pebble filter blocks %v", layout.Filter)
		}
		props, err := reader.ReadPropertiesBlock(ctx, nil)
		if err != nil {
			t.Fatalf("SST properties: %v", err)
		}
		if props.FilterPolicyName != "" || props.FilterSize != 0 {
			t.Fatalf("SST filter policy=%q size=%d, want none", props.FilterPolicyName, props.FilterSize)
		}
	}

	t.Run("writeSST", func(t *testing.T) {
		result, err := writeSST(ctx, &sliceSSTIter{entries: metaOffsetTestEntries(1_000)}, opts, 1)
		if err != nil {
			t.Fatalf("writeSST: %v", err)
		}
		requireNoPebbleFilter(t, result.Meta, result.SSTData)
	})
	t.Run("writeSSTStreaming", func(t *testing.T) {
		var uploaded []byte
		result, err := writeSSTStreaming(ctx, &sliceSSTIter{entries: metaOffsetTestEntries(1_000)}, opts,
			testSSTStreamIdentity(1, 1, 1_000),
			func(_ context.Context, _ string, r io.Reader) error {
				var err error
				uploaded, err = io.ReadAll(r)
				return err
			})
		if err != nil {
			t.Fatalf("writeSSTStreaming: %v", err)
		}
		requireNoPebbleFilter(t, result.Meta, uploaded)
	})
}
