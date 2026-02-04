package isledb

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

func TestReader_RangeRead_UsesBlockCacheForLargeSST(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := blobstore.NewMemory("range-read")
	ms := manifest.NewStore(store)
	t.Cleanup(func() { _ = store.Close() })

	value := bytes.Repeat([]byte("v"), 2048)
	entries := make([]internal.MemEntry, 0, 200)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%06d", i)
		entries = append(entries, internal.MemEntry{
			Key:    []byte(key),
			Value:  value,
			Kind:   internal.OpPut,
			Seq:    uint64(i + 1),
			Inline: true,
		})
	}

	res := writeTestSST(t, ctx, store, ms, entries, 0, 1)
	if res.Meta.Size <= 32<<10 {
		t.Fatalf("expected large SST, got size %d", res.Meta.Size)
	}

	opts := ReaderOptions{
		CacheDir:                 t.TempDir(),
		BlockCacheSize:           1 << 20,
		RangeReadMinSSTSize:      32 << 10,
		ValidateSSTChecksum:      false,
		AllowUnverifiedRangeRead: false,
	}
	reader, err := newReader(ctx, store, opts)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	if reader.blockCache == nil {
		t.Fatalf("expected block cache to be initialized")
	}

	beforeSSTEntries := reader.SSTCacheStats().EntryCount

	got, found, err := reader.Get(ctx, []byte("key-000100"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || len(got) == 0 {
		t.Fatalf("expected value for key")
	}

	reader.blockCache.Wait()
	afterSSTEntries := reader.SSTCacheStats().EntryCount

	if afterSSTEntries != beforeSSTEntries {
		t.Fatalf("expected no SST cache entries, got %d -> %d", beforeSSTEntries, afterSSTEntries)
	}

	cached := cachedDataBlocks(t, reader, store, res.Meta.ID)
	for i, h := range cached {
		if i >= 5 {
			t.Logf("cached data block offsets: (showing first 5 of %d)", len(cached))
			break
		}
		t.Logf("cached data block offset=%d length=%d", h.Offset, h.Length)
	}
	if len(cached) == 0 {
		t.Fatalf("expected at least one data block cached")
	}

	if err := store.Delete(ctx, store.SSTPath(res.Meta.ID)); err != nil {
		t.Fatalf("delete sst: %v", err)
	}

	got2, found, err := reader.Get(ctx, []byte("key-000100"))
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if !found || len(got2) == 0 {
		t.Fatalf("expected value after delete")
	}
}

func cachedDataBlocks(t *testing.T, reader *Reader, store *blobstore.Store, sstID string) []block.Handle {
	t.Helper()

	data, _, err := store.Read(context.Background(), store.SSTPath(sstID))
	if err != nil {
		t.Fatalf("read sst: %v", err)
	}

	r, err := sstable.NewReader(context.Background(), newSSTReadable(data), sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer func() {
		_ = r.Close()
	}()

	layout, err := r.Layout()
	if err != nil {
		t.Fatalf("layout: %v", err)
	}

	var cached []block.Handle
	for _, h := range layout.Data {
		key := blockCacheKey(sstID, int64(h.Offset), int(h.Length)+block.TrailerLen)
		if _, ok := reader.blockCache.Get(key); ok {
			cached = append(cached, h.Handle)
		}
	}
	return cached
}
