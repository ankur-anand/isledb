package isledb

import (
	"context"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSSTBloomKeys_HashesEachDistinctKeyOnce(t *testing.T) {
	keys := newSSTBloomKeys(12)
	for _, key := range []string{"a", "a", "a", "b", "c", "c"} {
		keys.add([]byte(key))
	}
	if len(keys.hashes) != 3 {
		t.Fatalf("collected %d hashes, want 3 distinct keys", len(keys.hashes))
	}

	keys.reset()
	keys.add([]byte("c"))
	if len(keys.hashes) != 1 {
		t.Fatalf("after reset collected %d hashes, want 1", len(keys.hashes))
	}
}

func TestSSTBloomKeys_BuildDescribesSidecar(t *testing.T) {
	keys := newSSTBloomKeys(12)
	for i := 0; i < 1_000; i++ {
		keys.add([]byte(fmt.Sprintf("key-%08d", i)))
	}
	filter, meta, err := keys.build(4096)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	want := manifest.BloomMeta{
		Format:     manifest.BloomFormatExactV1,
		BitsPerKey: 12,
		K:          8,
		Offset:     4096,
		Length:     int64(len(filter)),
		Checksum:   bloomChecksum(filter),
	}
	if meta != want {
		t.Fatalf("meta=%+v, want %+v", meta, want)
	}
	if err := validateBloomChecksum(meta.Checksum, filter); err != nil {
		t.Fatalf("checksum: %v", err)
	}
}

func TestSSTBloomKeys_NoSidecarWhenDisabledOrEmpty(t *testing.T) {
	disabled := newSSTBloomKeys(0)
	disabled.add([]byte("key"))
	for name, keys := range map[string]*sstBloomKeys{
		"disabled": disabled,
		"no keys":  newSSTBloomKeys(12),
	} {
		filter, meta, err := keys.build(4096)
		if err != nil {
			t.Fatalf("%s: build: %v", name, err)
		}
		if filter != nil || meta != (manifest.BloomMeta{Offset: 4096}) {
			t.Fatalf("%s: filter bytes=%d meta=%+v, want no sidecar", name, len(filter), meta)
		}
	}
}

func TestHasUsableBloom(t *testing.T) {
	cases := []struct {
		name  string
		bloom manifest.BloomMeta
		want  bool
	}{
		{name: "exact-v1", bloom: manifest.BloomMeta{Format: manifest.BloomFormatExactV1, Length: 64}, want: true},
		{name: "no sidecar", bloom: manifest.BloomMeta{Format: manifest.BloomFormatExactV1}},
		{name: "legacy JSON sidecar", bloom: manifest.BloomMeta{Length: 64}},
		{name: "unknown format", bloom: manifest.BloomMeta{Format: "exact-v2", Length: 64}},
	}
	for _, tc := range cases {
		if got := hasUsableBloom(manifest.SSTMeta{Bloom: tc.bloom}); got != tc.want {
			t.Errorf("%s: hasUsableBloom=%t, want %t", tc.name, got, tc.want)
		}
	}
}

func TestWriteSST_WritesExactBloomSidecar(t *testing.T) {
	entries := make([]internal.MemEntry, 0, 3_000)
	for i := 0; i < 1_000; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		// Three versions per key, newest first, as a flush may hold them.
		for seq := uint64(3); seq >= 1; seq-- {
			entries = append(entries, internal.MemEntry{
				Key: key, Seq: uint64(i)*3 + seq, Kind: internal.OpPut, Value: []byte("v"),
			})
		}
	}
	result, err := writeSST(context.Background(), &sliceSSTIter{entries: entries},
		sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 12, Compression: "snappy"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}

	bloom := result.Meta.Bloom
	if bloom.Format != manifest.BloomFormatExactV1 || bloom.BitsPerKey != 12 || bloom.K != 8 {
		t.Fatalf("bloom meta=%+v", bloom)
	}
	data := result.SSTData[bloom.Offset : bloom.Offset+bloom.Length]
	if err := validateBloomChecksum(bloom.Checksum, data); err != nil {
		t.Fatalf("bloom checksum: %v", err)
	}
	// Sized by the 1,000 distinct keys, not the 3,000 entries.
	if want := int64(sstBloomHeaderLen + (1_000*12+63)/64*8); bloom.Length != want {
		t.Fatalf("bloom length=%d, want %d", bloom.Length, want)
	}
	filter, err := parseSSTBloomFilter(data)
	if err != nil {
		t.Fatalf("parse bloom: %v", err)
	}
	for i := 0; i < 1_000; i++ {
		if !filter.mayContain(bloomHashKey([]byte(fmt.Sprintf("key-%08d", i)))) {
			t.Fatalf("key %d reported absent", i)
		}
	}
}

// TestReader_SkipsBloomInOtherFormat opens an SST whose manifest entry
// describes its filter in another format, as SSTs written before exact-v1 do.
// The reader must neither fetch nor parse that filter, and still answer
// lookups by opening the SST.
func TestReader_SkipsBloomInOtherFormat(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-skips-legacy-bloom")
	t.Cleanup(func() { _ = store.Close() })
	metrics := DefaultReaderMetrics(nil)
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir(), Metrics: metrics})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	result, err := writeSST(ctx, &sliceSSTIter{entries: bloomReaderTestEntries()},
		sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 12, Compression: "snappy"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
		t.Fatalf("store SST: %v", err)
	}

	for _, format := range []string{"", "exact-v2"} {
		t.Run(fmt.Sprintf("format=%q", format), func(t *testing.T) {
			meta := result.Meta
			meta.Level = 1
			meta.Bloom.Format = format
			state := &manifestState{Levels: []manifest.Level{{Number: 1, SSTs: []manifest.SSTMeta{meta}}}}

			value, found, err := reader.getWithManifest(ctx, state, []byte("a-present"))
			if err != nil || !found || string(value) != "value" {
				t.Fatalf("Get(a-present)=%q found=%t err=%v", value, found, err)
			}
			if _, found, err := reader.getWithManifest(ctx, state, []byte("b-absent")); err != nil || found {
				t.Fatalf("Get(absent) found=%t err=%v", found, err)
			}
			if stats := reader.BloomCacheStats(); stats.Hits+stats.Misses != 0 || stats.EntryCount != 0 {
				t.Fatalf("filter in another format was consulted: %+v", stats)
			}
			if got := testutil.ToFloat64(metrics.BloomFilterErrors); got != 0 {
				t.Fatalf("Bloom errors=%v, want 0", got)
			}
		})
	}
}

// TestReader_UsesExactBloom checks the reader loads an exact-v1 filter once and
// answers later lookups from the cached filter.
func TestReader_UsesExactBloom(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-uses-exact-bloom")
	t.Cleanup(func() { _ = store.Close() })
	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	result, err := writeSST(ctx, &sliceSSTIter{entries: bloomReaderTestEntries()},
		sstWriterOptions{BlockSize: 4096, BloomBitsPerKey: 12, Compression: "snappy"}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}
	if _, err := store.Write(ctx, store.SSTPath(result.Meta.ID), result.SSTData); err != nil {
		t.Fatalf("store SST: %v", err)
	}
	meta := result.Meta
	meta.Level = 1
	state := &manifestState{Levels: []manifest.Level{{Number: 1, SSTs: []manifest.SSTMeta{meta}}}}

	for i := 0; i < 3; i++ {
		if _, found, err := reader.getWithManifest(ctx, state, []byte("b-absent")); err != nil || found {
			t.Fatalf("Get(absent) found=%t err=%v", found, err)
		}
	}
	// The first lookup loads the filter; the next two answer from the cache.
	stats := reader.BloomCacheStats()
	if stats.EntryCount != 1 || stats.Hits != 2 {
		t.Fatalf("bloom cache stats=%+v, want one filter and two hits", stats)
	}
}

// bloomReaderTestEntries brackets "b-absent" so a lookup for it passes the
// SST's key-range check and reaches the filter.
func bloomReaderTestEntries() []internal.MemEntry {
	return []internal.MemEntry{
		{Key: []byte("a-present"), Seq: 2, Kind: internal.OpPut, Value: []byte("value")},
		{Key: []byte("c-present"), Seq: 1, Kind: internal.OpPut, Value: []byte("value")},
	}
}
