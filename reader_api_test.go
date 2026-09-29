package isledb

import (
	"errors"
	"testing"
)

func TestReaderOpenOptionsMapsCacheOptions(t *testing.T) {
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.SSTCacheSize = 1234
	opts.BlockCacheSize = 5678
	opts.BloomCacheSize = 9012
	opts.BloomDiskCacheSize = 3456

	internal, err := readerOptionsFromPublic(opts)
	if err != nil {
		t.Fatalf("readerOptionsFromPublic: %v", err)
	}
	if internal.SSTCacheSize != opts.SSTCacheSize || internal.BlockCacheSize != opts.BlockCacheSize ||
		internal.BloomCacheSize != opts.BloomCacheSize ||
		internal.BloomDiskCacheSize != opts.BloomDiskCacheSize {
		t.Fatalf("cache options were not propagated: %+v", internal)
	}
}

func TestReaderOpenOptionsRejectsNegativeBloomDiskCacheSize(t *testing.T) {
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.BloomDiskCacheSize = -1
	if _, err := readerOptionsFromPublic(opts); !errors.Is(err, ErrInvalidReaderOptions) {
		t.Fatalf("readerOptionsFromPublic error=%v want=%v", err, ErrInvalidReaderOptions)
	}
}

func TestReaderOpenOptionsRejectsEmptyCacheDir(t *testing.T) {
	opts := DefaultReaderOpenOptions("")
	if _, err := readerOptionsFromPublic(opts); !errors.Is(err, ErrInvalidReaderOptions) {
		t.Fatalf("readerOptionsFromPublic error=%v want=%v", err, ErrInvalidReaderOptions)
	}
}

func TestReaderOpenOptionsRejectsNegativeBloomCacheSize(t *testing.T) {
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.BloomCacheSize = -1
	if _, err := readerOptionsFromPublic(opts); !errors.Is(err, ErrInvalidReaderOptions) {
		t.Fatalf("readerOptionsFromPublic error=%v want=%v", err, ErrInvalidReaderOptions)
	}
}

func TestReaderOpenOptionsRejectsOtherNegativeCacheLimits(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ReaderOpenOptions)
	}{
		{name: "sst_cache", mutate: func(opts *ReaderOpenOptions) { opts.SSTCacheSize = -1 }},
		{name: "block_cache", mutate: func(opts *ReaderOpenOptions) { opts.BlockCacheSize = -1 }},
		{name: "range_read_min_sst", mutate: func(opts *ReaderOpenOptions) { opts.RangeReadMinSSTSize = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := DefaultReaderOpenOptions(t.TempDir())
			test.mutate(&opts)
			if _, err := readerOptionsFromPublic(opts); !errors.Is(err, ErrInvalidReaderOptions) {
				t.Fatalf("readerOptionsFromPublic error=%v want=%v", err, ErrInvalidReaderOptions)
			}
		})
	}
}

func TestDefaultReaderOpenOptionsEnableRangeRead(t *testing.T) {
	opts := DefaultReaderOpenOptions(t.TempDir())
	if !opts.RangeRead {
		t.Fatal("DefaultReaderOpenOptions: RangeRead=false, want true")
	}
	internal, err := readerOptionsFromPublic(opts)
	if err != nil {
		t.Fatalf("readerOptionsFromPublic: %v", err)
	}
	if !internal.RangeRead {
		t.Fatal("RangeRead was not propagated")
	}
}

func TestReaderOpenOptionsRangeReadSizes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ReaderOpenOptions)
		valid  bool
	}{
		{name: "disabled", mutate: func(o *ReaderOpenOptions) { o.RangeRead = false }, valid: true},
		{name: "block_cache_without_range_read", mutate: func(o *ReaderOpenOptions) {
			o.RangeRead = false
			o.BlockCacheSize = 1 << 20
		}},
		{name: "min_sst_without_range_read", mutate: func(o *ReaderOpenOptions) {
			o.RangeRead = false
			o.RangeReadMinSSTSize = 1
		}},
		{name: "chunk_without_range_read", mutate: func(o *ReaderOpenOptions) {
			o.RangeRead = false
			o.RangeReadChunkSize = 128 << 10
		}},
		{name: "chunk_min", mutate: func(o *ReaderOpenOptions) { o.RangeReadChunkSize = 16 << 10 }, valid: true},
		{name: "chunk_max", mutate: func(o *ReaderOpenOptions) { o.RangeReadChunkSize = 16 << 20 }, valid: true},
		{name: "chunk_too_small", mutate: func(o *ReaderOpenOptions) { o.RangeReadChunkSize = 16<<10 - 1 }},
		{name: "chunk_too_large", mutate: func(o *ReaderOpenOptions) { o.RangeReadChunkSize = 16<<20 + 1 }},
		{name: "chunk_negative", mutate: func(o *ReaderOpenOptions) { o.RangeReadChunkSize = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := DefaultReaderOpenOptions(t.TempDir())
			test.mutate(&opts)
			_, err := readerOptionsFromPublic(opts)
			if test.valid && err != nil {
				t.Fatalf("readerOptionsFromPublic: %v", err)
			}
			if !test.valid && !errors.Is(err, ErrInvalidReaderOptions) {
				t.Fatalf("readerOptionsFromPublic error=%v want=%v", err, ErrInvalidReaderOptions)
			}
		})
	}
}
