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
