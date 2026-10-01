package isledb

import (
	"errors"
	"testing"
	"time"
)

func TestReaderOpenOptionsMapsCacheOptions(t *testing.T) {
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.DiskCacheSize = 1234
	opts.BlockCacheSize = 5678
	opts.BloomCacheSize = 9012

	internal, err := readerOptionsFromPublic(opts)
	if err != nil {
		t.Fatalf("readerOptionsFromPublic: %v", err)
	}
	if internal.DiskCacheSize != opts.DiskCacheSize || internal.BlockCacheSize != opts.BlockCacheSize ||
		internal.BloomCacheSize != opts.BloomCacheSize || internal.CacheDir != opts.CacheDir {
		t.Fatalf("cache options were not propagated: %+v", internal)
	}
}

func TestDefaultReaderOpenOptions(t *testing.T) {
	opts := DefaultReaderOpenOptions(t.TempDir())
	if opts.DiskCacheSize != defaultDiskCacheSize || opts.BlockCacheSize != defaultBlockCacheSize ||
		opts.BloomCacheSize != defaultBloomCacheSize {
		t.Fatalf("defaults = %+v", opts)
	}
	if _, err := readerOptionsFromPublic(opts); err != nil {
		t.Fatalf("readerOptionsFromPublic(defaults): %v", err)
	}
	// Zero sizes select the defaults.
	if _, err := readerOptionsFromPublic(ReaderOpenOptions{CacheDir: t.TempDir()}); err != nil {
		t.Fatalf("readerOptionsFromPublic(zero sizes): %v", err)
	}
}

func TestReaderOpenOptionsRejectsEmptyCacheDir(t *testing.T) {
	opts := DefaultReaderOpenOptions("")
	if _, err := readerOptionsFromPublic(opts); !errors.Is(err, ErrInvalidReaderOptions) {
		t.Fatalf("readerOptionsFromPublic error=%v want=%v", err, ErrInvalidReaderOptions)
	}
}

func TestReaderOpenOptionsRejectsNegativeCacheSizes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ReaderOpenOptions)
	}{
		{name: "disk_cache", mutate: func(opts *ReaderOpenOptions) { opts.DiskCacheSize = -1 }},
		{name: "block_cache", mutate: func(opts *ReaderOpenOptions) { opts.BlockCacheSize = -1 }},
		{name: "bloom_cache", mutate: func(opts *ReaderOpenOptions) { opts.BloomCacheSize = -1 }},
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

func TestReaderViewPolicyRefreshAfterBounds(t *testing.T) {
	for _, tc := range []struct {
		refreshAfter time.Duration
		valid        bool
	}{
		{0, true}, {time.Second, true}, {time.Minute, true},
		{time.Second - 1, false}, {time.Millisecond, false}, {-time.Second, false},
	} {
		_, err := normalizeReaderViewPolicy(ReaderViewPolicy{RefreshAfter: tc.refreshAfter})
		if got := err == nil; got != tc.valid {
			t.Errorf("RefreshAfter=%s valid=%t err=%v, want valid=%t", tc.refreshAfter, got, err, tc.valid)
		}
	}
}
