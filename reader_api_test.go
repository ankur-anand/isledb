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
