package isledb

import "testing"

func TestReaderOpenOptionsMapsCacheOptions(t *testing.T) {
	opts := DefaultReaderOpenOptions(t.TempDir())
	opts.SSTCacheSize = 1234
	opts.BlockCacheSize = 5678

	internal, err := readerOptionsFromPublic(opts)
	if err != nil {
		t.Fatalf("readerOptionsFromPublic: %v", err)
	}
	if internal.SSTCacheSize != opts.SSTCacheSize || internal.BlockCacheSize != opts.BlockCacheSize {
		t.Fatalf("cache options were not propagated: %+v", internal)
	}
}
