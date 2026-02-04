package isledb

import (
	"bytes"
	"context"
	"testing"

	"github.com/ankur-anand/isledb/internal"
)

func TestTrimSSTData_UsesTrailerWhenMetaSizeMissing(t *testing.T) {
	entries := []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Inline: true, Value: []byte("x")},
		{Key: []byte("b"), Seq: 2, Kind: internal.OpPut, Inline: true, Value: []byte("y")},
	}

	it := &sliceSSTIter{entries: entries}
	res, err := writeSST(context.Background(), it, SSTWriterOptions{BlockSize: 4096, Compression: "none", BloomBitsPerKey: 10}, 1)
	if err != nil {
		t.Fatalf("writeSST: %v", err)
	}
	if res.Meta.Bloom.Length == 0 {
		t.Fatalf("expected bloom bytes to be written")
	}

	meta := res.Meta
	meta.Size = 0
	meta.Checksum = ""
	meta.Signature = nil

	trimmed, err := trimSSTData(meta, res.SSTData)
	if err != nil {
		t.Fatalf("trimSSTData: %v", err)
	}
	if !bytes.Equal(trimmed, res.SSTData[:res.Meta.Size]) {
		t.Fatalf("trimmed data mismatch with sst payload")
	}
}

func TestParseBloomTrailer(t *testing.T) {
	payload := []byte("sst-payload")
	bloom := []byte("bloom-bytes")

	var buf bytes.Buffer
	buf.Write(payload)
	buf.Write(bloom)
	if err := appendBloomTrailer(&buf, int64(len(bloom))); err != nil {
		t.Fatalf("appendBloomTrailer: %v", err)
	}

	bloomLen, ok := parseBloomTrailer(buf.Bytes())
	if !ok {
		t.Fatalf("expected trailer to be parsed")
	}
	if bloomLen != int64(len(bloom)) {
		t.Fatalf("unexpected bloom len: got %d want %d", bloomLen, len(bloom))
	}
}
