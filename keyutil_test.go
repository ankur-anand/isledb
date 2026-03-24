package isledb

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestIncrementKey_AllFF(t *testing.T) {
	in := []byte{0xFF, 0xFF}
	got := incrementKey(in)
	want := []byte{0xFF, 0xFF, 0x00}

	if !bytes.Equal(got, want) {
		t.Fatalf("incrementKey(all-ff): got %v want %v", got, want)
	}

	if !bytes.Equal(in, []byte{0xFF, 0xFF}) {
		t.Fatalf("incrementKey should not modify input, got %v", in)
	}
}

func TestBigEndianUint64LSNExtractor(t *testing.T) {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 42)

	lsn, ok := BigEndianUint64LSNExtractor(key)
	if !ok {
		t.Fatal("expected extractor to accept 8-byte key")
	}
	if lsn != 42 {
		t.Fatalf("unexpected lsn: got=%d want=42", lsn)
	}

	if _, ok := BigEndianUint64LSNExtractor([]byte("short")); ok {
		t.Fatal("expected extractor to reject non-8-byte key")
	}
}
