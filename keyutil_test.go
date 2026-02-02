package isledb

import (
	"bytes"
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
